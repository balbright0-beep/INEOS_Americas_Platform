"""Logistics endpoints — FO Performance report viewer.

Admins upload the weekly Vehicle Distribution xlsx via the existing
/api/admin/upload-source/vehicle_distribution endpoint, which stores the raw
file in CachedFile and the computed FO Performance dict as JSON under
cache/data/vehicle_distribution.json. The ingest module parses the Data File
sheet, groups FOs by create date, computes flow-through counts, business-day
SLA compliance, and cumulative / MTD pickup totals — reproducing the Daily
Freight Order Activity report the logistics team publishes in Excel.

This router exposes these authenticated read endpoints:

  GET /api/logistics/fo-performance        → JSON data (accepts ?objective=N)
  GET /api/logistics/fo-performance/html   → rendered HTML fragment (?objective=N)
  GET /api/logistics/fo-performance/export → downloadable standalone HTML file
  GET /api/logistics/fo-performance/meta   → freshness + filename + objective

All GET endpoints accept an optional ``?objective=N`` query parameter. When
provided, the FO Pacing to Objective section is recomputed on the fly and the
new value is persisted to the ``logistics_monthly_objective`` AppState key so
it becomes the default for subsequent callers. When omitted, the persisted
value is used (falling back to the ingest default of 477).
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from typing import Any, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse, Response
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import CachedFile, AppState
from app.routers.auth import get_current_user, require_admin

router = APIRouter(prefix="/api/logistics", tags=["logistics"])


_CACHE_KEY = 'vehicle_distribution'
_RAW_KEY = 'vehicle_distribution_raw'
_OBJECTIVE_KEY = 'logistics_monthly_objective'
_DEFAULT_OBJECTIVE = 477


def _cache_dir() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'cache',
        'data',
    )


def _get_stored_objective(db: Session) -> int:
    try:
        state = db.query(AppState).filter(AppState.key == _OBJECTIVE_KEY).first()
        if state and state.value:
            return int(float(state.value))
    except Exception as e:
        print(f"  [logistics] stored objective read failed: {e}")
    return _DEFAULT_OBJECTIVE


def _set_stored_objective(db: Session, value: int) -> None:
    try:
        state = db.query(AppState).filter(AppState.key == _OBJECTIVE_KEY).first()
        if state:
            state.value = str(value)
        else:
            db.add(AppState(key=_OBJECTIVE_KEY, value=str(value)))
        db.commit()
    except Exception as e:
        print(f"  [logistics] stored objective write failed: {e}")


def _is_stale(data: dict) -> bool:
    """Detect cached JSON that was written by a pre-base_metrics build.

    These older caches can't be live-updated because apply_monthly_objective
    needs base_metrics to rebuild the pacing section. When we see one we
    throw it away and fall through to re-ingest from the raw xlsx.
    """
    return not isinstance(data, dict) or not data.get('base_metrics')


def _reingest_from_raw(db: Session, data_dir: str, json_path: str) -> Optional[dict]:
    """Re-parse the raw vehicle_distribution xlsx from the CachedFile table
    and persist the fresh JSON to disk. Returns the parsed dict or None."""
    try:
        raw_cf = db.query(CachedFile).filter(CachedFile.key == _RAW_KEY).first()
        if not (raw_cf and raw_cf.data):
            return None

        import sys
        backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        if backend_dir not in sys.path:
            sys.path.insert(0, backend_dir)
        from data_hub.ingest.vehicle_distribution import ingest_vehicle_distribution

        suffix = os.path.splitext(raw_cf.filename or 'vehicle_distribution.xlsx')[1] or '.xlsx'
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(raw_cf.data)
            tmp_path = tmp.name
        try:
            data = ingest_vehicle_distribution(tmp_path)
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

        os.makedirs(data_dir, exist_ok=True)
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, default=str)
        print(f"  [logistics] re-ingested from raw xlsx -> {json_path}")
        return data
    except Exception as e:
        print(f"  [logistics] raw re-parse failed: {e}")
        return None


def _load_data(db: Session) -> Optional[dict]:
    """Load the parsed FO Performance dict.

    Preference order:
      1. cache/data/vehicle_distribution.json on disk (fastest).
      2. CachedFile[vehicle_distribution] bytes (re-hydrate disk cache + return).
      3. CachedFile[vehicle_distribution_raw] bytes -> re-parse via ingest module.

    If either of the first two paths returns a dict missing base_metrics (a
    stale cache written by an earlier build), that cache is discarded and we
    re-ingest from the raw xlsx so the pacing section can be live-updated.

    Returns None if nothing has been uploaded yet.
    """
    data_dir = _cache_dir()
    json_path = os.path.join(data_dir, f'{_CACHE_KEY}.json')

    # 1. Disk cache
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                disk_data = json.load(f)
            if not _is_stale(disk_data):
                return disk_data
            print("  [logistics] disk cache missing base_metrics — forcing re-ingest")
            try:
                os.unlink(json_path)
            except OSError:
                pass
        except Exception as e:
            print(f"  [logistics] disk cache read failed: {e}")

    # 2. Processed bytes in DB
    try:
        cf = db.query(CachedFile).filter(CachedFile.key == _CACHE_KEY).first()
        if cf and cf.data:
            try:
                db_data = json.loads(cf.data.decode('utf-8'))
                if _is_stale(db_data):
                    print("  [logistics] DB processed cache missing base_metrics — forcing re-ingest")
                else:
                    os.makedirs(data_dir, exist_ok=True)
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(db_data, f)
                    return db_data
            except Exception as e:
                print(f"  [logistics] DB processed decode failed: {e}")
    except Exception as e:
        print(f"  [logistics] DB processed query failed: {e}")

    # 3. Raw xlsx bytes in DB → re-parse
    return _reingest_from_raw(db, data_dir, json_path)


def _resolved_data(
    db: Session, objective: Optional[int], persist: bool = True
) -> Tuple[Optional[dict], int]:
    """Load the base data and apply the requested (or stored) objective.

    Returns ``(data, resolved_objective)``. If ``objective`` is provided and
    ``persist`` is True, the new value is saved to AppState so it becomes the
    default for subsequent callers.
    """
    import sys
    backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)
    from data_hub.ingest.vehicle_distribution import apply_monthly_objective

    data = _load_data(db)
    if data is None:
        resolved = objective if objective is not None else _get_stored_objective(db)
        return None, resolved

    if objective is not None:
        if persist:
            _set_stored_objective(db, objective)
        resolved = objective
    else:
        resolved = _get_stored_objective(db)
        # If the cached JSON was persisted with a different objective (first
        # load after ingest), align it with the stored setting.
        if data.get('monthly_objective') != resolved:
            pass  # always call apply_monthly_objective below

    apply_monthly_objective(data, resolved)
    return data, resolved


@router.get('/fo-performance')
def get_fo_performance_json(
    objective: Optional[int] = Query(None, ge=0, le=100000),
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return the parsed FO Performance data as JSON."""
    data, _ = _resolved_data(db, objective)
    if data is None:
        raise HTTPException(404, 'FO Performance report has not been uploaded yet.')
    return data


@router.get('/fo-performance/html', response_class=HTMLResponse)
def get_fo_performance_html(
    objective: Optional[int] = Query(None, ge=0, le=100000),
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return a rendered HTML fragment for the FO Performance report."""
    data, _ = _resolved_data(db, objective)
    if data is None:
        return HTMLResponse(
            content=(
                '<div style="padding:32px;text-align:center;color:#6F6558;'
                'font-family:-apple-system,Segoe UI,sans-serif">'
                '<h3 style="color:#2A1F0F;margin:0 0 8px">No report uploaded</h3>'
                '<p style="margin:0;font-size:13px">An administrator needs to '
                'upload the Vehicle Distribution workbook in <strong>Data Upload</strong>.</p>'
                '</div>'
            ),
            status_code=200,
        )

    import sys
    backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)
    from data_hub.render.fo_performance_html import render_fo_performance
    return HTMLResponse(content=render_fo_performance(data))


@router.get('/fo-performance/export')
def export_fo_performance_html(
    objective: Optional[int] = Query(None, ge=0, le=100000),
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return a standalone downloadable HTML document for offline viewing.

    The returned file is a complete `<!doctype html>` document with inline
    CSS + the collapsible-month JS, so it can be shared by email, opened
    directly, or dropped into a shared drive without any extra assets.
    """
    data, resolved = _resolved_data(db, objective, persist=False)
    if data is None:
        raise HTTPException(404, 'FO Performance report has not been uploaded yet.')

    import sys
    backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)
    from data_hub.render.fo_performance_html import render_fo_performance

    html_doc = render_fo_performance(data, standalone=True)
    stamp = datetime.utcnow().strftime('%Y-%m-%d')
    filename = f'FO_Performance_{stamp}.html'
    return Response(
        content=html_doc,
        media_type='text/html; charset=utf-8',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )


@router.get('/fo-performance/meta')
def get_fo_performance_meta(user=Depends(get_current_user), db: Session = Depends(get_db)):
    """Return freshness metadata for the FO Performance report."""
    meta: dict[str, Any] = {'uploaded': False}

    try:
        raw_cf = db.query(CachedFile).filter(CachedFile.key == _RAW_KEY).first()
        if raw_cf:
            meta['uploaded'] = True
            meta['filename'] = raw_cf.filename
            meta['uploaded_at'] = raw_cf.uploaded_at.isoformat() if raw_cf.uploaded_at else None
            meta['size_bytes'] = len(raw_cf.data) if raw_cf.data else 0
    except Exception as e:
        print(f"  [logistics] meta raw query failed: {e}")

    try:
        last = db.query(AppState).filter(AppState.key == 'source_vehicle_distribution_last').first()
        if last and last.value:
            meta['last_ingest'] = last.value
    except Exception:
        pass

    meta['monthly_objective'] = _get_stored_objective(db)

    # Count months / daily rows and surface the base metrics so the
    # frontend can preview the pacing without a full re-render round-trip.
    data = _load_data(db)
    if data:
        meta['months'] = len(data.get('months') or [])
        meta['daily_rows'] = sum(len(m.get('days', [])) for m in data.get('months') or [])
        meta['generated_at'] = data.get('generated_at')
        meta['base_metrics'] = data.get('base_metrics')
    return meta


@router.post('/fo-performance/objective')
def set_fo_performance_objective(
    data: dict,
    user=Depends(require_admin),
    db: Session = Depends(get_db),
):
    """Persist a new monthly objective (admin only)."""
    try:
        value = int(float(data.get('objective', 0)))
    except (TypeError, ValueError):
        raise HTTPException(400, 'objective must be a number')
    if value < 0 or value > 100000:
        raise HTTPException(400, 'objective out of range (0–100000)')
    _set_stored_objective(db, value)
    return {'ok': True, 'monthly_objective': value}
