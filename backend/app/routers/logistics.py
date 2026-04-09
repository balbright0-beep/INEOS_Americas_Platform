"""Logistics endpoints — FO Performance report viewer.

Admins upload the Logistics Freight Order xlsx via the existing
/api/admin/upload-source/fo_performance endpoint, which stores the raw file
in CachedFile and the parsed dict as JSON under cache/data/fo_performance.json.

This router exposes two public (authenticated, any role) read endpoints:

  GET /api/logistics/fo-performance       → JSON data
  GET /api/logistics/fo-performance/html  → rendered HTML fragment
  GET /api/logistics/fo-performance/meta  → freshness + filename

Data is re-loaded on each request. If the processed JSON is missing (e.g. the
cache dir was wiped but the raw xlsx survived in the DB), we transparently
re-parse from the raw bytes and repopulate the cache.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import CachedFile, AppState
from app.routers.auth import get_current_user

router = APIRouter(prefix="/api/logistics", tags=["logistics"])


_CACHE_KEY = 'fo_performance'
_RAW_KEY = 'fo_performance_raw'


def _cache_dir() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'cache',
        'data',
    )


def _load_data(db: Session) -> dict[str, Any] | None:
    """Load the parsed FO Performance dict.

    Preference order:
      1. cache/data/fo_performance.json on disk (fastest).
      2. CachedFile[fo_performance] bytes (re-hydrate disk cache + return).
      3. CachedFile[fo_performance_raw] bytes → re-parse via ingest module.

    Returns None if nothing has been uploaded yet.
    """
    data_dir = _cache_dir()
    json_path = os.path.join(data_dir, f'{_CACHE_KEY}.json')

    # 1. Disk cache
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"  [logistics] disk cache read failed: {e}")

    # 2. Processed bytes in DB
    try:
        cf = db.query(CachedFile).filter(CachedFile.key == _CACHE_KEY).first()
        if cf and cf.data:
            try:
                data = json.loads(cf.data.decode('utf-8'))
                os.makedirs(data_dir, exist_ok=True)
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f)
                return data
            except Exception as e:
                print(f"  [logistics] DB processed decode failed: {e}")
    except Exception as e:
        print(f"  [logistics] DB processed query failed: {e}")

    # 3. Raw xlsx bytes in DB → re-parse
    try:
        raw_cf = db.query(CachedFile).filter(CachedFile.key == _RAW_KEY).first()
        if raw_cf and raw_cf.data:
            import sys
            backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            if backend_dir not in sys.path:
                sys.path.insert(0, backend_dir)
            from data_hub.ingest.fo_performance import ingest_fo_performance

            suffix = os.path.splitext(raw_cf.filename or 'fo_performance.xlsx')[1] or '.xlsx'
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(raw_cf.data)
                tmp_path = tmp.name
            try:
                data = ingest_fo_performance(tmp_path)
            finally:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

            os.makedirs(data_dir, exist_ok=True)
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, default=str)
            return data
    except Exception as e:
        print(f"  [logistics] raw re-parse failed: {e}")

    return None


@router.get('/fo-performance')
def get_fo_performance_json(user=Depends(get_current_user), db: Session = Depends(get_db)):
    """Return the parsed FO Performance data as JSON."""
    data = _load_data(db)
    if data is None:
        raise HTTPException(404, 'FO Performance report has not been uploaded yet.')
    return data


@router.get('/fo-performance/html', response_class=HTMLResponse)
def get_fo_performance_html(user=Depends(get_current_user), db: Session = Depends(get_db)):
    """Return a rendered HTML fragment for the FO Performance report."""
    data = _load_data(db)
    if data is None:
        return HTMLResponse(
            content=(
                '<div style="padding:32px;text-align:center;color:#6F6558;'
                'font-family:-apple-system,Segoe UI,sans-serif">'
                '<h3 style="color:#2A1F0F;margin:0 0 8px">No report uploaded</h3>'
                '<p style="margin:0;font-size:13px">An administrator needs to '
                'upload the FO Performance workbook in <strong>Data Upload</strong>.</p>'
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
        last = db.query(AppState).filter(AppState.key == 'source_fo_performance_last').first()
        if last and last.value:
            meta['last_ingest'] = last.value
    except Exception:
        pass

    # Count months / daily rows if we have processed data (cheap — the JSON is small).
    data = _load_data(db)
    if data:
        meta['months'] = len(data.get('months') or [])
        meta['daily_rows'] = sum(len(m.get('days', [])) for m in data.get('months') or [])
        meta['generated_at'] = data.get('generated_at')
    return meta
