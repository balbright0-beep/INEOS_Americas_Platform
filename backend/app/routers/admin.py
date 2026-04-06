import bcrypt
import httpx
import csv
import io
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import User, AppState, AuditLog, UploadHistory, MonthlySnapshot, DealerPerformance, RetailSale, Vehicle
from app.routers.auth import require_admin
from app.config import DASHBOARD_URL, ALLOCATION_URL

router = APIRouter(prefix="/api/admin", tags=["admin"])


def hash_pw(pw):
    return bcrypt.hashpw(pw.encode("utf-8")[:72], bcrypt.gensalt()).decode("utf-8")


def audit(db, action, user, detail=""):
    db.add(AuditLog(action=action, user=user, detail=detail))
    db.commit()


# --- User Management ---
@router.get("/users")
def list_users(admin=Depends(require_admin), db: Session = Depends(get_db)):
    users = db.query(User).order_by(User.role, User.username).all()
    return [{"id": u.id, "username": u.username, "role": u.role, "dealer_name": u.dealer_name, "created_at": str(u.created_at) if u.created_at else None} for u in users]


@router.post("/users")
def create_user(data: dict, admin=Depends(require_admin), db: Session = Depends(get_db)):
    if db.query(User).filter(User.username == data["username"]).first():
        raise HTTPException(400, "Username already exists")
    user = User(
        username=data["username"],
        password_hash=hash_pw(data["password"]),
        role=data.get("role", "dealer"),
        dealer_name=data.get("dealer_name"),
    )
    db.add(user)
    db.commit()
    audit(db, "create_user", admin.username, f"Created {data['username']} ({data.get('role','dealer')})")
    return {"id": user.id, "username": user.username, "role": user.role}


@router.post("/users/bulk")
async def bulk_create_users(file: UploadFile = File(...), admin=Depends(require_admin), db: Session = Depends(get_db)):
    """Bulk create dealer accounts from CSV. Columns: username,password,dealer_name"""
    contents = await file.read()
    text = contents.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    created, skipped, errors = 0, 0, []
    for row in reader:
        username = row.get("username", "").strip()
        password = row.get("password", "").strip()
        dealer_name = row.get("dealer_name", "").strip()
        if not username or not password:
            errors.append(f"Missing username/password: {row}")
            continue
        if db.query(User).filter(User.username == username).first():
            skipped += 1
            continue
        db.add(User(username=username, password_hash=hash_pw(password), role="dealer", dealer_name=dealer_name))
        created += 1
    db.commit()
    audit(db, "bulk_create_users", admin.username, f"Created {created}, skipped {skipped}")
    return {"created": created, "skipped": skipped, "errors": errors}


@router.delete("/users/{user_id}")
def delete_user(user_id: int, admin=Depends(require_admin), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    if user.role == "admin":
        raise HTTPException(400, "Cannot delete admin")
    username = user.username
    db.delete(user)
    db.commit()
    audit(db, "delete_user", admin.username, f"Deleted {username}")
    return {"ok": True}


@router.put("/users/{user_id}/reset-password")
def reset_password(user_id: int, data: dict, admin=Depends(require_admin), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    user.password_hash = hash_pw(data["password"])
    db.commit()
    audit(db, "reset_password", admin.username, f"Reset password for {user.username}")
    return {"ok": True}


# --- Master File Upload ---
@router.post("/upload-master")
async def upload_master(file: UploadFile = File(...), admin=Depends(require_admin), db: Session = Depends(get_db)):
    contents = await file.read()
    results = {"dashboard": None, "allocation": None}

    async with httpx.AsyncClient(timeout=300) as client:
        try:
            r = await client.post(f"{DASHBOARD_URL}/upload", files={"file": (file.filename, contents, file.content_type)}, follow_redirects=True)
            results["dashboard"] = "ok" if r.status_code < 400 else f"error: {r.status_code}"
        except Exception as e:
            results["dashboard"] = f"error: {e}"
        try:
            r = await client.post(f"{ALLOCATION_URL}/upload", files={"file": (file.filename, contents, file.content_type)}, follow_redirects=True)
            results["allocation"] = "ok" if r.status_code < 400 else f"error: {r.status_code}"
        except Exception as e:
            results["allocation"] = f"error: {e}"

    # Process data for Platform's own database
    counts = {"vehicles": 0, "retail_sales": 0, "dealer_performance": 0}
    try:
        from app.processor import process_master_file
        counts = process_master_file(contents, db)
        results["platform_data"] = counts
    except Exception as e:
        results["platform_data"] = f"error: {e}"

    # Save upload history
    db.add(UploadHistory(
        filename=file.filename, uploaded_by=admin.username,
        vehicles_count=counts.get("vehicles", 0),
        retail_sales_count=counts.get("retail_sales", 0),
        performance_count=counts.get("dealer_performance", 0),
    ))

    # Save monthly snapshot for historical trends
    month = datetime.utcnow().strftime("%Y-%m")
    month_start = f"{month}-01"
    month_end = f"{month}-31"
    for dp in db.query(DealerPerformance).all():
        # Use uppercase exact match (processor normalizes to uppercase)
        dlr_upper = dp.dealer.upper()
        sales = db.query(RetailSale).filter(
            RetailSale.dealer == dlr_upper,
            RetailSale.handover_date >= month_start,
            RetailSale.handover_date <= month_end,
        ).count()
        og = db.query(Vehicle).filter(Vehicle.dealer == dlr_upper, Vehicle.status == "Dealer Stock").count()
        avg_dts_rows = db.query(RetailSale.days_to_sell).filter(
            RetailSale.dealer == dlr_upper,
            RetailSale.handover_date >= month_start,
            RetailSale.handover_date <= month_end,
            RetailSale.days_to_sell > 0,
        ).all()
        avg_dts = round(sum(r[0] for r in avg_dts_rows) / len(avg_dts_rows)) if avg_dts_rows else 0
        existing = db.query(MonthlySnapshot).filter(MonthlySnapshot.month == month, MonthlySnapshot.dealer == dp.dealer).first()
        if existing:
            # Update existing snapshot with latest data
            existing.sales = sales
            existing.handovers = dp.handovers
            existing.on_ground = og
            existing.leads = dp.leads
            existing.test_drives = dp.test_drives
            existing.won = dp.won
            existing.avg_days_to_sell = avg_dts
        else:
            db.add(MonthlySnapshot(month=month, dealer=dp.dealer, market=dp.market, sales=sales,
                                   handovers=dp.handovers, on_ground=og, leads=dp.leads,
                                   test_drives=dp.test_drives, won=dp.won, avg_days_to_sell=avg_dts))

    # Store timestamp
    state = db.query(AppState).filter(AppState.key == "last_master_upload").first()
    if not state:
        state = AppState(key="last_master_upload")
        db.add(state)
    state.value = datetime.utcnow().isoformat()
    db.commit()

    audit(db, "upload_master", admin.username, f"Uploaded {file.filename}")
    return {"results": results, "timestamp": state.value}


@router.get("/last-update")
def last_update(db: Session = Depends(get_db)):
    state = db.query(AppState).filter(AppState.key == "last_master_upload").first()
    return {"last_update": state.value if state else None}


# --- Data Sources Status ---
@router.get("/data-sources")
def get_data_sources(admin=Depends(require_admin), db: Session = Depends(get_db)):
    """Return freshness status for all data sources."""
    sources = {}
    now = datetime.utcnow()
    SOURCES = {
        'sap_export': ('SAP Vehicle Export', 'Daily'),
        'sap_handover': ('SAP Handover Report', 'Daily'),
        'stock_pipeline': ('Stock & Pipeline Report', 'Daily'),
        'c4c_leads': ('C4C Leads (Marketing)', 'Daily'),
        'santander': ('Santander Daily Report', 'Daily'),
        'urban_science': ('Urban Science Extract', 'Monthly'),
        'ga4': ('Google Analytics (GA4)', 'Weekly'),
    }
    for key, (label, cadence) in SOURCES.items():
        state_key = f"source_{key}_last"
        state = db.query(AppState).filter(AppState.key == state_key).first()
        row_state = db.query(AppState).filter(AppState.key == f"source_{key}_rows").first()

        freshness = 'gray'
        last_upload = None
        row_count = 0

        if state and state.value:
            last_upload = state.value
            try:
                last_dt = datetime.fromisoformat(state.value)
                age_hours = (now - last_dt).total_seconds() / 3600
                if cadence == 'Daily':
                    freshness = 'green' if age_hours < 28 else 'yellow' if age_hours < 52 else 'red'
                elif cadence == 'Weekly':
                    freshness = 'green' if age_hours < 192 else 'yellow' if age_hours < 360 else 'red'
                elif cadence == 'Monthly':
                    freshness = 'green' if age_hours < 768 else 'yellow' if age_hours < 1440 else 'red'
            except:
                pass

        if row_state and row_state.value:
            try:
                row_count = int(row_state.value)
            except:
                pass

        sources[key] = {
            'label': label, 'cadence': cadence,
            'freshness': freshness, 'last_upload': last_upload, 'row_count': row_count,
        }

    # Also check GA4 from last_ga4_pull
    ga4_state = db.query(AppState).filter(AppState.key == "last_ga4_pull").first()
    if ga4_state and ga4_state.value:
        sources['ga4']['last_upload'] = ga4_state.value
        try:
            age = (now - datetime.fromisoformat(ga4_state.value)).total_seconds() / 3600
            sources['ga4']['freshness'] = 'green' if age < 192 else 'yellow' if age < 360 else 'red'
        except:
            pass

    return sources


# --- Individual Source Upload ---
@router.post("/upload-source/{source_id}")
async def upload_source(source_id: str, file: UploadFile = File(...), admin=Depends(require_admin), db: Session = Depends(get_db)):
    """Upload a specific data source file."""
    import tempfile, os
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1])
    contents = await file.read()
    tmp.write(contents)
    tmp.close()

    try:
        # Direct ingest using source_id (skip auto-detection)
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

        row_count = 0
        INGEST_MAP = {
            'sap_export': ('data_hub.ingest.sap_export', 'ingest_sap_export'),
            'sap_handover': ('data_hub.ingest.sap_handover', 'ingest_handover'),
            'stock_pipeline': ('data_hub.ingest.stock_pipeline', 'ingest_stock_pipeline'),
            'c4c_leads': ('data_hub.ingest.c4c_leads', 'ingest_c4c_leads'),
            'santander': ('data_hub.ingest.santander', 'ingest_santander'),
            'urban_science': ('data_hub.ingest.urban_science', 'ingest_urban_science'),
        }

        if source_id in INGEST_MAP:
            mod_name, func_name = INGEST_MAP[source_id]
            mod = __import__(mod_name, fromlist=[func_name])
            ingest_fn = getattr(mod, func_name)
            result_data = ingest_fn(tmp.name)

            if isinstance(result_data, dict):
                # Santander returns dict of lists
                row_count = sum(len(v) for v in result_data.values() if isinstance(v, list))
            else:
                # DataFrame
                row_count = len(result_data)

            # Cache the data
            cache_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'cache', 'data')
            os.makedirs(cache_dir, exist_ok=True)
            if hasattr(result_data, 'to_parquet'):
                try:
                    result_data.to_parquet(os.path.join(cache_dir, f'{source_id}.parquet'), index=False)
                except Exception:
                    # Fallback: convert all columns to string for mixed-type DataFrames
                    result_data.astype(str).to_parquet(os.path.join(cache_dir, f'{source_id}.parquet'), index=False)
            else:
                import json
                with open(os.path.join(cache_dir, f'{source_id}.json'), 'w') as f:
                    json.dump(result_data, f, default=str)
        else:
            return {"status": "error", "error": f"Unknown source: {source_id}"}

        # Update source status in DB
        for key_suffix, val in [('_last', datetime.utcnow().isoformat()), ('_rows', str(row_count))]:
            s = db.query(AppState).filter(AppState.key == f"source_{source_id}{key_suffix}").first()
            if not s:
                s = AppState(key=f"source_{source_id}{key_suffix}")
                db.add(s)
            s.value = val
        db.commit()
        audit(db, "upload_source", admin.username, f"Uploaded {source_id}: {file.filename} ({row_count} rows)")

        return {"status": "success", "detected": source_id, "rows": row_count}

    except ImportError as ie:
        # Fallback: just store the file and track the upload
        state = db.query(AppState).filter(AppState.key == f"source_{source_id}_last").first()
        if not state:
            state = AppState(key=f"source_{source_id}_last")
            db.add(state)
        state.value = datetime.utcnow().isoformat()
        db.commit()
        audit(db, "upload_source", admin.username, f"Uploaded {source_id}: {file.filename} (stored, pending Data Hub integration)")
        return {"status": "success", "rows": 0, "note": "File stored. Data Hub processing not yet connected."}
    except Exception as e:
        return {"status": "error", "error": str(e)}
    finally:
        os.unlink(tmp.name)


# --- Rebuild All ---
@router.post("/rebuild-all")
async def rebuild_all(admin=Depends(require_admin), db: Session = Depends(get_db)):
    """Trigger full rebuild of all dashboards from cached source data."""
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        from data_hub.orchestrator import DataHub
        hub = DataHub(cache_dir='cache', ref_db_path='reference/reference.db')
        result = hub.rebuild_dashboard()
        audit(db, "rebuild_all", admin.username, f"Full rebuild: {result.get('vehicle_count',0)} vehicles")
        return result
    except ImportError as e:
        return {"status": "error", "error": f"Data Hub not connected: {e}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# --- GA4 API Pull ---
@router.post("/pull-ga4")
async def pull_ga4(days: int = 90, admin=Depends(require_admin), db: Session = Depends(get_db)):
    """Pull all 6 GA4 reports via Google Analytics Data API."""
    import os
    try:
        from app.ga4_api import fetch_all_reports, save_reports_to_cache
        client_secret = os.environ.get('GA4_CLIENT_SECRET_PATH', 'ga4_credentials.json')
        start = (datetime.utcnow() - __import__('datetime').timedelta(days=days)).strftime('%Y-%m-%d')
        results = fetch_all_reports(client_secret, start_date=start)
        # Save to cache
        cache_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        meta = save_reports_to_cache(results, cache_dir)
        # Store last GA4 pull timestamp
        state = db.query(AppState).filter(AppState.key == "last_ga4_pull").first()
        if not state:
            state = AppState(key="last_ga4_pull")
            db.add(state)
        state.value = datetime.utcnow().isoformat()
        db.commit()
        audit(db, "pull_ga4", admin.username, f"Pulled {days}d GA4 data: {sum(v.get('row_count',0) for v in meta.values())} total rows")
        return {"status": "success", "reports": {k: {"rows": v.get("row_count", 0)} for k, v in meta.items()}}
    except ImportError as ie:
        return {"status": "error", "error": f"GA4 API libraries not installed: {ie}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/last-ga4-pull")
def last_ga4_pull(db: Session = Depends(get_db)):
    state = db.query(AppState).filter(AppState.key == "last_ga4_pull").first()
    return {"last_pull": state.value if state else None}


# --- Audit Log ---
@router.get("/audit-log")
def get_audit_log(limit: int = 100, admin=Depends(require_admin), db: Session = Depends(get_db)):
    rows = db.query(AuditLog).order_by(AuditLog.created_at.desc()).limit(limit).all()
    return [{"id": r.id, "action": r.action, "user": r.user, "detail": r.detail, "created_at": str(r.created_at) if r.created_at else None} for r in rows]


# --- Upload History ---
@router.get("/upload-history")
def get_upload_history(admin=Depends(require_admin), db: Session = Depends(get_db)):
    rows = db.query(UploadHistory).order_by(UploadHistory.created_at.desc()).limit(20).all()
    return [{"id": r.id, "filename": r.filename, "uploaded_by": r.uploaded_by, "vehicles_count": r.vehicles_count, "retail_sales_count": r.retail_sales_count, "performance_count": r.performance_count, "status": r.status, "created_at": str(r.created_at) if r.created_at else None} for r in rows]


# --- Historical Trends ---
@router.get("/trends")
def get_trends(dealer: str = None, admin=Depends(require_admin), db: Session = Depends(get_db)):
    q = db.query(MonthlySnapshot)
    if dealer:
        q = q.filter(MonthlySnapshot.dealer.ilike(f"%{dealer}%"))
    rows = q.order_by(MonthlySnapshot.month, MonthlySnapshot.dealer).all()
    return [{"month": r.month, "dealer": r.dealer, "market": r.market, "sales": r.sales, "handovers": r.handovers, "on_ground": r.on_ground, "leads": r.leads, "test_drives": r.test_drives, "won": r.won} for r in rows]
