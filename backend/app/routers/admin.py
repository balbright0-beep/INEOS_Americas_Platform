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
