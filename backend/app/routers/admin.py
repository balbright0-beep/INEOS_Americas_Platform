import bcrypt
import httpx
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import User, AppState
from app.routers.auth import require_admin
from app.config import DASHBOARD_URL, ALLOCATION_URL

router = APIRouter(prefix="/api/admin", tags=["admin"])


def hash_pw(pw):
    return bcrypt.hashpw(pw.encode("utf-8")[:72], bcrypt.gensalt()).decode("utf-8")


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
    return {"id": user.id, "username": user.username, "role": user.role}


@router.delete("/users/{user_id}")
def delete_user(user_id: int, admin=Depends(require_admin), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    if user.role == "admin":
        raise HTTPException(400, "Cannot delete admin")
    db.delete(user)
    db.commit()
    return {"ok": True}


# --- Master File Upload ---
@router.post("/upload-master")
async def upload_master(file: UploadFile = File(...), admin=Depends(require_admin), db: Session = Depends(get_db)):
    contents = await file.read()
    results = {"dashboard": None, "allocation": None}

    async with httpx.AsyncClient(timeout=300) as client:
        # Forward to Dashboard App
        try:
            r = await client.post(
                f"{DASHBOARD_URL}/upload",
                files={"file": (file.filename, contents, file.content_type)},
                follow_redirects=True,
            )
            results["dashboard"] = "ok" if r.status_code < 400 else f"error: {r.status_code}"
        except Exception as e:
            results["dashboard"] = f"error: {e}"

        # Forward to Allocation App
        try:
            r = await client.post(
                f"{ALLOCATION_URL}/upload",
                files={"file": (file.filename, contents, file.content_type)},
                follow_redirects=True,
            )
            results["allocation"] = "ok" if r.status_code < 400 else f"error: {r.status_code}"
        except Exception as e:
            results["allocation"] = f"error: {e}"

    # Store timestamp
    state = db.query(AppState).filter(AppState.key == "last_master_upload").first()
    if not state:
        state = AppState(key="last_master_upload")
        db.add(state)
    state.value = datetime.utcnow().isoformat()
    db.commit()

    return {"results": results, "timestamp": state.value}


@router.get("/last-update")
def last_update(db: Session = Depends(get_db)):
    state = db.query(AppState).filter(AppState.key == "last_master_upload").first()
    return {"last_update": state.value if state else None}
