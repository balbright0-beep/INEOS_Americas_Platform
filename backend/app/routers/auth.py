import bcrypt
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from jose import jwt
from app.database import get_db
from app.models import User
from app.config import JWT_SECRET

router = APIRouter(prefix="/api/auth", tags=["auth"])


def create_token(data: dict):
    return jwt.encode({**data, "exp": datetime.utcnow() + timedelta(hours=12)}, JWT_SECRET, algorithm="HS256")


def get_current_user(request: Request, db: Session = Depends(get_db)):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(401, "Not authenticated")
    try:
        payload = jwt.decode(auth[7:], JWT_SECRET, algorithms=["HS256"])
        user = db.query(User).filter(User.username == payload["sub"]).first()
        if not user:
            raise HTTPException(401, "User not found")
        return user
    except Exception:
        raise HTTPException(401, "Invalid token")


def require_admin(user: User = Depends(get_current_user)):
    if user.role != "admin":
        raise HTTPException(403, "Admin access required")
    return user


@router.post("/login")
def login(data: dict, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == data.get("username")).first()
    if not user or not bcrypt.checkpw(data.get("password", "").encode("utf-8")[:72], user.password_hash.encode("utf-8")):
        raise HTTPException(401, "Invalid credentials")
    token = create_token({"sub": user.username, "role": user.role})
    # Audit login
    from app.models import AuditLog
    db.add(AuditLog(action="login", user=user.username, detail=f"Role: {user.role}"))
    db.commit()
    return {"token": token, "username": user.username, "role": user.role, "dealer_name": user.dealer_name}


@router.get("/me")
def me(user: User = Depends(get_current_user)):
    return {"username": user.username, "role": user.role, "dealer_name": user.dealer_name}
