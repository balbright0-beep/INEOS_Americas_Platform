import bcrypt
import secrets
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from jose import jwt
from app.database import get_db
from app.models import User, AuditLog
from app.config import JWT_SECRET
from app.email_sender import send_mfa_code

router = APIRouter(prefix="/api/auth", tags=["auth"])

# MFA settings
MFA_CODE_EXPIRY_MINUTES = 5
MFA_MAX_ATTEMPTS = 5
# Admins are required to use MFA. Non-admin users can opt in per account.
MFA_REQUIRED_ROLES = {"admin"}


def create_token(data: dict):
    return jwt.encode({**data, "exp": datetime.utcnow() + timedelta(hours=12)}, JWT_SECRET, algorithm="HS256")


def _create_mfa_challenge_token(username: str):
    """A short-lived token that proves the user completed step 1 (password)."""
    return jwt.encode(
        {"sub": username, "mfa": "pending", "exp": datetime.utcnow() + timedelta(minutes=MFA_CODE_EXPIRY_MINUTES + 1)},
        JWT_SECRET, algorithm="HS256",
    )


def get_current_user(request: Request, db: Session = Depends(get_db)):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(401, "Not authenticated")
    try:
        payload = jwt.decode(auth[7:], JWT_SECRET, algorithms=["HS256"])
        if payload.get("mfa") == "pending":
            raise HTTPException(401, "MFA verification required")
        user = db.query(User).filter(User.username == payload["sub"]).first()
        if not user:
            raise HTTPException(401, "User not found")
        return user
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(401, "Invalid token")


def require_admin(user: User = Depends(get_current_user)):
    if user.role != "admin":
        raise HTTPException(403, "Admin access required")
    return user


def _user_must_mfa(user: User) -> bool:
    """True if this user needs to complete a 2nd factor."""
    if user.role in MFA_REQUIRED_ROLES:
        return True
    return bool(user.mfa_enabled)


def _generate_and_send_code(user: User, db: Session) -> bool:
    code = f"{secrets.randbelow(1_000_000):06d}"
    user.mfa_code_hash = bcrypt.hashpw(code.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    user.mfa_code_expires = datetime.utcnow() + timedelta(minutes=MFA_CODE_EXPIRY_MINUTES)
    user.mfa_attempts = 0
    db.commit()
    # Fallback: if the admin hasn't set an email yet, print the code to logs
    # so the person deploying can still sign in and set one.
    target_email = user.email or ""
    ok = send_mfa_code(target_email, user.username, code, MFA_CODE_EXPIRY_MINUTES)
    if not target_email:
        print(f"[mfa] WARNING: user {user.username} has no email on file — code = {code}")
    return ok


@router.post("/login")
def login(data: dict, db: Session = Depends(get_db)):
    """
    Step 1: validate username + password. If the user requires MFA, return
    { mfaRequired: True, challengeToken, emailHint }. Otherwise return a
    normal session token directly.
    """
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        raise HTTPException(400, "Username and password required")

    user = db.query(User).filter(User.username == username).first()
    if not user or not bcrypt.checkpw(password.encode("utf-8")[:72], user.password_hash.encode("utf-8")):
        raise HTTPException(401, "Invalid credentials")

    if _user_must_mfa(user):
        _generate_and_send_code(user, db)
        db.add(AuditLog(action="mfa_challenge", user=user.username,
                        detail=f"Code sent to {user.email or '(no email on file, see logs)'}"))
        db.commit()
        # Mask the email for display: j***@ineos.com
        hint = ""
        if user.email and "@" in user.email:
            local, _, domain = user.email.partition("@")
            hint = (local[:1] + "***@" + domain) if local else "***@" + domain
        return {
            "mfaRequired": True,
            "challengeToken": _create_mfa_challenge_token(user.username),
            "emailHint": hint,
            "hasEmail": bool(user.email),
            "expiresInMinutes": MFA_CODE_EXPIRY_MINUTES,
        }

    # No MFA required — issue session token immediately
    token = create_token({"sub": user.username, "role": user.role})
    db.add(AuditLog(action="login", user=user.username, detail=f"Role: {user.role}"))
    db.commit()
    return {"token": token, "username": user.username, "role": user.role, "dealer_name": user.dealer_name}


@router.post("/verify-mfa")
def verify_mfa(data: dict, db: Session = Depends(get_db)):
    """Step 2: verify the 6-digit code against the challenge token."""
    challenge_token = data.get("challengeToken") or ""
    code = (data.get("code") or "").strip()
    if not challenge_token or not code:
        raise HTTPException(400, "challengeToken and code are required")

    try:
        payload = jwt.decode(challenge_token, JWT_SECRET, algorithms=["HS256"])
        if payload.get("mfa") != "pending":
            raise HTTPException(400, "Not an MFA challenge token")
        username = payload["sub"]
    except Exception:
        raise HTTPException(401, "Challenge token invalid or expired — please login again")

    user = db.query(User).filter(User.username == username).first()
    if not user or not user.mfa_code_hash or not user.mfa_code_expires:
        raise HTTPException(400, "No active MFA challenge for this user")

    if datetime.utcnow() > user.mfa_code_expires:
        user.mfa_code_hash = None
        user.mfa_code_expires = None
        db.commit()
        raise HTTPException(401, "Code expired — please login again")

    if (user.mfa_attempts or 0) >= MFA_MAX_ATTEMPTS:
        user.mfa_code_hash = None
        user.mfa_code_expires = None
        db.commit()
        db.add(AuditLog(action="mfa_locked", user=user.username, detail="Too many failed attempts"))
        db.commit()
        raise HTTPException(429, "Too many failed attempts — please login again")

    if not bcrypt.checkpw(code.encode("utf-8"), user.mfa_code_hash.encode("utf-8")):
        user.mfa_attempts = (user.mfa_attempts or 0) + 1
        db.commit()
        raise HTTPException(401, "Incorrect code")

    # Success — clear the challenge and issue a full session token
    user.mfa_code_hash = None
    user.mfa_code_expires = None
    user.mfa_attempts = 0
    db.commit()

    token = create_token({"sub": user.username, "role": user.role, "mfa": "passed"})
    db.add(AuditLog(action="login", user=user.username, detail=f"Role: {user.role} (MFA)"))
    db.commit()
    return {"token": token, "username": user.username, "role": user.role, "dealer_name": user.dealer_name}


@router.post("/resend-mfa")
def resend_mfa(data: dict, db: Session = Depends(get_db)):
    """Re-issue a new 6-digit code for a still-valid challenge token."""
    challenge_token = data.get("challengeToken") or ""
    try:
        payload = jwt.decode(challenge_token, JWT_SECRET, algorithms=["HS256"])
        if payload.get("mfa") != "pending":
            raise HTTPException(400, "Not an MFA challenge token")
        username = payload["sub"]
    except Exception:
        raise HTTPException(401, "Challenge expired — please login again")

    user = db.query(User).filter(User.username == username).first()
    if not user:
        raise HTTPException(404, "User not found")
    _generate_and_send_code(user, db)
    return {"ok": True, "expiresInMinutes": MFA_CODE_EXPIRY_MINUTES}


@router.get("/me")
def me(user: User = Depends(get_current_user)):
    return {
        "username": user.username,
        "role": user.role,
        "dealer_name": user.dealer_name,
        "email": user.email or "",
        "mfa_enabled": bool(user.mfa_enabled),
        "mfa_required": user.role in MFA_REQUIRED_ROLES,
    }


@router.post("/bootstrap-email")
def bootstrap_email(data: dict, db: Session = Depends(get_db)):
    """
    First-time setup: an admin whose account was seeded before MFA rolled out
    has no email on file. This endpoint accepts username + password (so MFA
    is bypassed once) and sets an email so the next login can complete MFA.
    Only works when the user currently has NO email address.
    """
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    email = (data.get("email") or "").strip()
    if not username or not password or not email:
        raise HTTPException(400, "username, password, and email are required")
    if "@" not in email or "." not in email.split("@")[-1] or len(email) > 200:
        raise HTTPException(400, "Invalid email address")

    user = db.query(User).filter(User.username == username).first()
    if not user or not bcrypt.checkpw(password.encode("utf-8")[:72], user.password_hash.encode("utf-8")):
        raise HTTPException(401, "Invalid credentials")
    if user.email:
        raise HTTPException(400, "Email is already set — use /me/email to change it after logging in")
    user.email = email
    db.add(AuditLog(action="bootstrap_email", user=user.username, detail=f"Set initial email to {email}"))
    db.commit()
    return {"ok": True}


@router.post("/me/email")
def update_my_email(data: dict, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """
    Let a user set/update their email so MFA codes can be delivered.
    Admins must set an email once before MFA enforcement kicks in on their
    next login. Basic sanity check on the address.
    """
    email = (data.get("email") or "").strip()
    if "@" not in email or "." not in email.split("@")[-1] or len(email) > 200:
        raise HTTPException(400, "Invalid email address")
    user.email = email
    db.add(AuditLog(action="update_email", user=user.username, detail=f"Set email to {email}"))
    db.commit()
    return {"ok": True, "email": email}


@router.post("/me/mfa")
def toggle_my_mfa(data: dict, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Opt-in / opt-out of MFA (non-admin users only — admins are always required)."""
    if user.role in MFA_REQUIRED_ROLES:
        raise HTTPException(400, "MFA is required for your role and cannot be disabled")
    enabled = bool(data.get("enabled"))
    if enabled and not user.email:
        raise HTTPException(400, "Set an email address before enabling MFA")
    user.mfa_enabled = enabled
    db.add(AuditLog(action="toggle_mfa", user=user.username, detail=f"Set mfa_enabled={enabled}"))
    db.commit()
    return {"ok": True, "mfa_enabled": enabled}
