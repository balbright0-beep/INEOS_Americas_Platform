from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import Bulletin
from app.routers.auth import get_current_user, require_admin

router = APIRouter(prefix="/api/bulletins", tags=["bulletins"])


@router.get("")
def list_bulletins(audience: str = None, user=Depends(get_current_user), db: Session = Depends(get_db)):
    q = db.query(Bulletin).order_by(Bulletin.created_at.desc())
    if audience:
        q = q.filter((Bulletin.audience == audience) | (Bulletin.audience == "both"))
    elif user.role == "dealer":
        q = q.filter((Bulletin.audience == "dealer") | (Bulletin.audience == "both"))
    elif user.role in ("internal", "admin"):
        q = q.filter((Bulletin.audience == "internal") | (Bulletin.audience == "both"))
    return [{"id": b.id, "title": b.title, "content": b.content, "priority": b.priority, "audience": b.audience, "created_by": b.created_by, "created_at": str(b.created_at) if b.created_at else None} for b in q.limit(50).all()]


@router.post("")
def create_bulletin(data: dict, admin=Depends(require_admin), db: Session = Depends(get_db)):
    b = Bulletin(
        title=data["title"],
        content=data["content"],
        priority=data.get("priority", "info"),
        audience=data.get("audience", "both"),
        created_by=admin.username,
    )
    db.add(b)
    db.commit()
    db.refresh(b)
    return {"id": b.id, "title": b.title}


@router.delete("/{bulletin_id}")
def delete_bulletin(bulletin_id: int, admin=Depends(require_admin), db: Session = Depends(get_db)):
    b = db.query(Bulletin).filter(Bulletin.id == bulletin_id).first()
    if not b:
        raise HTTPException(404, "Bulletin not found")
    db.delete(b)
    db.commit()
    return {"ok": True}
