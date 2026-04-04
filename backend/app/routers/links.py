from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import LinkCategory, Link
from app.routers.auth import get_current_user, require_admin

router = APIRouter(prefix="/api/links", tags=["links"])


@router.get("")
def get_all_links(user=Depends(get_current_user), db: Session = Depends(get_db)):
    cats = db.query(LinkCategory).order_by(LinkCategory.sort_order).all()
    result = []
    for c in cats:
        links = db.query(Link).filter(Link.category_id == c.id).order_by(Link.sort_order).all()
        result.append({
            "id": c.id, "name": c.name, "sort_order": c.sort_order,
            "links": [{"id": l.id, "name": l.name, "url": l.url, "description": l.description} for l in links]
        })
    return result


@router.post("/categories")
def create_category(data: dict, admin=Depends(require_admin), db: Session = Depends(get_db)):
    c = LinkCategory(name=data["name"], sort_order=data.get("sort_order", 0))
    db.add(c)
    db.commit()
    db.refresh(c)
    return {"id": c.id, "name": c.name}


@router.delete("/categories/{cat_id}")
def delete_category(cat_id: int, admin=Depends(require_admin), db: Session = Depends(get_db)):
    db.query(Link).filter(Link.category_id == cat_id).delete()
    db.query(LinkCategory).filter(LinkCategory.id == cat_id).delete()
    db.commit()
    return {"ok": True}


@router.post("/categories/{cat_id}/items")
def create_link(cat_id: int, data: dict, admin=Depends(require_admin), db: Session = Depends(get_db)):
    l = Link(category_id=cat_id, name=data["name"], url=data["url"], description=data.get("description", ""), sort_order=data.get("sort_order", 0))
    db.add(l)
    db.commit()
    db.refresh(l)
    return {"id": l.id, "name": l.name}


@router.delete("/items/{link_id}")
def delete_link(link_id: int, admin=Depends(require_admin), db: Session = Depends(get_db)):
    db.query(Link).filter(Link.id == link_id).delete()
    db.commit()
    return {"ok": True}
