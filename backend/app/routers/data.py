from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional
from app.database import get_db
from app.models import Vehicle, RetailSale, DealerPerformance, RegionalSales
from app.routers.auth import get_current_user

router = APIRouter(prefix="/api/data", tags=["data"])


@router.get("/vehicles")
def list_vehicles(
    dealer: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(500, le=5000),
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    q = db.query(Vehicle)
    # Dealer users can only see their own vehicles
    if user.role == "dealer" and user.dealer_name:
        q = q.filter(Vehicle.dealer == user.dealer_name)
    elif dealer:
        q = q.filter(Vehicle.dealer == dealer)
    if status:
        q = q.filter(Vehicle.status == status)
    if search:
        s = f"%{search}%"
        q = q.filter((Vehicle.vin.ilike(s)) | (Vehicle.so_number.ilike(s)))
    total = q.count()
    rows = q.limit(limit).all()
    return {"total": total, "items": [_veh(v) for v in rows]}


@router.get("/vehicles/search")
def search_vins(
    q: str = "",
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Single and mass VIN/SO# lookup."""
    if not q or len(q) < 3:
        return {"results": [], "stats": {}}

    # Split by newlines, commas, spaces for mass lookup
    terms = [t.strip() for t in q.replace(",", "\n").replace(" ", "\n").split("\n") if len(t.strip()) >= 3]

    results = []
    found_vins = set()
    for term in terms[:200]:  # max 200 terms
        matches = db.query(Vehicle).filter(
            (Vehicle.vin.ilike(f"%{term}%")) | (Vehicle.so_number.ilike(f"%{term}%"))
        ).limit(50).all()
        for v in matches:
            if v.vin not in found_vins:
                found_vins.add(v.vin)
                results.append(_veh(v))

    # Stats
    stats = {
        "queries": len(terms),
        "found": len(results),
        "not_found": len(terms) - len(set(r["vin"] for r in results)),
        "dealer_stock": sum(1 for r in results if r["status"] == "Dealer Stock"),
        "in_pipeline": sum(1 for r in results if r["status"] in ("In-Transit to Dealer", "At Americas Port", "On Water")),
        "sold": sum(1 for r in results if r["status"] == "Sold"),
        "total_msrp": sum(r["msrp"] for r in results),
    }
    return {"results": results, "stats": stats}


@router.get("/retail-sales")
def list_retail_sales(
    dealer: Optional[str] = None,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    q = db.query(RetailSale)
    if user.role == "dealer" and user.dealer_name:
        q = q.filter(RetailSale.dealer == user.dealer_name)
    elif dealer:
        q = q.filter(RetailSale.dealer == dealer)
    rows = q.order_by(RetailSale.handover_date.desc()).all()
    return [{"id": r.id, "dealer": r.dealer, "market": r.market, "vin": r.vin, "vin_full": r.vin_full,
             "body": r.body, "model_year": r.model_year, "trim": r.trim, "ext_color": r.ext_color,
             "int_color": r.int_color, "wheels": r.wheels, "channel": r.channel, "msrp": r.msrp,
             "days_to_sell": r.days_to_sell, "cvp": r.cvp, "handover_date": r.handover_date} for r in rows]


@router.get("/dealer-performance")
def list_dealer_performance(
    dealer: Optional[str] = None,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    q = db.query(DealerPerformance)
    if user.role == "dealer" and user.dealer_name:
        q = q.filter(DealerPerformance.dealer == user.dealer_name)
    elif dealer:
        q = q.filter(DealerPerformance.dealer == dealer)
    rows = q.order_by(DealerPerformance.market, DealerPerformance.dealer).all()
    return [{"id": r.id, "dealer": r.dealer, "market": r.market, "handovers": r.handovers,
             "cvp": r.cvp, "wholesales": r.wholesales, "on_ground": r.on_ground,
             "dealer_stock": r.dealer_stock, "leads": r.leads, "test_drives": r.test_drives,
             "td_completed": r.td_completed, "td_show_pct": r.td_show_pct,
             "lead_to_td_pct": r.lead_to_td_pct, "won": r.won, "lost": r.lost,
             "mb30": r.mb30, "mb60": r.mb60, "mb90": r.mb90} for r in rows]


@router.get("/regional-sales")
def list_regional_sales(user=Depends(get_current_user), db: Session = Depends(get_db)):
    rows = db.query(RegionalSales).all()
    return [{"id": r.id, "region": r.region, "sw": r.sw, "qm": r.qm, "svo": r.svo,
             "total": r.total, "objective": r.objective, "pct_objective": r.pct_objective,
             "cvp": r.cvp} for r in rows]


@router.get("/dealer-stats")
def dealer_stats(user=Depends(get_current_user), db: Session = Depends(get_db)):
    """Summary stats for dealer overview page."""
    dealer_filter = user.dealer_name if user.role == "dealer" and user.dealer_name else None

    vq = db.query(Vehicle)
    if dealer_filter:
        vq = vq.filter(Vehicle.dealer == dealer_filter)

    total_vehicles = vq.count()
    dealer_stock = vq.filter(Vehicle.status == "Dealer Stock").count()
    in_pipeline = vq.filter(Vehicle.status.in_(["In-Transit to Dealer", "At Americas Port", "On Water"])).count()
    sold = vq.filter(Vehicle.status == "Sold").count()

    rq = db.query(RetailSale)
    if dealer_filter:
        rq = rq.filter(RetailSale.dealer == dealer_filter)
    mtd_sales = rq.count()
    mtd_msrp = db.query(func.sum(RetailSale.msrp)).filter(
        RetailSale.dealer == dealer_filter if dealer_filter else True
    ).scalar() or 0

    # Dealer performance
    perf = None
    if dealer_filter:
        perf = db.query(DealerPerformance).filter(DealerPerformance.dealer == dealer_filter).first()

    return {
        "total_vehicles": total_vehicles,
        "dealer_stock": dealer_stock,
        "in_pipeline": in_pipeline,
        "sold": sold,
        "mtd_sales": mtd_sales,
        "mtd_msrp": mtd_msrp,
        "leads": perf.leads if perf else 0,
        "test_drives": perf.test_drives if perf else 0,
        "handovers": perf.handovers if perf else 0,
    }


def _veh(v):
    return {
        "id": v.id, "vin": v.vin, "dealer": v.dealer, "market": v.market,
        "country": v.country, "body": v.body, "model_year": v.model_year,
        "status": v.status, "msrp": v.msrp, "trim": v.trim,
        "ext_color": v.ext_color, "int_color": v.int_color, "roof": v.roof,
        "wheels": v.wheels, "channel": v.channel, "plant": v.plant,
        "handover_date": v.handover_date, "eta": v.eta, "vessel": v.vessel,
        "days_on_lot": v.days_on_lot, "so_number": v.so_number,
    }
