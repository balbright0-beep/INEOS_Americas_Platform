from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional
from app.database import get_db
from app.models import Vehicle, RetailSale, DealerPerformance, RegionalSales
from app.routers.auth import get_current_user

router = APIRouter(prefix="/api/data", tags=["data"])


def _dealer_filter(user):
    """Return dealer_name if user is a dealer, else None."""
    return user.dealer_name if user.role == "dealer" and user.dealer_name else None


@router.get("/dealer-stats")
def dealer_stats(user=Depends(get_current_user), db: Session = Depends(get_db)):
    df = _dealer_filter(user)

    vq = db.query(Vehicle)
    if df:
        vq = vq.filter(Vehicle.dealer == df)

    dealer_stock = vq.filter(Vehicle.status == "Dealer Stock").count()
    in_pipeline = vq.filter(Vehicle.status.in_(["In-Transit to Dealer", "At Americas Port", "On Water"])).count()
    sold = vq.filter(Vehicle.status == "Sold").count()

    rq = db.query(RetailSale)
    if df:
        rq = rq.filter(RetailSale.dealer == df)
    mtd_sales = rq.count()

    # Performance KPIs
    perf = None
    if df:
        perf = db.query(DealerPerformance).filter(DealerPerformance.dealer == df).first()

    return {
        "mtd_sales": mtd_sales,
        "dealer_stock": dealer_stock,
        "in_pipeline": in_pipeline,
        "sold": sold,
        "leads": perf.leads if perf else 0,
        "test_drives": perf.test_drives if perf else 0,
        "handovers": perf.handovers if perf else 0,
        "on_ground": perf.on_ground if perf else 0,
    }


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
    df = _dealer_filter(user)
    if df:
        q = q.filter(Vehicle.dealer == df)
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
def search_vins(q: str = "", user=Depends(get_current_user), db: Session = Depends(get_db)):
    if not q or len(q) < 3:
        return {"results": [], "stats": {}}
    terms = [t.strip() for t in q.replace(",", "\n").replace(" ", "\n").split("\n") if len(t.strip()) >= 3]
    results = []
    found_vins = set()
    for term in terms[:200]:
        query = db.query(Vehicle).filter(
            (Vehicle.vin.ilike(f"%{term}%")) | (Vehicle.so_number.ilike(f"%{term}%"))
        )
        df = _dealer_filter(user)
        if df:
            query = query.filter(Vehicle.dealer == df)
        for v in query.limit(50).all():
            if v.vin not in found_vins:
                found_vins.add(v.vin)
                results.append(_veh(v))
    stats = {
        "queries": len(terms), "found": len(results),
        "not_found": max(0, len(terms) - len(results)),
        "dealer_stock": sum(1 for r in results if r["status"] == "Dealer Stock"),
        "in_pipeline": sum(1 for r in results if r["status"] in ("In-Transit to Dealer", "At Americas Port", "On Water")),
        "sold": sum(1 for r in results if r["status"] == "Sold"),
        "total_msrp": sum(r["msrp"] or 0 for r in results),
    }
    return {"results": results, "stats": stats}


@router.get("/retail-sales")
def list_retail_sales(dealer: Optional[str] = None, user=Depends(get_current_user), db: Session = Depends(get_db)):
    q = db.query(RetailSale)
    df = _dealer_filter(user)
    if df:
        # Show all dealers in same market for comparison
        my_perf = db.query(DealerPerformance).filter(DealerPerformance.dealer == df).first()
        if my_perf and my_perf.market:
            # Get all dealers in same market
            market_dealers = [d.dealer for d in db.query(DealerPerformance).filter(DealerPerformance.market == my_perf.market).all()]
            q = q.filter(RetailSale.dealer.in_(market_dealers))
    elif dealer:
        q = q.filter(RetailSale.dealer == dealer)
    rows = q.order_by(RetailSale.dealer, RetailSale.handover_date.desc()).all()
    return [{"id": r.id, "dealer": r.dealer, "market": r.market, "vin": r.vin, "vin_full": r.vin_full,
             "body": r.body, "model_year": r.model_year, "trim": r.trim, "ext_color": r.ext_color,
             "int_color": r.int_color, "wheels": r.wheels, "channel": r.channel, "msrp": r.msrp,
             "days_to_sell": r.days_to_sell, "cvp": r.cvp, "handover_date": r.handover_date} for r in rows]


@router.get("/retail-sales/mtd-sold")
def mtd_sold_units(user=Depends(get_current_user), db: Session = Depends(get_db)):
    """Get the logged-in dealer's MTD sold vehicles with full details."""
    df = _dealer_filter(user)
    if not df:
        return []
    rows = db.query(RetailSale).filter(RetailSale.dealer == df).order_by(RetailSale.handover_date.desc()).all()
    return [{"id": r.id, "dealer": r.dealer, "vin": r.vin, "vin_full": r.vin_full,
             "body": r.body, "model_year": r.model_year, "trim": r.trim, "ext_color": r.ext_color,
             "int_color": r.int_color, "wheels": r.wheels, "channel": r.channel, "msrp": r.msrp,
             "days_to_sell": r.days_to_sell, "handover_date": r.handover_date} for r in rows]


@router.get("/dealer-performance")
def list_dealer_performance(dealer: Optional[str] = None, user=Depends(get_current_user), db: Session = Depends(get_db)):
    q = db.query(DealerPerformance)
    df = _dealer_filter(user)
    if df:
        q = q.filter(DealerPerformance.dealer == df)
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


@router.get("/inventory")
def dealer_inventory(user=Depends(get_current_user), db: Session = Depends(get_db)):
    """Dealer inventory breakdown: on-ground by body/MY, in-transit, turn rate."""
    df = _dealer_filter(user)
    q = db.query(Vehicle)
    if df:
        q = q.filter(Vehicle.dealer == df)

    all_vehicles = q.all()
    og = [v for v in all_vehicles if v.status == "Dealer Stock"]
    sold = [v for v in all_vehicles if v.status == "Sold"]
    in_transit = [v for v in all_vehicles if v.status in ("In-Transit to Dealer", "At Americas Port", "On Water")]

    # Body breakdown for on-ground
    og_sw = sum(1 for v in og if v.body == "SW")
    og_qm = sum(1 for v in og if v.body == "QM")
    og_svo = sum(1 for v in og if v.body == "SVO")

    # MY breakdown for on-ground
    og_by_my = {}
    for v in og:
        my = v.model_year or "Unknown"
        og_by_my[my] = og_by_my.get(my, 0) + 1

    # Avg days on lot for on-ground
    dol_values = [v.days_on_lot for v in og if v.days_on_lot and v.days_on_lot > 0]
    avg_dol = round(sum(dol_values) / len(dol_values)) if dol_values else 0

    # Turn rate: sold / (sold + og) * 100
    total_og = len(og)
    total_sold = len(sold)
    turn_rate = round(total_sold / (total_sold + total_og) * 100, 1) if (total_sold + total_og) > 0 else 0

    # Days supply: og / (avg monthly sales)  where avg = sold count (approximation for MTD)
    days_supply = round(total_og / (total_sold / 30) if total_sold > 0 else 0)

    # Individual vehicles on ground
    og_list = [_veh(v) for v in og]
    it_list = [_veh(v) for v in in_transit]

    return {
        "on_ground_total": total_og,
        "on_ground_sw": og_sw,
        "on_ground_qm": og_qm,
        "on_ground_svo": og_svo,
        "on_ground_by_my": og_by_my,
        "in_transit_total": len(in_transit),
        "sold_mtd": total_sold,
        "avg_days_on_lot": avg_dol,
        "turn_rate": turn_rate,
        "days_supply": days_supply,
        "on_ground_vehicles": og_list,
        "in_transit_vehicles": it_list,
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
