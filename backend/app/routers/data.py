from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct
from typing import Optional
from app.database import get_db
from app.models import Vehicle, RetailSale, DealerPerformance, RegionalSales, MonthlySnapshot
from app.routers.auth import get_current_user

router = APIRouter(prefix="/api/data", tags=["data"])


def _dealer_filter(user):
    return user.dealer_name if user.role == "dealer" and user.dealer_name else None


def _dfq(q, col, dealer_name):
    """Apply case-insensitive partial dealer filter."""
    if not dealer_name:
        return q
    return q.filter(col.ilike(f"%{dealer_name}%"))


# ===== DEBUG: See what dealers exist in DB =====
@router.get("/debug/dealers")
def debug_dealers(user=Depends(get_current_user), db: Session = Depends(get_db)):
    v_dealers = [r[0] for r in db.query(distinct(Vehicle.dealer)).all() if r[0]]
    dp_dealers = [r[0] for r in db.query(distinct(DealerPerformance.dealer)).all() if r[0]]
    rs_dealers = [r[0] for r in db.query(distinct(RetailSale.dealer)).all() if r[0]]
    return {
        "user_dealer_name": user.dealer_name,
        "user_role": user.role,
        "vehicles_dealers": sorted(v_dealers)[:50],
        "performance_dealers": sorted(dp_dealers)[:50],
        "retail_sales_dealers": sorted(rs_dealers)[:50],
        "vehicle_count": db.query(Vehicle).count(),
        "retail_sale_count": db.query(RetailSale).count(),
        "performance_count": db.query(DealerPerformance).count(),
    }


@router.get("/dealer-stats")
def dealer_stats(user=Depends(get_current_user), db: Session = Depends(get_db)):
    df = _dealer_filter(user)

    vq = _dfq(db.query(Vehicle), Vehicle.dealer, df)
    dealer_stock = _dfq(db.query(Vehicle).filter(Vehicle.status == "Dealer Stock"), Vehicle.dealer, df).count()
    in_pipeline = _dfq(db.query(Vehicle).filter(Vehicle.status.in_(["In-Transit to Dealer", "At Americas Port", "On Water"])), Vehicle.dealer, df).count()
    sold = _dfq(db.query(Vehicle).filter(Vehicle.status == "Sold"), Vehicle.dealer, df).count()

    rq = _dfq(db.query(RetailSale), RetailSale.dealer, df)
    mtd_sales = rq.count()

    perf = None
    if df:
        perf = db.query(DealerPerformance).filter(DealerPerformance.dealer.ilike(f"%{df}%")).first()

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
    dealer: Optional[str] = None, status: Optional[str] = None,
    search: Optional[str] = None, limit: int = Query(500, le=5000),
    user=Depends(get_current_user), db: Session = Depends(get_db),
):
    q = db.query(Vehicle)
    df = _dealer_filter(user)
    q = _dfq(q, Vehicle.dealer, df or dealer)
    if status:
        q = q.filter(Vehicle.status == status)
    if search:
        q = q.filter((Vehicle.vin.ilike(f"%{search}%")) | (Vehicle.so_number.ilike(f"%{search}%")))
    total = q.count()
    return {"total": total, "items": [_veh(v) for v in q.limit(limit).all()]}


@router.get("/vehicles/search")
def search_vins(q: str = "", user=Depends(get_current_user), db: Session = Depends(get_db)):
    if not q or len(q) < 3:
        return {"results": [], "stats": {}}
    terms = [t.strip() for t in q.replace(",", "\n").replace(" ", "\n").split("\n") if len(t.strip()) >= 3]
    results, found_vins = [], set()
    df = _dealer_filter(user)
    for term in terms[:200]:
        query = db.query(Vehicle).filter((Vehicle.vin.ilike(f"%{term}%")) | (Vehicle.so_number.ilike(f"%{term}%")))
        query = _dfq(query, Vehicle.dealer, df)
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
        my_perf = db.query(DealerPerformance).filter(DealerPerformance.dealer.ilike(f"%{df}%")).first()
        if my_perf and my_perf.market:
            market_dealers = [d.dealer for d in db.query(DealerPerformance).filter(DealerPerformance.market == my_perf.market).all()]
            q = q.filter(RetailSale.dealer.in_(market_dealers))
        else:
            # Security fix: only show this dealer's sales if not in performance table
            q = _dfq(q, RetailSale.dealer, df)
    elif dealer:
        q = _dfq(q, RetailSale.dealer, dealer)
    rows = q.order_by(RetailSale.dealer, RetailSale.handover_date.desc()).all()
    return [{"id": r.id, "dealer": r.dealer, "market": r.market, "vin": r.vin, "vin_full": r.vin_full,
             "body": r.body, "model_year": r.model_year, "trim": r.trim, "ext_color": r.ext_color,
             "int_color": r.int_color, "wheels": r.wheels, "channel": r.channel, "msrp": r.msrp,
             "days_to_sell": r.days_to_sell, "cvp": r.cvp, "handover_date": r.handover_date} for r in rows]


@router.get("/retail-sales/mtd-sold")
def mtd_sold_units(user=Depends(get_current_user), db: Session = Depends(get_db)):
    df = _dealer_filter(user)
    if not df:
        return []
    rows = _dfq(db.query(RetailSale), RetailSale.dealer, df).order_by(RetailSale.handover_date.desc()).all()
    return [{"id": r.id, "dealer": r.dealer, "vin": r.vin, "vin_full": r.vin_full,
             "body": r.body, "model_year": r.model_year, "trim": r.trim, "ext_color": r.ext_color,
             "int_color": r.int_color, "wheels": r.wheels, "channel": r.channel, "msrp": r.msrp,
             "days_to_sell": r.days_to_sell, "handover_date": r.handover_date} for r in rows]


@router.get("/dealer-performance")
def list_dealer_performance(dealer: Optional[str] = None, user=Depends(get_current_user), db: Session = Depends(get_db)):
    q = db.query(DealerPerformance)
    df = _dealer_filter(user)
    q = _dfq(q, DealerPerformance.dealer, df or dealer)
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
    df = _dealer_filter(user)
    q = _dfq(db.query(Vehicle), Vehicle.dealer, df)
    all_vehicles = q.all()

    og = [v for v in all_vehicles if v.status == "Dealer Stock"]
    sold = [v for v in all_vehicles if v.status == "Sold"]
    in_transit = [v for v in all_vehicles if v.status in ("In-Transit to Dealer", "At Americas Port", "On Water")]

    og_sw = sum(1 for v in og if v.body == "SW")
    og_qm = sum(1 for v in og if v.body == "QM")
    og_svo = sum(1 for v in og if v.body == "SVO")

    og_by_my = {}
    for v in og:
        my = v.model_year or "Unknown"
        og_by_my[my] = og_by_my.get(my, 0) + 1

    dol_values = [v.days_on_lot for v in og if v.days_on_lot and v.days_on_lot > 0]
    avg_dol = round(sum(dol_values) / len(dol_values)) if dol_values else 0

    total_og, total_sold = len(og), len(sold)
    turn_rate = round(total_sold / (total_sold + total_og) * 100, 1) if (total_sold + total_og) > 0 else 0
    days_supply = round(total_og / (total_sold / 30)) if total_sold > 0 else 0

    return {
        "on_ground_total": total_og, "on_ground_sw": og_sw, "on_ground_qm": og_qm,
        "on_ground_svo": og_svo, "on_ground_by_my": og_by_my,
        "in_transit_total": len(in_transit), "sold_mtd": total_sold,
        "avg_days_on_lot": avg_dol, "turn_rate": turn_rate, "days_supply": days_supply,
        "on_ground_vehicles": [_veh(v) for v in og],
        "in_transit_vehicles": [_veh(v) for v in in_transit],
    }


@router.get("/scorecard")
def dealer_scorecard(user=Depends(get_current_user), db: Session = Depends(get_db)):
    """Dealer scorecard with grades compared to network averages."""
    df = _dealer_filter(user)
    perf = db.query(DealerPerformance).filter(DealerPerformance.dealer.ilike(f"%{df}%")).first() if df else None
    if not perf:
        return {"dealer": df, "grades": {}, "network_avg": {}}

    # Network averages
    all_perf = db.query(DealerPerformance).all()
    n = len(all_perf) or 1
    net_ho = sum(p.handovers for p in all_perf) / n
    net_leads = sum(p.leads for p in all_perf) / n
    net_td = sum(p.test_drives for p in all_perf) / n
    net_won = sum(p.won for p in all_perf) / n
    net_og = sum(p.on_ground for p in all_perf) / n

    def grade(val, avg):
        if avg == 0: return "B"
        ratio = val / avg
        if ratio >= 1.2: return "A"
        if ratio >= 0.9: return "B"
        if ratio >= 0.7: return "C"
        if ratio >= 0.5: return "D"
        return "F"

    return {
        "dealer": perf.dealer, "market": perf.market,
        "metrics": {
            "handovers": {"value": perf.handovers, "network_avg": round(net_ho, 1), "grade": grade(perf.handovers, net_ho)},
            "leads": {"value": perf.leads, "network_avg": round(net_leads, 1), "grade": grade(perf.leads, net_leads)},
            "test_drives": {"value": perf.test_drives, "network_avg": round(net_td, 1), "grade": grade(perf.test_drives, net_td)},
            "won": {"value": perf.won, "network_avg": round(net_won, 1), "grade": grade(perf.won, net_won)},
            "on_ground": {"value": perf.on_ground, "network_avg": round(net_og, 1), "grade": grade(perf.on_ground, net_og)},
        },
    }


@router.get("/trends")
def dealer_trends(user=Depends(get_current_user), db: Session = Depends(get_db)):
    """Historical monthly trends for the dealer."""
    df = _dealer_filter(user)
    q = db.query(MonthlySnapshot)
    if df:
        q = q.filter(MonthlySnapshot.dealer.ilike(f"%{df}%"))
    rows = q.order_by(MonthlySnapshot.month).all()
    return [{"month": r.month, "dealer": r.dealer, "sales": r.sales, "handovers": r.handovers, "on_ground": r.on_ground, "leads": r.leads, "test_drives": r.test_drives, "won": r.won} for r in rows]


@router.get("/leaderboard")
def regional_leaderboard(user=Depends(get_current_user), db: Session = Depends(get_db)):
    """Regional ranking showing position without exact numbers for other dealers."""
    df = _dealer_filter(user)
    if not df:
        return {"rank": 0, "total": 0, "market": ""}

    perf = db.query(DealerPerformance).filter(DealerPerformance.dealer.ilike(f"%{df}%")).first()
    if not perf:
        return {"rank": 0, "total": 0, "market": ""}

    market_dealers = db.query(DealerPerformance).filter(DealerPerformance.market == perf.market).order_by(DealerPerformance.handovers.desc()).all()
    rank = next((i + 1 for i, d in enumerate(market_dealers) if d.dealer == perf.dealer), 0)

    return {
        "rank": rank,
        "total": len(market_dealers),
        "market": perf.market,
        "your_handovers": perf.handovers,
        "market_leader_handovers": market_dealers[0].handovers if market_dealers else 0,
        "market_avg_handovers": round(sum(d.handovers for d in market_dealers) / len(market_dealers)) if market_dealers else 0,
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
