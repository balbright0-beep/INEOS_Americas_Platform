"""Simplified Master File processor for dealer-relevant data extraction."""
import io
import os
from datetime import datetime, timedelta

import msoffcrypto
from pyxlsb import open_workbook
from sqlalchemy.orm import Session

from app.models import Vehicle, RetailSale, DealerPerformance, RegionalSales


def vi(x):
    if x is None: return 0
    try: return int(float(x))
    except: return 0


def vf(x):
    if x is None: return 0.0
    try: return float(x)
    except: return 0.0


def ss(x):
    return str(x).strip() if x else ""


def serial_to_date(s):
    if not s: return None
    try: return datetime(1899, 12, 30) + timedelta(days=int(float(s)))
    except: return None


def clean_dealer(name):
    d = ss(name)
    for rm in [" INEOS Grenadier", " INEOS GRENADIER", " INEOS", " Grenadier", " GRENADIER"]:
        d = d.replace(rm, "")
    return d.strip()


def export_dealer(r):
    bt = ss(r[58]).strip() if len(r) > 58 and r[58] else ""
    if bt and bt != "Not Handed Over":
        d = bt
    else:
        d = ss(r[0])
    return clean_dealer(d)


def process_master_file(file_bytes: bytes, db: Session):
    """Process Master File and store dealer data in database."""
    # Decrypt
    office_file = msoffcrypto.OfficeFile(io.BytesIO(file_bytes))
    office_file.load_key(password="INEOS26")
    buf = io.BytesIO()
    office_file.decrypt(buf)
    buf.seek(0)

    tmp_path = "/tmp/master_decrypted.xlsb"
    with open(tmp_path, "wb") as f:
        f.write(buf.getvalue())

    wb = open_workbook(tmp_path)

    # Build market map from RBM Assignments
    mkt_map = {}
    try:
        with wb.get_sheet("RBM Assignments") as sheet:
            for i, row in enumerate(sheet.rows()):
                if i < 2: continue
                vals = [c.v for c in row]
                dealer = clean_dealer(vals[1] if len(vals) > 1 else "")
                mkt = ss(vals[3] if len(vals) > 3 else "")
                if dealer and mkt:
                    mkt_map[dealer.upper()] = mkt
    except:
        pass

    # Clear existing data
    db.query(Vehicle).delete()
    db.query(RetailSale).delete()
    db.query(DealerPerformance).delete()
    db.query(RegionalSales).delete()

    # === EXTRACT VEHICLES (Export sheet) ===
    export_rows = []
    try:
        with wb.get_sheet("Export") as sheet:
            for i, row in enumerate(sheet.rows()):
                vals = [c.v for c in row]
                if i <= 1: continue
                country = ss(vals[6]) if len(vals) > 6 else ""
                if country not in ("United States", "Canada", "US", "CA"):
                    continue
                export_rows.append(vals)
    except:
        pass

    now = datetime.now()
    cur_month = now.strftime("%Y-%m")

    for r in export_rows:
        dealer = export_dealer(r)
        vin = ss(r[3]) if len(r) > 3 else ""
        if not vin or len(vin) < 5:
            continue

        mkt = mkt_map.get(dealer.upper(), "")
        body_raw = ss(r[7]) if len(r) > 7 else ""
        body = "QM" if "quarter" in body_raw.lower() else "SVO" if "svo" in body_raw.lower() else "SW"

        my_raw = ss(r[7])
        my = ""
        for y in ("27", "26", "25", "24"):
            if y in my_raw:
                my = f"MY{y}"
                break

        # Status mapping
        status_raw = ss(r[13]) if len(r) > 13 else ""
        channel = ss(r[14]) if len(r) > 14 else ""
        ho_serial = r[51] if len(r) > 51 else None
        ho_date = serial_to_date(ho_serial)
        ho_str = ho_date.strftime("%Y-%m-%d") if ho_date else ""

        # Determine status
        if channel in ("STOCK", "PRIVATE - RETAILER") and not ho_date:
            status = "Dealer Stock"
        elif ho_date:
            status = "Sold"
        else:
            status = ss(r[13]) if len(r) > 13 else "Unknown"

        eta_serial = r[50] if len(r) > 50 else None
        eta_date = serial_to_date(eta_serial)

        v = Vehicle(
            vin=vin, dealer=dealer, market=mkt, country=ss(r[6]) if len(r) > 6 else "",
            body=body, model_year=my, status=status, msrp=vi(r[18]) if len(r) > 18 else 0,
            trim=ss(r[19]) if len(r) > 19 else "", ext_color=ss(r[21]) if len(r) > 21 else "",
            int_color=ss(r[22]) if len(r) > 22 else "", roof=ss(r[23]) if len(r) > 23 else "",
            wheels=ss(r[25]) if len(r) > 25 else "", channel=channel,
            plant=ss(r[15]) if len(r) > 15 else "",
            handover_date=ho_str, eta=eta_date.strftime("%Y-%m-%d") if eta_date else "",
            vessel=ss(r[52])[:20] if len(r) > 52 else "",
            days_on_lot=vi(r[56]) if len(r) > 56 else 0,
            so_number=ss(r[57]) if len(r) > 57 else "",
        )
        db.add(v)

        # Retail sale if handover in current month
        if ho_str.startswith(cur_month):
            rs = RetailSale(
                dealer=dealer, market=mkt, vin=vin[-6:], vin_full=vin,
                body=body, model_year=my, trim=ss(r[19]) if len(r) > 19 else "",
                ext_color=ss(r[21]) if len(r) > 21 else "",
                int_color=ss(r[22]) if len(r) > 22 else "",
                wheels=ss(r[25]) if len(r) > 25 else "", channel=channel,
                msrp=vi(r[18]) if len(r) > 18 else 0,
                days_to_sell=vi(r[56]) if len(r) > 56 else 0,
                cvp=ss(r[62]) if len(r) > 62 else "",
                handover_date=ho_str,
            )
            db.add(rs)

    # === EXTRACT DEALER PERFORMANCE ===
    try:
        with wb.get_sheet("Dealer Performance Dashboard") as sheet:
            for i, row in enumerate(sheet.rows()):
                if i < 2: continue
                vals = [c.v for c in row]
                dealer = clean_dealer(vals[1] if len(vals) > 1 else "")
                if not dealer: continue
                dp = DealerPerformance(
                    dealer=dealer, market=ss(vals[0]),
                    handovers=vi(vals[2]), cvp=vi(vals[3]), wholesales=vi(vals[4]),
                    on_ground=vi(vals[6]), dealer_stock=vi(vals[7]),
                    leads=vi(vals[17]) if len(vals) > 17 else 0,
                    test_drives=vi(vals[19]) if len(vals) > 19 else 0,
                    td_completed=vi(vals[21]) if len(vals) > 21 else 0,
                    td_show_pct=f"{vf(vals[10])*100:.1f}" if len(vals) > 10 and vals[10] else "0",
                    lead_to_td_pct=f"{vf(vals[11])*100:.1f}" if len(vals) > 11 and vals[11] else "0",
                    won=vi(vals[12]) if len(vals) > 12 else 0,
                    lost=vi(vals[13]) if len(vals) > 13 else 0,
                    mb30=f"{vf(vals[23])*100:.1f}" if len(vals) > 23 and vals[23] else "0",
                    mb60=f"{vf(vals[24])*100:.1f}" if len(vals) > 24 and vals[24] else "0",
                    mb90=f"{vf(vals[25])*100:.1f}" if len(vals) > 25 and vals[25] else "0",
                )
                db.add(dp)
    except:
        pass

    # === EXTRACT REGIONAL SALES ===
    try:
        with wb.get_sheet("Retail Sales Report") as sheet:
            for i, row in enumerate(sheet.rows()):
                if i < 5 or i > 13: continue
                vals = [c.v for c in row]
                region = ss(vals[2]) if len(vals) > 2 else ""
                if not region: continue
                rs = RegionalSales(
                    region=region, sw=vi(vals[3]), qm=vi(vals[4]), svo=vi(vals[5]),
                    total=vi(vals[6]), objective=vi(vals[7]),
                    pct_objective=f"{vf(vals[8])*100:.1f}" if len(vals) > 8 and vals[8] else "0",
                    cvp=vi(vals[15]) if len(vals) > 15 else 0,
                )
                db.add(rs)
    except:
        pass

    db.commit()

    # Cleanup
    try:
        os.remove(tmp_path)
    except:
        pass

    counts = {
        "vehicles": db.query(Vehicle).count(),
        "retail_sales": db.query(RetailSale).count(),
        "dealer_performance": db.query(DealerPerformance).count(),
        "regional_sales": db.query(RegionalSales).count(),
    }
    return counts
