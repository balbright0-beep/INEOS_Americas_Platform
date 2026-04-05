"""Master File processor — column indices aligned with dashboard_refresh_all_in_one.py"""
import io
import os
import tempfile
from datetime import datetime, timedelta
from collections import defaultdict

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
    """Convert Excel serial date to datetime (same formula as Dashboard)."""
    if not s: return None
    try:
        return datetime(1899, 12, 30) + timedelta(days=int(float(s)))
    except:
        return None


def clean_dealer(name):
    d = ss(name)
    d = d.replace(" INEOS Grenadier", "").replace(" INEOS GRENADIER", "")
    d = d.replace(" INEOS", "").replace(" Grenadier", "").replace(" GRENADIER", "")
    d = " ".join(w for w in d.split() if w.upper() != "GRENADIER")
    return d.strip()


def export_dealer(r):
    """Same logic as Dashboard — col 58 (Bill To) if available, else col 0."""
    bt = ss(r[58]).strip() if len(r) > 58 and r[58] else ""
    if bt and bt != "Not Handed Over":
        d = bt
    else:
        d = ss(r[0])
    return clean_dealer(d)


def process_master_file(file_bytes: bytes, db: Session):
    """Process Master File — column indices match Dashboard exactly."""
    errors = []

    # Decrypt
    office_file = msoffcrypto.OfficeFile(io.BytesIO(file_bytes))
    office_file.load_key(password="INEOS26")
    buf = io.BytesIO()
    office_file.decrypt(buf)
    buf.seek(0)

    tmp = tempfile.NamedTemporaryFile(suffix=".xlsb", delete=False)
    try:
        tmp.write(buf.getvalue())
        tmp.close()
        wb = open_workbook(tmp.name)

        # === BUILD MARKET MAP (same as Dashboard: row 5+, dealer=col3, market=col5) ===
        mkt_map = {}
        try:
            rows = []
            with wb.get_sheet("RBM Assignments") as sheet:
                for row in sheet.rows():
                    rows.append([c.v for c in row])
            for r in rows[5:]:
                if r and len(r) > 5 and r[3] and r[5]:
                    name = ss(r[3]).replace(" INEOS Grenadier", "").replace(" INEOS", "").strip()
                    market = ss(r[5])
                    mkt_map[name] = market
                    mkt_map[name.upper()] = market
            # Extras (same as Dashboard)
            extras = {
                "Mossy SD": "Western", "MOSSY SD": "Western",
                "Mossy TX": "Central", "MOSSY TX": "Central",
                "RTGT": "Western", "Crown Dublin": "Northeast", "CROWN DUBLIN": "Northeast",
                "Sewell SA": "Central", "SEWELL SAN ANTONIO": "Central",
                "DILAWRI": "Canada", "WEISSACH": "Canada", "CALGARY": "Canada",
            }
            mkt_map.update(extras)
        except Exception as e:
            errors.append(f"RBM Assignments: {e}")

        def lookup_mkt(dealer_name):
            n = dealer_name.strip()
            if n in mkt_map: return mkt_map[n]
            if n.upper() in mkt_map: return mkt_map[n.upper()]
            for k, v in mkt_map.items():
                if n.upper() in k.upper() or k.upper() in n.upper():
                    return v
            return ""

        # Clear existing data and commit
        db.query(Vehicle).delete()
        db.query(RetailSale).delete()
        db.query(DealerPerformance).delete()
        db.query(RegionalSales).delete()
        db.commit()

        # === EXTRACT VEHICLES (Export sheet) ===
        # Column indices aligned with Dashboard:
        # [0]=Customer, [7]=Material, [8]=VIN, [11]=Country, [13]=Status, [14]=Channel
        # [18]=MSRP, [19]=Trim, [21]=ExtColor, [22]=IntColor, [23]=Roof, [25]=Wheels
        # [50]=Plant, [51]=Handover, [52]=ETA, [53]=Vessel, [56]=DaysOnLot/DIS, [57]=SO#, [58]=BillTo
        export_rows = []
        try:
            with wb.get_sheet("Export") as sheet:
                for i, row in enumerate(sheet.rows()):
                    vals = [c.v for c in row]
                    if i <= 1: continue
                    country = ss(vals[11]) if len(vals) > 11 else ""
                    cu = country.upper()
                    if "UNITED STATES" not in cu and "CANADA" not in cu:
                        continue
                    export_rows.append(vals)
        except Exception as e:
            errors.append(f"Export sheet: {e}")

        now = datetime.now()
        cur_month = now.strftime("%Y-%m")
        vehicle_count = 0

        for r in export_rows:
            dealer = export_dealer(r).upper()  # Normalize uppercase
            vin = ss(r[8]) if len(r) > 8 else ""  # Col 8 = VIN
            if not vin or len(vin) < 5:
                continue

            mkt = lookup_mkt(dealer)

            material = ss(r[7]) if len(r) > 7 else ""
            material_upper = material.upper()
            body = "SVO" if "SVO" in material_upper else "QM" if "QUARTERMASTER" in material_upper else "SW"

            my = ""
            for y in ("27", "26", "25", "24"):
                if y in material:
                    my = f"MY{y}"
                    break

            channel = ss(r[14]) if len(r) > 14 else ""
            ho_date = serial_to_date(r[51]) if len(r) > 51 else None
            ho_str = ho_date.strftime("%Y-%m-%d") if ho_date else ""

            if channel in ("STOCK", "PRIVATE - RETAILER") and not ho_date:
                status = "Dealer Stock"
            elif ho_date:
                status = "Sold"
            else:
                status = ss(r[13]) if len(r) > 13 else "Unknown"

            eta_date = serial_to_date(r[52]) if len(r) > 52 else None  # Col 52 = ETA

            v = Vehicle(
                vin=vin, dealer=dealer, market=mkt,
                country=ss(r[11]) if len(r) > 11 else "",  # Col 11 = Country
                body=body, model_year=my, status=status,
                msrp=vi(r[18]) if len(r) > 18 else 0,
                trim=ss(r[19]) if len(r) > 19 else "",
                ext_color=ss(r[21]) if len(r) > 21 else "",
                int_color=ss(r[22]) if len(r) > 22 else "",
                roof=ss(r[23]) if len(r) > 23 else "",
                wheels=ss(r[25]) if len(r) > 25 else "",
                channel=channel,
                plant=ss(r[50]) if len(r) > 50 else "",  # Col 50 = Plant
                handover_date=ho_str,
                eta=eta_date.strftime("%Y-%m-%d") if eta_date else "",
                vessel=ss(r[53]) if len(r) > 53 else "",  # Col 53 = Vessel
                days_on_lot=vi(r[56]) if len(r) > 56 else 0,
                so_number=ss(r[57]) if len(r) > 57 else "",
            )
            db.add(v)
            vehicle_count += 1

            if ho_str.startswith(cur_month):
                rs = RetailSale(
                    dealer=dealer, market=mkt, vin=vin[-6:], vin_full=vin,
                    body=body, model_year=my,
                    trim=ss(r[19]) if len(r) > 19 else "",
                    ext_color=ss(r[21]) if len(r) > 21 else "",
                    int_color=ss(r[22]) if len(r) > 22 else "",
                    wheels=ss(r[25]) if len(r) > 25 else "",
                    channel=channel,
                    msrp=vi(r[18]) if len(r) > 18 else 0,
                    days_to_sell=vi(r[56]) if len(r) > 56 else 0,
                    cvp=ss(r[62]) if len(r) > 62 else "",
                    handover_date=ho_str,
                )
                db.add(rs)

        # === DEALER PERFORMANCE (same columns as Dashboard build_DPD) ===
        perf_count = 0
        try:
            with wb.get_sheet("Dealer Performance Dashboard") as sheet:
                for i, row in enumerate(sheet.rows()):
                    if i < 2: continue
                    vals = [c.v for c in row]
                    dealer = clean_dealer(ss(vals[1]) if len(vals) > 1 else "")
                    if not dealer: continue
                    dp = DealerPerformance(
                        dealer=dealer, market=ss(vals[0]) if len(vals) > 0 else "",
                        handovers=vi(vals[2]) if len(vals) > 2 else 0,
                        cvp=vi(vals[3]) if len(vals) > 3 else 0,
                        wholesales=vi(vals[4]) if len(vals) > 4 else 0,
                        on_ground=vi(vals[6]) if len(vals) > 6 else 0,
                        dealer_stock=vi(vals[7]) if len(vals) > 7 else 0,
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
                    perf_count += 1
        except Exception as e:
            errors.append(f"Dealer Performance: {e}")

        # === REGIONAL SALES ===
        regional_count = 0
        try:
            with wb.get_sheet("Retail Sales Report") as sheet:
                for i, row in enumerate(sheet.rows()):
                    if i < 5 or i > 13: continue
                    vals = [c.v for c in row]
                    region = ss(vals[2]) if len(vals) > 2 else ""
                    if not region: continue
                    rs = RegionalSales(
                        region=region,
                        sw=vi(vals[3]) if len(vals) > 3 else 0,
                        qm=vi(vals[4]) if len(vals) > 4 else 0,
                        svo=vi(vals[5]) if len(vals) > 5 else 0,
                        total=vi(vals[6]) if len(vals) > 6 else 0,
                        objective=vi(vals[7]) if len(vals) > 7 else 0,
                        pct_objective=f"{vf(vals[8])*100:.1f}" if len(vals) > 8 and vals[8] else "0",
                        cvp=vi(vals[15]) if len(vals) > 15 else 0,
                    )
                    db.add(rs)
                    regional_count += 1
        except Exception as e:
            errors.append(f"Regional Sales: {e}")

        db.commit()

    finally:
        try:
            os.unlink(tmp.name)
        except:
            pass

    return {
        "vehicles": vehicle_count,
        "retail_sales": db.query(RetailSale).count(),
        "dealer_performance": perf_count,
        "regional_sales": regional_count,
        "errors": errors,
    }
