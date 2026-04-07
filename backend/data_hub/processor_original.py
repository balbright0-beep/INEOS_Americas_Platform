#!/usr/bin/env python3
"""All-in-one INEOS Americas dashboard refresh.

Combines the logic from:
- refresh_dashboard.py
- refresh_pass3.py
- refresh_pass4.py

Notes:
- Uses the compact VEX/TR builders from pass 4.
- Preserves pass 3 additions such as SC_DATA, MIG, MIG_INV, and PL_AGE.
- Handles workbook decryption like refresh_dashboard.py.
"""

import io
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta

import msoffcrypto
from pyxlsb import open_workbook


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def vi(x):
    if x is None:
        return 0
    try:
        return int(float(x))
    except Exception:
        return 0


def vf(x):
    if x is None:
        return 0.0
    try:
        return float(x)
    except Exception:
        return 0.0


def safe_str(x):
    return str(x).strip() if x else ""


def export_dealer(r):
    """Get the transacting dealer from an Export row.
    Uses col 58 (Handover Bill to Dealer) if available, falls back to col 0."""
    bt = safe_str(r[58]).strip() if len(r) > 58 and r[58] else ""
    if bt and bt != "Not Handed Over":
        d = bt
    else:
        d = safe_str(r[0])
    d = d.replace(" INEOS Grenadier", "").replace(" INEOS GRENADIER", "")
    d = d.replace(" INEOS", "").replace(" Grenadier", "").replace(" GRENADIER", "").strip()
    d = " ".join(w for w in d.split() if w.upper() != "GRENADIER")
    return d


def serial_to_date(s):
    if not s:
        return None
    try:
        return datetime(1899, 12, 30) + timedelta(days=int(float(s)))
    except Exception:
        return None


DEFAULT_BASE_DIR = r"C:\Users\bxa68077\Documents\INEOS_Dashboard"
DEFAULT_MASTER_PATH = os.path.join(DEFAULT_BASE_DIR, "Master File V14 binary.xlsb")
DEFAULT_TEMPLATE_PATH = os.path.join(DEFAULT_BASE_DIR, "Americas_Daily_Dashboard.html")
DEFAULT_OUTPUT_DIR = r"C:\Users\bxa68077\Documents\INEOS_Dashboard_Output"
DEFAULT_OUTPUT_PATH = os.path.join(DEFAULT_OUTPUT_DIR, "Americas_Daily_Dashboard_refreshed.html")
DEFAULT_DECRYPTED_PATH = os.path.join(DEFAULT_BASE_DIR, "master_decrypted.xlsb")


def decrypt_master(path, pw="INEOS26", output_path=DEFAULT_DECRYPTED_PATH):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(path, "rb") as f:
        office_file = msoffcrypto.OfficeFile(f)
        office_file.load_key(password=pw)
        buf = io.BytesIO()
        office_file.decrypt(buf)
        buf.seek(0)
        with open(output_path, "wb") as out:
            out.write(buf.getvalue())
    return output_path


def read_sheet(wb, name, max_rows=99999):
    rows = []
    with wb.get_sheet(name) as sheet:
        for i, row in enumerate(sheet.rows()):
            if i >= max_rows:
                break
            rows.append([c.v for c in row])
    return rows


def load_export_rows(wb):
    rows = []
    headers = None
    with wb.get_sheet("Export") as sheet:
        for i, row in enumerate(sheet.rows()):
            vals = [c.v for c in row]
            if i == 0:
                continue
            if i == 1:
                headers = vals
                continue
            rows.append(vals)
    return rows, headers


def replace_const(html, name, data):
    pattern = rf"(const {name}=).*?;"
    replacement = f"const {name}={json.dumps(data, separators=(',', ':'))};"
    html2, count = re.subn(pattern, replacement.replace("\\", "\\\\"), html, count=1, flags=re.DOTALL)
    if count == 0:
        print(f"  WARNING: {name} not found in HTML")
    else:
        print(f"  {name}: replaced ({len(replacement):,} chars)")
    return html2


# ---------------------------------------------------------------------------
# Market mapping
# ---------------------------------------------------------------------------

def build_mkt_map(wb):
    rows = []
    with wb.get_sheet("RBM Assignments") as sheet:
        for row in sheet.rows():
            rows.append([c.v for c in row])

    market_map = {}
    for r in rows[5:]:
        if r and len(r) > 5 and r[3] and r[5]:
            name = safe_str(r[3]).replace(" INEOS Grenadier", "").replace(" INEOS", "").strip()
            market = safe_str(r[5])
            market_map[name] = market
            market_map[name.upper()] = market

    extras = {
        "Mossy SD": "Western",
        "MOSSY SD": "Western",
        "Mossy TX": "Central",
        "MOSSY TX": "Central",
        "RTGT": "Western",
        "Crown Dublin": "Northeast",
        "CROWN DUBLIN": "Northeast",
        "Sewell SA": "Central",
        "SEWELL SAN ANTONIO": "Central",
        "Herrera": "Mexico",
        "HERRERA": "Mexico",
        "Herrera Premium de Mexico SA de CV": "Mexico",
        "DILAWRI": "Canada",
        "WEISSACH": "Canada",
        "CALGARY": "Canada",
        "Orlando": "Southeast",
        "ORLANDO": "Southeast",
        "Roseville": "Western",
        "ROSEVILLE": "Western",
        "Mossy San Diego": "Western",
        "MOSSY SAN DIEGO": "Western",
        "Montreal": "Canada",
        "MONTREAL": "Canada",
        "Uptown Toronto": "Canada",
        "UPTOWN TORONTO": "Canada",
    }
    market_map.update(extras)
    return market_map


def lookup_mkt(market_map, name):
    n = name.strip()
    if n in market_map:
        return market_map[n]
    if n.upper() in market_map:
        return market_map[n.upper()]
    for k, v in market_map.items():
        if n.upper() in k.upper() or k.upper() in n.upper():
            return v
    return ""


# ---------------------------------------------------------------------------
# Base dashboard builders (from refresh_dashboard.py)
# ---------------------------------------------------------------------------

def build_RS(wb):
    rows = read_sheet(wb, "Retail Sales Report", 15)
    rs = []
    for i in range(6, 14):
        r = rows[i] if i < len(rows) else [None] * 20
        region = safe_str(r[2])
        if not region:
            continue
        sw = vi(r[3])
        qm = vi(r[4])
        svo = vi(r[5])
        total = vi(r[6])
        obj = vi(r[7])
        po = round(vf(r[8]) * 100, 1) if r[8] else 0
        mx = round(vf(r[9]) * 100, 1) if r[9] else 0
        cvp = vi(r[15]) if len(r) > 15 else 0
        rs.append({
            "r": region,
            "sw": sw,
            "qm": qm,
            "svo": svo,
            "t": total,
            "obj": obj,
            "po": po,
            "mx": mx,
            "cvp": cvp,
        })
    # Fix Total row CVP: sum from individual regions if Total has 0
    tot_row = next((r for r in rs if r["r"] == "Total"), None)
    if tot_row and tot_row["cvp"] == 0:
        tot_row["cvp"] = sum(r["cvp"] for r in rs if r["r"] != "Total")
    return rs


def build_DPD(wb):
    rows = read_sheet(wb, "Dealer Performance Dashboard", 50)
    dpd = []
    totals = defaultdict(int)

    for i in range(3, len(rows)):
        r = rows[i]
        if not r or not r[0]:
            continue
        market = safe_str(r[0])
        dealer = safe_str(r[1])
        if not market or not dealer:
            continue

        dealer = dealer.replace(" INEOS Grenadier", "").replace(" INEOS", "").strip()
        ho = vi(r[2])
        cvp = vi(r[3])
        ws = vi(r[4])
        gap = safe_str(r[5]) if r[5] else "1.00:1"
        og = vi(r[6])
        ds = vi(r[7])
        dsc = vi(r[8])
        sep = vi(r[9])
        oct_ = vi(r[10])
        nov = vi(r[11])
        dec_ = vi(r[12])
        jan = vi(r[13])
        feb = vi(r[14])
        r3 = round(vf(r[15]), 1)
        r3t = round(vf(r[16]), 1)
        rl = vi(r[17])
        rlt = vi(r[18])
        td = vi(r[19])
        tdt = vi(r[20])
        tdw = vi(r[21])
        tdp = vi(r[22])
        mb30 = round(vf(r[23]) * 100, 1) if r[23] else 0
        mb60 = round(vf(r[24]) * 100, 1) if r[24] else 0
        mb90 = round(vf(r[25]) * 100, 1) if r[25] else 0
        mbat = round(vf(r[26]) * 100, 1) if r[26] else 0

        dpd.append({
            "m": market,
            "d": dealer,
            "ho": ho,
            "cvp": cvp,
            "ws": ws,
            "gap": gap,
            "og": og,
            "ds": ds,
            "dsc": dsc,
            "sep": sep,
            "oct": oct_,
            "nov": nov,
            "dec": dec_,
            "jan": jan,
            "feb": feb,
            "r3": r3,
            "r3t": r3t,
            "rl": rl,
            "rlt": rlt,
            "td": td,
            "tdt": tdt,
            "tdw": tdw,
            "tdp": tdp,
            "mb30": mb30,
            "mb60": mb60,
            "mb90": mb90,
            "mbat": mbat,
            "tds": 0,
        })

        totals["ho"] += ho
        totals["cvp"] += cvp
        totals["ws"] += ws
        totals["sep"] += sep
        totals["oct"] += oct_
        totals["nov"] += nov
        totals["dec"] += dec_
        totals["jan"] += jan
        totals["feb"] += feb
        totals["rl"] += rl
        totals["td"] += td
        totals["tdw"] += tdw
        totals["tdp"] += tdp

    totals["r3"] = round(sum(x["r3"] for x in dpd), 1)
    dpd.append({
        "m": "TOTAL",
        "d": "TOTAL",
        "ho": totals["ho"],
        "cvp": totals["cvp"],
        "ws": totals["ho"] + totals["cvp"],
        "og": 0,
        "sep": totals["sep"],
        "oct": totals["oct"],
        "nov": totals["nov"],
        "dec": totals["dec"],
        "jan": totals["jan"],
        "feb": totals["feb"],
        "rl": totals["rl"],
        "td": totals["td"],
        "tdw": totals["tdw"],
        "tdp": totals["tdp"],
        "r3": totals["r3"],
        "r3t": 0,
        "gap": "",
        "ds": 0,
        "dsc": 0,
        "rlt": 0,
        "tdt": 0,
        "mb30": 0,
        "mb60": 0,
        "mb90": 0,
        "mbat": 0,
        "tds": 0,
    })
    return dpd


def build_INV(wb, market_map):
    rows = read_sheet(wb, "Dealer Inventory Report", 100)

    inv_all, inv_24, inv_25, inv_26 = [], [], [], []

    for i in range(3, len(rows)):
        r = rows[i]
        if not r or len(r) < 40 or not r[2]:
            continue

        name = safe_str(r[2]).replace(" INEOS Grenadier", "").replace(" INEOS", "").strip()
        if not name or "total" in name.lower() or "region" in name.lower() or name == "Retail Partner Name":
            continue

        market = lookup_mkt(market_map, name)

        ogS = vi(r[11])
        ogQ = vi(r[12])
        my25S = vi(r[5])
        my25Q = vi(r[6])
        my26S = vi(r[7])
        my26Q = vi(r[8])
        mtdS = vi(r[21])
        mtdQ = vi(r[22])
        pmS = vi(r[23])
        pmQ = vi(r[24])
        r90S = round(vf(r[25]), 1)
        r90Q = round(vf(r[26]), 1)
        dsS = round(vf(r[31]), 1) if r[31] else 0
        dsQ = round(vf(r[32]), 1) if r[32] else 0
        itS = vi(r[33])
        itQ = vi(r[34])
        apS = vi(r[35])
        apQ = vi(r[36])
        owS = vi(r[37])
        owQ = vi(r[38])
        plS = vi(r[39])
        plQ = vi(r[40]) if len(r) > 40 else 0

        rec = {
            "n": name,
            "m": market,
            "ogS": ogS,
            "ogQ": ogQ,
            "my25S": my25S,
            "my25Q": my25Q,
            "my26S": my26S,
            "my26Q": my26Q,
            "mtdS": mtdS,
            "mtdQ": mtdQ,
            "pmS": pmS,
            "pmQ": pmQ,
            "r90S": r90S,
            "r90Q": r90Q,
            "dsS": dsS,
            "dsQ": dsQ,
            "itS": itS,
            "itQ": itQ,
            "apS": apS,
            "apQ": apQ,
            "owS": owS,
            "owQ": owQ,
            "plS": plS,
            "plQ": plQ,
        }
        inv_all.append(rec)

        my24S = vi(r[3])
        my24Q = vi(r[4])
        s13 = vi(r[13])
        q14 = vi(r[14])
        s15 = vi(r[15])
        q16 = vi(r[16])
        s17 = vi(r[17])
        q18 = vi(r[18])

        if my25S > 0 or my25Q > 0:
            inv_25.append({
                "n": name,
                "m": market,
                "ogS": my25S,
                "ogQ": my25Q,
                "mtdS": s15,
                "mtdQ": q16,
                "pmS": 0,
                "pmQ": 0,
                "r90S": 0,
                "r90Q": 0,
                "dsS": 0,
                "dsQ": 0,
                "itS": 0,
                "itQ": 0,
                "apS": 0,
                "apQ": 0,
                "owS": 0,
                "owQ": 0,
                "plS": 0,
                "plQ": 0,
            })
        if my26S > 0 or my26Q > 0:
            inv_26.append({
                "n": name,
                "m": market,
                "ogS": my26S,
                "ogQ": my26Q,
                "mtdS": s17,
                "mtdQ": q18,
                "pmS": 0,
                "pmQ": 0,
                "r90S": 0,
                "r90Q": 0,
                "dsS": 0,
                "dsQ": 0,
                "itS": 0,
                "itQ": 0,
                "apS": 0,
                "apQ": 0,
                "owS": 0,
                "owQ": 0,
                "plS": 0,
                "plQ": 0,
            })
        if my24S > 0 or my24Q > 0:
            inv_24.append({
                "n": name,
                "m": market,
                "ogS": my24S,
                "ogQ": my24Q,
                "mtdS": s13,
                "mtdQ": q14,
                "pmS": 0,
                "pmQ": 0,
                "r90S": 0,
                "r90Q": 0,
                "dsS": 0,
                "dsQ": 0,
                "itS": 0,
                "itQ": 0,
                "apS": 0,
                "apQ": 0,
                "owS": 0,
                "owQ": 0,
                "plS": 0,
                "plQ": 0,
            })

    return inv_all, inv_24, inv_25, inv_26


def build_HIST(wb):
    rows = read_sheet(wb, "Historical Sales", 15)
    date_row = rows[1] if len(rows) > 1 else []
    months = []
    for j in range(2, len(date_row)):
        d = serial_to_date(date_row[j])
        if d:
            months.append(d.strftime("%Y-%m"))
        else:
            break

    def get_vals(row_idx):
        if row_idx >= len(rows):
            return [0] * len(months)
        r = rows[row_idx]
        return [vi(r[j]) if j < len(r) else 0 for j in range(2, 2 + len(months))]

    retail = get_vals(2)
    sw = get_vals(3)
    qm = get_vals(4)
    svo = get_vals(5)
    wholesale = get_vals(10)
    wsSW = get_vals(11)
    wsQM = get_vals(12)

    return {
        "months": months,
        "retail": retail,
        "wholesale": wholesale,
        "sw": sw,
        "qm": qm,
        "svo": svo,
        "wsSW": wsSW,
        "wsQM": wsQM,
    }


def build_HD(wb):
    """Build the HD (Historical Dealer) data from the 'Historical Sales' sheet."""
    rows = read_sheet(wb, "Historical Sales", 110)
    if len(rows) < 70:
        return None

    # --- parse months from row 1 (index 1) ---
    date_row = rows[1] if len(rows) > 1 else []
    all_months = []
    for j in range(2, len(date_row)):
        d = serial_to_date(date_row[j])
        if d:
            all_months.append((j, d.strftime("%Y-%m")))
        else:
            break

    # --- total retail (row 2 = index 2) and total wholesale (row 69 = index 69) ---
    total_r_row = rows[2] if len(rows) > 2 else []
    total_w_row = rows[69] if len(rows) > 69 else []

    # filter months where both totals are 0
    keep = []
    for j, ym in all_months:
        tr = vi(total_r_row[j]) if j < len(total_r_row) else 0
        tw = vi(total_w_row[j]) if j < len(total_w_row) else 0
        if tr != 0 or tw != 0:
            keep.append((j, ym))

    months = [ym for _, ym in keep]
    col_indices = [j for j, _ in keep]

    if not months:
        return None

    def extract_vals(row):
        return [vi(row[j]) if j < len(row) else 0 for j in col_indices]

    # --- all-network totals ---
    all_retail = extract_vals(total_r_row)
    all_wholesale = extract_vals(total_w_row)

    SUFFIX = " INEOS Grenadier"

    dealers = {}   # dealer_key -> {"m": market, "r": [...], "w": [...]}
    markets = {}   # market_name -> {"r": [...], "w": [...]}

    def process_dealer_rows(start, end, key):
        """key is 'r' or 'w'."""
        for i in range(start, min(end, len(rows))):
            row = rows[i]
            if not row or len(row) < 2:
                continue
            market = safe_str(row[0])
            raw_name = safe_str(row[1])
            if not raw_name:
                continue
            # strip suffix and uppercase
            if raw_name.endswith(SUFFIX):
                raw_name = raw_name[: -len(SUFFIX)]
            dealer_key = raw_name.strip().upper()
            if not dealer_key:
                continue

            vals = extract_vals(row)

            if dealer_key not in dealers:
                dealers[dealer_key] = {"m": market, "r": [0] * len(months), "w": [0] * len(months)}
            dealers[dealer_key][key] = vals
            # keep the market from the first occurrence
            if not dealers[dealer_key]["m"]:
                dealers[dealer_key]["m"] = market

            # aggregate markets
            if market:
                if market not in markets:
                    markets[market] = {"r": [0] * len(months), "w": [0] * len(months)}
                for idx, v in enumerate(vals):
                    markets[market][key][idx] += v

    # rows 30-68 => index 29-67 (retail), rows 70-102 => index 69-101 (wholesale)
    process_dealer_rows(29, 68, "r")
    process_dealer_rows(69, 102, "w")

    return {
        "months": months,
        "all": {"r": all_retail, "w": all_wholesale},
        "markets": markets,
        "dealers": dealers,
    }


def build_OBJ(wb):
    rows = read_sheet(wb, "Objectives", 15)
    obj = {}
    # Row 2=US, 4=Retailer, 5=Rental, 6=Fleet, 7=IECP, 8=Total
    cat_map = {2: "US", 4: "Retailer", 5: "Rental", 6: "Fleet", 7: "IECP"}
    for row_idx, cat in cat_map.items():
        if row_idx < len(rows):
            r = rows[row_idx]
            vals = [vi(r[j]) if j < len(r) else 0 for j in range(9, 21)]
            obj[cat] = vals
    # Read Total from row 8 instead of copying US
    if 8 < len(rows):
        r = rows[8]
        obj["Total"] = [vi(r[j]) if j < len(r) else 0 for j in range(9, 21)]
    else:
        obj["Total"] = obj.get("US", [0] * 12)
    return obj


def build_SAN(wb):
    today = datetime.now().date()

    def read_daily_sheet(sheet_name):
        rows = read_sheet(wb, sheet_name, 500)
        data = {}
        for r in rows[1:]:
            if not r or r[0] is None:
                continue
            if isinstance(r[0], str):  # second header row — stop
                break
            if not isinstance(r[0], float) or r[0] < 40000:
                continue
            d = serial_to_date(r[0])
            if d and d.date() <= today:
                data[d.strftime("%Y-%m-%d")] = vi(r[1])
        return data

    all_data = read_daily_sheet("App Report MoM")
    fin_data = read_daily_sheet("App Report Finance")
    lease_data = read_daily_sheet("App Report Lease")

    if not all_data:
        return [], [], [], [], {}

    days_list = sorted(all_data.keys())
    all_vol = [all_data[d] for d in days_list]
    fin_vol = [fin_data.get(d, 0) for d in days_list]
    lease_vol = [lease_data.get(d, 0) for d in days_list]

    # Trim leading carryover zeros: start from the first day of the month
    # containing the first non-zero daily volume.
    first_nonzero = next((d for d, v in zip(days_list, all_vol) if v > 0), days_list[0])
    month_start = first_nonzero[:7] + "-01"
    trim = next((i for i, d in enumerate(days_list) if d >= month_start), 0)
    days_list = days_list[trim:]
    all_vol = all_vol[trim:]
    fin_vol = fin_vol[trim:]
    lease_vol = lease_vol[trim:]

    # Build SAN_MO: historical monthly totals from Santander Report pivot,
    # then override only months that have non-zero daily data.
    san_mo = {}
    try:
        mo_rows = read_sheet(wb, "Santander Report ", 50)
        for r in mo_rows[9:]:
            if not r or not r[0] or not isinstance(r[0], float) or r[0] < 40000:
                continue
            d = serial_to_date(r[0])
            if d:
                san_mo[d.strftime("%Y-%m")] = vi(r[1])
    except Exception:
        pass

    daily_mo = defaultdict(int)
    for d, v in zip(days_list, all_vol):
        daily_mo[d[:7]] += v
    # Only override months that have actual recorded volume
    for mo, total in daily_mo.items():
        if total > 0:
            san_mo[mo] = total

    return days_list, all_vol, fin_vol, lease_vol, dict(sorted(san_mo.items()))


def build_LK(wb):
    def parse_lk_sheet(rows, start_row=4):
        data = []
        for r in rows[start_row:]:
            if not r or not r[1]:
                continue
            market = safe_str(r[0])
            dealer = safe_str(r[1]).replace(" INEOS Grenadier", "").replace(" INEOS", "").strip()
            if not dealer or "total" in dealer.lower() or "network" in dealer.lower():
                continue
            rbm = safe_str(r[2])
            data.append({
                "m": market,
                "d": dealer,
                "rbm": rbm,
                "leads": vi(r[3]),
                "cp": vi(r[4]),
                "cpP": round(vf(r[5]) * 100, 1) if r[5] else 0,
                "utc": vi(r[6]),
                "utcP": round(vf(r[7]) * 100, 1) if r[7] else 0,
                "tdB": vi(r[8]),
                "tdC": vi(r[9]),
                "tdSh": round(vf(r[10]) * 100, 1) if r[10] else 0,
                "ltdP": round(vf(r[11]) * 100, 1) if r[11] else 0,
                "won": vi(r[12]),
                "lost": vi(r[13]),
                "lsP": round(vf(r[14]) * 100, 1) if r[14] else 0,
                "mb30": round(vf(r[15]) * 100, 1) if r[15] else 0,
                "mb60": round(vf(r[16]) * 100, 1) if r[16] else 0,
                "mb90": round(vf(r[17]) * 100, 1) if r[17] else 0,
            })
        return data

    def build_net_row(r):
        return {
            "leads": vi(r[3]),
            "cp": vi(r[4]),
            "cpP": round(vf(r[5]) * 100, 1) if r[5] else 0,
            "utc": vi(r[6]),
            "utcP": round(vf(r[7]) * 100, 1) if r[7] else 0,
            "tdB": vi(r[8]),
            "tdC": vi(r[9]),
            "tdSh": round(vf(r[10]) * 100, 1) if r[10] else 0,
            "ltdP": round(vf(r[11]) * 100, 1) if r[11] else 0,
            "won": vi(r[12]),
            "lost": vi(r[13]),
            "lsP": round(vf(r[14]) * 100, 1) if r[14] else 0,
            "mb30": round(vf(r[15]) * 100, 1) if r[15] else 0,
            "mb60": round(vf(r[16]) * 100, 1) if r[16] else 0,
            "mb90": round(vf(r[17]) * 100, 1) if r[17] else 0,
        }

    rows = read_sheet(wb, "Lead Handling KPIs", 200)
    lk_all = parse_lk_sheet(rows, 4)
    net_all = None
    for r in rows[4:]:
        label = (safe_str(r[0]) + " " + safe_str(r[1])).lower()
        if "network" in label:
            net_all = build_net_row(r)
            break
    # Fallback: aggregate from individual dealer rows
    if net_all is None:
        if lk_all:
            tot_leads = sum(d["leads"] for d in lk_all)
            tot_cp = sum(d["cp"] for d in lk_all)
            tot_utc = sum(d["utc"] for d in lk_all)
            tot_tdB = sum(d["tdB"] for d in lk_all)
            tot_tdC = sum(d["tdC"] for d in lk_all)
            tot_won = sum(d["won"] for d in lk_all)
            tot_lost = sum(d["lost"] for d in lk_all)
            # Matchback: weighted average by won count (each d["mb*"] is already a %)
            mb30_w = sum(d["mb30"] * d["won"] for d in lk_all)
            mb60_w = sum(d["mb60"] * d["won"] for d in lk_all)
            mb90_w = sum(d["mb90"] * d["won"] for d in lk_all)
            net_all = {
                "leads": tot_leads,
                "cp": tot_cp,
                "cpP": round(tot_cp / tot_leads * 100, 1) if tot_leads else 0,
                "utc": tot_utc,
                "utcP": round(tot_utc / tot_lost * 100, 1) if tot_lost else 0,
                "tdB": tot_tdB,
                "tdC": tot_tdC,
                "tdSh": round(tot_tdC / tot_tdB * 100, 1) if tot_tdB else 0,
                "ltdP": round(tot_tdB / tot_leads * 100, 1) if tot_leads else 0,
                "won": tot_won,
                "lost": tot_lost,
                "lsP": round(tot_won / tot_leads * 100, 1) if tot_leads else 0,
                "mb30": round(mb30_w / tot_won, 1) if tot_won else 0,
                "mb60": round(mb60_w / tot_won, 1) if tot_won else 0,
                "mb90": round(mb90_w / tot_won, 1) if tot_won else 0,
            }
        else:
            net_all = {"leads": 0, "cp": 0, "cpP": 0, "utc": 0, "utcP": 0,
                       "tdB": 0, "tdC": 0, "tdSh": 0, "ltdP": 0,
                       "won": 0, "lost": 0, "lsP": 0, "mb30": 0, "mb60": 0, "mb90": 0}
    return lk_all, net_all


def build_TD_MB_and_LK120(wb):
    """Read Matchback Report sheet to build TD_MB and LK_120_NET constants."""
    rows = read_sheet(wb, "Matchback Report", 120)
    # Section 1 (R120): rows 3..Total row
    #   col1=Retailer, col2=R120 Brand Leads, col3=All Time Brand Leads,
    #   col5=R120 Retail Sales, col7=R30 MB Count, col8=R30 MB%,
    #   col9=R60 MB Count, col10=R60 MB%, col11=R90 MB Count, col12=R90 MB%,
    #   col13=R120 MB Count, col14=R120 MB%, col15=All Time MB Count, col16=All Time MB%
    # Section 2 (Since Inception): starts after blank rows, same dealers with all-time data.

    # --- Parse R120 section (stop at Total row) ---
    total_row = None
    dealers_r120 = []
    r120_retail_sales = 0
    for r in rows[3:]:
        if not r or not r[1]:
            continue
        name = safe_str(r[1]).strip()
        if "total" in name.lower():
            total_row = r
            break  # stop at Total row — don't process the since-inception section
        retail_sales = vi(r[5]) if len(r) > 5 and r[5] else 0
        r120_retail_sales += retail_sales
        d_name = name.replace(" INEOS Grenadier", "").replace(" INEOS", "").strip().upper()
        r120_leads = vi(r[2]) if len(r) > 2 and r[2] else 0
        td_to_sale = round(retail_sales / r120_leads * 100, 1) if r120_leads else 0
        dealers_r120.append((d_name, td_to_sale))

    # --- Parse Since Inception section for total_td / matched ---
    inception_retail = 0
    inception_matched = 0
    found_inception = False
    for r in rows:
        if not r or not r[1]:
            continue
        name = safe_str(r[1]).strip()
        if "since inception" in name.lower():
            found_inception = True
            continue
        if found_inception:
            if "total" in name.lower() or name == "Retailer":
                continue
            rs = vi(r[5]) if len(r) > 5 and r[5] else 0
            inception_retail += rs

    # Build TD_MB from total row
    td_mb = {}
    if total_row:
        all_time_mb_count = vi(total_row[15]) if len(total_row) > 15 and total_row[15] else 0
        all_time_mb_pct = round(vf(total_row[16]) * 100, 1) if len(total_row) > 16 and total_row[16] else 0
        td_mb = {
            "td30": round(vf(total_row[8]) * 100, 1) if total_row[8] else 0,
            "td60": round(vf(total_row[10]) * 100, 1) if total_row[10] else 0,
            "td90": round(vf(total_row[12]) * 100, 1) if total_row[12] else 0,
            "td120": round(vf(total_row[14]) * 100, 1) if total_row[14] else 0,
            "td180": all_time_mb_pct,
            "total_td": inception_retail if inception_retail else r120_retail_sales,
            "matched": all_time_mb_count,
        }

    # Build TD_MB_DLR
    td_mb_dlr = {d: v for d, v in dealers_r120}

    # Build LK_120_NET from total row.
    # Only include fields the Matchback Report provides — omit cpP, utcP, etc.
    # so they inherit from LK_ALL_NET during the JS spread merge.
    lk_120_net = {}
    if total_row:
        r120_leads = vi(total_row[2]) if total_row[2] else 0
        lk_120_net = {
            "m": "Network", "d": "TOTAL", "rbm": "",
            "leads": r120_leads,
        }

    return td_mb, td_mb_dlr, lk_120_net


# ---------------------------------------------------------------------------
# Pass 3 builders (excluding VEX/TR because pass 4 supersedes them)
# ---------------------------------------------------------------------------

def build_SC(wb, export_rows):
    dpd_rows = []
    with wb.get_sheet("Dealer Performance Dashboard") as sheet:
        for row in sheet.rows():
            dpd_rows.append([c.v for c in row])

    dir_rows = []
    with wb.get_sheet("Dealer Inventory Report") as sheet:
        for row in sheet.rows():
            dir_rows.append([c.v for c in row])

    # Lead Handling KPIs (All-Time) — per-dealer contact/UTC/show-rate/lead-to-sale
    lhk_rows = []
    with wb.get_sheet("Lead Handling KPIs") as sheet:
        for row in sheet.rows():
            lhk_rows.append([c.v for c in row])
    lhk_map = {}
    for r in lhk_rows[4:]:
        if r and len(r) > 17 and r[1]:
            name = safe_str(r[1]).replace("  ", " ").replace(" INEOS Grenadier", "").replace(" INEOS", "").strip().upper()
            leads = vi(r[3])
            cp = round(vf(r[5]) * 100, 1) if r[5] and vf(r[5]) <= 1 else round(vf(r[5]), 1)
            utc = round(vf(r[7]) * 100, 1) if r[7] and vf(r[7]) <= 1 else round(vf(r[7]), 1)
            td_book = vi(r[8])
            td_comp = vi(r[9])
            td_show = round(vf(r[10]) * 100, 1) if r[10] and vf(r[10]) <= 1 else round(vf(r[10]), 1)
            ltd_pct = round(vf(r[11]) * 100, 1) if r[11] and vf(r[11]) <= 1 else round(vf(r[11]), 1)
            won = vi(r[12])
            lost = vi(r[13])
            lts = round(vf(r[14]) * 100, 1) if r[14] and vf(r[14]) <= 1 else round(vf(r[14]), 1)
            lhk_map[name] = {
                "cp": cp, "utc": utc, "tdShow": td_show,
                "ltd": ltd_pct, "lts": lts, "won": won, "lost": lost,
            }

    # --- Body/trim mix from Export (on-ground dealer stock only) ---
    mix_map = {}  # dealer_upper -> {"bodies": set, "trims": set}
    for r in export_rows:
        status = safe_str(r[13]).lower()
        if "dealer stock" not in status and "7." not in status:
            continue
        channel = safe_str(r[14]).strip()
        if channel not in ("STOCK", "PRIVATE - RETAILER"):
            continue
        dealer_raw = export_dealer(r).upper()
        material = safe_str(r[7]).upper()
        trim = safe_str(r[19]).strip()
        # Determine body: SVO if material contains SVO, else QM if Quartermaster, else SW
        if "SVO" in material:
            body = "SVO"
        elif "QUARTERMASTER" in material:
            body = "QM"
        else:
            body = "SW"
        if dealer_raw not in mix_map:
            mix_map[dealer_raw] = {"bodies": set(), "trims": set()}
        mix_map[dealer_raw]["bodies"].add(body)
        if trim:
            mix_map[dealer_raw]["trims"].add(trim)

    dir_map = {}
    for r in dir_rows[3:]:
        if r and len(r) > 31 and r[2]:
            name = safe_str(r[2]).replace(" INEOS Grenadier", "").replace(" INEOS", "").strip().upper()
            og_sw = vi(r[11])
            og_qm = vi(r[12])
            og = og_sw + og_qm
            dol_sw = round(vf(r[31]), 0) if len(r) > 31 and r[31] else 0
            dol_qm = round(vf(r[32]), 0) if len(r) > 32 and r[32] else 0
            # Weighted avg DOL
            dol_avg = round((dol_sw * og_sw + dol_qm * og_qm) / max(og, 1), 0)
            # R90 avg monthly sales from DIR (cols 25-26)
            r90_sw = vf(r[25]) if len(r) > 25 else 0
            r90_qm = vf(r[26]) if len(r) > 26 else 0
            r90_total = r90_sw + r90_qm
            # Turn rate: sales / (sales + OG) * 100 (dashboard standard formula)
            turn_r90 = round(r90_total / (r90_total + og) * 100, 1) if (r90_total + og) > 0 else 0

            # Body/trim mix from Export data
            mix = mix_map.get(name, {})
            body_count = len(mix.get("bodies", set()))  # 0-3 (SW, QM, SVO)
            trim_count = len(mix.get("trims", set()))    # 0-6 trims

            dir_map[name] = {
                "og": og, "dol": dol_avg,
                "bodyMix": body_count, "trimMix": trim_count,
                "turn": turn_r90,
            }

    sc = []
    for r in dpd_rows[3:]:
        if not r or not r[0]:
            continue
        market = safe_str(r[0])
        dealer_raw = safe_str(r[1])
        if not market or not dealer_raw:
            continue

        dealer = dealer_raw.replace(" INEOS Grenadier", "").replace(" INEOS", "").strip()
        ctry = "CA" if market == "Canada" else ("MX" if market == "Mexico" else "US")
        ho = vi(r[2])
        cvp = vi(r[3])
        ws = vi(r[4])
        ws_gap = safe_str(r[5]) if r[5] else "1.00:1"
        r3 = round(vf(r[15]), 1)
        mtd_val = vi(r[14])
        prev_val = vi(r[13])
        ho_trend = round(vf(r[16]), 1)   # H/O trend vs prior R3M
        lead_trend = round(vf(r[18]), 1)  # Lead trend vs prior R3M
        td_trend = round(vf(r[20]), 1)    # TD trend vs prior R3M
        rl = vi(r[17])                     # R3M leads
        td = vi(r[19])                     # R3M TD bookings
        td_wknd = vi(r[21])                # TD weekend bookings
        td_prog = vi(r[22])                # TD program bookings
        mb30 = round(vf(r[23]) * 100, 1) if r[23] and vf(r[23]) <= 1 else round(vf(r[23]), 1)
        mb60 = round(vf(r[24]) * 100, 1) if r[24] and vf(r[24]) <= 1 else round(vf(r[24]), 1)
        mb90 = round(vf(r[25]) * 100, 1) if r[25] and vf(r[25]) <= 1 else round(vf(r[25]), 1)
        mb_at = round(vf(r[26]) * 100, 1) if r[26] and vf(r[26]) <= 1 else round(vf(r[26]), 1)

        dir_d = dir_map.get(dealer.upper(), dir_map.get(dealer_raw.upper(), {}))
        og = dir_d.get("og", vi(r[6]))
        dol = dir_d.get("dol", vi(r[7]))
        turn = dir_d.get("turn", 0)
        body_mix = dir_d.get("bodyMix", 0)   # 0-3: SW, QM, SVO
        trim_mix = dir_d.get("trimMix", 0)   # 0-6: distinct trims in stock

        # Parse W/S:H/O gap ratio
        try:
            ws_ratio = float(ws_gap.split(":")[0])
        except Exception:
            ws_ratio = 1.0

        # Lead handling metrics from LHK (names uppercased in lhk_map)
        lhk = lhk_map.get(dealer.upper(), lhk_map.get(dealer_raw.upper(), {}))
        cp = lhk.get("cp", 0)         # Contact Pending %
        utc = lhk.get("utc", 0)       # UTC % of lost
        td_show = lhk.get("tdShow", 0)  # TD show rate
        ltd_lhk = lhk.get("ltd", 0)   # Lead-to-TD % (all-time)
        lts = lhk.get("lts", 0)       # Lead-to-Sale %

        # R3M lead-to-TD from DPD
        ltd_r3 = round(vf(r[19]) / max(vf(r[17]), 1) * 100, 1) if r[17] and vf(r[17]) > 0 else 0

        sc.append({
            "d": dealer, "mkt": market, "ctry": ctry,
            # Sales
            "og": og, "mtd": mtd_val, "prev": prev_val, "r90": r3, "turn": turn,
            "ho": ho, "cvp": cvp,
            # Inventory health
            "dol": dol, "bodyMix": body_mix, "trimMix": trim_mix,
            # Lead management
            "leads": rl, "td": td, "cp": cp, "tdShow": td_show,
            "ltd": ltd_r3, "ltdAt": ltd_lhk,
            # Conversion
            "lts": lts, "mb90": mb90, "mbAt": mb_at, "wsR": ws_ratio,
            # Trend
            "hoTr": ho_trend, "ldTr": lead_trend,
            # Engagement
            "tdWk": td_wknd, "tdPr": td_prog,
        })

    return sc


def build_MIG(export_rows, mkt_map):
    from datetime import datetime as _dt, timedelta as _td
    _r90_start = _dt.today() - _td(days=90)

    mig_mo = defaultdict(lambda: {"my25": 0, "my26": 0})
    mig_dlr = defaultdict(lambda: {"og25": 0, "og26": 0, "dol25": 0, "r90_25": 0, "r90_26": 0})

    for r in export_rows:
        country = safe_str(r[11])
        if not country:
            continue
        cu = country.upper()
        if "UNITED STATES" not in cu and "CANADA" not in cu:
            continue

        material = safe_str(r[7])
        ho_date = serial_to_date(r[51])
        status = safe_str(r[13]).lower()
        dealer = export_dealer(r).upper()  # Normalize to uppercase for consistent dict keys
        dis = vi(r[57])
        is_25 = "25" in material
        is_26 = "26" in material

        # Monthly sales totals (all channels)
        if ho_date:
            mo = ho_date.strftime("%Y-%m")
            if is_25:
                mig_mo[mo]["my25"] += 1
            elif is_26:
                mig_mo[mo]["my26"] += 1

            # R90 per-dealer sales
            if ho_date >= _r90_start:
                if is_25:
                    mig_dlr[dealer]["r90_25"] += 1
                elif is_26:
                    mig_dlr[dealer]["r90_26"] += 1

        # On-ground inventory (retail channels only)
        if "dealer stock" in status or "7." in status:
            channel = safe_str(r[14]).strip()
            if channel not in ("STOCK", "PRIVATE - RETAILER"):
                continue
            if is_25:
                mig_dlr[dealer]["og25"] += 1
                mig_dlr[dealer]["dol25"] = max(mig_dlr[dealer]["dol25"], dis)
            elif is_26:
                mig_dlr[dealer]["og26"] += 1

    mo_list = [{"m": k, "my25": v["my25"], "my26": v["my26"]} for k, v in sorted(mig_mo.items())]
    dlr_list = []
    for dealer, vals in sorted(mig_dlr.items()):
        mkt = lookup_mkt(mkt_map, dealer)
        if not mkt:
            continue
        dlr_list.append({
            "d": dealer,
            "mkt": mkt,
            "og25": vals["og25"],
            "og26": vals["og26"],
            "dol25": vals["dol25"],
            "r90_25": vals["r90_25"],
            "r90_26": vals["r90_26"],
        })

    return mo_list[-14:], dlr_list


def build_PL_AGE(export_rows):
    buckets = [
        ("0-30", 0, 30),
        ("31-60", 31, 60),
        ("61-90", 61, 90),
        ("91-120", 91, 120),
        ("121-180", 121, 180),
        ("181-270", 181, 270),
        ("271-365", 271, 365),
        ("365+", 366, 99999),
    ]

    result = []
    for label, lo, hi in buckets:
        n = 0
        dol = 0
        my25 = 0
        my26 = 0
        for r in export_rows:
            country = safe_str(r[11])
            if not country or "UNITED STATES" not in country.upper():
                continue
            status = safe_str(r[13]).lower()
            if "dealer stock" not in status and "7." not in status:
                continue
            channel = safe_str(r[14]).strip()
            if channel not in ("STOCK", "PRIVATE - RETAILER"):
                continue
            dis = vi(r[57])
            if lo <= dis <= hi:
                n += 1
                dol += dis
                material = safe_str(r[7])
                if "25" in material:
                    my25 += 1
                elif "26" in material:
                    my26 += 1
        result.append({"s": label, "n": n, "dol": round(dol / max(n, 1)), "my25": my25, "my26": my26})
    return result


def build_MIG_INV(export_rows):
    inv = {"MY25": {"og": 0, "dol_sum": 0, "it": 0, "ap": 0},
           "MY26": {"og": 0, "dol_sum": 0, "it": 0, "ap": 0}}
    for r in export_rows:
        country = safe_str(r[11])
        if not country or "UNITED STATES" not in country.upper():
            continue
        status = safe_str(r[13]).lower()
        material = safe_str(r[7])
        is_25 = "25" in material
        is_26 = "26" in material
        tgt = inv["MY25"] if is_25 else (inv["MY26"] if is_26 else None)
        if not tgt:
            continue
        if "dealer stock" in status or "7." in status:
            channel = safe_str(r[14]).strip()
            if channel in ("STOCK", "PRIVATE - RETAILER"):
                tgt["og"] += 1
                tgt["dol_sum"] += vi(r[57])
        elif "in-transit" in status or "6." in status:
            tgt["it"] += 1
        elif "port" in status or "5." in status:
            tgt["ap"] += 1
    return inv


# ---------------------------------------------------------------------------
# Pass 4 compact VEX/TR builders
# ---------------------------------------------------------------------------

def build_VEX_compact(export_rows, mkt_map):
    dealers = sorted({
        export_dealer(r).upper()
        for r in export_rows if r[0]
    } - {""})
    dealer_idx = {d: i for i, d in enumerate(dealers)}

    mkts = sorted({lookup_mkt(mkt_map, d) or "Unknown" for d in dealers})
    mkt_idx = {m: i for i, m in enumerate(mkts)}

    country_list = ["Canada", "Mexico", "United States"]
    country_idx = {c: i for i, c in enumerate(country_list)}

    cat_map = {
        "8. sold": "Sold",
        "7. dealer stock": "Dealer Stock",
        "6. in-transit": "In-Transit to Dealer",
        "5. arrived": "At Americas Port",
        "4. departed": "On Water",
        "3. built": "Built at Plant",
        "2. in production": "In Production",
        "1. preplan": "Preplanning",
        "(blank)": "Awaiting Status",
        "planned": "Planned for Transfer",
        "vehicle written": "Written Off",
    }
    cats = [
        "Dealer Stock",
        "In-Transit to Dealer",
        "Planned for Transfer",
        "At Americas Port",
        "On Water",
        "Built at Plant",
        "In Production",
        "Preplanning",
        "Awaiting Status",
        "Sold",
        "Written Off",
    ]
    cat_idx = {c: i for i, c in enumerate(cats)}

    ext_colors, trims_set, seats_set, roof_set = set(), set(), set(), set()
    safari_set, wheels_set, tyre_set, frame_set = set(), set(), set(), set()
    pack_set, plant_set = set(), set()
    my_set, body_set = set(), set()
    # Option columns 28-49: collect unique values per column
    opt_sets = [set() for _ in range(22)]  # cols 28..49

    for r in export_rows:
        ext_colors.add(safe_str(r[21]))
        trims_set.add(safe_str(r[19]))
        seats_set.add(safe_str(r[22]))
        roof_set.add(safe_str(r[23]))
        safari_set.add(safe_str(r[24]))
        wheels_set.add(safe_str(r[25]))
        tyre_set.add(safe_str(r[26]))
        frame_set.add(safe_str(r[27]))
        pack_set.add(safe_str(r[20]))
        plant_set.add(safe_str(r[50]))
        mat = safe_str(r[7])
        body_set.add("SVO" if "svo" in mat.lower() else "QM" if "quartermaster" in mat.lower() else "SW")
        if "26" in mat:
            my_set.add("MY26")
        elif "25" in mat:
            my_set.add("MY25")
        elif "24" in mat:
            my_set.add("MY24")
        elif "27" in mat:
            my_set.add("MY27")
        # Collect option column values
        for oi in range(22):
            col = 28 + oi
            if col < len(r):
                opt_sets[oi].add(safe_str(r[col]))

    # Build option lookup lists and index maps (VEX_D keys "17"-"38")
    opt_lists = []
    opt_idxs = []
    for oi in range(22):
        vals = sorted(opt_sets[oi] - {""})
        opt_lists.append(vals)
        opt_idxs.append({v: i for i, v in enumerate(vals)})

    ext_list = sorted(ext_colors - {""})
    trim_list = sorted(trims_set - {""})
    seats_list = sorted(seats_set - {""})
    roof_list = sorted(roof_set - {""})
    safari_list = sorted(safari_set - {""})
    wheels_list = sorted(wheels_set - {""})
    tyre_list = sorted(tyre_set - {""})
    frame_list = sorted(frame_set - {""})
    pack_list = sorted(pack_set - {""})
    plant_list = sorted(plant_set - {""})
    body_list = sorted(body_set)
    my_list = sorted(my_set)

    ext_i = {v: i for i, v in enumerate(ext_list)}
    trim_i = {v: i for i, v in enumerate(trim_list)}
    seats_i = {v: i for i, v in enumerate(seats_list)}
    roof_i = {v: i for i, v in enumerate(roof_list)}
    safari_i = {v: i for i, v in enumerate(safari_list)}
    wheels_i = {v: i for i, v in enumerate(wheels_list)}
    tyre_i = {v: i for i, v in enumerate(tyre_list)}
    frame_i = {v: i for i, v in enumerate(frame_list)}
    pack_i = {v: i for i, v in enumerate(pack_list)}
    plant_i = {v: i for i, v in enumerate(plant_list)}
    body_i = {v: i for i, v in enumerate(body_list)}
    my_i = {v: i for i, v in enumerate(my_list)}

    vex_d = {
        "0": dealers,
        "1": mkts,
        "2": country_list,
        "4": body_list,
        "5": my_list,
        "6": cats,
        "8": trim_list,
        "9": pack_list,
        "10": ext_list,
        "11": seats_list,
        "12": roof_list,
        "13": safari_list,
        "14": wheels_list,
        "15": tyre_list,
        "16": frame_list,
        "plant": plant_list,
    }
    # Add option column lookups (keys "17"-"38")
    for oi in range(22):
        vex_d[str(17 + oi)] = opt_lists[oi]

    vex_mkt_dlr = defaultdict(list)
    for d in dealers:
        m = lookup_mkt(mkt_map, d) or "Unknown"
        if d not in vex_mkt_dlr[m]:
            vex_mkt_dlr[m].append(d)

    vex = []
    for r in export_rows:
        country = safe_str(r[11])
        if not country:
            continue
        is_americas = any(x in country.upper() for x in ["UNITED STATES", "CANADA", "MEXICO"])
        if not is_americas:
            continue

        d_name = export_dealer(r).upper()
        di = dealer_idx.get(d_name, 0)

        market = lookup_mkt(mkt_map, d_name) or "Unknown"
        mi = mkt_idx.get(market, 0)

        if "UNITED STATES" in country.upper():
            ctry = "United States"
        elif "CANADA" in country.upper():
            ctry = "Canada"
        else:
            ctry = "Mexico"
        ci = country_idx.get(ctry, 0)

        status = safe_str(r[13]).lower()
        status_i = cat_idx.get("Awaiting Status", 8)
        for key, cat_name in cat_map.items():
            if key in status:
                status_i = cat_idx.get(cat_name, 8)
                break

        vin = safe_str(r[8])
        mat = safe_str(r[7])
        bi = body_i.get("SVO" if "svo" in mat.lower() else "QM" if "quartermaster" in mat.lower() else "SW", 0)

        my_val = ""
        if "26" in mat:
            my_val = "MY26"
        elif "25" in mat:
            my_val = "MY25"
        elif "24" in mat:
            my_val = "MY24"
        elif "27" in mat:
            my_val = "MY27"
        myi = my_i.get(my_val, 0)

        msrp = vi(r[18])
        tri = trim_i.get(safe_str(r[19]), 0)
        pi = pack_i.get(safe_str(r[20]), 0)
        ei = ext_i.get(safe_str(r[21]), 0)
        si = seats_i.get(safe_str(r[22]), 0)
        ri = roof_i.get(safe_str(r[23]), 0)
        sai = safari_i.get(safe_str(r[24]), 0)
        wi = wheels_i.get(safe_str(r[25]), 0)
        tyi = tyre_i.get(safe_str(r[26]), 0)
        fi = frame_i.get(safe_str(r[27]), 0)

        opts = ""
        for oi in range(22):
            col = 28 + oi
            if col < len(r):
                val = safe_str(r[col])
                idx = opt_idxs[oi].get(val, 0) if val else 0
                opts += str(idx)
            else:
                opts += "0"

        pli = plant_i.get(safe_str(r[50]), 0)
        ho_date = serial_to_date(r[51])
        if not ho_date and "sold" in status:
            ho_date = serial_to_date(r[6]) or serial_to_date(r[55])  # fallback to invoice / rev rec
        ho_str = ho_date.strftime("%Y-%m-%d") if ho_date else ""
        eta_date = serial_to_date(r[52])
        eta_str = eta_date.strftime("%Y-%m-%d") if eta_date else ""
        vessel = safe_str(r[53])[:20] if r[53] else ""

        dis = vi(r[57])
        channel = safe_str(r[14]).strip()
        campaign = safe_str(r[75]).strip() if len(r) > 75 and r[75] else ""
        if campaign == "0" or campaign == "0.0":
            campaign = ""
        var_spend = round(vf(r[72]), 2) if len(r) > 72 and r[72] else 0
        so_num = str(int(float(r[3]))) if r[3] and safe_str(r[3]).replace(".","").replace("0","").strip() else ""
        rr_date = serial_to_date(r[55])
        rr_str = rr_date.strftime("%Y-%m-%d") if rr_date else ""
        bill_to_raw = safe_str(r[58]).strip() if len(r) > 58 and r[58] else ""
        if bill_to_raw == "Not Handed Over" or not bill_to_raw:
            # Fall back to Updated Customer Name (col 0) for non-handed-over units
            bill_to = d_name.replace(" INEOS GRENADIER", "").replace(" INEOS", "").strip()
            bill_to = " ".join(w for w in bill_to.split() if w != "GRENADIER")
            # Title case the uppercase name
            if bill_to == bill_to.upper() and len(bill_to) > 3:
                bill_to = bill_to.title()
        else:
            bill_to = bill_to_raw.replace(" INEOS Grenadier", "").replace(" INEOS GRENADIER", "").replace(" INEOS", "").strip()

        vex.append([
            di,
            mi,
            ci,
            vin,
            bi,
            myi,
            status_i,
            msrp,
            tri,
            pi,
            ei,
            si,
            ri,
            sai,
            wi,
            tyi,
            fi,
            opts,
            pli,
            ho_str,
            eta_str,
            vessel,
            dis,
            channel,
            campaign,
            var_spend,
            so_num,
            rr_str,
            bill_to,
        ])

    return vex, vex_d, dict(vex_mkt_dlr)


def build_TR_compact(export_rows, mkt_map):
    today = datetime.now()

    dealers = set()
    bodies = {"SW", "QM"}
    trims = set()
    mys = set()
    mkts = set()
    ctrys = set()
    months = set()

    records_og = []
    records_sales = []

    _tr_valid_mkts = {"Canada", "Central", "Northeast", "Southeast", "Western", "Mexico"}

    for r in export_rows:
        country = safe_str(r[11])
        if not country:
            continue
        is_us = "UNITED STATES" in country.upper()
        is_ca = "CANADA" in country.upper()
        if not is_us and not is_ca:
            continue

        # Only include retail dealer stock; exclude rental, fleet, internal, employee
        channel = safe_str(r[14]).strip()
        if channel not in ("STOCK", "PRIVATE - RETAILER"):
            continue

        dealer = export_dealer(r).upper()
        status = safe_str(r[13]).lower()
        material = safe_str(r[7])
        trim = safe_str(r[19])
        body = "SVO" if "svo" in material.lower() else "QM" if "quartermaster" in material.lower() else "SW"
        dis = vi(r[57])
        market_raw = safe_str(r[54]).strip() if r[54] else ""
        market = market_raw if market_raw in _tr_valid_mkts else lookup_mkt(mkt_map, dealer)
        ho_date = serial_to_date(r[51])
        ctry = "US" if is_us else "Canada"
        my = ""
        if "26" in material:
            my = "MY26"
        elif "25" in material:
            my = "MY25"
        elif "24" in material:
            my = "MY24"

        dealers.add(dealer)
        if trim:
            trims.add(trim)
        if my:
            mys.add(my)
        if market:
            mkts.add(market)
        ctrys.add(ctry)

        if "dealer stock" in status or "7." in status:
            records_og.append((dealer, body, trim, my, market, ctry, dis))

        if ho_date:
            mo = ho_date.strftime("%Y-%m")
            if (today - ho_date).days <= 395:
                months.add(mo)
                records_sales.append((dealer, body, trim, my, market, ctry, mo))

    d_list = sorted(dealers)
    b_list = sorted(bodies)
    t_list = sorted(trims)
    my_list = sorted(mys)
    mk_list = sorted(mkts)
    c_list = sorted(ctrys)
    mo_list = sorted(months)

    d_i = {v: i for i, v in enumerate(d_list)}
    b_i = {v: i for i, v in enumerate(b_list)}
    t_i = {v: i for i, v in enumerate(t_list)}
    my_i = {v: i for i, v in enumerate(my_list)}
    mk_i = {v: i for i, v in enumerate(mk_list)}
    c_i = {v: i for i, v in enumerate(c_list)}
    mo_i = {v: i for i, v in enumerate(mo_list)}

    og_agg = defaultdict(lambda: [0, 0])  # key -> [count, sum_dis]
    for dealer, body, trim, my, market, ctry, dis in records_og:
        key = (
            d_i.get(dealer, 0),
            b_i.get(body, 0),
            t_i.get(trim, 0),
            my_i.get(my, 0),
            mk_i.get(market, 0),
            c_i.get(ctry, 0),
        )
        og_agg[key][0] += 1
        og_agg[key][1] += dis
    og_arr = [list(k) + v for k, v in sorted(og_agg.items())]

    sales_agg = defaultdict(int)
    for dealer, body, trim, my, market, ctry, mo in records_sales:
        key = (
            d_i.get(dealer, 0),
            b_i.get(body, 0),
            t_i.get(trim, 0),
            my_i.get(my, 0),
            mk_i.get(market, 0),
            c_i.get(ctry, 0),
            mo_i.get(mo, 0),
        )
        sales_agg[key] += 1

    sales_arr = [list(k) + [v] for k, v in sorted(sales_agg.items())]
    cur_mo = today.strftime("%Y-%m")
    prev_mo = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

    return og_arr, sales_arr, d_list, b_list, t_list, my_list, mk_list, c_list, mo_list, today.day, cur_mo, prev_mo


# ---------------------------------------------------------------------------
# MTD Dealer-level retail sales
# ---------------------------------------------------------------------------

def build_MTD_DLR(wb):
    rows = read_sheet(wb, "Retail Sales Report", 300)
    # Region names in col 1: "Internal/Fleet/Rental", "Eastern Region", etc.
    region_map = {"Eastern Region": "Northeast", "Southern Region": "Southeast",
                  "Central Region": "Central", "Western Region": "Western",
                  "Eastern": "Northeast", "Southern": "Southeast",
                  "CANADA": "Canada", "MEXICO": "Mexico"}
    dealers = []
    cur_region = ""

    for r in rows[22:]:  # Start after the summary sections
        if not r or len(r) < 7:
            continue

        # Col 1 = region header or "Retail Partner Code" or partner code or "Total"
        c1 = safe_str(r[1]).strip() if r[1] is not None else ""
        # Col 2 = dealer name (or "Retail Partner Name" header)
        c2 = safe_str(r[2]).strip() if len(r) > 2 and r[2] is not None else ""

        # Region header: col 1 has text, col 2 is empty, not a number
        if c1 and not c2 and c1 not in ("Total", "Grand Total") \
                and "Retail Partner" not in c1 \
                and not c1.replace(".", "").replace("-", "").isdigit():
            # Stop if we hit historical/prior month sections
            if "February" in c1 or "January" in c1 or "Final Results" in c1:
                break
            # Strip " Region" suffix if present
            key = c1.replace(" Region", "").strip() if " Region" in c1 else c1
            cur_region = region_map.get(c1, region_map.get(key, key))
            continue

        # Stop if col 2 has summary/historical content
        if c2 and ("Americas Final" in c2 or "Final Results" in c2):
            break

        # Skip header rows
        if "Retail Partner" in c1 or "Retail Partner" in c2:
            continue

        # Skip Total/Grand Total rows
        if c1 in ("Total", "Grand Total"):
            continue

        # Dealer row: col 2 has dealer name
        name = c2
        if not name:
            continue

        name = name.replace(" INEOS Grenadier", "").replace(" INEOS", "").strip()

        sw  = vi(r[3]) if len(r) > 3 else 0
        qm  = vi(r[4]) if len(r) > 4 else 0
        svo = vi(r[5]) if len(r) > 5 else 0
        total = vi(r[6]) if len(r) > 6 else 0
        prev = vi(r[7]) if len(r) > 7 else 0
        py  = vi(r[8]) if len(r) > 8 else 0
        ppm  = round(vf(r[9]) * 100, 1) if len(r) > 9 and r[9] else None
        ppy  = round(vf(r[10]) * 100, 1) if len(r) > 10 and r[10] else None
        dcsS = round(vf(r[11]) * 100, 1) if len(r) > 11 and r[11] else None
        dcsQ = round(vf(r[12]) * 100, 1) if len(r) > 12 and r[12] else None
        cvp  = vi(r[15]) if len(r) > 15 else 0

        dealers.append({
            "d": name, "m": cur_region,
            "sw": sw, "qm": qm, "svo": svo, "t": total,
            "prev": prev, "py": py,
            "ppm": ppm, "ppy": ppy,
            "dcsS": dcsS, "dcsQ": dcsQ,
            "cvp": cvp,
        })
    return dealers


# ---------------------------------------------------------------------------
# RSR Retailed Units (current month handovers from Export)
# ---------------------------------------------------------------------------

def build_RSR_RETAILED(export_rows, market_map):
    from datetime import datetime, timedelta
    today = datetime.today()
    cur_mo = today.strftime("%Y-%m")

    units = []
    for r in export_rows:
        if not r or len(r) < 52:
            continue
        ho_date = serial_to_date(r[51])
        if not ho_date:
            continue
        if ho_date.strftime("%Y-%m") != cur_mo:
            continue

        dealer_raw = export_dealer(r)
        mkt = lookup_mkt(market_map, dealer_raw)

        material = safe_str(r[7])
        mat_l = material.lower()
        if "quartermaster" in mat_l:
            body = "QM"
        elif "svo" in mat_l:
            body = "SVO"
        else:
            body = "SW"
        my = ""
        if "26" in material:
            my = "MY26"
        elif "25" in material:
            my = "MY25"
        elif "24" in material:
            my = "MY24"

        vin = safe_str(r[8])
        channel = safe_str(r[14]).strip()
        trim = safe_str(r[19]).strip()
        ext_color = safe_str(r[21]).strip()
        interior = safe_str(r[22]).strip()
        wheels = safe_str(r[25]).strip()
        msrp = round(vf(r[18]), 0) if r[18] else 0
        days_to_sell = vi(r[56]) if r[56] else ""
        cvp = safe_str(r[62]).strip()

        units.append({
            "d": dealer_raw,
            "mkt": mkt,
            "vin": vin[-6:] if len(vin) >= 6 else vin,
            "vinFull": vin,
            "body": body,
            "my": my,
            "trim": trim,
            "ext": ext_color,
            "int": interior,
            "whl": wheels,
            "ch": channel,
            "msrp": int(msrp),
            "dts": days_to_sell,
            "cvp": cvp,
            "ho": ho_date.strftime("%m/%d"),
        })

    units.sort(key=lambda u: (u["mkt"] or "ZZZ", u["d"], u["ho"]))
    return units


# ---------------------------------------------------------------------------
# Customer Experience Scorecard (CX) — Google, Yelp, DealerRater, Cars.com
# ---------------------------------------------------------------------------

def _cx_cache_path():
    """Return the CX cache file path. Looks first at the bundled repo
    cache (data_hub/cx_cache.json) which always exists in the deploy,
    then falls back to the legacy Windows path for local dev.
    """
    bundled = os.path.join(os.path.dirname(__file__), "cx_cache.json")
    if os.path.exists(bundled):
        return bundled
    return os.path.join(DEFAULT_BASE_DIR, "cx_cache.json")

CX_CACHE_PATH = _cx_cache_path()
# TTL is intentionally generous: the bundled cache is committed in the repo
# and acts as a "last known good" snapshot when the live fetch (Google
# Places / Yelp) has no API key or no Dealer Address sheet to work from.
# Without this, build_CX would return [] and overwrite CX_DATA on every
# refresh, leaving the scorecard blank.
CX_CACHE_TTL_HOURS = 24 * 365 * 5

def _cx_load_cache():
    path = _cx_cache_path()
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                cache = json.load(f)
            ts = cache.get("_ts", 0)
            from datetime import datetime as _dt
            age_hours = (_dt.now().timestamp() - ts) / 3600
            if age_hours < CX_CACHE_TTL_HOURS:
                return cache
        except Exception:
            pass
    return None

def _cx_save_cache(data):
    from datetime import datetime as _dt
    data["_ts"] = _dt.now().timestamp()
    try:
        with open(CX_CACHE_PATH, "w") as f:
            json.dump(data, f)
    except Exception:
        pass

def _cx_fetch_google(name, city, state, api_key):
    """Fetch Google Places rating and review count."""
    if not api_key:
        return None
    import requests
    try:
        query = f"{name} {city} {state}"
        url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
        resp = requests.get(url, params={
            "input": query, "inputtype": "textquery",
            "fields": "place_id,name,rating,user_ratings_total",
            "key": api_key
        }, timeout=10)
        data = resp.json()
        if data.get("candidates"):
            c = data["candidates"][0]
            pid = c.get("place_id", "")
            return {
                "rating": round(c.get("rating", 0), 1),
                "count": c.get("user_ratings_total", 0),
                "url": f"https://www.google.com/maps/place/?q=place_id:{pid}" if pid else ""
            }
    except Exception:
        pass
    return None

def _cx_fetch_yelp(name, city, state, api_key):
    """Fetch Yelp rating and review count via Fusion API. Tries multiple search strategies."""
    if not api_key:
        return None
    import requests
    short = name.replace(" INEOS Grenadier", "").replace(" INEOS", "").strip()
    # Try multiple search terms in order of specificity
    searches = [
        name,                              # "Arrowhead INEOS Grenadier"
        f"{short} INEOS",                  # "Arrowhead INEOS"
        f"{short} Grenadier",              # "Arrowhead Grenadier"
        short,                             # "Arrowhead"
        f"{short} auto",                   # "Arrowhead auto"
    ]
    url = "https://api.yelp.com/v3/businesses/search"
    for term in searches:
        try:
            resp = requests.get(url, headers={"Authorization": f"Bearer {api_key}"},
                               params={"term": term, "location": f"{city}, {state}", "limit": 3,
                                        "categories": "car_dealers,auto"},
                               timeout=10)
            data = resp.json()
            if data.get("businesses"):
                # Pick the best match — prefer one with the short dealer name in the business name
                for b in data["businesses"]:
                    bname = b.get("name", "").upper()
                    if short.upper() in bname or any(w.upper() in bname for w in short.split() if len(w) > 3):
                        return {
                            "rating": round(b.get("rating", 0), 1),
                            "count": b.get("review_count", 0),
                            "url": b.get("url", "")
                        }
                # If no name match, take the first result from the first search that had results
                b = data["businesses"][0]
                return {
                    "rating": round(b.get("rating", 0), 1),
                    "count": b.get("review_count", 0),
                    "url": b.get("url", "")
                }
        except Exception:
            pass
    return None

def _cx_scrape_dealerater(name, city, state):
    """Scrape DealerRater public page for rating and review count."""
    import requests
    from bs4 import BeautifulSoup
    try:
        slug = name.lower().replace("ineos grenadier", "").replace("ineos", "").strip()
        slug = slug.replace(" ", "-").replace(".", "").replace("'", "")
        search_url = f"https://www.dealerrater.com/sales/search/?q={name.replace(' ', '+')}+{city.replace(' ', '+')}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(search_url, headers=headers, timeout=5, allow_redirects=True)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        # Try to find dealer card with rating
        rating_el = soup.select_one(".rating-static, .dealership-rating, [class*='rating']")
        if rating_el:
            # Extract rating from class like "rating-static rating-45" => 4.5
            for cls in rating_el.get("class", []):
                if cls.startswith("rating-") and cls != "rating-static":
                    try:
                        val = int(cls.replace("rating-", ""))
                        return {
                            "rating": round(val / 10, 1),
                            "count": 0,
                            "url": resp.url
                        }
                    except ValueError:
                        pass
        # Try meta tags
        for meta in soup.select('meta[itemprop="ratingValue"]'):
            try:
                return {
                    "rating": round(float(meta.get("content", 0)), 1),
                    "count": 0,
                    "url": resp.url
                }
            except ValueError:
                pass
    except Exception:
        pass
    return None

def _cx_scrape_carscom(name, city, state):
    """Scrape Cars.com dealer page for rating and review count."""
    import requests
    from bs4 import BeautifulSoup
    try:
        query = f"{name} {city} {state}"
        search_url = f"https://www.cars.com/dealers/buy/?dealerName={query.replace(' ', '+')}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(search_url, headers=headers, timeout=5, allow_redirects=True)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        # Try to find rating in search results
        rating_el = soup.select_one("[class*='rating'], [data-rating], .sds-rating__count")
        if rating_el:
            text = rating_el.get_text(strip=True)
            try:
                val = float(text.split("/")[0].strip())
                if 0 < val <= 5:
                    return {"rating": round(val, 1), "count": 0, "url": resp.url}
            except ValueError:
                pass
        # Try structured data
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                ld = json.loads(script.string)
                if isinstance(ld, dict) and "aggregateRating" in ld:
                    ar = ld["aggregateRating"]
                    return {
                        "rating": round(float(ar.get("ratingValue", 0)), 1),
                        "count": int(ar.get("reviewCount", 0)),
                        "url": resp.url
                    }
            except (json.JSONDecodeError, ValueError):
                pass
    except Exception:
        pass
    return None


def build_CX(wb, market_map):
    """Build Customer Experience Scorecard from review platforms."""
    import time

    # Check cache first
    cached = _cx_load_cache()
    if cached and "_ts" in cached:
        dealers_cached = [v for k, v in cached.items() if k != "_ts"]
        if dealers_cached:
            print(f"  CX: Using cached data ({len(dealers_cached)} dealers, {CX_CACHE_TTL_HOURS}h TTL)")
            return dealers_cached

    # Read Dealer Address sheet
    rows = read_sheet(wb, "Dealer Address", 50)
    dealers = []
    for r in rows[1:]:
        if not r or not r[0]:
            continue
        full_name = safe_str(r[0]).strip()
        short = full_name.replace(" INEOS Grenadier", "").replace(" INEOS", "").strip()
        city = safe_str(r[2]).strip() if r[2] else ""
        state = safe_str(r[3]).strip() if r[3] else ""
        mkt = lookup_mkt(market_map, short)
        dealers.append({"full": full_name, "short": short, "city": city, "state": state, "mkt": mkt})

    # Deduplicate (Sewell PLANO = Sewell)
    seen = set()
    unique_dealers = []
    for d in dealers:
        key = d["short"].upper()
        if key not in seen:
            seen.add(key)
            unique_dealers.append(d)
    dealers = unique_dealers

    # API keys from environment
    google_key = os.environ.get("GOOGLE_PLACES_KEY", "")
    yelp_key = os.environ.get("YELP_API_KEY", "")

    print(f"  CX: Fetching reviews for {len(dealers)} dealers...")
    if google_key:
        print(f"    Google Places API: key configured")
    else:
        print(f"    Google Places API: no key (set GOOGLE_PLACES_KEY env var)")
    if yelp_key:
        print(f"    Yelp Fusion API: key configured")
    else:
        print(f"    Yelp Fusion API: no key (set YELP_API_KEY env var)")
    print(f"    DealerRater: scraping public pages")
    print(f"    Cars.com: scraping public pages")

    results = []
    cache_data = {}

    for i, d in enumerate(dealers):
        name = d["full"]
        city = d["city"]
        state = d["state"]
        short = d["short"]
        mkt = d["mkt"]

        print(f"    [{i+1}/{len(dealers)}] {short} ({city}, {state})...", end="", flush=True)

        # Fetch from each platform (skip scraping if first attempt fails)
        g = _cx_fetch_google(name, city, state, google_key)
        y = _cx_fetch_yelp(name, city, state, yelp_key)
        dr = _cx_scrape_dealerater(name, city, state) if i < 2 or results and any(r.get("dr", {}).get("rating", 0) > 0 for r in results) else None
        ca = _cx_scrape_carscom(name, city, state) if i < 2 or results and any(r.get("cars", {}).get("rating", 0) > 0 for r in results) else None

        # Build search URLs as fallback for platforms without data
        g_url = f"https://www.google.com/maps/search/{name.replace(' ', '+')}+{city.replace(' ', '+')}+{state}"
        y_url = f"https://www.yelp.com/search?find_desc={name.replace(' ', '+')}+{city.replace(' ', '+')}"
        dr_url = f"https://www.dealerrater.com/sales/search/?q={name.replace(' ', '+')}"
        ca_url = f"https://www.cars.com/dealers/buy/?dealerName={name.replace(' ', '+')}"

        if not g:
            g = {"rating": 0, "count": 0, "url": g_url}
        elif not g.get("url"):
            g["url"] = g_url
        if not y:
            y = {"rating": 0, "count": 0, "url": y_url}
        elif not y.get("url"):
            y["url"] = y_url
        if not dr:
            dr = {"rating": 0, "count": 0, "url": dr_url}
        elif not dr.get("url"):
            dr["url"] = dr_url
        if not ca:
            ca = {"rating": 0, "count": 0, "url": ca_url}
        elif not ca.get("url"):
            ca["url"] = ca_url

        # Compute composite score (weighted avg of available platforms)
        weights = {"google": 0.35, "yelp": 0.25, "dr": 0.25, "cars": 0.15}
        platforms = {"google": g, "yelp": y, "dr": dr, "cars": ca}
        total_weight = 0
        weighted_sum = 0
        total_reviews = 0
        active_platforms = 0

        for pkey, pdata in platforms.items():
            if pdata["rating"] > 0:
                weighted_sum += pdata["rating"] * weights[pkey]
                total_weight += weights[pkey]
                total_reviews += pdata["count"]
                active_platforms += 1

        composite = round(weighted_sum / total_weight, 2) if total_weight > 0 else 0

        sources = "+".join([k[0].upper() for k, v in platforms.items() if v["rating"] > 0])
        print(f" composite={composite} ({sources or 'no data'})")

        rec = {
            "d": short,
            "mkt": mkt,
            "google": g,
            "yelp": y,
            "dr": dr,
            "cars": ca,
            "composite": composite,
            "totalReviews": total_reviews,
            "platforms": active_platforms,
        }
        results.append(rec)
        cache_data[short] = rec

        # Small delay to be respectful to scraping targets
        if google_key or yelp_key:
            time.sleep(0.3)

    # Sort by composite score descending
    results.sort(key=lambda r: -r["composite"])

    # Save cache
    _cx_save_cache(cache_data)

    return results


# ---------------------------------------------------------------------------
# PM — Previous Month Results (built from Export handover data)
# ---------------------------------------------------------------------------

def build_PM(export_rows, market_map, wb):
    """Build monthly results for the Previous Month Results tab."""
    from collections import defaultdict
    from datetime import datetime as _dt

    MONTH_NAMES = {
        "01": "January", "02": "February", "03": "March", "04": "April",
        "05": "May", "06": "June", "07": "July", "08": "August",
        "09": "September", "10": "October", "11": "November", "12": "December"
    }

    # Classification based on Bill To Dealer (col BG/58)
    # "Fleet" = rental/fleet, "Internal" = INEOS internal, "Enterprise" = employee program
    # Everything else with a dealer name = retail
    non_retail_bt = {"Fleet", "Internal", "Enterprise"}

    # Collect all handovers from Export grouped by YYYY-MM
    # mo_data[ym][dealer_upper] = {total, sw, qm, svo, channel_counts, country, market, cvp}
    mo_data = defaultdict(lambda: defaultdict(lambda: {
        "total": 0, "sw": 0, "qm": 0, "svo": 0, "cvp": 0, "demo": 0,
        "ret_sw": 0, "ret_qm": 0, "ret_svo": 0,
        "country": "", "market": "", "channels": defaultdict(int),
        "bt_cats": defaultdict(int)
    }))

    for r in export_rows:
        ho_date = serial_to_date(r[51])
        if not ho_date:
            # Fallback for sold units without handover date
            status = safe_str(r[13]).lower()
            if "sold" in status or "8." in status:
                ho_date = serial_to_date(r[6]) or serial_to_date(r[55])
            if not ho_date:
                continue

        ym = ho_date.strftime("%Y-%m")
        dealer = export_dealer(r).upper()
        if not dealer:
            continue

        country = safe_str(r[11]).upper()
        channel = safe_str(r[14]).strip()
        material = safe_str(r[7]).lower()
        campaign = safe_str(r[75]).strip().upper() if len(r) > 75 and r[75] else ""
        if campaign in ("0", "0.0"):
            campaign = ""
        is_cvp = "CVP" in campaign
        is_demo = "DEMO" in campaign

        # Bill-to category from col BG (raw, before normalization)
        bt_raw = safe_str(r[58]).strip() if len(r) > 58 and r[58] else ""
        if bt_raw == "Not Handed Over":
            bt_raw = ""
        bt_cat = bt_raw if bt_raw in non_retail_bt else "Retail"

        body = "svo" if "svo" in material else ("qm" if "quartermaster" in material else "sw")
        mkt = lookup_mkt(market_map, dealer)

        rec = mo_data[ym][dealer]
        rec["total"] += 1
        rec[body] += 1
        is_retail = bt_cat == "Retail"
        if is_retail:
            rec["ret_" + body] += 1
        if is_cvp:
            rec["cvp"] += 1
        if is_demo:
            rec["demo"] += 1
        rec["country"] = "US" if "UNITED STATES" in country else ("CA" if "CANADA" in country else "MX")
        rec["market"] = mkt
        rec["channels"][channel] += 1
        rec["bt_cats"][bt_cat] += 1

    # Build monthly lead/TD totals from TDD if available
    # (we'll compute from Raw Lead Data in the same way as TDD)
    lead_rows = []
    try:
        with wb.get_sheet("Raw Lead Data") as sheet:
            for i, row in enumerate(sheet.rows()):
                if i == 0:
                    continue
                lead_rows.append([c.v for c in row])
    except Exception:
        pass

    mo_leads = defaultdict(int)  # ym -> total leads
    mo_tds = defaultdict(int)    # ym -> total TD bookings
    for lr in lead_rows:
        lead_date = serial_to_date(lr[16]) if len(lr) > 16 else None
        td_date = serial_to_date(lr[25]) if len(lr) > 25 else None
        if lead_date:
            mo_leads[lead_date.strftime("%Y-%m")] += 1
        if td_date:
            mo_tds[td_date.strftime("%Y-%m")] += 1

    # Santander monthly
    san_mo = {}
    try:
        san_mo_const = {}  # Will use SAN_MO from template if available
    except Exception:
        pass

    # Build PM dict for each month
    pm = {}
    today = _dt.today()
    # Include months from 12 months back to current month
    all_months = sorted(mo_data.keys())
    recent_months = [m for m in all_months if m >= (today.replace(day=1) - __import__('datetime').timedelta(days=400)).strftime("%Y-%m")]

    for ym in recent_months:
        year, month = ym.split("-")
        label = f"{MONTH_NAMES[month]} {year}"
        py_ym = f"{int(year)-1}-{month}"
        py_label = f"{MONTH_NAMES[month]} {int(year)-1}"

        dealers = mo_data[ym]
        py_dealers = mo_data.get(py_ym, {})

        # Summary by country/channel
        us_c, us_p, ca_c, ca_p, mx_c, mx_p = 0, 0, 0, 0, 0, 0
        fleet_c, fleet_p, internal_c, internal_p, ent_c, ent_p = 0, 0, 0, 0, 0, 0
        cvp_c, cvp_p, demo_c, demo_p = 0, 0, 0, 0

        for dk, rec in dealers.items():
            retail = rec["bt_cats"].get("Retail", 0)
            if rec["country"] == "US":
                us_c += retail
            elif rec["country"] == "CA":
                ca_c += retail
            elif rec["country"] == "MX":
                mx_c += retail
            fleet_c += rec["bt_cats"].get("Fleet", 0)
            internal_c += rec["bt_cats"].get("Internal", 0)
            ent_c += rec["bt_cats"].get("Enterprise", 0)
            cvp_c += rec["cvp"]
            demo_c += rec["demo"]

        for dk, rec in py_dealers.items():
            retail = rec["bt_cats"].get("Retail", 0)
            if rec["country"] == "US":
                us_p += retail
            elif rec["country"] == "CA":
                ca_p += retail
            elif rec["country"] == "MX":
                mx_p += retail
            fleet_p += rec["bt_cats"].get("Fleet", 0)
            internal_p += rec["bt_cats"].get("Internal", 0)
            ent_p += rec["bt_cats"].get("Enterprise", 0)
            cvp_p += rec["cvp"]
            demo_p += rec["demo"]

        total_c = us_c + ca_c + mx_c + fleet_c + internal_c + ent_c
        total_p = us_p + ca_p + mx_p + fleet_p + internal_p + ent_p

        summary = [
            {"n": "United States", "c": us_c, "p": us_p},
            {"n": "Canada", "c": ca_c, "p": ca_p},
            {"n": "Mexico", "c": mx_c, "p": mx_p},
            {"n": "Fleet", "c": fleet_c, "p": fleet_p},
            {"n": "Internal", "c": internal_c, "p": internal_p},
            {"n": "Enterprise", "c": ent_c, "p": ent_p},
            {"n": "Total", "c": total_c, "p": total_p},
        ]

        # US dealer ranking (retail only, sorted by current desc)
        us_dlr = {}
        for d, rec in dealers.items():
            if rec["country"] != "US":
                continue
            retail = rec["bt_cats"].get("Retail", 0)
            mkt = rec["market"]
            if (retail > 0 or mkt) and mkt not in ("", None):
                key = d
                us_dlr[key] = {"c": retail, "sw": rec["ret_sw"], "qm": rec["ret_qm"], "svo": rec["ret_svo"], "cvp": rec["cvp"], "demo": rec["demo"]}

        for d, rec in py_dealers.items():
            if rec["country"] != "US":
                continue
            retail = rec["bt_cats"].get("Retail", 0)
            if d not in us_dlr:
                us_dlr[d] = {"c": 0, "sw": 0, "qm": 0, "svo": 0, "cvp": 0, "demo": 0}
            us_dlr[d]["p"] = retail

        us_dealer_list = sorted(
            [{"n": d.title(), "c": v["c"], "p": v.get("p", 0),
              "sw": v["sw"], "qm": v["qm"], "svo": v["svo"], "cvp": v["cvp"], "demo": v["demo"]}
             for d, v in us_dlr.items() if v["c"] > 0 or v.get("p", 0) > 0],
            key=lambda x: -x["c"]
        )

        # Market breakdowns
        markets = {}
        for mkt_name in ["Northeast", "Southeast", "Central", "Western", "Canada", "Mexico"]:
            mkt_dealers = {}
            for d, rec in dealers.items():
                if rec["market"] != mkt_name:
                    continue
                retail = rec["bt_cats"].get("Retail", 0)
                mkt_dealers[d] = {"c": retail, "cvp": rec["cvp"], "demo": rec["demo"]}

            for d, rec in py_dealers.items():
                if rec["market"] != mkt_name:
                    continue
                retail = rec["bt_cats"].get("Retail", 0)
                if d not in mkt_dealers:
                    mkt_dealers[d] = {"c": 0, "cvp": 0, "demo": 0}
                mkt_dealers[d]["p"] = retail

            dlr_list = sorted(
                [{"n": d.title(), "c": v["c"], "p": v.get("p", 0), "cvp": v["cvp"], "demo": v["demo"]}
                 for d, v in mkt_dealers.items()],
                key=lambda x: -x["c"]
            )
            tc = sum(v["c"] for v in mkt_dealers.values())
            tp = sum(v.get("p", 0) for v in mkt_dealers.values())
            markets[mkt_name] = {"dealers": dlr_list, "tc": tc, "tp": tp}

        # Leads/TD/Santander for the month
        leads_total = mo_leads.get(ym, 0)
        tds_total = mo_tds.get(ym, 0)
        days_in_mo = 31  # approximate
        try:
            import calendar
            days_in_mo = calendar.monthrange(int(year), int(month))[1]
        except Exception:
            pass

        pm[ym] = {
            "label": label,
            "pyLabel": py_label,
            "summary": summary,
            "usDealers": us_dealer_list,
            "markets": markets,
            "leads": leads_total,
            "leadsPerDay": round(leads_total / days_in_mo, 1) if leads_total else 0,
            "tds": tds_total,
            "tdsPerDay": round(tds_total / days_in_mo, 1) if tds_total else 0,
            "cvp": cvp_c,
            "demo": demo_c,
            "days": days_in_mo,
        }

    return pm


# ---------------------------------------------------------------------------
# TDD / TDays / TTODAY — daily lead & TD booking counts from Raw Lead Data
# ---------------------------------------------------------------------------

def build_TDD(wb, market_map):
    from datetime import datetime as _dt, timedelta as _td
    from collections import defaultdict

    rows = []
    with wb.get_sheet("Raw Lead Data") as sheet:
        for i, row in enumerate(sheet.rows()):
            if i == 0:
                continue  # skip header
            rows.append([c.v for c in row])

    today = _dt.today().date()

    # Build date range: 13 months back to 15 days forward
    start_date = (today - _td(days=395)).replace(day=1)
    end_date = today + _td(days=15)
    num_days = (end_date - start_date).days + 1
    all_dates = [start_date + _td(days=d) for d in range(num_days)]
    date_strs = [d.strftime("%Y-%m-%d") for d in all_dates]
    date_idx = {d.strftime("%Y-%m-%d"): i for i, d in enumerate(all_dates)}

    # Find TTODAY
    today_str = today.strftime("%Y-%m-%d")
    ttoday = date_idx.get(today_str, len(all_dates) - 16)

    # Accumulators: key -> [sw_leads[], qm_leads[], sw_td[], qm_td[]]
    def make_arrays():
        return [
            [0] * num_days,
            [0] * num_days,
            [0] * num_days,
            [0] * num_days,
        ]

    all_data = make_arrays()
    mkt_data = defaultdict(make_arrays)
    dlr_data = defaultdict(make_arrays)

    for r in rows:
        if not r or len(r) < 20:
            continue

        # Lead creation date
        lead_date = serial_to_date(r[16])
        td_book_date = serial_to_date(r[25])

        is_qm = safe_str(r[39]).strip().lower() == "yes" if len(r) > 39 and r[39] else False

        # Dealer and market
        dealer = safe_str(r[2]).replace(" INEOS Grenadier", "").replace(" INEOS", "").strip()
        dealer = dealer.replace(" Grenadier", "").replace(" GRENADIER", "").strip()
        mkt = ""
        if len(r) > 31 and r[31]:
            mkt = safe_str(r[31]).strip()
        if not mkt:
            mkt = lookup_mkt(market_map, dealer)

        # Count brand leads by creation date
        if lead_date:
            ds = lead_date.strftime("%Y-%m-%d")
            di = date_idx.get(ds)
            if di is not None:
                arr_idx = 1 if is_qm else 0
                all_data[arr_idx][di] += 1
                if mkt:
                    mkt_data[mkt][arr_idx][di] += 1
                if dealer:
                    dlr_data[dealer][arr_idx][di] += 1

        # Count TD bookings by booking date
        if td_book_date:
            ds = td_book_date.strftime("%Y-%m-%d")
            di = date_idx.get(ds)
            if di is not None:
                arr_idx = 3 if is_qm else 2
                all_data[arr_idx][di] += 1
                if mkt:
                    mkt_data[mkt][arr_idx][di] += 1
                if dealer:
                    dlr_data[dealer][arr_idx][di] += 1

    # Build TDD dict
    tdd = {"ALL": all_data}
    for mkt, arrs in sorted(mkt_data.items()):
        tdd[mkt] = arrs
    for dlr, arrs in sorted(dlr_data.items()):
        tdd[dlr] = arrs

    return date_strs, tdd, ttoday


# ---------------------------------------------------------------------------
# Pipeline (P_MY25 / P_MY26) from Export data
# ---------------------------------------------------------------------------

def build_PIPELINE(export_rows):
    p25 = {"og": 0, "it": 0, "ap": 0, "ow": 0, "ip": 0, "pl": 0}
    p26 = {"og": 0, "it": 0, "ap": 0, "ow": 0, "ip": 0, "pl": 0}

    for r in export_rows:
        # col 11 = Country Name (same as build_MIG, build_PL_AGE, etc.)
        country = safe_str(r[11]).strip().upper()
        if "UNITED STATES" not in country:
            continue

        # col 7 = Material Desc (e.g. "Grenadier MY26 ...")
        material = safe_str(r[7]).strip().lower()
        is_25 = "25" in material
        is_26 = "26" in material
        if not is_25 and not is_26:
            continue

        tgt = p25 if is_25 else p26
        # col 13 = Vehicle Current Primary Status Text (groups)
        status = safe_str(r[13]).strip().lower()
        channel = safe_str(r[14]).strip()

        if "dealer stock" in status or "7." in status:
            if channel in ("STOCK", "PRIVATE - RETAILER"):
                tgt["og"] += 1
        elif "in-transit" in status or "6." in status:
            tgt["it"] += 1
        elif "at americas port" in status or "5." in status or "at port" in status:
            tgt["ap"] += 1
        elif "on water" in status or "4." in status:
            tgt["ow"] += 1
        elif "built at plant" in status or "in production" in status or "3." in status or "2." in status:
            tgt["ip"] += 1
        elif "preplanning" in status or "planned for transfer" in status or "1." in status:
            tgt["pl"] += 1

    return p25, p26


# ---------------------------------------------------------------------------
# Web Analytics – Engagement
# ---------------------------------------------------------------------------

def build_WEB_ENGAGEMENT(wb):
    rows = read_sheet(wb, "G - Engagement Overview", 2500)

    start_date = datetime(2025, 1, 1)
    daily = []
    for r in rows[9:]:
        if not r or r[0] is None:
            break
        day_idx = vi(r[0])
        dt = start_date + timedelta(days=day_idx)
        if dt.date() > datetime.now().date():
            break
        daily.append({
            "date": dt.strftime("%Y-%m-%d"),
            "all": round(vf(r[1]), 1),
            "org": round(vf(r[2]), 1),
            "paid": round(vf(r[3]), 1),
            "dir": round(vf(r[4]), 1),
        })

    # WEB_MA7: 7-day moving average
    ma7 = []
    for i, d in enumerate(daily):
        window = daily[max(0, i - 6):i + 1]
        ma7.append({
            "date": d["date"],
            "all": round(sum(x["all"] for x in window) / len(window), 1),
            "org": round(sum(x["org"] for x in window) / len(window), 1),
            "paid": round(sum(x["paid"] for x in window) / len(window), 1),
            "dir": round(sum(x["dir"] for x in window) / len(window), 1),
        })

    # WEB_MO: monthly averages
    mo_data = defaultdict(lambda: {"all": [], "org": [], "paid": [], "dir": []})
    for d in daily:
        mo = d["date"][:7]
        mo_data[mo]["all"].append(d["all"])
        mo_data[mo]["org"].append(d["org"])
        mo_data[mo]["paid"].append(d["paid"])
        mo_data[mo]["dir"].append(d["dir"])

    web_mo = []
    for mo in sorted(mo_data.keys()):
        md = mo_data[mo]
        web_mo.append({
            "month": mo,
            "all": round(sum(md["all"]) / len(md["all"]), 1),
            "org": round(sum(md["org"]) / len(md["org"]), 1),
            "paid": round(sum(md["paid"]) / len(md["paid"]), 1),
            "dir": round(sum(md["dir"]) / len(md["dir"]), 1),
            "days": len(md["all"]),
        })

    # WEB_KPI
    cur = web_mo[-1] if web_mo else None
    prev = web_mo[-2] if len(web_mo) > 1 else None
    best = max(web_mo, key=lambda x: x["all"]) if web_mo else None
    worst = min(web_mo, key=lambda x: x["all"]) if web_mo else None

    # R90: average of last 90 days vs prior 90
    last90 = daily[-90:] if len(daily) >= 90 else daily
    prev90 = daily[-180:-90] if len(daily) >= 180 else daily[:len(daily) // 2]
    r90_cur = round(sum(d["all"] for d in last90) / len(last90), 1) if last90 else 0
    r90_prev = round(sum(d["all"] for d in prev90) / len(prev90), 1) if prev90 else 0

    web_kpi = {
        "cur_all": cur["all"] if cur else 0,
        "cur_org": cur["org"] if cur else 0,
        "cur_paid": cur["paid"] if cur else 0,
        "cur_dir": cur["dir"] if cur else 0,
        "cur_month": cur["month"] if cur else "",
        "prev_all": prev["all"] if prev else 0,
        "prev_month": prev["month"] if prev else "",
        "mom_pct": round((cur["all"] - prev["all"]) / prev["all"] * 100, 1) if prev and prev["all"] else 0,
        "best_month": best["month"] if best else "",
        "best_val": best["all"] if best else 0,
        "worst_month": worst["month"] if worst else "",
        "worst_val": worst["all"] if worst else 0,
        "r90_cur": r90_cur,
        "r90_prev": r90_prev,
        "r90_chg": round(r90_cur - r90_prev, 1),
    }

    return ma7, web_mo, web_kpi


# ---------------------------------------------------------------------------
# Web Analytics – Acquisition
# ---------------------------------------------------------------------------

def build_WEB_ACQUISITION(wb):
    rows = read_sheet(wb, "G - Acquisition Overview", 1500)

    start_date = datetime(2025, 1, 1)
    daily = []
    for r in rows[9:]:
        if not r or r[0] is None:
            break
        if not isinstance(r[0], (int, float)):
            break  # Hit the channel section
        day_idx = vi(r[0])
        dt = start_date + timedelta(days=day_idx)
        if dt.date() > datetime.now().date():
            break
        # Note: col order is All, Direct, Organic, Paid
        daily.append({
            "date": dt.strftime("%Y-%m-%d"),
            "all": vi(r[1]),
            "org": vi(r[3]),   # Organic is col 3
            "paid": vi(r[4]),  # Paid is col 4
            "dir": vi(r[2]),   # Direct is col 2
        })

    # WEB_ACQ_MA7
    ma7 = []
    for i, d in enumerate(daily):
        window = daily[max(0, i - 6):i + 1]
        ma7.append({
            "date": d["date"],
            "all": round(sum(x["all"] for x in window) / len(window)),
            "org": round(sum(x["org"] for x in window) / len(window)),
            "paid": round(sum(x["paid"] for x in window) / len(window)),
            "dir": round(sum(x["dir"] for x in window) / len(window)),
        })

    # WEB_ACQ_MO
    mo_data = defaultdict(lambda: {"all": 0, "org": 0, "paid": 0, "dir": 0, "days": 0})
    for d in daily:
        mo = d["date"][:7]
        mo_data[mo]["all"] += d["all"]
        mo_data[mo]["org"] += d["org"]
        mo_data[mo]["paid"] += d["paid"]
        mo_data[mo]["dir"] += d["dir"]
        mo_data[mo]["days"] += 1

    acq_mo = [{"month": mo, **mo_data[mo]} for mo in sorted(mo_data.keys())]

    # WEB_ACQ_KPI
    cur = acq_mo[-1] if acq_mo else None
    prev = acq_mo[-2] if len(acq_mo) > 1 else None
    best = max(acq_mo, key=lambda x: x["all"]) if acq_mo else None
    worst = min(acq_mo, key=lambda x: x["all"]) if acq_mo else None
    total_all = sum(m["all"] for m in acq_mo)
    total_days = sum(m["days"] for m in acq_mo)

    last90 = daily[-90:] if len(daily) >= 90 else daily
    prev90 = daily[-180:-90] if len(daily) >= 180 else daily[:len(daily) // 2]
    r90_cur = round(sum(d["all"] for d in last90) / len(last90)) if last90 else 0
    r90_prev = round(sum(d["all"] for d in prev90) / len(prev90)) if prev90 else 0

    acq_kpi = {
        "cur_all": cur["all"] if cur else 0,
        "cur_org": cur["org"] if cur else 0,
        "cur_paid": cur["paid"] if cur else 0,
        "cur_dir": cur["dir"] if cur else 0,
        "cur_month": cur["month"] if cur else "",
        "cur_days": cur["days"] if cur else 0,
        "prev_all": prev["all"] if prev else 0,
        "prev_month": prev["month"] if prev else "",
        "mom_pct": round((cur["all"] - prev["all"]) / prev["all"] * 100, 1) if prev and prev["all"] else 0,
        "best_month": best["month"] if best else "",
        "best_val": best["all"] if best else 0,
        "worst_month": worst["month"] if worst else "",
        "worst_val": worst["all"] if worst else 0,
        "r90_cur": r90_cur,
        "r90_prev": r90_prev,
        "r90_chg": round((r90_cur - r90_prev) / r90_prev * 100, 1) if r90_prev else 0,
        "total": total_all,
        "avg_daily": round(total_all / total_days) if total_days else 0,
    }

    # WEB_CHANNELS - from the channel section starting around row 919
    channels = []
    found_channels = False
    for r in rows[900:]:
        if not r:
            continue
        label = safe_str(r[0]).strip()
        if "Default Channel Group" in label or "channel group" in label.lower():
            found_channels = True
            continue
        if found_channels:
            if not label or label.startswith('#'):
                break
            sessions = vi(r[1])
            if sessions > 0:
                channels.append({"name": label, "sessions": sessions})

    return ma7, acq_mo, acq_kpi, channels


# ---------------------------------------------------------------------------
# Web Analytics – User Attributes
# ---------------------------------------------------------------------------

def build_WEB_USER_ATTR(wb):
    rows = read_sheet(wb, "G - User Attributes", 92000)

    # Find section starts
    sections = {}
    for i, r in enumerate(rows):
        if not r or not r[0]:
            continue
        label = safe_str(r[0]).strip()
        if label in ("Country", "City", "Language", "Gender", "Age", "Interests category"):
            if label not in sections:
                sections[label] = i

    # Parse a simple 2-col section: name, users
    def parse_section(start_row, max_items=500):
        result = []
        for r in rows[start_row + 1: start_row + max_items + 1]:
            if not r or not r[0]:
                break
            lbl = safe_str(r[0]).strip()
            if not lbl or lbl.startswith('#') or lbl in (
                "Country", "City", "Language", "Gender", "Age",
                "Interests category", "Country ID",
            ):
                break
            users = vi(r[1])
            if users > 0:
                result.append({"name": lbl, "users": users})
        return result

    # Countries
    countries_raw = parse_section(sections.get("Country", 250)) if "Country" in sections else []
    COUNTRY_CODES = {
        "United States": "US", "Germany": "DE", "United Kingdom": "GB", "Australia": "AU",
        "Canada": "CA", "France": "FR", "Saudi Arabia": "SA", "India": "IN",
        "Switzerland": "CH", "Italy": "IT", "Netherlands": "NL", "Japan": "JP",
        "South Africa": "ZA", "Austria": "AT", "Spain": "ES", "Brazil": "BR",
        "United Arab Emirates": "AE", "Sweden": "SE", "Belgium": "BE", "Mexico": "MX",
        "Turkey": "TR", "New Zealand": "NZ", "Ireland": "IE", "Norway": "NO",
        "Denmark": "DK", "Poland": "PL", "Indonesia": "ID", "Portugal": "PT",
        "Thailand": "TH", "Singapore": "SG", "Philippines": "PH", "Malaysia": "MY",
        "Israel": "IL", "Romania": "RO", "Egypt": "EG", "Kuwait": "KW",
        "Czech Republic": "CZ", "Oman": "OM", "South Korea": "KR", "Colombia": "CO",
        "Pakistan": "PK", "Argentina": "AR", "Chile": "CL", "Nigeria": "NG",
        "Morocco": "MA", "Greece": "GR", "Hungary": "HU", "Finland": "FI",
        "Qatar": "QA", "Bahrain": "BH", "Kenya": "KE", "Vietnam": "VN",
        "Peru": "PE", "Bangladesh": "BD", "Ukraine": "UA", "Croatia": "HR",
        "Serbia": "RS", "Slovakia": "SK", "Bulgaria": "BG", "Lithuania": "LT",
        "Jordan": "JO", "Slovenia": "SI", "Estonia": "EE", "Latvia": "LV",
        "Lebanon": "LB", "Iraq": "IQ", "Tunisia": "TN", "Luxembourg": "LU",
        "Ghana": "GH", "Ecuador": "EC", "Tanzania": "TZ", "Puerto Rico": "PR",
        "Taiwan": "TW", "Hong Kong": "HK", "China": "CN", "Russia": "RU",
    }
    ua_countries = [
        {"code": COUNTRY_CODES.get(c["name"], ""), "name": c["name"], "users": c["users"]}
        for c in countries_raw[:50]
    ]

    # Cities
    cities_raw = parse_section(sections.get("City", 976), 1000) if "City" in sections else []
    ua_cities_global = [{"name": c["name"], "users": c["users"]} for c in cities_raw[:50]]

    US_CITIES = {
        "New York", "Los Angeles", "Dallas", "Chicago", "Houston", "Miami", "Ashburn",
        "Atlanta", "Phoenix", "San Diego", "San Francisco", "Denver", "San Antonio",
        "Seattle", "Austin", "Charlotte", "Columbus", "Fort Worth", "Nashville",
        "Portland", "Las Vegas", "Jacksonville", "Oklahoma City", "Memphis", "Louisville",
        "Sacramento", "Indianapolis", "Milwaukee", "Raleigh", "Kansas City", "Minneapolis",
        "Tampa", "St. Louis", "Pittsburgh", "Cincinnati", "Orlando", "Cleveland", "Tucson",
        "Omaha", "Tulsa", "Henderson", "Scottsdale", "Plano", "Arlington", "Irvine",
        "Frisco", "Gilbert", "Chandler", "Boise", "Richmond", "Chesapeake", "Norfolk",
        "Virginia Beach", "Colorado Springs", "Fort Lauderdale", "Washington", "Boston",
        "Philadelphia", "Detroit", "Baltimore", "Salt Lake City", "San Jose", "Honolulu",
        "Albuquerque", "El Paso", "Bakersfield", "Fresno", "Mesa", "Riverside", "Stockton",
        "Corpus Christi", "Lexington", "Anchorage", "Newark", "Greensboro", "Buffalo",
        "Reno", "Madison", "Durham", "Chattanooga", "Knoxville", "Savannah", "Baton Rouge",
    }
    ua_cities_us = [c for c in ua_cities_global if c["name"] in US_CITIES][:50]

    # Languages
    lang_raw = parse_section(sections.get("Language", 90288)) if "Language" in sections else []
    LANG_MAP = {
        "en": "English", "de": "German", "es": "Spanish", "fr": "French",
        "it": "Italian", "ar": "Arabic", "nl": "Dutch", "zh": "Chinese",
        "pt": "Portuguese", "pl": "Polish", "sv": "Swedish", "ru": "Russian",
        "ja": "Japanese", "ko": "Korean", "tr": "Turkish", "th": "Thai",
        "vi": "Vietnamese", "id": "Indonesian", "cs": "Czech", "ro": "Romanian",
        "hu": "Hungarian", "da": "Danish", "fi": "Finnish", "no": "Norwegian",
        "he": "Hebrew", "uk": "Ukrainian", "el": "Greek", "sk": "Slovak",
        "bg": "Bulgarian", "hr": "Croatian", "ms": "Malay", "hi": "Hindi",
        "bn": "Bengali", "ta": "Tamil", "te": "Telugu",
    }
    ua_languages = []
    for l in lang_raw[:20]:
        name = l["name"]
        if len(name) <= 3:
            name = LANG_MAP.get(name.lower().split("-")[0], name)
        ua_languages.append({"name": name, "users": l["users"]})

    # Gender
    gender_raw = parse_section(sections.get("Gender", 89670), 10) if "Gender" in sections else []
    ua_gender = [
        {"label": g["name"].capitalize(), "users": g["users"]}
        for g in gender_raw if g["name"].lower() in ("male", "female")
    ]

    # Age
    age_raw = parse_section(sections.get("Age", 90247), 10) if "Age" in sections else []
    ua_age = [{"bracket": a["name"], "users": a["users"]} for a in age_raw]
    age_order = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
    ua_age.sort(key=lambda a: age_order.index(a["bracket"]) if a["bracket"] in age_order else 99)

    # Interests
    interests_raw = parse_section(sections.get("Interests category", 0), 50) if "Interests category" in sections else []
    ua_interests = []
    for ir in interests_raw[:20]:
        parts = ir["name"].rsplit("/", 1)
        short = parts[-1].strip() if len(parts) > 1 else ir["name"]
        ua_interests.append({"name": short, "full": ir["name"], "users": ir["users"]})

    return ua_countries, ua_cities_global, ua_cities_us, ua_languages, ua_gender, ua_age, ua_interests


# ---------------------------------------------------------------------------
# Web Analytics – Demographics
# ---------------------------------------------------------------------------

def build_WEB_DEMOGRAPHICS(wb):
    rows = read_sheet(wb, "G - Demographics", 1000)

    # Find section starts (header rows with "Country" in col 0)
    section_starts = []
    for i, r in enumerate(rows):
        if r and safe_str(r[0]).strip() == "Country":
            section_starts.append(i)

    def parse_demo_section(start, max_rows=250):
        result = {}
        for r in rows[start + 1:start + max_rows]:
            if not r or not r[0]:
                break
            country = safe_str(r[0]).strip()
            if not country or country == "Country":
                break
            result[country] = {
                "u": vi(r[1]), "nu": vi(r[2]), "es": vi(r[3]),
                "er": round(vf(r[4]) * 100, 1) if r[4] else 0,
                "spu": round(vf(r[5]), 2) if r[5] else 0,
                "aet": round(vf(r[6]), 1) if r[6] else 0,
                "ec": vi(r[7]), "ke": vi(r[8]),
                "ker": round(vf(r[9]) * 100, 2) if r[9] else 0,
            }
        return result

    # Parse all 4 sections
    seg_names = ["all", "dir", "org", "paid"]
    segments = {}
    for idx, start in enumerate(section_starts[:4]):
        seg = seg_names[idx] if idx < len(seg_names) else f"seg{idx}"
        segments[seg] = parse_demo_section(start)

    all_data = segments.get("all", {})

    # DEM_TOP20: top 20 by users from "all" segment
    top20 = sorted(all_data.items(), key=lambda x: -x[1]["u"])[:20]
    dem_top20 = [{"c": c, **d} for c, d in top20]

    # DEM_SCATTER: subset of fields
    dem_scatter = [
        {"c": d["c"], "u": d["u"], "aet": d["aet"], "ker": d["ker"], "er": d["er"]}
        for d in dem_top20
    ]

    # DEM_TOP_KER: top 20 by ker
    top_ker = sorted(all_data.items(), key=lambda x: -x[1]["ker"])[:20]
    dem_top_ker = [{"c": c, **d} for c, d in top_ker]

    # DEM_CHANNEL: combine all segments
    dem_channel = []
    for c, d in top20:
        entry = {"c": c, "all": {"er": d["er"], "aet": d["aet"], "ker": d["ker"], "u": d["u"]}}
        for seg in ["org", "paid", "dir"]:
            if seg in segments and c in segments[seg]:
                sd = segments[seg][c]
                entry[seg] = {"er": sd["er"], "aet": sd["aet"], "ker": sd["ker"]}
            else:
                entry[seg] = {"er": 0, "aet": 0, "ker": 0}
        dem_channel.append(entry)

    return dem_top20, dem_channel, dem_scatter, dem_top_ker


# ---------------------------------------------------------------------------
# Web Analytics – Tech
# ---------------------------------------------------------------------------

def build_WEB_TECH(wb):
    rows = read_sheet(wb, "G - Tech", 32000)

    # Find all section headers
    sections = []
    for i, r in enumerate(rows):
        if r and r[0] and isinstance(r[0], str):
            label = safe_str(r[0]).strip()
            if label in ("Operating system", "Platform / device category", "Browser",
                         "Device category", "Screen resolution", "Device model"):
                sections.append((i, label))

    def parse_section(start, next_start=None, max_items=100):
        result = []
        end = next_start if next_start else start + max_items
        for r in rows[start + 1:end]:
            if not r or not r[0]:
                break
            lbl = safe_str(r[0]).strip()
            if not lbl or lbl.startswith('#') or lbl in (
                "Operating system", "Platform / device category", "Browser",
                "Device category", "Screen resolution", "Device model",
            ):
                break
            users = vi(r[1])
            if users > 0:
                result.append({"name": lbl, "users": users})
        return result

    # Group sections by type
    type_sections = {}
    for idx, (start, label) in enumerate(sections):
        next_start = sections[idx + 1][0] if idx + 1 < len(sections) else len(rows)
        if label not in type_sections:
            type_sections[label] = []
        type_sections[label].append(parse_section(start, next_start))

    # TECH_OS: first segment (All Users)
    os_segs = type_sections.get("Operating system", [[]])
    tech_os = os_segs[0] if os_segs else []

    # TECH_DEV: from "Device category" section
    dev_segs = type_sections.get("Device category", [[]])
    tech_dev = dev_segs[0] if dev_segs else []
    for d in tech_dev:
        d["name"] = d["name"].capitalize() if d["name"].islower() else d["name"]

    # TECH_BROWSER
    br_segs = type_sections.get("Browser", [[]])
    tech_browser = br_segs[0][:10] if br_segs else []

    # TECH_RES: screen resolution with type inference
    res_segs = type_sections.get("Screen resolution", [[]])
    tech_res = []
    for r in (res_segs[0] if res_segs else [])[:30]:
        dims = r["name"].split("x")
        if len(dims) == 2:
            try:
                w, h = int(dims[0]), int(dims[1])
                rtype = "Desktop" if w >= 1024 else ("Tablet" if w >= 600 else "Mobile")
            except ValueError:
                rtype = "Unknown"
        else:
            rtype = "Unknown"
        tech_res.append({"name": r["name"], "users": r["users"], "type": rtype})

    # TECH_DEV_CH: device by channel (all 4 segments)
    tech_dev_ch = []
    if len(dev_segs) >= 4:
        all_d, dir_d, org_d, paid_d = dev_segs[0], dev_segs[1], dev_segs[2], dev_segs[3]
        for d in all_d:
            name = d["name"]
            entry = {"name": name, "all": d["users"]}
            entry["org"] = next((x["users"] for x in org_d if x["name"] == name), 0)
            entry["paid"] = next((x["users"] for x in paid_d if x["name"] == name), 0)
            entry["dir"] = next((x["users"] for x in dir_d if x["name"] == name), 0)
            tech_dev_ch.append(entry)

    # TECH_OS_CH: OS by channel
    tech_os_ch = []
    if len(os_segs) >= 4:
        all_o, dir_o, org_o, paid_o = os_segs[0], os_segs[1], os_segs[2], os_segs[3]
        for o in all_o[:5]:
            name = o["name"]
            entry = {"name": name, "all": o["users"]}
            entry["org"] = next((x["users"] for x in org_o if x["name"] == name), 0)
            entry["paid"] = next((x["users"] for x in paid_o if x["name"] == name), 0)
            entry["dir"] = next((x["users"] for x in dir_o if x["name"] == name), 0)
            tech_os_ch.append(entry)

    return tech_os, tech_dev, tech_browser, tech_res, tech_dev_ch, tech_os_ch


# ---------------------------------------------------------------------------
# Web Analytics – Audiences
# ---------------------------------------------------------------------------

def build_WEB_AUDIENCES(wb):
    rows = read_sheet(wb, "G - Audiences", 200)

    # Find section starts (header rows)
    section_starts = []
    for i, r in enumerate(rows):
        if r and safe_str(r[0]).strip() == "Audience name":
            section_starts.append(i)

    def parse_aud(start, next_start=None):
        result = []
        end = next_start if next_start else start + 100
        for r in rows[start + 1:end]:
            if not r or not r[0]:
                break
            name = safe_str(r[0]).strip()
            if not name or name == "Audience name" or name.startswith('#'):
                break
            result.append({
                "name": name,
                "users": vi(r[1]),
                "new": vi(r[2]),
                "sessions": vi(r[3]),
                "vps": round(vf(r[4]), 1) if r[4] else 0,
                "dur": round(vf(r[5]), 1) if r[5] else 0,
            })
        return result

    segments = []
    for idx, start in enumerate(section_starts):
        next_s = section_starts[idx + 1] if idx + 1 < len(section_starts) else len(rows)
        segments.append(parse_aud(start, next_s))

    # AUD_ALL: first segment
    aud_all = segments[0] if segments else []

    # AUD_CHANNEL: combine segments
    aud_channel = []
    if len(segments) >= 4:
        all_a, dir_a, org_a, paid_a = segments[0], segments[1], segments[2], segments[3]
        for a in all_a:
            name = a["name"]
            entry = {
                "name": name,
                "all": {"name": name, "users": a["users"], "new": a["new"],
                        "sessions": a["sessions"], "vps": a["vps"], "dur": a["dur"]},
            }
            for seg, data in [("org", org_a), ("paid", paid_a), ("dir", dir_a)]:
                match = next((x for x in data if x["name"] == name), None)
                if match:
                    entry[seg] = {"users": match["users"], "sessions": match["sessions"],
                                  "vps": match["vps"], "dur": match["dur"]}
                else:
                    entry[seg] = {"users": 0, "sessions": 0, "vps": 0, "dur": 0}
            aud_channel.append(entry)

    return aud_all, aud_channel


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_refresh(master_path, template_path, output_path, decrypted_path=None):
    """Entry point for programmatic use (e.g. from FastAPI app)."""
    if decrypted_path is None:
        decrypted_path = os.path.join(os.path.dirname(master_path), "master_decrypted.xlsb")
    # Override defaults for this run
    global DEFAULT_DECRYPTED_PATH
    old_dec = DEFAULT_DECRYPTED_PATH
    DEFAULT_DECRYPTED_PATH = decrypted_path
    try:
        import sys as _sys
        _sys.argv = ["", master_path, template_path, output_path]
        main()
    finally:
        DEFAULT_DECRYPTED_PATH = old_dec


def main():
    master_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MASTER_PATH
    html_template_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_TEMPLATE_PATH
    output_path = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_OUTPUT_PATH

    print("Step 1: Decrypt Master File...")
    print(f"  Master: {master_path}")
    print(f"  Template: {html_template_path}")
    print(f"  Output: {output_path}")
    if not os.path.exists(master_path):
        raise FileNotFoundError(f"Master workbook not found: {master_path}")
    if not os.path.exists(html_template_path):
        raise FileNotFoundError(f"HTML template not found: {html_template_path}")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    dec_path = decrypt_master(master_path)

    print("Step 2: Open workbook...")
    wb = open_workbook(dec_path)

    print("Step 3: Read HTML template...")
    with open(html_template_path, "r", encoding="utf-8") as f:
        html = f.read()

    print("Step 4: Build base dashboard data...")
    rs = build_RS(wb)
    if rs:
        html = replace_const(html, "RS", rs)
    else:
        print("  RS: no data found in Retail Sales Report sheet")

    dpd_data = build_DPD(wb)

    market_map = build_mkt_map(wb)

    inv_all, inv_24, inv_25, inv_26 = build_INV(wb, market_map)
    html = replace_const(html, "INV", inv_all)
    html = replace_const(html, "INV_MY24", inv_24)
    html = replace_const(html, "INV_MY25", inv_25)
    html = replace_const(html, "INV_MY26", inv_26)

    html = replace_const(html, "HIST", build_HIST(wb))

    hd = build_HD(wb)
    if hd:
        html = replace_const(html, "HD", hd)

    html = replace_const(html, "OBJ", build_OBJ(wb))

    san_days, san_all, san_fin, san_lease, san_mo = build_SAN(wb)
    if san_days:
        html = replace_const(html, "SAN_DAYS", san_days)
        html = replace_const(html, "SAN_ALL", san_all)
        html = replace_const(html, "SAN_FIN", san_fin)
        html = replace_const(html, "SAN_LEASE", san_lease)
        html = replace_const(html, "SAN_TODAY", len(san_days) - 1)
        html = replace_const(html, "SAN_MO", san_mo)

    lk_all, net_all = build_LK(wb)
    if lk_all:
        html = replace_const(html, "LK_ALL", lk_all)
        html = replace_const(html, "LK_ALL_NET", net_all)

    td_mb, td_mb_dlr, lk_120_net = build_TD_MB_and_LK120(wb)
    if td_mb:
        html = replace_const(html, "TD_MB", td_mb)
    if td_mb_dlr:
        html = replace_const(html, "TD_MB_DLR", td_mb_dlr)
        # Merge TD-to-Sale percentages into DPD dealer rows
        for row in dpd_data:
            dlr_name = row.get("d", "").upper()
            if dlr_name in td_mb_dlr:
                row["tds"] = td_mb_dlr[dlr_name]
    if lk_120_net:
        html = replace_const(html, "LK_120_NET", lk_120_net)

    html = replace_const(html, "DPD", dpd_data)

    print("Step 5: Load export data...")
    export_rows, _headers = load_export_rows(wb)

    # ---------------------------------------------------------------
    # Recompute INV metrics from Export data.
    # The Excel "Dealer Inventory Report" sheet includes all channels
    # (rental, employee, internal fleet, etc.) which inflates OG.
    # Recount from Export using only STOCK / PRIVATE - RETAILER for OG
    # and compute per-MY breakdowns for the full pipeline.
    # ---------------------------------------------------------------
    from datetime import datetime, timedelta as _td
    _today = datetime.today()
    _cur_mo = _today.strftime("%Y-%m")
    _prev_dt = (_today.replace(day=1) - _td(days=1))
    _prev_mo = _prev_dt.strftime("%Y-%m")
    _r90_start = _today - _td(days=90)

    def _norm_dealer(raw):
        d = raw.replace(" INEOS Grenadier", "").replace(" INEOS", "")
        d = d.replace(" Grenadier", "").replace(" GRENADIER", "").strip().upper()
        d = " ".join(w for w in d.split() if w != "GRENADIER")
        return d

    def _detect_my(material):
        if "26" in material: return "26"
        if "25" in material: return "25"
        if "24" in material: return "24"
        return "?"

    # Accumulator: _inv_exp[dealer_upper][(my, body)] -> {og, it, ap, ow, pl, dis_sum, mtd, pm, r90_sales}
    _inv_exp = {}

    for r in export_rows:
        if not r or len(r) < 15:
            continue
        country = safe_str(r[11])
        if not country:
            continue
        cu = country.upper()
        if "UNITED STATES" not in cu and "CANADA" not in cu:
            continue

        dealer = export_dealer(r).upper()
        material = safe_str(r[7])
        body = "q" if "quartermaster" in material.lower() else "s"
        my = _detect_my(material)
        status = safe_str(r[13]).lower()
        channel = safe_str(r[14]).strip()

        if dealer not in _inv_exp:
            _inv_exp[dealer] = {}
        key = (my, body)
        if key not in _inv_exp[dealer]:
            _inv_exp[dealer][key] = {"og": 0, "it": 0, "ap": 0, "ow": 0, "pl": 0,
                                     "dis_sum": 0, "dis_cnt": 0,
                                     "mtd": 0, "pm": 0, "r90": 0}
        bucket = _inv_exp[dealer][key]

        # Pipeline counts (all channels for pipeline statuses)
        if "in-transit" in status or "6." in status:
            bucket["it"] += 1
        elif "at americas port" in status or "5." in status or "at port" in status:
            bucket["ap"] += 1
        elif "on water" in status or "4." in status:
            bucket["ow"] += 1
        elif "built at plant" in status or "in production" in status or "preplanning" in status or "planned for transfer" in status:
            bucket["pl"] += 1
        elif "dealer stock" in status or "7." in status:
            # OG: only retail channels
            if channel in ("STOCK", "PRIVATE - RETAILER"):
                bucket["og"] += 1
                dis = vi(r[57])
                bucket["dis_sum"] += dis
                bucket["dis_cnt"] += 1

        # Sales from handover date (all channels for sales)
        ho_date = serial_to_date(r[51])
        if ho_date:
            ho_mo = ho_date.strftime("%Y-%m")
            if ho_mo == _cur_mo:
                bucket["mtd"] += 1
            if ho_mo == _prev_mo:
                bucket["pm"] += 1
            if ho_date >= _r90_start:
                bucket["r90"] += 1

    def _get_inv_val(dealer_upper, body, my_filter, field):
        d = _inv_exp.get(dealer_upper, {})
        total = 0
        for (my, b), bucket in d.items():
            if b != body:
                continue
            if my_filter and my != my_filter:
                continue
            total += bucket[field]
        return total

    def _get_inv_dol(dealer_upper, body, my_filter):
        d = _inv_exp.get(dealer_upper, {})
        s, c = 0, 0
        for (my, b), bucket in d.items():
            if b != body:
                continue
            if my_filter and my != my_filter:
                continue
            s += bucket["dis_sum"]
            c += bucket["dis_cnt"]
        return round(s / c, 1) if c > 0 else 0

    def _apply_inv(inv_list, my_filter=None):
        for rec in inv_list:
            dk = rec["n"].upper()
            rec["ogS"]  = _get_inv_val(dk, "s", my_filter, "og")
            rec["ogQ"]  = _get_inv_val(dk, "q", my_filter, "og")
            rec["itS"]  = _get_inv_val(dk, "s", my_filter, "it")
            rec["itQ"]  = _get_inv_val(dk, "q", my_filter, "it")
            rec["apS"]  = _get_inv_val(dk, "s", my_filter, "ap")
            rec["apQ"]  = _get_inv_val(dk, "q", my_filter, "ap")
            rec["owS"]  = _get_inv_val(dk, "s", my_filter, "ow")
            rec["owQ"]  = _get_inv_val(dk, "q", my_filter, "ow")
            rec["plS"]  = _get_inv_val(dk, "s", my_filter, "pl")
            rec["plQ"]  = _get_inv_val(dk, "q", my_filter, "pl")
            rec["mtdS"] = _get_inv_val(dk, "s", my_filter, "mtd")
            rec["mtdQ"] = _get_inv_val(dk, "q", my_filter, "mtd")
            rec["pmS"]  = _get_inv_val(dk, "s", my_filter, "pm")
            rec["pmQ"]  = _get_inv_val(dk, "q", my_filter, "pm")
            r90s = _get_inv_val(dk, "s", my_filter, "r90")
            r90q = _get_inv_val(dk, "q", my_filter, "r90")
            rec["r90S"] = round(r90s / 3, 1)
            rec["r90Q"] = round(r90q / 3, 1)
            rec["dsS"]  = _get_inv_dol(dk, "s", my_filter)
            rec["dsQ"]  = _get_inv_dol(dk, "q", my_filter)

    # Apply to all INV lists
    _apply_inv(inv_all)            # All model years
    _apply_inv(inv_25, "25")       # MY25 only
    _apply_inv(inv_26, "26")       # MY26 only
    _apply_inv(inv_24, "24")       # MY24 only

    # Add dealers found in Export but not in DIR (e.g. Canadian dealers)
    _valid_mkts = {"Northeast", "Southeast", "Central", "Western", "Canada", "Mexico"}
    existing_names = {rec["n"].upper() for rec in inv_all}
    for dk in _inv_exp:
        if dk in existing_names or dk in ("", "INEOS CA STOCK"):
            continue
        # Build a display name (title-case the uppercase key)
        disp = dk.title()
        mkt = lookup_mkt(market_map, dk)
        if mkt not in _valid_mkts:
            continue
        def _sum_exp(body, my_filter, field):
            total = 0
            for (my, b), bucket in _inv_exp[dk].items():
                if b != body:
                    continue
                if my_filter and my != my_filter:
                    continue
                total += bucket[field]
            return total
        def _dol_exp(body, my_filter):
            s, c = 0, 0
            for (my, b), bucket in _inv_exp[dk].items():
                if b != body:
                    continue
                if my_filter and my != my_filter:
                    continue
                s += bucket["dis_sum"]; c += bucket["dis_cnt"]
            return round(s / c, 1) if c > 0 else 0
        def _make_rec(my_filter=None):
            ogS = _sum_exp("s", my_filter, "og"); ogQ = _sum_exp("q", my_filter, "og")
            r90s = _sum_exp("s", my_filter, "r90"); r90q = _sum_exp("q", my_filter, "r90")
            return {
                "n": disp, "m": mkt,
                "ogS": ogS, "ogQ": ogQ,
                "my25S": _sum_exp("s", "25", "og") if not my_filter else 0,
                "my25Q": _sum_exp("q", "25", "og") if not my_filter else 0,
                "my26S": _sum_exp("s", "26", "og") if not my_filter else 0,
                "my26Q": _sum_exp("q", "26", "og") if not my_filter else 0,
                "mtdS": _sum_exp("s", my_filter, "mtd"), "mtdQ": _sum_exp("q", my_filter, "mtd"),
                "pmS": _sum_exp("s", my_filter, "pm"), "pmQ": _sum_exp("q", my_filter, "pm"),
                "r90S": round(r90s / 3, 1), "r90Q": round(r90q / 3, 1),
                "dsS": _dol_exp("s", my_filter), "dsQ": _dol_exp("q", my_filter),
                "itS": _sum_exp("s", my_filter, "it"), "itQ": _sum_exp("q", my_filter, "it"),
                "apS": _sum_exp("s", my_filter, "ap"), "apQ": _sum_exp("q", my_filter, "ap"),
                "owS": _sum_exp("s", my_filter, "ow"), "owQ": _sum_exp("q", my_filter, "ow"),
                "plS": _sum_exp("s", my_filter, "pl"), "plQ": _sum_exp("q", my_filter, "pl"),
            }
        rec_all = _make_rec()
        if any(rec_all[k] for k in ("ogS","ogQ","itS","itQ","apS","apQ","owS","owQ","plS","plQ")):
            inv_all.append(rec_all)
        rec_25 = _make_rec("25")
        if rec_25["ogS"] or rec_25["ogQ"]:
            inv_25.append(rec_25)
        rec_26 = _make_rec("26")
        if rec_26["ogS"] or rec_26["ogQ"]:
            inv_26.append(rec_26)
        rec_24 = _make_rec("24")
        if rec_24["ogS"] or rec_24["ogQ"]:
            inv_24.append(rec_24)

    # Re-replace the INV constants with corrected values
    html = replace_const(html, "INV", inv_all)
    html = replace_const(html, "INV_MY24", inv_24)
    html = replace_const(html, "INV_MY25", inv_25)
    html = replace_const(html, "INV_MY26", inv_26)

    print("Step 6: Build pass-3 metrics...")
    html = replace_const(html, "SC_DATA", build_SC(wb, export_rows))

    mig_mo, mig_dlr = build_MIG(export_rows, market_map)
    html = replace_const(html, "MIG_MO", mig_mo)
    html = replace_const(html, "MIG_DLR", mig_dlr)
    html = replace_const(html, "MIG_INV", build_MIG_INV(export_rows))
    html = replace_const(html, "PL_AGE", build_PL_AGE(export_rows))

    # --- MTD_DLR ---
    try:
        mtd_dlr = build_MTD_DLR(wb)
        if mtd_dlr:
            html = replace_const(html, "MTD_DLR", mtd_dlr)
    except Exception as e:
        print(f"  MTD_DLR: {e}")

    # --- Pipeline ---
    try:
        p25, p26 = build_PIPELINE(export_rows)
        html = replace_const(html, "P_MY25", p25)
        html = replace_const(html, "P_MY26", p26)
    except Exception as e:
        print(f"  Pipeline: {e}")

    # --- RSR Retailed Units ---
    try:
        rsr_ret = build_RSR_RETAILED(export_rows, market_map)
        html = replace_const(html, "RSR_RET", rsr_ret)
    except Exception as e:
        print(f"  RSR_RET: {e}")

    # --- PM (Previous Month Results) ---
    try:
        pm_data = build_PM(export_rows, market_map, wb)
        html = replace_const(html, "PM", pm_data)
    except Exception as e:
        print(f"  PM: {e}")
        import traceback; traceback.print_exc()

    # --- CX Scorecard (Customer Reviews) ---
    try:
        cx_data = build_CX(wb, market_map)
        html = replace_const(html, "CX_DATA", cx_data)
    except Exception as e:
        print(f"  CX_DATA: {e}")
        import traceback; traceback.print_exc()

    # --- TDD / TDays / TTODAY (daily lead & TD counts) ---
    try:
        tdays, tdd, ttoday = build_TDD(wb, market_map)
        html = replace_const(html, "TDays", tdays)
        html = replace_const(html, "TDD", tdd)
        html = replace_const(html, "TTODAY", ttoday)
    except Exception as e:
        print(f"  TDD: {e}")
        import traceback; traceback.print_exc()

    # --- Web Analytics ---
    print("Step 6b: Build web analytics...")
    try:
        web_ma7, web_mo, web_kpi = build_WEB_ENGAGEMENT(wb)
        html = replace_const(html, "WEB_MA7", web_ma7)
        html = replace_const(html, "WEB_MO", web_mo)
        html = replace_const(html, "WEB_KPI", web_kpi)
    except Exception as e:
        print(f"  WEB_ENGAGEMENT: {e}")

    try:
        acq_ma7, acq_mo, acq_kpi, channels = build_WEB_ACQUISITION(wb)
        html = replace_const(html, "WEB_ACQ_MA7", acq_ma7)
        html = replace_const(html, "WEB_ACQ_MO", acq_mo)
        html = replace_const(html, "WEB_ACQ_KPI", acq_kpi)
        html = replace_const(html, "WEB_CHANNELS", channels)
    except Exception as e:
        print(f"  WEB_ACQUISITION: {e}")

    try:
        ua_countries, ua_cities_global, ua_cities_us, ua_languages, ua_gender, ua_age, ua_interests = build_WEB_USER_ATTR(wb)
        html = replace_const(html, "UA_COUNTRIES", ua_countries)
        html = replace_const(html, "UA_CITIES_GLOBAL", ua_cities_global)
        html = replace_const(html, "UA_CITIES_US", ua_cities_us)
        html = replace_const(html, "UA_LANGUAGES", ua_languages)
        if ua_gender:
            html = replace_const(html, "UA_GENDER", ua_gender)
        if ua_age:
            html = replace_const(html, "UA_AGE", ua_age)
        if ua_interests:
            html = replace_const(html, "UA_INTERESTS", ua_interests)
    except Exception as e:
        print(f"  WEB_USER_ATTR: {e}")

    try:
        dem_top20, dem_channel, dem_scatter, dem_top_ker = build_WEB_DEMOGRAPHICS(wb)
        html = replace_const(html, "DEM_TOP20", dem_top20)
        html = replace_const(html, "DEM_CHANNEL", dem_channel)
        html = replace_const(html, "DEM_SCATTER", dem_scatter)
        html = replace_const(html, "DEM_TOP_KER", dem_top_ker)
    except Exception as e:
        print(f"  WEB_DEMOGRAPHICS: {e}")

    try:
        tech_os, tech_dev, tech_browser, tech_res, tech_dev_ch, tech_os_ch = build_WEB_TECH(wb)
        html = replace_const(html, "TECH_OS", tech_os)
        html = replace_const(html, "TECH_DEV", tech_dev)
        html = replace_const(html, "TECH_BROWSER", tech_browser)
        html = replace_const(html, "TECH_RES", tech_res)
        html = replace_const(html, "TECH_DEV_CH", tech_dev_ch)
        html = replace_const(html, "TECH_OS_CH", tech_os_ch)
    except Exception as e:
        print(f"  WEB_TECH: {e}")

    try:
        aud_all, aud_channel = build_WEB_AUDIENCES(wb)
        html = replace_const(html, "AUD_ALL", aud_all)
        html = replace_const(html, "AUD_CHANNEL", aud_channel)
    except Exception as e:
        print(f"  WEB_AUDIENCES: {e}")

    print("Step 7: Build compact VEX/TR...")
    vex, vex_d, vex_mkt_dlr = build_VEX_compact(export_rows, market_map)
    html = replace_const(html, "VEX", vex)
    html = replace_const(html, "VEX_D", vex_d)
    html = replace_const(html, "VEX_MKT_DLR", vex_mkt_dlr)

    tr = build_TR_compact(export_rows, market_map)
    og, sales, d_list, b_list, t_list, my_list, mk_list, c_list, mo_list, mtd_days, cur_mo, prev_mo = tr
    html = replace_const(html, "TR_OG", og)
    html = replace_const(html, "TR_SALES", sales)
    html = replace_const(html, "TR_DEALERS", d_list)
    html = replace_const(html, "TR_BODIES", b_list)
    html = replace_const(html, "TR_TRIMS", t_list)
    html = replace_const(html, "TR_MYS", my_list)
    html = replace_const(html, "TR_MKTS", mk_list)
    html = replace_const(html, "TR_CTRYS", c_list)
    html = replace_const(html, "TR_MONTHS", mo_list)
    html = replace_const(html, "TR_MTD_DAYS", mtd_days)
    html = replace_const(html, "TR_CUR_MO", cur_mo)
    html = replace_const(html, "TR_PREV_MO", prev_mo)

    print("Step 8: Write output...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Dashboard written to {output_path}")
    print(f"File size: {len(html):,} bytes")


if __name__ == "__main__":
    main()
