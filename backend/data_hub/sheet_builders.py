"""Sheet Builders — compute pre-formatted sheet data from source DataFrames.

The original Master File has pivot tables and VLOOKUPs that produce
pre-computed sheets (Retail Sales Report, DPD, Objectives, etc.).
These functions replicate that computation from raw source data so the
assembled xlsx has the same content the processor expects.
"""

import os
import re
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

def _safe_str(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ''
    s = str(v).strip()
    return '' if s in ('nan', 'NaT', 'None') else s


def _date_to_serial(d):
    """Convert date to Excel serial number. Always returns float (not int).
    The processor checks isinstance(value, float) so int serials get skipped."""
    if d is None:
        return None
    if isinstance(d, (int, float)):
        if isinstance(d, float) and np.isnan(d):
            return None
        return float(d)
    try:
        if pd.isna(d):
            return None
    except (TypeError, ValueError):
        pass
    try:
        if isinstance(d, str):
            d = pd.to_datetime(d, errors='coerce')
            if pd.isna(d):
                return None
        if hasattr(d, 'toordinal'):
            delta = d - datetime(1899, 12, 30)
            return float(delta.days) + (getattr(delta, 'seconds', 0) / 86400.0)
    except Exception:
        pass
    return None


def _detect_body(material):
    m = str(material).lower()
    if 'quartermaster' in m or ' qm' in m:
        return 'qm'
    if 'svo' in m:
        return 'svo'
    return 'sw'


def _detect_my(material):
    m = str(material)
    if '27' in m: return '27'
    if '26' in m: return '26'
    if '25' in m: return '25'
    if '24' in m: return '24'
    return '?'


def _norm_dealer(name):
    d = str(name).strip()
    d = d.replace(' INEOS Grenadier', '').replace(' INEOS GRENADIER', '')
    d = d.replace(' INEOS', '').replace(' Grenadier', '').replace(' GRENADIER', '')
    d = ' '.join(w for w in d.split() if w.upper() != 'GRENADIER')
    return d.strip()


def _get_country_code(country):
    c = str(country).upper()
    if 'UNITED STATES' in c: return 'US'
    if 'CANADA' in c: return 'CA'
    if 'MEXICO' in c: return 'MX'
    return ''


def _safe_date(val):
    """Safely convert a value to a datetime or return None."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    try:
        dt = pd.to_datetime(val, errors='coerce')
        if pd.isna(dt):
            return None
        return dt
    except Exception:
        return None


def compute_matchback(leads, urban_science):
    """Compute per-dealer matchback ONCE for reuse across all builders.
    Returns dict: dealer_upper → {mb30, mb60, mb90, mb_all, sales}
    """
    result = defaultdict(lambda: {'mb30': 0, 'mb60': 0, 'mb90': 0, 'mb_all': 0, 'sales': 0})

    if leads is None or len(leads) == 0 or urban_science is None or len(urban_science) == 0:
        return result

    def _frags(name):
        n = str(name).strip().upper().split()[-1] if name and str(name).strip() else ''
        return set(n[i:i+4] for i in range(len(n)-3)) if len(n) >= 4 else set()

    def _norm(d):
        d = str(d).replace(' INEOS Grenadier', '').replace(' INEOS', '').strip().upper()
        return ' '.join(w for w in d.split() if w != 'GRENADIER').strip()

    # Index leads by dealer (compute once)
    dlr_leads = defaultdict(list)
    for _, lr in leads.iterrows():
        dk = _norm(_safe_str(lr.get('retailer_name', '')))
        ld = _safe_date(lr.get('start_date', lr.get('created_on', None)))
        cf = _frags(lr.get('customer_name', ''))
        if dk and ld and cf:
            dlr_leads[dk].append((ld, cf))

    # Match each sale to leads
    for _, sr in urban_science.iterrows():
        dk = _norm(_safe_str(sr.get('dealer_name', '')))
        sd = _safe_date(sr.get('sale_date', None))
        bf = _frags(sr.get('customer_last_name', ''))
        if not dk or not sd or not bf:
            continue
        result[dk]['sales'] += 1
        for ld, lf in dlr_leads.get(dk, []):
            diff = (sd - ld).days
            if 0 <= diff <= 365 and (bf & lf):
                if diff <= 30: result[dk]['mb30'] += 1
                if diff <= 60: result[dk]['mb60'] += 1
                if diff <= 90: result[dk]['mb90'] += 1
                result[dk]['mb_all'] += 1
                break

    total_sales = sum(d['sales'] for d in result.values())
    total_mb30 = sum(d['mb30'] for d in result.values())
    print(f"  [Matchback] Computed: {len(dlr_leads)} lead dealers, {len(result)} sale dealers, MB30={total_mb30}/{total_sales}")
    return result


def _classify_bt(channel, billto_val=''):
    """Classify a vehicle as Retail/Fleet/Internal/Enterprise.
    Matches the exact logic in the assembler's Export sheet column 58."""
    NON_RETAIL = {'Fleet', 'Internal', 'Enterprise'}
    if billto_val and billto_val in NON_RETAIL:
        return billto_val
    ch = str(channel).upper().strip()
    if ch == 'RENTAL':
        return 'Fleet'
    elif ch == 'EMPLOYEE':
        return 'Enterprise'
    elif ch in ('INTERNAL FLEET', 'ICO'):
        return 'Internal'
    return 'Retail'


def _build_market_map(sap, template_path=None):
    """Build dealer→market mapping.

    The SAP export has market_area="AMERICAS" for all vehicles — useless for
    regional breakdown. The real mapping comes from:
    1. The Dashboard template's existing DPD/INV constants (from last Master File)
    2. Hardcoded dealer→market extras (same as the processor)
    3. SAP data as last resort

    Returns dict of dealer_name_upper → market region.
    """
    mkt_map = {}

    # 1. Extract from template HTML (most complete source)
    if template_path and os.path.exists(template_path):
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                html = f.read()
            # Extract from DPD constant
            m = re.search(r'const\s+DPD\s*=\s*(\[.*?\]);', html, re.DOTALL)
            if m:
                dpd = json.loads(m.group(1))
                for row in dpd:
                    d = _safe_str(row.get('d', '')).upper()
                    mk = _safe_str(row.get('m', ''))
                    if d and mk and mk != 'TOTAL':
                        mkt_map[d] = mk
            # Extract from INV constant
            m = re.search(r'const\s+INV\s*=\s*(\[.*?\]);', html, re.DOTALL)
            if m:
                inv = json.loads(m.group(1))
                for row in inv:
                    d = _safe_str(row.get('n', '')).upper()
                    mk = _safe_str(row.get('m', ''))
                    if d and mk:
                        mkt_map[d] = mk
            # Extract from MTD_DLR constant
            m = re.search(r'const\s+MTD_DLR\s*=\s*(\[.*?\]);', html, re.DOTALL)
            if m:
                mtd = json.loads(m.group(1))
                for row in mtd:
                    d = _safe_str(row.get('d', '')).upper()
                    mk = _safe_str(row.get('m', ''))
                    if d and mk:
                        mkt_map[d] = mk
            if mkt_map:
                print(f"  [Market Map] Extracted {len(mkt_map)} dealer mappings from template")
        except Exception as e:
            print(f"  [Market Map] Template extraction failed: {e}")

    # 2. Hardcoded extras (same as processor's build_mkt_map extras)
    extras = {
        "MOSSY": "Central", "MOSSY TX": "Central", "MOSSY TEXAS": "Central",
        "MOSSY SD": "Western", "MOSSY SAN DIEGO": "Western",
        "SEWELL": "Central", "SEWELL SA": "Central", "SEWELL SAN ANTONIO": "Central",
        "SEWELL DALLAS": "Central",
        "FELDMAN": "Northeast", "FREEHOLD": "Northeast",
        "RED NOLAND": "Western", "KNAUZ": "Central",
        "MILEONE": "Northeast", "NORTH SHORE": "Northeast",
        "KO": "Northeast", "CROWN DUBLIN": "Northeast",
        "CURRY": "Northeast", "RDS": "Northeast",
        "WARNER": "Southeast", "HOLMAN": "Southeast",
        "REGAL": "Southeast", "CROWN": "Southeast",
        "VICTORY": "Southeast", "CHARLOTTE": "Southeast",
        "HENDRICK": "Southeast", "GREENSBORO": "Southeast",
        "ORLANDO": "Southeast",
        "RTGT": "Western", "ROSEVILLE": "Western",
        "LYLE PEARSON": "Western", "RENO": "Western",
        "LUTHER": "Central",
        "DILAWRI": "Canada", "WEISSACH": "Canada", "CALGARY": "Canada",
        "MONTREAL": "Canada", "UPTOWN TORONTO": "Canada",
        "HERRERA": "Mexico",
        "HERRERA PREMIUM DE MEXICO SA DE CV": "Mexico",
        "INEOS CA STOCK": "Canada",
    }
    for k, v in extras.items():
        if k not in mkt_map:
            mkt_map[k] = v

    # 3. SAP data as fallback (skip "AMERICAS" - it's useless)
    for col in ('market_area', 'region_group'):
        if col in sap.columns:
            for _, r in sap.iterrows():
                dealer = _norm_dealer(r.get('customer_name', '')).upper()
                mkt = _safe_str(r.get(col, ''))
                if dealer and mkt and mkt.upper() != 'AMERICAS' and dealer not in mkt_map:
                    mkt_map[dealer] = mkt

    print(f"  [Market Map] Total: {len(mkt_map)} dealer→market mappings")
    return mkt_map


def _parse_export_rows(sap, handover=None, sales_order=None, campaign_codes=None, template_path=None):
    """Parse SAP export into enriched row dicts matching the Master File's Export sheet."""
    rows = []

    # VIN → handover data
    ho_map = {}
    if handover is not None and len(handover) > 0:
        for _, r in handover.iterrows():
            vin = _safe_str(r.get('vin', '')).upper()
            if len(vin) > 3:
                ho_map[vin] = r

    # VIN → bill-to-dealer
    billto_map = {}
    if sales_order is not None and len(sales_order) > 0:
        for _, r in sales_order.iterrows():
            vin = _safe_str(r.get('vin', '')).upper()
            if len(vin) > 3:
                billto_map[vin] = _safe_str(r.get('bill_to_dealer', r.get('customer_name', '')))

    # VIN → CVP
    cvp_vins = set()
    if campaign_codes is not None and len(campaign_codes) > 0:
        if 'campaign_type' in campaign_codes.columns:
            cvp_vins = set(campaign_codes[campaign_codes['campaign_type'] == 'CVP']['vin'].dropna().astype(str).str.upper())

    # Bill-to category classification (matching assembler logic)
    NON_RETAIL_BT = {'Fleet', 'Internal', 'Enterprise'}

    mkt_map = _build_market_map(sap, template_path)

    for _, r in sap.iterrows():
        vin = _safe_str(r.get('vin', '')).upper()
        material = _safe_str(r.get('material', ''))
        customer = _safe_str(r.get('customer_name', ''))
        country = _safe_str(r.get('country', ''))
        status = _safe_str(r.get('status', '')).lower()
        channel = _safe_str(r.get('channel', ''))

        # Dealer name (bill-to overrides customer)
        bill_to = billto_map.get(vin, '')
        dealer_raw = bill_to if (bill_to and bill_to != 'Not Handed Over') else customer
        dealer = _norm_dealer(dealer_raw)

        # Handover date from handover report
        ho = ho_map.get(vin, None)
        ho_date = None
        rev_rec_date = None
        if ho is not None:
            hd = ho.get('handover_date', None) if isinstance(ho, dict) else getattr(ho, 'handover_date', None)
            ho_date = _safe_date(hd)
            rr = ho.get('rev_rec_date', None) if isinstance(ho, dict) else getattr(ho, 'rev_rec_date', None)
            rev_rec_date = _safe_date(rr)

        # NO fallback to invoice_date — the Master File only uses handover dates
        # from the Handover Report for column 51. Invoice dates would inflate counts.

        # Market
        # Use mkt_map (from template/hardcoded) FIRST, not SAP's market_area which is "AMERICAS"
        market = mkt_map.get(dealer.upper(), '')
        if not market:
            # Fuzzy match: check if dealer name contains or is contained in any key
            for k, v in mkt_map.items():
                if dealer.upper() in k or k in dealer.upper():
                    market = v
                    break
        if not market:
            # Last resort: SAP columns (skip "AMERICAS")
            for col in ('market_area', 'region_group'):
                val = _safe_str(r.get(col, ''))
                if val and val.upper() != 'AMERICAS':
                    market = val
                    break

        rows.append({
            'vin': vin,
            'dealer': dealer,
            'dealer_upper': dealer.upper(),
            'material': material,
            'country': country,
            'country_code': _get_country_code(country),
            'status': status,
            'channel': channel,
            'market': market,
            'body': _detect_body(material),
            'my': _detect_my(material),
            'msrp': r.get('msrp', 0),
            'ho_date': ho_date,
            'is_cvp': vin in cvp_vins,
            'plant': _safe_str(r.get('plant_code', '')),
            'trim': _safe_str(r.get('trim', '')),
            'bt_cat': _classify_bt(channel, billto_map.get(vin, '')),
            'rev_rec_date': rev_rec_date,
            'days_on_lot': (ho_date - rev_rec_date).days if (ho_date and rev_rec_date) else None,
        })

    return rows, mkt_map


# ═══════════════════════════════════════════════════════════════════════
# Retail Sales Report
# ═══════════════════════════════════════════════════════════════════════

def build_retail_sales_sheet(ws, export_rows, mkt_map, objectives=None, template_path=None):
    """Populate Retail Sales Report matching the Master File structure.

    Structure: "Internal/Fleet/Rental" as first region row, then dealer regions,
    then Total. Non-retail vehicles (Fleet/Internal/Enterprise) are separated
    from regional retail counts — matching how build_PM classifies them.
    """
    today = datetime.now()
    cur_month = today.strftime('%Y-%m')
    prev_month = (today.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    prev_year_month = f'{today.year - 1}-{today.strftime("%m")}'
    # Same-day-of-month cutoff so prior MTD compares apples-to-apples:
    # if today is the 7th, only count handovers on day 1-7 of prior month
    # (and prior-year same month).
    cur_day = today.day

    # Region order matches the known-good RS exactly
    region_order = ['Internal/Fleet/Rental', 'Central', 'Western', 'Mexico',
                    'Southeast', 'Northeast', 'Canada']

    # Load per-region objectives from DB first, then template as fallback
    cur_month_num = today.month  # 1-12
    obj_by_region = {}

    # Try DB (editable via admin UI)
    try:
        from app.database import SessionLocal
        from app.models import MonthlyObjective
        db = SessionLocal()
        try:
            rows = db.query(MonthlyObjective).filter(MonthlyObjective.month == cur_month_num).all()
            for r in rows:
                if r.target > 0:
                    obj_by_region[r.region] = r.target
            if obj_by_region:
                print(f"  [RS] Objectives from DB for month {cur_month_num}: {obj_by_region}")
        finally:
            db.close()
    except Exception:
        pass

    # Fallback: extract from template RS constant
    if not obj_by_region and template_path and os.path.exists(template_path):
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                html = f.read()
            m = re.search(r'const\s+RS\s*=\s*(\[.*?\]);', html, re.DOTALL)
            if m:
                for row in json.loads(m.group(1)):
                    if row.get('r') and row.get('obj'):
                        obj_by_region[row['r']] = row['obj']
                print(f"  [RS] Objectives from template: {obj_by_region}")
        except Exception:
            pass

    # ── Count MTD sales: separate retail from fleet/internal/enterprise ──
    region_mtd = defaultdict(lambda: {'sw': 0, 'qm': 0, 'svo': 0, 'cvp': 0})
    nonretail_mtd = {'sw': 0, 'qm': 0, 'svo': 0, 'cvp': 0}
    dealer_data = defaultdict(lambda: {'sw': 0, 'qm': 0, 'svo': 0, 'total': 0,
                                        'pm': 0, 'py': 0, 'cvp': 0, 'market': ''})

    for r in export_rows:
        if r['country_code'] not in ('US', 'CA', 'MX'):
            continue
        if not r['ho_date']:
            continue

        ho_ym = r['ho_date'].strftime('%Y-%m')
        mkt = r['market'] or 'Unknown'
        dk = r['dealer_upper']
        bt = r.get('bt_cat', 'Retail')

        if not dealer_data[dk]['market']:
            dealer_data[dk]['market'] = mkt

        if ho_ym == cur_month:
            if bt != 'Retail':
                nonretail_mtd[r['body']] += 1
                if r['is_cvp']:
                    nonretail_mtd['cvp'] += 1
            else:
                region_mtd[mkt][r['body']] += 1
                if r['is_cvp']:
                    region_mtd[mkt]['cvp'] += 1
            dealer_data[dk][r['body']] += 1
            dealer_data[dk]['total'] += 1
            if r['is_cvp']:
                dealer_data[dk]['cvp'] += 1

        # Same-day-of-month MTD comparison: prior month and prior-year month
        # only count days 1..cur_day so we compare same elapsed window.
        if ho_ym == prev_month and r['ho_date'].day <= cur_day:
            dealer_data[dk]['pm'] += 1
        if ho_ym == prev_year_month and r['ho_date'].day <= cur_day:
            dealer_data[dk]['py'] += 1

    # ── Header rows 0-5 ──
    for i in range(6):
        ws.append([''] * 20)

    # ── Regional summary rows 6-13 ──
    total = {'sw': 0, 'qm': 0, 'svo': 0, 'cvp': 0, 'obj': 0}

    retail_total = sum(d['sw'] + d['qm'] + d['svo'] for d in region_mtd.values())
    nr_total = nonretail_mtd['sw'] + nonretail_mtd['qm'] + nonretail_mtd['svo']
    grand_total = retail_total + nr_total

    print(f"  [RS] MTD: retail={retail_total}, non-retail={nr_total}, total={grand_total}")

    for region in region_order:
        if region == 'Internal/Fleet/Rental':
            d = nonretail_mtd
        else:
            d = region_mtd.get(region, {'sw': 0, 'qm': 0, 'svo': 0, 'cvp': 0})

        sw, qm, svo, cvp_n = d['sw'], d['qm'], d['svo'], d['cvp']
        t = sw + qm + svo
        obj = obj_by_region.get(region, 0)
        po = (t / obj) if obj > 0 else 0

        row = [''] * 20
        row[2] = region
        row[3] = sw
        row[4] = qm
        row[5] = svo
        row[6] = t
        row[7] = obj
        row[8] = po
        row[9] = (t / grand_total) if grand_total > 0 else 0
        row[15] = cvp_n
        ws.append(row)

        total['sw'] += sw; total['qm'] += qm; total['svo'] += svo
        total['cvp'] += cvp_n; total['obj'] += obj

    while ws.max_row < 13:
        ws.append([''] * 20)

    t_total = total['sw'] + total['qm'] + total['svo']
    row = [''] * 20
    row[2] = 'Total'
    row[3] = total['sw']
    row[4] = total['qm']
    row[5] = total['svo']
    row[6] = t_total
    row[7] = total['obj']
    row[8] = (t_total / total['obj']) if total['obj'] > 0 else 0
    row[9] = 1.0
    row[15] = total['cvp']
    ws.append(row)

    # ── Pad to row 22 for MTD_DLR section ──
    while ws.max_row < 22:
        ws.append([''] * 20)

    # ── MTD_DLR rows 22+ ──
    dealer_regions = ['Central', 'Western', 'Mexico', 'Southeast', 'Northeast', 'Canada']
    for region in dealer_regions:
        row = [''] * 20
        row[1] = region
        ws.append(row)

        dealers = [(dk, dd) for dk, dd in dealer_data.items()
                   if dd['market'] == region and dd['total'] > 0]
        dealers.sort(key=lambda x: -x[1]['total'])

        for dk, dd in dealers:
            row = [''] * 20
            row[2] = dk.title()
            row[3] = dd['sw']
            row[4] = dd['qm']
            row[5] = dd['svo']
            row[6] = dd['total']
            row[7] = dd['pm']
            row[8] = dd['py']
            # ppm = current/prev_month ratio (processor * 100)
            row[9] = round(dd['total'] / dd['pm'], 3) if dd['pm'] > 0 else ''
            # ppy = current/prev_year ratio
            row[10] = round(dd['total'] / dd['py'], 3) if dd['py'] > 0 else ''
            row[15] = dd['cvp']
            ws.append(row)


# ═══════════════════════════════════════════════════════════════════════
# Dealer Performance Dashboard (DPD)
# ═══════════════════════════════════════════════════════════════════════

def build_dpd_sheet(ws, export_rows, mkt_map, leads=None, urban_science=None, dealer_mb=None):
    """Populate DPD sheet — exactly 27 columns per dealer row.

    Processor reads rows 3+:
    [0]=market [1]=dealer [2]=handovers(MTD) [3]=CVP [4]=wholesale [5]=W/S gap
    [6]=on-ground [7]=dollar_sales [8]=dollar_count
    [9-14]=monthly sales (6 recent months)
    [15]=R3M_avg [16]=R3M_total [17]=R_leads [18]=R_leads_total
    [19]=TD_booked [20]=TD_total [21]=TD_won [22]=TD_pct
    [23-26]=MB30%/MB60%/MB90%/MB_all_time%
    """
    today = datetime.now()
    cur_month = today.strftime('%Y-%m')
    cur_day = today.day
    prev_month_str = (today.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')

    # Build 6 recent month labels (e.g., for Apr 2026: Oct,Nov,Dec,Jan,Feb,Mar)
    recent_months = []
    for i in range(6, 0, -1):
        dt = today.replace(day=1) - timedelta(days=30 * i)
        recent_months.append(dt.strftime('%Y-%m'))

    prev_3_months = recent_months[-3:]

    # Accumulate per-dealer stats
    stats = defaultdict(lambda: {
        'market': '', 'mtd_ho': 0, 'cvp': 0, 'og': 0,
        'pm_mtd': 0,  # prior-month handovers up to same day-of-month as today
        'monthly': defaultdict(int),  # ym → handovers (full month)
    })

    # Lead stats by dealer
    lead_stats = defaultdict(lambda: {'leads': 0, 'td_booked': 0, 'td_completed': 0})
    if leads is not None and len(leads) > 0:
        for _, lr in leads.iterrows():
            dealer = _norm_dealer(_safe_str(lr.get('retailer_name', ''))).upper()
            if not dealer:
                continue
            ld = _safe_date(lr.get('start_date', lr.get('created_on', None)))
            if ld and ld.strftime('%Y-%m') >= recent_months[0]:
                lead_stats[dealer]['leads'] += 1
            td = _safe_date(lr.get('td_booking_date', None))
            if td:
                lead_stats[dealer]['td_booked'] += 1
            if _safe_str(lr.get('td_completed_flag', '')).lower() in ('yes', 'true', '1'):
                lead_stats[dealer]['td_completed'] += 1

    for r in export_rows:
        if r['country_code'] not in ('US', 'CA', 'MX'):
            continue

        dk = r['dealer_upper']
        if not stats[dk]['market']:
            stats[dk]['market'] = r['market']

        # On-ground
        if ('dealer stock' in r['status'] or '7.' in r['status']):
            if r['channel'] in ('STOCK', 'PRIVATE - RETAILER'):
                stats[dk]['og'] += 1

        # Sales by month
        if r['ho_date']:
            ho_ym = r['ho_date'].strftime('%Y-%m')
            stats[dk]['monthly'][ho_ym] += 1
            if ho_ym == cur_month:
                stats[dk]['mtd_ho'] += 1
                if r['is_cvp']:
                    stats[dk]['cvp'] += 1
            # Same-day-of-month prior MTD for fair comparison
            if ho_ym == prev_month_str and r['ho_date'].day <= cur_day:
                stats[dk]['pm_mtd'] += 1

    # Use pre-computed matchback (or empty default)
    if dealer_mb is None:
        dealer_mb = defaultdict(lambda: {'mb30': 0, 'mb60': 0, 'mb90': 0, 'mb_all': 0, 'sales': 0})

    # ── Header rows (0-2) ──
    ws.append(['Market', 'Dealer', 'Handovers', 'CVP', 'Wholesale', 'Gap',
               'On Ground', 'DollarSales', 'DollarCount'] +
              [f'Mo{i}' for i in range(6)] +
              ['R3M', 'R3M_T', 'Leads', 'Leads_T', 'TD', 'TD_T', 'TD_Won', 'TD%',
               'MB30', 'MB60', 'MB90', 'MB_AT'])
    ws.append([''] * 27)
    ws.append([''] * 27)

    # ── Data rows (3+) sorted by market ──
    region_order = ['Central', 'Southeast', 'Northeast', 'Western', 'Canada', 'Mexico']

    # Group dealers by market, include "Other" for unmapped
    dealers_by_region = defaultdict(list)
    for dk, s in stats.items():
        # Include dealers with any activity (MTD, on-ground, or any historical month)
        has_activity = s['mtd_ho'] > 0 or s['og'] > 0 or sum(s['monthly'].values()) > 0
        if not has_activity:
            continue
        mkt = s['market'] if s['market'] in region_order else 'Other'
        dealers_by_region[mkt].append((dk, s))

    for region in region_order + ['Other']:
        dealers = dealers_by_region.get(region, [])
        dealers.sort(key=lambda x: -x[1]['mtd_ho'])

        for dk, s in dealers:
            # Monthly sales history — use day-of-month-aligned prior MTD
            # so the dashboard's prior-month delta compares the same elapsed
            # window in both months (e.g., Apr 1-7 vs Mar 1-7).
            prev_mo_val = s['pm_mtd']
            mtd_val = s['mtd_ho']

            # R3M (rolling 3-month) - last 3 months
            r3m_vals = [s['monthly'].get(ym, 0) for ym in prev_3_months]
            r3m = round(sum(r3m_vals) / 3, 1) if r3m_vals else 0

            # Prior R3M (the 3 months before that) for trend
            prior_3_months = recent_months[:3]
            prior_r3m_vals = [s['monthly'].get(ym, 0) for ym in prior_3_months]
            prior_r3m = round(sum(prior_r3m_vals) / 3, 1) if prior_r3m_vals else 0

            # H/O Trend = (current R3M / prior R3M) - 1, or just ratio
            ho_trend = round(r3m / prior_r3m, 2) if prior_r3m > 0 else 1.0

            # Lead data
            ld = lead_stats.get(dk, {'leads': 0, 'td_booked': 0, 'td_completed': 0})
            lead_trend = 1.0  # Would need historical lead data
            td_trend = 1.0

            # Processor reads:
            # [2]=ho [3]=cvp [4]=ws [5]=ws_gap [6]=og [7]=dol [8]=dsc
            # [9-12]=sep/oct/nov/dec (older months, 0 for now)
            # [13]=prev [14]=mtd [15]=r3m_avg [16]=ho_trend
            # [17]=r3m_leads [18]=lead_trend [19]=r3m_td [20]=td_trend
            # [21]=td_wknd [22]=td_prog [23-26]=mb30/60/90/at
            row = [''] * 27
            row[0] = region
            row[1] = dk.title()
            row[2] = mtd_val           # handovers MTD
            row[3] = s['cvp']          # CVP
            row[4] = 0                 # wholesale (not tracked)
            row[5] = '1.00:1'          # W/S:H/O gap
            row[6] = s['og']           # on-ground
            row[7] = 0                 # dol (overridden by INV recompute)
            row[8] = 0                 # dollar count
            # Cols 9-12: older monthly history (optional)
            for i, ym in enumerate(recent_months[:4]):
                row[9 + i] = s['monthly'].get(ym, 0)
            row[13] = prev_mo_val      # prev month
            row[14] = mtd_val          # current mtd
            row[15] = r3m              # R3M avg
            row[16] = ho_trend         # H/O trend ratio
            row[17] = ld['leads']      # R3M leads
            row[18] = lead_trend       # lead trend
            row[19] = ld['td_booked']  # R3M TD bookings
            row[20] = td_trend         # TD trend
            row[21] = 0                # TD weekend
            row[22] = 0                # TD program
            # Matchback percentages
            mb = dealer_mb.get(dk, {'mb30': 0, 'mb60': 0, 'mb90': 0, 'mb_all': 0, 'sales': 0})
            ms = mb['sales'] or 1
            row[23] = round(mb['mb30'] / ms, 3)
            row[24] = round(mb['mb60'] / ms, 3)
            row[25] = round(mb['mb90'] / ms, 3)
            row[26] = round(mb['mb_all'] / ms, 3)
            ws.append(row)


# ═══════════════════════════════════════════════════════════════════════
# Dealer Inventory Report
# ═══════════════════════════════════════════════════════════════════════

def build_inventory_sheet(ws, export_rows, mkt_map):
    """Populate Dealer Inventory Report.

    Note: The processor RECOMPUTES INV from Export data (lines 3090-3278),
    so this sheet primarily needs dealer names and markets for the initial
    INV build. The Export-based recomputation overrides most values.

    Processor reads rows 3+: [1]=market, [2]=dealer name
    """
    # Collect unique dealers
    dealers = {}
    for r in export_rows:
        if r['country_code'] not in ('US', 'CA', 'MX'):
            continue
        dk = r['dealer_upper']
        if dk not in dealers:
            dealers[dk] = {'name': r['dealer'], 'market': r['market']}

    # Header rows (0-2)
    ws.append(['', 'Market', 'Dealer'] + [''] * 42)
    ws.append([''] * 45)
    ws.append([''] * 45)

    # Data rows (row 3+)
    region_order = ['Central', 'Southeast', 'Northeast', 'Western', 'Canada', 'Mexico']
    for region in region_order:
        rd = [(dk, d) for dk, d in dealers.items() if d['market'] == region]
        rd.sort(key=lambda x: x[1]['name'])
        for dk, d in rd:
            row = [''] * 45
            row[1] = d['market']
            row[2] = d['name']
            ws.append(row)


# ═══════════════════════════════════════════════════════════════════════
# Objectives
# ═══════════════════════════════════════════════════════════════════════

def build_objectives_sheet(ws, template_path=None):
    """Populate Objectives from existing Dashboard template HTML.

    Processor reads: row 2=US, row 4=Retailer, row 5=Rental, row 6=Fleet,
    row 7=IECP, row 8=Total. Cols 9-20 = 12 monthly values.
    """
    obj_data = {}
    if template_path and os.path.exists(template_path):
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                html = f.read()
            m = re.search(r'const\s+OBJ\s*=\s*(\{.*?\});', html, re.DOTALL)
            if m:
                obj_data = json.loads(m.group(1))
        except Exception as e:
            print(f"  [Objectives] Could not extract from template: {e}")

    cat_rows = {'US': 2, 'Retailer': 4, 'Rental': 5, 'Fleet': 6, 'IECP': 7, 'Total': 8}

    for i in range(15):
        row = [''] * 25
        for cat, row_idx in cat_rows.items():
            if i == row_idx:
                row[0] = cat
                vals = obj_data.get(cat, [0] * 12)
                for j, v in enumerate(vals[:12]):
                    row[9 + j] = int(v) if v else 0
                break
        ws.append(row)

    return obj_data


# ═══════════════════════════════════════════════════════════════════════
# Historical Sales
# ═══════════════════════════════════════════════════════════════════════

def build_historical_sheet(ws, export_rows, mkt_map):
    """Populate Historical Sales.

    Processor reads: Row 1=dates(serial), Row 2=total, Row 3=SW, Row 4=QM,
    Row 5=SVO, Rows 30-68=retail by dealer.
    """
    monthly = defaultdict(lambda: {'sw': 0, 'qm': 0, 'svo': 0, 'total': 0})
    dealer_monthly = defaultdict(lambda: defaultdict(int))

    for r in export_rows:
        if not r['ho_date'] or r['country_code'] not in ('US', 'CA', 'MX'):
            continue
        ym = r['ho_date'].strftime('%Y-%m')
        monthly[ym][r['body']] += 1
        monthly[ym]['total'] += 1
        dealer_monthly[r['dealer_upper']][ym] += 1

    if not monthly:
        for i in range(15):
            ws.append([''] * 15)
        return

    all_months = sorted(monthly.keys())
    ncols = len(all_months) + 2

    # Row 0: empty header
    ws.append([''] * ncols)

    # Row 1: date serials
    date_row = ['', 'Month']
    for ym in all_months:
        y, m = ym.split('-')
        date_row.append(_date_to_serial(datetime(int(y), int(m), 1)))
    ws.append(date_row)

    # Row 2-5: totals by body type
    for label, key in [('Total Retail', 'total'), ('SW', 'sw'), ('QM', 'qm'), ('SVO', 'svo')]:
        ws.append(['', label] + [monthly[ym][key] for ym in all_months])

    # Rows 6-9: padding
    for _ in range(4):
        ws.append([''] * ncols)

    # Row 10-12: wholesale (no data)
    for label in ['Wholesale Total', 'WS SW', 'WS QM']:
        ws.append(['', label] + [0] * len(all_months))

    # Pad to row 29
    while ws.max_row < 30:
        ws.append([''] * ncols)

    # Rows 30+: retail by dealer
    all_dealers = sorted(dealer_monthly.keys())
    for dk in all_dealers[:38]:
        mkt = mkt_map.get(dk, '')
        ws.append([mkt, dk.title()] + [dealer_monthly[dk].get(ym, 0) for ym in all_months])


# ═══════════════════════════════════════════════════════════════════════
# Lead Handling KPIs
# ═══════════════════════════════════════════════════════════════════════

def build_lead_kpis_sheet(ws, leads, mkt_map, urban_science=None, dealer_mb=None):
    """Populate Lead Handling KPIs with matchback percentages.

    Processor reads rows 4+:
    [0]=market [1]=dealer [2]=RBM [3]=leads [4]=contacted [5]=contact%
    [6]=UTC [7]=UTC% [8]=TD_booked [9]=TD_completed [10]=show_rate
    [11]=lead_to_sale% [12]=won [13]=lost [14]=loss_rate
    [15-17]=MB30%/MB60%/MB90%
    Network total row has "network" in col[0] or col[1].
    """
    if leads is None or len(leads) == 0:
        for i in range(5):
            ws.append([''] * 20)
        return

    # Header rows (0-3)
    ws.append(['Market', 'Dealer', 'RBM', 'Leads', 'Contacted', 'Contact%',
               'UTC', 'UTC%', 'TD Booked', 'TD Completed', 'Show Rate',
               'Lead-to-Sale%', 'Won', 'Lost', 'Loss Rate', 'MB30%', 'MB60%', 'MB90%'])
    for _ in range(3):
        ws.append([''] * 20)

    # Per-dealer KPIs
    dk_data = defaultdict(lambda: {
        'market': '', 'leads': 0, 'contacted': 0, 'td_booked': 0,
        'td_completed': 0, 'won': 0, 'lost': 0
    })

    for _, lr in leads.iterrows():
        dealer = _norm_dealer(_safe_str(lr.get('retailer_name', ''))).upper()
        if not dealer:
            continue

        dk_data[dealer]['leads'] += 1
        if not dk_data[dealer]['market']:
            # Use mkt_map FIRST (has correct region), fall back to marketing_unit
            mkt = mkt_map.get(dealer, '')
            if not mkt or mkt.upper() == 'AMERICAS':
                # Fuzzy match
                for k, v in mkt_map.items():
                    if dealer in k or k in dealer:
                        mkt = v
                        break
            if not mkt or mkt.upper() == 'AMERICAS':
                mu = _safe_str(lr.get('marketing_unit', ''))
                if mu and mu.upper() != 'AMERICAS':
                    mkt = mu
            dk_data[dealer]['market'] = mkt

        status = _safe_str(lr.get('retailer_status', lr.get('lead_status', ''))).lower()
        if status in ('contacted', 'won', 'lost', 'qualified', 'under control'):
            dk_data[dealer]['contacted'] += 1

        td_book = _safe_date(lr.get('td_booking_date', None))
        if td_book:
            dk_data[dealer]['td_booked'] += 1

        if _safe_str(lr.get('td_completed_flag', '')).lower() in ('yes', 'true', '1', 'completed'):
            dk_data[dealer]['td_completed'] += 1

        if 'won' in status:
            dk_data[dealer]['won'] += 1
        elif 'lost' in status:
            dk_data[dealer]['lost'] += 1

    # Use pre-computed matchback (or empty default)
    if dealer_mb is None:
        dealer_mb = defaultdict(lambda: {'mb30': 0, 'mb60': 0, 'mb90': 0, 'sales': 0})

    # Write data rows (row 4+) and accumulate network total
    net = {k: 0 for k in ('leads', 'contacted', 'td_booked', 'td_completed', 'won', 'lost',
                           'mb30', 'mb60', 'mb90', 'mb_sales')}
    region_order = ['Central', 'Southeast', 'Northeast', 'Western', 'Canada', 'Mexico']

    # Group dealers by region, but also include "Other" for unmapped
    dealers_by_region = defaultdict(list)
    for dk, kpi in dk_data.items():
        if kpi['leads'] < 1:
            continue  # Skip dealers with no leads
        mkt = kpi['market'] if kpi['market'] in region_order else 'Other'
        dealers_by_region[mkt].append((dk, kpi))

    for region in region_order + ['Other']:
        dealers = dealers_by_region.get(region, [])
        dealers.sort(key=lambda x: -x[1]['leads'])

        for dk, kpi in dealers:
            n = kpi['leads']
            c = kpi['contacted']
            tb = kpi['td_booked']
            tc = kpi['td_completed']
            w = kpi['won']
            lo = kpi['lost']
            mb = dealer_mb.get(dk, {'mb30': 0, 'mb60': 0, 'mb90': 0, 'sales': 0})
            ms = mb['sales'] or 1

            row = [''] * 20
            row[0] = region
            row[1] = dk.title()
            row[2] = ''
            row[3] = n
            row[4] = c
            row[5] = round(c / n, 3) if n > 0 else 0
            row[6] = c
            row[7] = round(c / n, 3) if n > 0 else 0
            row[8] = tb
            row[9] = tc
            row[10] = round(tc / tb, 3) if tb > 0 else 0
            row[11] = round(w / n, 3) if n > 0 else 0
            row[12] = w
            row[13] = lo
            row[14] = round(lo / n, 3) if n > 0 else 0
            row[15] = round(mb['mb30'] / ms, 3)  # MB30%
            row[16] = round(mb['mb60'] / ms, 3)  # MB60%
            row[17] = round(mb['mb90'] / ms, 3)  # MB90%
            ws.append(row)

            for k in ('leads', 'contacted', 'td_booked', 'td_completed', 'won', 'lost'):
                net[k] += kpi[k]
            net['mb30'] += mb['mb30']
            net['mb60'] += mb['mb60']
            net['mb90'] += mb['mb90']
            net['mb_sales'] += mb['sales']

    # Network total row
    ms = net['mb_sales'] or 1
    row = [''] * 20
    row[0] = 'Network'
    row[1] = 'NETWORK TOTAL'
    row[3] = net['leads']
    row[4] = net['contacted']
    row[5] = round(net['contacted'] / net['leads'], 3) if net['leads'] > 0 else 0
    row[6] = net['contacted']
    row[7] = round(net['contacted'] / net['leads'], 3) if net['leads'] > 0 else 0
    row[8] = net['td_booked']
    row[9] = net['td_completed']
    row[10] = round(net['td_completed'] / net['td_booked'], 3) if net['td_booked'] > 0 else 0
    row[11] = round(net['won'] / net['leads'], 3) if net['leads'] > 0 else 0
    row[12] = net['won']
    row[13] = net['lost']
    row[14] = round(net['lost'] / net['leads'], 3) if net['leads'] > 0 else 0
    row[15] = round(net['mb30'] / ms, 3)
    row[16] = round(net['mb60'] / ms, 3)
    row[17] = round(net['mb90'] / ms, 3)
    ws.append(row)
    print(f"  LK MB: {net['mb30']}/{net['mb_sales']}={round(net['mb30']/ms*100,1)}% (30D), {round(net['mb90']/ms*100,1)}% (90D)")


# ═══════════════════════════════════════════════════════════════════════
# Santander sheets
# ═══════════════════════════════════════════════════════════════════════

def build_matchback_sheet(ws, export_rows, leads, urban_science=None, dealer_mb=None):
    """Populate Matchback Report using pre-computed dealer_mb data.
    Also computes R120 brand leads per dealer for TD-to-Sale calculation."""
    if dealer_mb is None:
        dealer_mb = {}

    # Compute R120 leads per dealer (last 120 days)
    today = datetime.now()
    r120_cutoff = today - timedelta(days=120)
    dealer_r120_leads = defaultdict(int)
    dealer_all_leads = defaultdict(int)
    if leads is not None and len(leads) > 0:
        for _, lr in leads.iterrows():
            dealer = _safe_str(lr.get('retailer_name', ''))
            if not dealer:
                continue
            dealer = dealer.replace(' INEOS Grenadier', '').replace(' INEOS', '').strip().upper()
            dealer = ' '.join(w for w in dealer.split() if w != 'GRENADIER').strip()
            if not dealer:
                continue
            dealer_all_leads[dealer] += 1
            ld = _safe_date(lr.get('start_date', lr.get('created_on', None)))
            if ld and ld >= r120_cutoff:
                dealer_r120_leads[dealer] += 1

    all_dealers = sorted(set(list(dealer_mb.keys()) + list(dealer_r120_leads.keys())))

    # Headers (rows 0-2)
    ws.append([0.0] + [''] * 19)  # row 0: numeric (processor skips)
    ws.append(['', 'Retailer', 'R120 Brand Leads', 'All Time Leads', '',
               'R120 Retail Sales', '', 'R30 MB Count', 'R30 MB%',
               'R60 MB Count', 'R60 MB%', 'R90 MB Count', 'R90 MB%',
               'R120 MB Count', 'R120 MB%', 'All Time MB Count', 'All Time MB%'])
    ws.append([''] * 20)

    t = {'sales': 0, 'mb30': 0, 'mb60': 0, 'mb90': 0, 'mb_all': 0}

    for dk in all_dealers:
        if not dk:
            continue
        mb = dealer_mb.get(dk, {'mb30': 0, 'mb60': 0, 'mb90': 0, 'mb_all': 0, 'sales': 0})
        s = mb['sales'] or 1

        row = [''] * 20
        row[1] = dk.title()
        row[2] = dealer_r120_leads.get(dk, 0)  # R120 leads for TD-to-Sale calc
        row[3] = dealer_all_leads.get(dk, 0)   # All-time leads
        row[5] = mb['sales']
        row[7] = mb['mb30']
        row[8] = round(mb['mb30'] / s, 4)
        row[9] = mb['mb60']
        row[10] = round(mb['mb60'] / s, 4)
        row[11] = mb['mb90']
        row[12] = round(mb['mb90'] / s, 4)
        row[13] = mb.get('mb120', mb['mb90'])
        row[14] = round(mb.get('mb120', mb['mb90']) / s, 4)
        row[15] = mb['mb_all']
        row[16] = round(mb['mb_all'] / s, 4)
        ws.append(row)

        for k in t:
            t[k] += mb.get(k, 0)

    # Total row
    s = t['sales'] or 1
    row = [''] * 20
    row[1] = 'Total'
    row[5] = t['sales']
    row[7] = t['mb30']
    row[8] = round(t['mb30'] / s, 4)
    row[9] = t['mb60']
    row[10] = round(t['mb60'] / s, 4)
    row[11] = t['mb90']
    row[12] = round(t['mb90'] / s, 4)
    row[13] = t['mb90']
    row[14] = round(t['mb90'] / s, 4)
    row[15] = t['mb_all']
    row[16] = round(t['mb_all'] / s, 4)
    ws.append(row)

    # Since Inception section
    ws.append([''] * 20)
    ws.append(['', 'Since Inception'] + [''] * 18)
    for dk in all_dealers:
        if not dk:
            continue
        row = [''] * 20
        row[1] = dk.title()
        row[5] = dealer_mb[dk]['sales']
        ws.append(row)

    print(f"  Matchback sheet: {len(all_dealers)} dealers, MB30={t['mb30']}/{t['sales']}")


def build_santander_sheets(wb, cache_dir):
    """Populate Santander sheets from cached JSON data.

    The processor reads:
    - "Santander Report " rows 9+: col[0]=date serial, col[1]=monthly volume
    - "App Report MoM" rows 1+: col[0]=date serial, col[1]=daily total
    - "App Report Finance" rows 1+: col[0]=date serial, col[1]=daily finance
    - "App Report Lease" rows 1+: col[0]=date serial, col[1]=daily lease
    """
    # Try multiple JSON locations — prefer data/ (from upload) over root (from old restore)
    sant_data = None
    for fname in ['data/santander.json', 'data/santander_finance.json', 'data/santander_lease.json', 'santander_latest.json']:
        spath = os.path.join(cache_dir, fname)
        if os.path.exists(spath):
            try:
                with open(spath) as f:
                    candidate = json.load(f)
                # Use the one with the most data (newest upload has more)
                if sant_data is None or len(json.dumps(candidate)) > len(json.dumps(sant_data)):
                    if 'monthly' in candidate or 'daily' in candidate:
                        sant_data = candidate
                        print(f"  Santander data from {fname}: {len(candidate.get('monthly',{}))} months, {len(candidate.get('daily',{}))} days")
            except Exception:
                continue

    if not sant_data:
        return

    # "Santander Report " - monthly pivot (rows 9+)
    monthly = sant_data.get('monthly', {})
    if 'Santander Report ' in wb.sheetnames and monthly:
        ws = wb['Santander Report ']
        while ws.max_row < 9:
            ws.append([''] * 10)
        for ym, volume in sorted(monthly.items()):
            # Convert YYYY-MM to 1st of month serial — must be float for processor
            serial = _date_to_serial(f'{ym}-01')
            if serial:
                ws.append([float(serial), int(volume)])
        print(f"  Santander Report: {len(monthly)} months")

    # Daily data → "App Report MoM" (all), "App Report Finance", "App Report Lease"
    daily = sant_data.get('daily', {})
    daily_finance = sant_data.get('daily_finance', {})
    daily_lease = sant_data.get('daily_lease', {})

    # Try loading separate Finance/Lease JSON from their own upload keys
    for fname, target in [('santander_finance.json', 'finance'), ('santander_lease.json', 'lease')]:
        fpath = os.path.join(cache_dir, 'data', fname)
        if os.path.exists(fpath):
            try:
                with open(fpath) as f:
                    sub_data = json.load(f)
                if target == 'finance':
                    daily_finance = sub_data.get('daily', {})
                    print(f"  Santander Finance: {len(daily_finance)} daily entries")
                else:
                    daily_lease = sub_data.get('daily', {})
                    print(f"  Santander Lease: {len(daily_lease)} daily entries")
            except Exception:
                pass

    # If still no split, estimate from total
    if daily and not daily_finance:
        for date_str, vol in daily.items():
            v = int(vol)
            daily_finance[date_str] = round(v * 0.8)
            daily_lease[date_str] = v - round(v * 0.8)

    for sheet_name, daily_data in [
        ('App Report MoM', daily),
        ('App Report Finance', daily_finance),
        ('App Report Lease', daily_lease),
    ]:
        if sheet_name not in wb.sheetnames:
            ws = wb.create_sheet(sheet_name)
        else:
            ws = wb[sheet_name]

        # Row 0: numeric placeholder (processor skips rows[0], reads rows[1:])
        # Do NOT write string headers — processor stops on isinstance(r[0], str)
        ws.append([0.0, 0])
        if isinstance(daily_data, dict):
            for date_str, vol in sorted(daily_data.items()):
                serial = _date_to_serial(date_str)
                if serial:
                    ws.append([float(serial), int(vol)])
        elif isinstance(daily_data, list):
            for entry in daily_data:
                if isinstance(entry, dict):
                    serial = _date_to_serial(entry.get('date', ''))
                    vol = entry.get('volume', entry.get('count', 0))
                    if serial:
                        ws.append([float(serial), int(vol)])

    if daily:
        print(f"  Santander Daily: {len(daily)} days -> MoM/Finance/Lease sheets")

    # VERIFY: read back what we actually wrote to each sheet
    for sn in ['App Report MoM', 'App Report Finance', 'App Report Lease']:
        if sn in wb.sheetnames:
            ws = wb[sn]
            data_rows = 0
            sample = None
            for i, row in enumerate(ws.iter_rows(values_only=True)):
                if i == 0:
                    continue
                if row and row[0] is not None:
                    try:
                        if float(row[0]) >= 40000:
                            data_rows += 1
                            if sample is None:
                                sample = (row[0], row[1])
                    except (ValueError, TypeError):
                        pass
            print(f"  [Verify] {sn}: {ws.max_row} total rows, {data_rows} valid data rows, sample: {sample}")


# ═══════════════════════════════════════════════════════════════════════
# GA4 Sheets
# ═══════════════════════════════════════════════════════════════════════

def compute_lead_quality(leads, export_rows, template_path=None):
    """Compute Lead Quality metrics from leads data.

    Returns a dict with LQ_PERIODS and LQ_MO data for the Lead Quality page.

    Metrics per period (R90/R120/R180/R365/ALL):
    - t = total leads
    - rep = repeat rate (same customer within period)
    - nph = % missing phone
    - smr = same-month repeat %
    - li = low intent % (no test drive requested)
    - bd = bad/duplicate count
    - bc = bad contact info %
    """
    if leads is None or len(leads) == 0:
        return {'LQ_PERIODS': {}, 'LQ_MO': [], 'LQ_REP_MO': {}}

    today = datetime.now()
    periods = [
        ('R90', today - timedelta(days=90)),
        ('R120', today - timedelta(days=120)),
        ('R180', today - timedelta(days=180)),
        ('R365', today - timedelta(days=365)),
        ('ALL', datetime(2020, 1, 1)),
    ]

    # Build dealer→market map for the retailers section
    dealer_market = {}
    for r in export_rows:
        dk = r['dealer_upper']
        if dk not in dealer_market and r.get('market'):
            dealer_market[dk] = r['market']

    # Prepare leads with dates
    leads_list = []
    for _, lr in leads.iterrows():
        ld = _safe_date(lr.get('start_date', lr.get('created_on', None)))
        if not ld:
            continue
        customer = _safe_str(lr.get('customer_name', ''))
        phone = _safe_str(lr.get('customer_phone', lr.get('customer_mobile', '')))
        email = _safe_str(lr.get('customer_email', ''))
        retailer = _safe_str(lr.get('retailer_name', ''))
        retailer_upper = retailer.replace(' INEOS Grenadier', '').replace(' INEOS', '').strip().upper()
        retailer_upper = ' '.join(w for w in retailer_upper.split() if w != 'GRENADIER').strip()
        td_req = _safe_date(lr.get('td_requested', None))
        td_book = _safe_date(lr.get('td_booking_date', None))
        leads_list.append({
            'date': ld, 'customer': customer.strip().lower(), 'phone': phone,
            'email': email.strip().lower(), 'retailer': retailer_upper,
            'has_td': bool(td_req or td_book),
        })

    def _period_stats(cutoff_date):
        filtered = [l for l in leads_list if l['date'] >= cutoff_date]
        total = len(filtered)
        if total == 0:
            return {'t': 0, 'rep': 0, 'nph': 0, 'smr': 0, 'li': 0, 'bd': 0, 'bc': 0}

        # Count repeats (same phone or email appearing multiple times)
        phone_counts = defaultdict(int)
        email_counts = defaultdict(int)
        for l in filtered:
            if l['phone']:
                phone_counts[l['phone']] += 1
            if l['email']:
                email_counts[l['email']] += 1
        repeats = sum(1 for l in filtered if (l['phone'] and phone_counts[l['phone']] > 1) or (l['email'] and email_counts[l['email']] > 1))

        # Missing phone
        no_phone = sum(1 for l in filtered if not l['phone'] or len(l['phone']) < 7)

        # Same-month repeat (same customer, same month)
        seen_month = defaultdict(set)
        smr_count = 0
        for l in filtered:
            key = l['phone'] or l['email']
            if key:
                mo_key = l['date'].strftime('%Y-%m')
                if key in seen_month[mo_key]:
                    smr_count += 1
                seen_month[mo_key].add(key)

        # Low intent (no TD request)
        low_intent = sum(1 for l in filtered if not l['has_td'])

        # Bad/duplicate (obvious test names)
        bad_patterns = ['test', 'asdf', 'xxx', 'fake', 'duplicate']
        bad = sum(1 for l in filtered if any(p in l['customer'] for p in bad_patterns))

        # Bad contact info (no phone AND no email)
        bad_contact = sum(1 for l in filtered if not l['phone'] and not l['email'])

        return {
            't': total,
            'rep': round(repeats / total * 100, 1),
            'nph': round(no_phone / total * 100, 1),
            'smr': round(smr_count / total * 100, 1),
            'li': round(low_intent / total * 100, 1),
            'bd': bad,
            'bc': round(bad_contact / total * 100, 1),
        }

    def _retailer_stats(cutoff_date):
        filtered = [l for l in leads_list if l['date'] >= cutoff_date]
        # Group by retailer
        by_retailer = defaultdict(list)
        for l in filtered:
            if l['retailer']:
                by_retailer[l['retailer']].append(l)

        result = []
        for retailer, rls in by_retailer.items():
            if len(rls) < 20:
                continue
            total = len(rls)
            phone_counts = defaultdict(int)
            email_counts = defaultdict(int)
            for l in rls:
                if l['phone']:
                    phone_counts[l['phone']] += 1
                if l['email']:
                    email_counts[l['email']] += 1
            repeats = sum(1 for l in rls if (l['phone'] and phone_counts[l['phone']] > 1) or (l['email'] and email_counts[l['email']] > 1))
            no_phone = sum(1 for l in rls if not l['phone'] or len(l['phone']) < 7)

            seen_month = defaultdict(set)
            smr = 0
            for l in rls:
                key = l['phone'] or l['email']
                if key:
                    mo_key = l['date'].strftime('%Y-%m')
                    if key in seen_month[mo_key]:
                        smr += 1
                    seen_month[mo_key].add(key)

            low_intent = sum(1 for l in rls if not l['has_td'])
            bad_patterns = ['test', 'asdf', 'xxx', 'fake']
            bad = sum(1 for l in rls if any(p in l['customer'] for p in bad_patterns))
            bad_contact = sum(1 for l in rls if not l['phone'] and not l['email'])

            rep_pct = round(repeats / total * 100, 1)
            nph_pct = round(no_phone / total * 100, 1)
            smr_pct = round(smr / total * 100, 1)
            li_pct = round(low_intent / total * 100, 1)
            bc_pct = round(bad_contact / total * 100, 1)

            score = max(0, round(100 - (rep_pct * 0.25) - (nph_pct * 0.20) - (smr_pct * 0.20)
                                    - (li_pct * 0.15) - (bad / total * 10) - (bc_pct * 0.10)))

            result.append({
                'd': retailer.title(), 'r': dealer_market.get(retailer, 'Unknown'),
                't': total, 'rep': rep_pct, 'nph': nph_pct, 'smr': smr_pct,
                'li': li_pct, 'bd': bad, 'bc': bc_pct, 'sc': score,
            })
        return result

    lq_periods = {}
    for key, cutoff in periods:
        lq_periods[key] = {
            'net': _period_stats(cutoff),
            'ret': _retailer_stats(cutoff),
        }

    # Monthly trend data
    from collections import OrderedDict
    mo_buckets = OrderedDict()
    for l in leads_list:
        mo = l['date'].strftime('%Y-%m')
        if mo not in mo_buckets:
            mo_buckets[mo] = []
        mo_buckets[mo].append(l)

    lq_mo = []
    for mo in sorted(mo_buckets.keys()):
        rls = mo_buckets[mo]
        total = len(rls)
        if total == 0:
            continue
        phone_counts = defaultdict(int)
        for l in rls:
            if l['phone']:
                phone_counts[l['phone']] += 1
        repeats = sum(1 for l in rls if l['phone'] and phone_counts[l['phone']] > 1)
        no_phone = sum(1 for l in rls if not l['phone'])
        low_intent = sum(1 for l in rls if not l['has_td'])
        bad_contact = sum(1 for l in rls if not l['phone'] and not l['email'])

        lq_mo.append({
            'm': mo,
            'rep': round(repeats / total * 100, 1),
            'nph': round(no_phone / total * 100, 1),
            'smr': 0,  # Simplified
            'li': round(low_intent / total * 100, 1),
            'bc': round(bad_contact / total * 100, 1),
        })

    return {
        'LQ_PERIODS': lq_periods,
        'LQ_MO': lq_mo,
        'LQ_REP_MO': {},  # Placeholder
    }


def build_ga4_sheet_formatted(ws, ga4_df, ga4_type):
    """Write GA4 data in processor-expected format.

    Engagement/Acquisition: 9 header rows, then col[0]=day_index (days since 2025-01-01),
    col[1-4]=metric values.
    User Attributes: section headers (Country/City/Language/Gender/Age/Interests category)
        followed by name, users rows.
    Demographics: "Country" header rows, followed by country, u, nu, es, er, spu, aet, ec, ke, ker rows.
    Tech: section headers (Operating system / Device category / Browser / Screen resolution)
        followed by name, users rows.
    Audiences: "Audience name" header rows, followed by name, users, new, sessions, vps, dur rows.
    """
    if ga4_df is None or len(ga4_df) == 0:
        for i in range(10):
            ws.append([''] * 10)
        return

    # 9 header rows for all types
    for i in range(9):
        ws.append([''] * 10)

    cols = list(ga4_df.columns)

    # ─── Engagement / Acquisition ─────────────────────────────────────────
    # Both reports come pivoted (date × sessionDefaultChannelGroup) and need
    # to be reshaped so each date becomes ONE row with 4 channel columns
    # in the order the processor expects.
    #   Engagement processor reads: col1=all, col2=org, col3=paid, col4=dir
    #     (sessions/day average per channel)
    #   Acquisition processor reads: col1=all, col2=dir, col3=org, col4=paid
    #     (raw daily session totals)
    if ga4_type in ('ga4_engagement', 'ga4_acquisition'):
        start_date = datetime(2025, 1, 1)
        date_col = _find_col(cols, ['date'])
        chan_col = _find_col(cols, ['sessionDefaultChannelGroup', 'channelGroup'])
        sess_col = _find_col(cols, ['sessions'])

        if not date_col or not sess_col:
            return

        # Normalize date to datetime
        df = ga4_df.copy()
        df['_dt'] = pd.to_datetime(df[date_col], errors='coerce', format='%Y%m%d')
        # Fallback for ISO format
        mask = df['_dt'].isna()
        if mask.any():
            df.loc[mask, '_dt'] = pd.to_datetime(df.loc[mask, date_col], errors='coerce')
        df = df.dropna(subset=['_dt'])
        df = df[df['_dt'] >= start_date]
        df = df[df['_dt'] <= pd.Timestamp(datetime.now().date())]

        # Map GA4 channel labels into 4 buckets: org, paid, dir, other
        def bucket(label):
            if not isinstance(label, str):
                return 'other'
            l = label.lower()
            if 'organic' in l:
                return 'org'
            if 'paid' in l or 'cpc' in l or 'display' in l or 'video' in l:
                return 'paid'
            if 'direct' in l:
                return 'dir'
            return 'other'

        if chan_col:
            df['_bucket'] = df[chan_col].apply(bucket)
        else:
            df['_bucket'] = 'other'

        if ga4_type == 'ga4_engagement':
            # Engagement uses sessions per day (one number per channel per day)
            # Pivot: date × bucket → sessions
            pivot = df.groupby(['_dt', '_bucket'])[sess_col].sum().unstack(fill_value=0)
            for b in ['org', 'paid', 'dir', 'other']:
                if b not in pivot.columns:
                    pivot[b] = 0
            pivot['all'] = pivot[['org', 'paid', 'dir', 'other']].sum(axis=1)

            for dt, row in pivot.iterrows():
                day_idx = (dt.date() - start_date.date()).days
                if day_idx < 0:
                    continue
                ws.append([
                    day_idx,
                    float(row['all']),
                    float(row['org']),
                    float(row['paid']),
                    float(row['dir']),
                ])

        else:  # ga4_acquisition
            # Acquisition: write daily session totals + Default Channel Group section
            pivot = df.groupby(['_dt', '_bucket'])[sess_col].sum().unstack(fill_value=0)
            for b in ['org', 'paid', 'dir', 'other']:
                if b not in pivot.columns:
                    pivot[b] = 0
            pivot['all'] = pivot[['org', 'paid', 'dir', 'other']].sum(axis=1)

            row_count = 0
            for dt, row in pivot.iterrows():
                day_idx = (dt.date() - start_date.date()).days
                if day_idx < 0:
                    continue
                # Note column order: all, dir, org, paid (matches acq processor)
                ws.append([
                    day_idx,
                    float(row['all']),
                    float(row['dir']),
                    float(row['org']),
                    float(row['paid']),
                ])
                row_count += 1

            # Pad with empty rows so the channel section starts around row 919
            target_section_row = 919
            current_row = 9 + row_count + 1  # 9 header + data + 1
            pad = max(0, target_section_row - current_row)
            for _ in range(pad):
                ws.append([''])

            # Write Default Channel Group section
            ws.append(['Default Channel Group'])
            if chan_col:
                ch_totals = df.groupby(chan_col)[sess_col].sum().sort_values(ascending=False)
                for name, total in ch_totals.items():
                    if total > 0 and name and name != '(not set)':
                        ws.append([str(name), float(total)])
        return

    # ─── User Attributes (Country / City / Language / Gender / Age / Interests) ───
    if ga4_type == 'ga4_user_attributes':
        users_col = _find_col(cols, ['totalUsers', 'users', 'activeUsers'])

        # Country
        if 'country' in cols and users_col:
            agg = ga4_df.groupby('country', as_index=False)[users_col].sum()
            agg = agg.sort_values(users_col, ascending=False)
            ws.append(['Country'])
            for _, r in agg.iterrows():
                name = str(r['country']).strip()
                if name and name != '(not set)':
                    ws.append([name, float(r[users_col])])
            ws.append([''])

        # City
        if 'city' in cols and users_col:
            agg = ga4_df.groupby('city', as_index=False)[users_col].sum()
            agg = agg.sort_values(users_col, ascending=False).head(1000)
            ws.append(['City'])
            for _, r in agg.iterrows():
                name = str(r['city']).strip()
                if name and name != '(not set)':
                    ws.append([name, float(r[users_col])])
            ws.append([''])

        # Language
        if 'language' in cols and users_col:
            agg = ga4_df.groupby('language', as_index=False)[users_col].sum()
            agg = agg.sort_values(users_col, ascending=False).head(50)
            ws.append(['Language'])
            for _, r in agg.iterrows():
                name = str(r['language']).strip()
                if name and name != '(not set)':
                    ws.append([name, float(r[users_col])])
            ws.append([''])

        # Gender
        if 'userGender' in cols and users_col:
            agg = ga4_df.groupby('userGender', as_index=False)[users_col].sum()
            ws.append(['Gender'])
            for _, r in agg.iterrows():
                name = str(r['userGender']).strip()
                if name and name not in ('(not set)', 'unknown'):
                    ws.append([name, float(r[users_col])])
            ws.append([''])

        # Age
        if 'userAgeBracket' in cols and users_col:
            agg = ga4_df.groupby('userAgeBracket', as_index=False)[users_col].sum()
            ws.append(['Age'])
            for _, r in agg.iterrows():
                name = str(r['userAgeBracket']).strip()
                if name and name not in ('(not set)', 'unknown'):
                    ws.append([name, float(r[users_col])])
            ws.append([''])

        # Interests
        if 'interests' in cols and users_col:
            agg = ga4_df.groupby('interests', as_index=False)[users_col].sum()
            agg = agg.sort_values(users_col, ascending=False).head(50)
            ws.append(['Interests category'])
            for _, r in agg.iterrows():
                name = str(r['interests']).strip()
                if name and name != '(not set)':
                    ws.append([name, float(r[users_col])])
            ws.append([''])
        return

    # ─── Demographics (4 sections by channel) ─────────────────────────────
    if ga4_type == 'ga4_demographics':
        users_col = _find_col(cols, ['totalUsers', 'users', 'activeUsers'])
        new_col = _find_col(cols, ['newUsers'])
        sess_col = _find_col(cols, ['sessions'])
        eng_sess_col = _find_col(cols, ['engagedSessions'])
        eng_rate_col = _find_col(cols, ['engagementRate'])
        spu_col = _find_col(cols, ['sessionsPerUser'])
        aet_col = _find_col(cols, ['averageSessionDuration', 'userEngagementDuration', 'engagementTime'])
        ec_col = _find_col(cols, ['eventCount'])
        ke_col = _find_col(cols, ['keyEvents', 'conversions'])
        ker_col = _find_col(cols, ['keyEventRate', 'sessionConversionRate'])

        if 'country' not in cols or not users_col:
            return

        # Group by channel if available
        chan_col = _find_col(cols, ['sessionDefaultChannelGroup', 'channelGroup'])
        if chan_col:
            channel_filters = [
                ('all', None),
                ('dir', 'Direct'),
                ('org', 'Organic Search'),
                ('paid', 'Paid Search'),
            ]
        else:
            channel_filters = [('all', None)]

        for seg_name, chan_val in channel_filters:
            if chan_val is None:
                seg_df = ga4_df
            else:
                seg_df = ga4_df[ga4_df[chan_col] == chan_val]
            if len(seg_df) == 0:
                ws.append(['Country'])
                ws.append([''])
                continue

            agg_dict = {users_col: 'sum'}
            for c in [new_col, sess_col, eng_sess_col, ec_col, ke_col]:
                if c:
                    agg_dict[c] = 'sum'
            for c in [eng_rate_col, spu_col, aet_col, ker_col]:
                if c:
                    agg_dict[c] = 'mean'

            agg = seg_df.groupby('country', as_index=False).agg(agg_dict)
            agg = agg.sort_values(users_col, ascending=False).head(250)

            ws.append(['Country'])
            for _, r in agg.iterrows():
                name = str(r['country']).strip()
                if not name or name == '(not set)':
                    continue
                ws.append([
                    name,
                    float(r[users_col]) if users_col else 0,
                    float(r[new_col]) if new_col else 0,
                    float(r[eng_sess_col]) if eng_sess_col else 0,
                    float(r[eng_rate_col]) if eng_rate_col else 0,
                    float(r[spu_col]) if spu_col else 0,
                    float(r[aet_col]) if aet_col else 0,
                    float(r[ec_col]) if ec_col else 0,
                    float(r[ke_col]) if ke_col else 0,
                    float(r[ker_col]) if ker_col else 0,
                ])
            ws.append([''])
        return

    # ─── Tech (Operating system / Device category / Browser / Screen resolution) ───
    if ga4_type == 'ga4_tech':
        users_col = _find_col(cols, ['totalUsers', 'users', 'activeUsers'])
        if not users_col:
            return

        chan_col = _find_col(cols, ['sessionDefaultChannelGroup', 'channelGroup'])
        if chan_col:
            channel_filters = [
                ('all', None),
                ('dir', 'Direct'),
                ('org', 'Organic Search'),
                ('paid', 'Paid Search'),
            ]
        else:
            channel_filters = [('all', None)]

        def write_dim_section(label, col_name, capitalize=False):
            if col_name not in cols:
                return
            for seg_name, chan_val in channel_filters:
                if chan_val is None:
                    seg_df = ga4_df
                else:
                    seg_df = ga4_df[ga4_df[chan_col] == chan_val]
                if len(seg_df) == 0:
                    ws.append([label])
                    ws.append([''])
                    continue
                agg = seg_df.groupby(col_name, as_index=False)[users_col].sum()
                agg = agg.sort_values(users_col, ascending=False).head(100)
                ws.append([label])
                for _, r in agg.iterrows():
                    name = str(r[col_name]).strip()
                    if not name or name == '(not set)':
                        continue
                    # Pre-capitalize device names so processor's mutating capitalization
                    # doesn't break cross-segment lookups (mobile vs Mobile etc.)
                    if capitalize and name.islower():
                        name = name.capitalize()
                    ws.append([name, float(r[users_col])])
                ws.append([''])

        write_dim_section('Operating system', 'operatingSystem')
        write_dim_section('Device category', 'deviceCategory', capitalize=True)
        write_dim_section('Browser', 'browser')
        write_dim_section('Screen resolution', 'screenResolution')
        return

    # ─── Audiences ─────────────────────────────────────────────────────────
    if ga4_type == 'ga4_audiences':
        users_col = _find_col(cols, ['totalUsers', 'users', 'activeUsers'])
        new_col = _find_col(cols, ['newUsers'])
        sess_col = _find_col(cols, ['sessions'])
        vps_col = _find_col(cols, ['screenPageViewsPerSession', 'viewsPerSession'])
        dur_col = _find_col(cols, ['averageSessionDuration', 'userEngagementDuration'])

        if 'audienceName' not in cols or not users_col:
            return

        chan_col = _find_col(cols, ['sessionDefaultChannelGroup', 'channelGroup'])
        if chan_col:
            channel_filters = [
                ('all', None),
                ('dir', 'Direct'),
                ('org', 'Organic Search'),
                ('paid', 'Paid Search'),
            ]
        else:
            channel_filters = [('all', None)]

        for seg_name, chan_val in channel_filters:
            if chan_val is None:
                seg_df = ga4_df
            else:
                seg_df = ga4_df[ga4_df[chan_col] == chan_val]
            if len(seg_df) == 0:
                ws.append(['Audience name'])
                ws.append([''])
                continue

            agg_dict = {users_col: 'sum'}
            for c in [new_col, sess_col]:
                if c:
                    agg_dict[c] = 'sum'
            for c in [vps_col, dur_col]:
                if c:
                    agg_dict[c] = 'mean'

            agg = seg_df.groupby('audienceName', as_index=False).agg(agg_dict)
            agg = agg.sort_values(users_col, ascending=False).head(100)

            ws.append(['Audience name'])
            for _, r in agg.iterrows():
                name = str(r['audienceName']).strip()
                if not name or name == '(not set)':
                    continue
                ws.append([
                    name,
                    float(r[users_col]) if users_col else 0,
                    float(r[new_col]) if new_col else 0,
                    float(r[sess_col]) if sess_col else 0,
                    float(r[vps_col]) if vps_col else 0,
                    float(r[dur_col]) if dur_col else 0,
                ])
            ws.append([''])
        return

    # Fallback: generic dump
    for _, r in ga4_df.iterrows():
        ws.append([r.get(c, '') for c in cols])


def _find_col(cols, candidates):
    """Return first matching column name from candidates (case-insensitive)."""
    cols_lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None
