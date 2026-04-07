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
    """Convert date to Excel serial number."""
    if d is None:
        return None
    if isinstance(d, (int, float)):
        if np.isnan(d):
            return None
        return d
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
            return delta.days + (getattr(delta, 'seconds', 0) / 86400.0)
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
        if ho is not None:
            hd = ho.get('handover_date', None) if isinstance(ho, dict) else getattr(ho, 'handover_date', None)
            ho_date = _safe_date(hd)

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

        if ho_ym == prev_month:
            dealer_data[dk]['pm'] += 1
        if ho_ym == prev_year_month:
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
            row[15] = dd['cvp']
            ws.append(row)


# ═══════════════════════════════════════════════════════════════════════
# Dealer Performance Dashboard (DPD)
# ═══════════════════════════════════════════════════════════════════════

def build_dpd_sheet(ws, export_rows, mkt_map, leads=None, urban_science=None):
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

    # Build 6 recent month labels (e.g., for Apr 2026: Oct,Nov,Dec,Jan,Feb,Mar)
    recent_months = []
    for i in range(6, 0, -1):
        dt = today.replace(day=1) - timedelta(days=30 * i)
        recent_months.append(dt.strftime('%Y-%m'))

    prev_3_months = recent_months[-3:]

    # Accumulate per-dealer stats
    stats = defaultdict(lambda: {
        'market': '', 'mtd_ho': 0, 'cvp': 0, 'og': 0,
        'monthly': defaultdict(int),  # ym → handovers
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

    # Compute per-dealer matchback (same logic as LK sheet)
    dealer_mb = defaultdict(lambda: {'mb30': 0, 'mb60': 0, 'mb90': 0, 'mb_all': 0, 'sales': 0})
    if urban_science is not None and len(urban_science) > 0 and leads is not None and len(leads) > 0:
        def _dpd_frags(name):
            n = str(name).strip().upper().split()[-1] if name and str(name).strip() else ''
            return set(n[i:i+4] for i in range(len(n)-3)) if len(n) >= 4 else set()

        def _dpd_norm(d):
            d = str(d).replace(' INEOS Grenadier','').replace(' INEOS','').strip().upper()
            return ' '.join(w for w in d.split() if w != 'GRENADIER').strip()

        dlr_lead_idx = defaultdict(list)
        for _, lr in leads.iterrows():
            dk = _dpd_norm(_safe_str(lr.get('retailer_name', '')))
            ld = _safe_date(lr.get('start_date', lr.get('created_on', None)))
            cf = _dpd_frags(lr.get('customer_name', ''))
            if dk and ld and cf:
                dlr_lead_idx[dk].append((ld, cf))

        for _, sr in urban_science.iterrows():
            dk = _dpd_norm(_safe_str(sr.get('dealer_name', '')))
            sd = _safe_date(sr.get('sale_date', None))
            bf = _dpd_frags(sr.get('customer_last_name', ''))
            if not dk or not sd or not bf:
                continue
            dealer_mb[dk]['sales'] += 1
            for ld, lf in dlr_lead_idx.get(dk, []):
                diff = (sd - ld).days
                if 0 <= diff <= 365 and (bf & lf):
                    if diff <= 30: dealer_mb[dk]['mb30'] += 1
                    if diff <= 60: dealer_mb[dk]['mb60'] += 1
                    if diff <= 90: dealer_mb[dk]['mb90'] += 1
                    dealer_mb[dk]['mb_all'] += 1
                    break

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
    for region in region_order:
        dealers = [(dk, s) for dk, s in stats.items()
                   if s['market'] == region and (s['mtd_ho'] > 0 or s['og'] > 0)]
        dealers.sort(key=lambda x: -x[1]['mtd_ho'])

        for dk, s in dealers:
            # Monthly values for 6 recent months
            mo_vals = [s['monthly'].get(ym, 0) for ym in recent_months]

            # R3M (rolling 3-month average)
            r3m_vals = [s['monthly'].get(ym, 0) for ym in prev_3_months]
            r3m = round(sum(r3m_vals) / 3, 1) if r3m_vals else 0
            r3m_total = sum(r3m_vals)

            # Lead data
            ld = lead_stats.get(dk, {'leads': 0, 'td_booked': 0, 'td_completed': 0})

            row = [''] * 27
            row[0] = region
            row[1] = dk.title()
            row[2] = s['mtd_ho']
            row[3] = s['cvp']
            row[4] = 0  # wholesale
            row[5] = '1.00:1'  # gap
            row[6] = s['og']
            row[7] = 0  # dollar sales
            row[8] = 0  # dollar count
            for i, mv in enumerate(mo_vals):
                row[9 + i] = mv
            row[15] = r3m
            row[16] = r3m_total
            row[17] = ld['leads']
            row[18] = ld['leads']  # total leads
            row[19] = ld['td_booked']
            row[20] = ld['td_booked']
            row[21] = ld['td_completed']
            row[22] = round(ld['td_completed'] / ld['td_booked'], 3) if ld['td_booked'] > 0 else 0
            # Matchback percentages from Urban Science + leads matching
            mb = dealer_mb.get(dk, {'mb30': 0, 'mb60': 0, 'mb90': 0, 'mb_all': 0, 'sales': 0})
            ms = mb['sales'] or 1
            row[23] = round(mb['mb30'] / ms, 3)   # MB30% (decimal, processor * 100)
            row[24] = round(mb['mb60'] / ms, 3)   # MB60%
            row[25] = round(mb['mb90'] / ms, 3)   # MB90%
            row[26] = round(mb['mb_all'] / ms, 3) # MB all-time%
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

def build_lead_kpis_sheet(ws, leads, mkt_map, urban_science=None):
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
            mkt = _safe_str(lr.get('marketing_unit', ''))
            if not mkt:
                mkt = mkt_map.get(dealer, '')
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

    # Compute per-dealer matchback from Urban Science buyer names
    dealer_mb = defaultdict(lambda: {'mb30': 0, 'mb60': 0, 'mb90': 0, 'sales': 0})
    if urban_science is not None and len(urban_science) > 0 and leads is not None and len(leads) > 0:
        def _mb_frags(name):
            n = str(name).strip().upper().split()[-1] if name and str(name).strip() else ''
            return set(n[i:i+4] for i in range(len(n)-3)) if len(n) >= 4 else set()

        def _mb_norm(d):
            d = str(d).replace(' INEOS Grenadier','').replace(' INEOS','').strip().upper()
            return ' '.join(w for w in d.split() if w != 'GRENADIER').strip()

        # Index leads by dealer
        dealer_lead_index = defaultdict(list)
        for _, lr in leads.iterrows():
            dk = _mb_norm(_safe_str(lr.get('retailer_name', '')))
            ld = _safe_date(lr.get('start_date', lr.get('created_on', None)))
            cfrags = _mb_frags(lr.get('customer_name', ''))
            if dk and ld and cfrags:
                dealer_lead_index[dk].append((ld, cfrags))

        for _, sr in urban_science.iterrows():
            dk = _mb_norm(_safe_str(sr.get('dealer_name', '')))
            sd = _safe_date(sr.get('sale_date', None))
            bfrags = _mb_frags(sr.get('customer_last_name', ''))
            if not dk or not sd or not bfrags:
                continue
            dealer_mb[dk]['sales'] += 1
            for ld, lfrags in dealer_lead_index.get(dk, []):
                diff = (sd - ld).days
                if 0 <= diff <= 90 and (bfrags & lfrags):
                    if diff <= 30: dealer_mb[dk]['mb30'] += 1
                    if diff <= 60: dealer_mb[dk]['mb60'] += 1
                    dealer_mb[dk]['mb90'] += 1
                    break

    # Write data rows (row 4+) and accumulate network total
    net = {k: 0 for k in ('leads', 'contacted', 'td_booked', 'td_completed', 'won', 'lost',
                           'mb30', 'mb60', 'mb90', 'mb_sales')}
    region_order = ['Central', 'Southeast', 'Northeast', 'Western', 'Canada', 'Mexico']

    for region in region_order:
        dealers = [(dk, kpi) for dk, kpi in dk_data.items() if kpi['market'] == region]
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

def build_matchback_sheet(ws, export_rows, leads, urban_science=None):
    """Populate Matchback Report using Urban Science buyer names + C4C lead names.

    Matching logic (replicates Master File):
    1. Dealer must match (Urban Science dealer ↔ Lead retailer)
    2. 3+ character substring match between buyer last name (Urban Science)
       and lead customer last name (C4C leads)
    3. Lead date must be within N days before sale date (30/60/90/120)
    """
    today = datetime.now()
    r120_start = today - timedelta(days=120)

    def _extract_last_name(name):
        """Extract last name and generate 3-char fragments."""
        if not name:
            return '', set()
        name = str(name).strip().upper()
        # Take last word as last name, or full name if single word
        parts = name.split()
        last = parts[-1] if parts else name
        # Remove common suffixes
        for suffix in ('JR', 'SR', 'III', 'II', 'IV'):
            if last == suffix and len(parts) > 1:
                last = parts[-2]
        # Generate 4-char substrings (reduces false positives vs 3-char)
        frags = set()
        for i in range(len(last) - 3):
            frags.add(last[i:i+4])
        return last, frags

    def _names_match(name1_frags, name2_frags):
        """Check if any 4-char fragment is shared between two names."""
        return bool(name1_frags & name2_frags)

    # Build per-dealer lead data: [(date, last_name, name_frags), ...]
    dealer_leads = defaultdict(list)
    if leads is not None and len(leads) > 0:
        for _, lr in leads.iterrows():
            dealer = _safe_str(lr.get('retailer_name', ''))
            if not dealer:
                continue
            dealer = dealer.replace(' INEOS Grenadier', '').replace(' INEOS', '').strip().upper()
            dealer = ' '.join(w for w in dealer.split() if w != 'GRENADIER').strip()
            ld = _safe_date(lr.get('start_date', lr.get('created_on', None)))
            if not ld:
                continue
            # Get customer name from lead
            cust = _safe_str(lr.get('customer_name', lr.get('lead_name', '')))
            last, frags = _extract_last_name(cust)
            if frags:
                dealer_leads[dealer].append((ld, last, frags))

    # Build per-dealer sale data from Urban Science (has buyer last names)
    dealer_sales = defaultdict(list)  # dealer_upper → [(sale_date, buyer_last, buyer_frags), ...]
    if urban_science is not None and len(urban_science) > 0:
        for _, sr in urban_science.iterrows():
            dealer = _safe_str(sr.get('dealer_name', ''))
            if not dealer:
                continue
            dealer = dealer.replace(' INEOS Grenadier', '').replace(' INEOS', '').strip().upper()
            dealer = ' '.join(w for w in dealer.split() if w != 'GRENADIER').strip()
            sd = _safe_date(sr.get('sale_date', None))
            if not sd:
                continue
            buyer_last = _safe_str(sr.get('customer_last_name', ''))
            last, frags = _extract_last_name(buyer_last)
            dealer_sales[dealer].append((sd, last, frags))

    all_dealers = sorted(set(list(dealer_leads.keys()) + list(dealer_sales.keys())))
    print(f"  [Matchback] {len(dealer_leads)} dealers with leads, {len(dealer_sales)} dealers with sales")

    # Headers
    ws.append([''] * 20)
    ws.append(['', 'Retailer', 'R120 Brand Leads', 'All Time Leads', '',
               'R120 Retail Sales', '', 'R30 MB Count', 'R30 MB%',
               'R60 MB Count', 'R60 MB%', 'R90 MB Count', 'R90 MB%',
               'R120 MB Count', 'R120 MB%', 'All Time MB Count', 'All Time MB%'])
    ws.append([''] * 20)

    t = {'leads_120': 0, 'leads_all': 0, 'sales_120': 0, 'sales_all': 0,
         'mb30': 0, 'mb60': 0, 'mb90': 0, 'mb120': 0, 'mb_all': 0}

    for dk in all_dealers:
        if not dk or dk in ('', 'INEOS CA STOCK'):
            continue

        leads_list = dealer_leads.get(dk, [])
        sales_list = dealer_sales.get(dk, [])

        r120_leads = sum(1 for ld, _, _ in leads_list if ld >= r120_start)
        all_leads_n = len(leads_list)
        r120_sales = sum(1 for sd, _, _ in sales_list if sd >= r120_start)
        all_sales_n = len(sales_list)

        # Matchback: for each sale, find if a lead with matching name fragment
        # existed within N days prior at the same dealer
        mb30 = mb60 = mb90 = mb120 = mb_all = 0
        for sd, buyer_last, buyer_frags in sales_list:
            if not buyer_frags:
                continue
            matched = False
            for ld, lead_last, lead_frags in leads_list:
                diff = (sd - ld).days
                if diff < 0 or diff > 365:
                    continue
                if _names_match(buyer_frags, lead_frags):
                    if diff <= 120:
                        mb120 += 1
                        if diff <= 90: mb90 += 1
                        if diff <= 60: mb60 += 1
                        if diff <= 30: mb30 += 1
                    mb_all += 1
                    matched = True
                    break  # count each sale once

        row = [''] * 20
        row[1] = dk.title()
        row[2] = r120_leads
        row[3] = all_leads_n
        row[5] = r120_sales
        row[7] = mb30
        row[8] = (mb30 / r120_sales) if r120_sales > 0 else 0
        row[9] = mb60
        row[10] = (mb60 / r120_sales) if r120_sales > 0 else 0
        row[11] = mb90
        row[12] = (mb90 / r120_sales) if r120_sales > 0 else 0
        row[13] = mb120
        row[14] = (mb120 / r120_sales) if r120_sales > 0 else 0
        row[15] = mb_all
        row[16] = (mb_all / all_sales_n) if all_sales_n > 0 else 0
        ws.append(row)

        t['leads_120'] += r120_leads
        t['leads_all'] += all_leads_n
        t['sales_120'] += r120_sales
        t['sales_all'] += all_sales_n
        t['mb30'] += mb30
        t['mb60'] += mb60
        t['mb90'] += mb90
        t['mb120'] += mb120
        t['mb_all'] += mb_all

    # Total row
    s120 = t['sales_120'] or 1
    s_all = t['sales_all'] or 1
    row = [''] * 20
    row[1] = 'Total'
    row[2] = t['leads_120']
    row[3] = t['leads_all']
    row[5] = t['sales_120']
    row[7] = t['mb30']
    row[8] = t['mb30'] / s120
    row[9] = t['mb60']
    row[10] = t['mb60'] / s120
    row[11] = t['mb90']
    row[12] = t['mb90'] / s120
    row[13] = t['mb120']
    row[14] = t['mb120'] / s120
    row[15] = t['mb_all']
    row[16] = t['mb_all'] / s_all
    ws.append(row)

    # Since Inception section
    ws.append([''] * 20)
    ws.append(['', 'Since Inception'] + [''] * 18)
    for dk in all_dealers:
        if not dk:
            continue
        row = [''] * 20
        row[1] = dk.title()
        row[2] = len(dealer_leads.get(dk, []))
        row[5] = len(dealer_sales.get(dk, []))
        ws.append(row)

    print(f"  Matchback: MB30={t['mb30']}/{t['sales_120']}={round(t['mb30']/s120*100,1)}%, MB90={t['mb90']}/{t['sales_120']}={round(t['mb90']/s120*100,1)}%")


def build_santander_sheets(wb, cache_dir):
    """Populate Santander sheets from cached JSON data.

    The processor reads:
    - "Santander Report " rows 9+: col[0]=date serial, col[1]=monthly volume
    - "App Report MoM" rows 1+: col[0]=date serial, col[1]=daily total
    - "App Report Finance" rows 1+: col[0]=date serial, col[1]=daily finance
    - "App Report Lease" rows 1+: col[0]=date serial, col[1]=daily lease
    """
    # Try multiple JSON locations
    sant_data = None
    for fname in ['santander_latest.json', 'data/santander.json']:
        spath = os.path.join(cache_dir, fname)
        if os.path.exists(spath):
            try:
                with open(spath) as f:
                    sant_data = json.load(f)
                break
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
            # Convert YYYY-MM to 1st of month serial
            serial = _date_to_serial(f'{ym}-01')
            if serial:
                ws.append([serial, int(volume)])
        print(f"  Santander Report: {len(monthly)} months")

    # Daily data → "App Report MoM" (all), split into Finance/Lease estimated
    daily = sant_data.get('daily', {})
    daily_finance = sant_data.get('daily_finance', {})
    daily_lease = sant_data.get('daily_lease', {})

    # If no Finance/Lease split, estimate from total (roughly 80% Finance, 20% Lease)
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

        ws.append(['Date', 'Volume'])
        if isinstance(daily_data, dict):
            for date_str, vol in sorted(daily_data.items()):
                serial = _date_to_serial(date_str)
                if serial:
                    ws.append([serial, int(vol)])
        elif isinstance(daily_data, list):
            for entry in daily_data:
                if isinstance(entry, dict):
                    serial = _date_to_serial(entry.get('date', ''))
                    vol = entry.get('volume', entry.get('count', 0))
                    if serial:
                        ws.append([serial, int(vol)])

    if daily:
        print(f"  Santander Daily: {len(daily)} days → MoM/Finance/Lease sheets")


# ═══════════════════════════════════════════════════════════════════════
# GA4 Sheets
# ═══════════════════════════════════════════════════════════════════════

def build_ga4_sheet_formatted(ws, ga4_df, ga4_type):
    """Write GA4 data in processor-expected format.

    Engagement/Acquisition: 9 header rows, then col[0]=day_index (days since 2025-01-01),
    col[1-4]=metric values.
    Other types: 9 header rows then raw data.
    """
    if ga4_df is None or len(ga4_df) == 0:
        for i in range(10):
            ws.append([''] * 10)
        return

    # 9 header rows
    for i in range(9):
        ws.append([''] * 10)

    cols = list(ga4_df.columns)

    if ga4_type in ('ga4_engagement', 'ga4_acquisition'):
        start_date = datetime(2025, 1, 1)
        date_col = None
        for c in cols:
            if 'date' in c.lower():
                date_col = c
                break

        if date_col:
            for _, r in ga4_df.iterrows():
                try:
                    dt = pd.to_datetime(r[date_col], errors='coerce')
                    if pd.isna(dt):
                        continue
                    day_idx = (dt - start_date).days
                    if day_idx < 0:
                        continue
                    metrics = []
                    for c in cols:
                        if c != date_col:
                            try:
                                metrics.append(float(r[c]) if not pd.isna(r[c]) else 0)
                            except (ValueError, TypeError):
                                metrics.append(0)
                    while len(metrics) < 4:
                        metrics.append(0)
                    ws.append([day_idx] + metrics[:4])
                except Exception:
                    continue
        else:
            for _, r in ga4_df.iterrows():
                ws.append([r.get(c, '') for c in cols])
    else:
        # Generic: write columns as-is
        for _, r in ga4_df.iterrows():
            ws.append([r.get(c, '') for c in cols])
