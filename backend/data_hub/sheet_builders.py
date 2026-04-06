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


def _build_market_map(sap):
    """Build dealer→market mapping from SAP data."""
    mkt_map = {}
    for col in ('market_area', 'region_group'):
        if col in sap.columns:
            for _, r in sap.iterrows():
                dealer = _norm_dealer(r.get('customer_name', '')).upper()
                mkt = _safe_str(r.get(col, ''))
                if dealer and mkt:
                    mkt_map[dealer] = mkt
            break
    return mkt_map


def _parse_export_rows(sap, handover=None, sales_order=None, campaign_codes=None):
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

    mkt_map = _build_market_map(sap)

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

        # Fallback: invoice date from SAP
        if ho_date is None:
            ho_date = _safe_date(r.get('invoice_date', None))

        # Market
        market = ''
        for col in ('market_area', 'region_group'):
            market = _safe_str(r.get(col, ''))
            if market:
                break
        if not market:
            market = mkt_map.get(dealer.upper(), '')

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
        })

    return rows, mkt_map


# ═══════════════════════════════════════════════════════════════════════
# Retail Sales Report
# ═══════════════════════════════════════════════════════════════════════

def build_retail_sales_sheet(ws, export_rows, mkt_map, objectives=None):
    """Populate Retail Sales Report.

    Rows 6-13: regional summary → [2]=region [3]=SW [4]=QM [5]=SVO [6]=total [7]=obj [8]=PO% [9]=MX% [15]=CVP
    Rows 22+: MTD_DLR per-dealer → [1]=region header or [2]=dealer name, [3]=SW [4]=QM [5]=SVO [6]=total [7]=PM [8]=PY
    """
    today = datetime.now()
    cur_month = today.strftime('%Y-%m')
    prev_month = (today.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    prev_year_month = f'{today.year - 1}-{today.strftime("%m")}'

    region_order = ['Central', 'Southeast', 'Northeast', 'Western', 'Canada', 'Mexico']
    obj_by_region = objectives or {}

    # ── Count MTD sales by region/body ──
    region_mtd = defaultdict(lambda: {'sw': 0, 'qm': 0, 'svo': 0, 'cvp': 0})
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

        if not dealer_data[dk]['market']:
            dealer_data[dk]['market'] = mkt

        if ho_ym == cur_month:
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
    total_all = sum(d['sw'] + d['qm'] + d['svo'] for d in region_mtd.values())
    total = {'sw': 0, 'qm': 0, 'svo': 0, 'cvp': 0, 'obj': 0}

    for region in region_order:
        d = region_mtd.get(region, {'sw': 0, 'qm': 0, 'svo': 0, 'cvp': 0})
        sw, qm, svo, cvp_n = d['sw'], d['qm'], d['svo'], d['cvp']
        t = sw + qm + svo
        obj = obj_by_region.get(region, 0)
        po = (t / obj) if obj > 0 else 0
        mx = (t / total_all) if total_all > 0 else 0

        row = [''] * 20
        row[2] = region
        row[3] = sw
        row[4] = qm
        row[5] = svo
        row[6] = t
        row[7] = obj
        row[8] = po
        row[9] = mx
        row[15] = cvp_n
        ws.append(row)

        for k in ('sw', 'qm', 'svo', 'cvp'):
            total[k] += d.get(k, 0)
        total['obj'] += obj

    # Pad remaining rows to index 12 (Total at 13)
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

    # ── MTD_DLR rows 22+ by region ──
    for region in region_order:
        # Region header row: col[1]=region name, col[2]=empty
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

def build_dpd_sheet(ws, export_rows, mkt_map, leads=None):
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
            # Matchback percentages (we don't have this data)
            row[23] = 0  # MB30
            row[24] = 0  # MB60
            row[25] = 0  # MB90
            row[26] = 0  # MB all-time
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

def build_lead_kpis_sheet(ws, leads, mkt_map):
    """Populate Lead Handling KPIs.

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

    # Write data rows (row 4+) and accumulate network total
    net = {k: 0 for k in ('leads', 'contacted', 'td_booked', 'td_completed', 'won', 'lost')}
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
            ws.append(row)

            for k in net:
                net[k] += kpi[k]

    # Network total row
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
    ws.append(row)


# ═══════════════════════════════════════════════════════════════════════
# Santander sheets
# ═══════════════════════════════════════════════════════════════════════

def build_santander_sheets(wb, cache_dir):
    """Populate Santander sheets from cached JSON data.

    The processor reads:
    - "Santander Report " rows 9+: col[0]=date serial, col[1]=monthly volume
    - "App Report MoM/Finance/Lease" rows 1+: col[0]=date serial, col[1]=daily volume
    """
    sant_path = os.path.join(cache_dir, 'santander_latest.json')
    if not os.path.exists(sant_path):
        return

    try:
        with open(sant_path) as f:
            sant_data = json.load(f)
    except Exception:
        return

    # "Santander Report " - monthly pivot
    if 'Santander Report ' in wb.sheetnames:
        ws = wb['Santander Report ']
        # Overwrite with data
        monthly = sant_data.get('monthly', sant_data.get('pivot', {}))
        if isinstance(monthly, dict) and monthly:
            # Skip existing empty rows, write from row 10 (index 9)
            while ws.max_row < 9:
                ws.append([''] * 10)
            for date_str, volume in sorted(monthly.items()):
                serial = _date_to_serial(date_str)
                if serial:
                    ws.append([serial, volume])

    # Daily data sheets
    for sheet_name, data_key in [
        ('App Report MoM', 'all'),
        ('App Report Finance', 'finance'),
        ('App Report Lease', 'lease'),
    ]:
        if sheet_name not in wb.sheetnames:
            ws = wb.create_sheet(sheet_name)
        else:
            ws = wb[sheet_name]

        ws.append(['Date', 'Volume'])
        daily = sant_data.get(data_key, sant_data.get(f'daily_{data_key}', []))
        if isinstance(daily, list):
            for entry in daily:
                if isinstance(entry, dict):
                    serial = _date_to_serial(entry.get('date', ''))
                    vol = entry.get('volume', entry.get('count', 0))
                    if serial:
                        ws.append([serial, vol])
        elif isinstance(daily, dict):
            for date_str, vol in sorted(daily.items()):
                serial = _date_to_serial(date_str)
                if serial:
                    ws.append([serial, vol])


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
