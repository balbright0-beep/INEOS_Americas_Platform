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


def _serial_to_date(s):
    if not s:
        return None
    try:
        return datetime(1899, 12, 30) + timedelta(days=int(float(s)))
    except Exception:
        return None


def _detect_body(material):
    m = str(material).lower()
    if 'quartermaster' in m or 'qm' in m:
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


# Market region mapping
MARKET_REGIONS = {
    'Central': 'Central', 'Southeast': 'Southeast',
    'Northeast': 'Northeast', 'Western': 'Western',
    'Canada': 'Canada', 'Mexico': 'Mexico',
}


def _get_market(sap_row, mkt_map=None):
    """Get market from SAP row, with fallback to market map."""
    for col in ('market_area', 'region_group', 'Country Region Group'):
        v = sap_row.get(col, '')
        if v and _safe_str(v):
            return _safe_str(v)
    if mkt_map:
        dealer = _norm_dealer(sap_row.get('customer_name', '')).upper()
        return mkt_map.get(dealer, '')
    return ''


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
    """Parse SAP export into a list of dicts with enriched fields."""
    rows = []

    # Build VIN lookups
    ho_map = {}
    if handover is not None and len(handover) > 0:
        for _, r in handover.iterrows():
            vin = _safe_str(r.get('vin', '')).upper()
            if len(vin) > 3:
                ho_map[vin] = r

    billto_map = {}
    if sales_order is not None and len(sales_order) > 0:
        for _, r in sales_order.iterrows():
            vin = _safe_str(r.get('vin', '')).upper()
            if len(vin) > 3:
                billto_map[vin] = _safe_str(r.get('bill_to_dealer', r.get('customer_name', '')))

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

        # Handover date
        ho = ho_map.get(vin, None)
        ho_date = None
        if ho is not None:
            hd = ho.get('handover_date', None) if isinstance(ho, dict) else getattr(ho, 'handover_date', None)
            if hd is not None:
                try:
                    if not pd.isna(hd):
                        ho_date = pd.to_datetime(hd, errors='coerce')
                        if pd.isna(ho_date):
                            ho_date = None
                except (TypeError, ValueError):
                    ho_date = None

        # Fallback: invoice date
        if ho_date is None:
            inv_d = r.get('invoice_date', None)
            if inv_d is not None:
                try:
                    if not pd.isna(inv_d):
                        ho_date = pd.to_datetime(inv_d, errors='coerce')
                        if pd.isna(ho_date):
                            ho_date = None
                except (TypeError, ValueError):
                    ho_date = None

        market = _get_market(r, mkt_map)
        body = _detect_body(material)
        my = _detect_my(material)
        cc = _get_country_code(country)
        is_cvp = vin in cvp_vins

        rows.append({
            'vin': vin,
            'dealer': dealer,
            'dealer_upper': dealer.upper(),
            'material': material,
            'country': country,
            'country_code': cc,
            'status': status,
            'channel': channel,
            'market': market,
            'body': body,
            'my': my,
            'msrp': r.get('msrp', 0),
            'ho_date': ho_date,
            'is_cvp': is_cvp,
            'plant': _safe_str(r.get('plant_code', '')),
            'trim': _safe_str(r.get('trim', '')),
        })

    return rows, mkt_map


# ═══════════════════════════════════════════════════════════════════════
# Retail Sales Report
# ═══════════════════════════════════════════════════════════════════════

def build_retail_sales_sheet(ws, export_rows, mkt_map, objectives=None):
    """Populate the Retail Sales Report sheet.

    Layout: rows 0-5 = header area, rows 6-13 = regional summary
    Cols: [2]=region, [3]=SW, [4]=QM, [5]=SVO, [6]=total, [7]=obj, [8]=PO%, [9]=MX%, [15]=CVP

    Then rows 22+ = MTD_DLR detailed dealer breakdown
    """
    today = datetime.now()
    cur_month = today.strftime('%Y-%m')
    prev_dt = (today.replace(day=1) - timedelta(days=1))
    prev_month = prev_dt.strftime('%Y-%m')

    # Count MTD sales by region/body
    region_data = defaultdict(lambda: {'sw': 0, 'qm': 0, 'svo': 0, 'cvp': 0})

    for r in export_rows:
        if not r['ho_date']:
            continue
        if r['ho_date'].strftime('%Y-%m') != cur_month:
            continue
        if r['country_code'] not in ('US', 'CA', 'MX'):
            continue

        mkt = r['market'] or 'Unknown'
        region_data[mkt][r['body']] += 1
        if r['is_cvp']:
            region_data[mkt]['cvp'] += 1

    # Objectives by region (from template if available)
    obj_by_region = objectives or {}

    # Compute region order
    region_order = ['Central', 'Southeast', 'Northeast', 'Western', 'Canada', 'Mexico']

    # Header rows (0-5)
    ws.append(['', '', 'RETAIL SALES REPORT'] + [''] * 17)
    ws.append(['', '', f'{today.strftime("%B %Y")} MTD'] + [''] * 17)
    for i in range(4):
        ws.append([''] * 20)

    # Regional summary rows (6-13)
    total = {'sw': 0, 'qm': 0, 'svo': 0, 'cvp': 0, 'obj': 0}
    for region in region_order:
        d = region_data.get(region, {'sw': 0, 'qm': 0, 'svo': 0, 'cvp': 0})
        sw, qm, svo, cvp = d['sw'], d['qm'], d['svo'], d['cvp']
        t = sw + qm + svo
        obj = obj_by_region.get(region, 0)
        po = round(t / obj * 100, 1) if obj > 0 else 0
        total_all = sum(dd['sw'] + dd['qm'] + dd['svo'] for dd in region_data.values())
        mx = round(t / total_all * 100, 1) if total_all > 0 else 0

        row = [''] * 20
        row[2] = region
        row[3] = sw
        row[4] = qm
        row[5] = svo
        row[6] = t
        row[7] = obj
        row[8] = po / 100  # stored as decimal, processor multiplies by 100
        row[9] = mx / 100
        row[15] = cvp
        ws.append(row)

        total['sw'] += sw
        total['qm'] += qm
        total['svo'] += svo
        total['cvp'] += cvp
        total['obj'] += obj

    # Total row (index 12 or 13 depending on region count)
    remaining = 8 - len(region_order)
    for _ in range(remaining):
        ws.append([''] * 20)

    t_total = total['sw'] + total['qm'] + total['svo']
    row = [''] * 20
    row[2] = 'Total'
    row[3] = total['sw']
    row[4] = total['qm']
    row[5] = total['svo']
    row[6] = t_total
    row[7] = total['obj']
    row[8] = round(t_total / total['obj'], 3) if total['obj'] > 0 else 0
    row[9] = 1.0
    row[15] = total['cvp']
    ws.append(row)

    # Padding to row 21
    while ws.max_row < 22:
        ws.append([''] * 20)

    # MTD_DLR section (rows 22+)
    # Group by region → dealer
    dealer_data = defaultdict(lambda: {'sw': 0, 'qm': 0, 'svo': 0, 'total': 0,
                                        'pm': 0, 'py': 0, 'cvp': 0, 'market': ''})

    prev_year_month = f'{today.year - 1}-{today.strftime("%m")}'

    for r in export_rows:
        if r['country_code'] not in ('US', 'CA', 'MX'):
            continue
        if not r['ho_date']:
            continue

        ho_ym = r['ho_date'].strftime('%Y-%m')
        dk = r['dealer_upper']

        if not dealer_data[dk]['market']:
            dealer_data[dk]['market'] = r['market']

        if ho_ym == cur_month:
            dealer_data[dk][r['body']] += 1
            dealer_data[dk]['total'] += 1
            if r['is_cvp']:
                dealer_data[dk]['cvp'] += 1

        if ho_ym == prev_month:
            dealer_data[dk]['pm'] += 1

        if ho_ym == prev_year_month:
            dealer_data[dk]['py'] += 1

    # Write MTD_DLR by region
    for region in region_order:
        # Region header
        row = [''] * 20
        row[1] = region
        ws.append(row)

        # Dealers in this region
        region_dealers = [(dk, dd) for dk, dd in dealer_data.items()
                          if dd['market'] == region and dd['total'] > 0]
        region_dealers.sort(key=lambda x: -x[1]['total'])

        for dk, dd in region_dealers:
            row = [''] * 20
            row[2] = dk.title()
            row[3] = dd['sw']
            row[4] = dd['qm']
            row[5] = dd['svo']
            row[6] = dd['total']
            row[7] = dd['pm']
            row[8] = dd['py']
            row[9] = round(dd['total'] / dd['pm'] * 100, 1) if dd['pm'] > 0 else 0
            row[10] = round(dd['total'] / dd['py'] * 100, 1) if dd['py'] > 0 else 0
            row[15] = dd['cvp']
            ws.append(row)


# ═══════════════════════════════════════════════════════════════════════
# Dealer Performance Dashboard
# ═══════════════════════════════════════════════════════════════════════

def build_dpd_sheet(ws, export_rows, mkt_map):
    """Populate the DPD sheet.

    Layout: rows 0-2 = header, rows 3+ = dealer rows
    Cols: [0]=market, [1]=dealer, [2]=handovers(MTD), [3]=CVP, [4]=wholesale,
          [5]=gap, [6]=on-ground, [7]=dollar, [8]=count
    """
    today = datetime.now()
    cur_month = today.strftime('%Y-%m')
    prev_dt = (today.replace(day=1) - timedelta(days=1))
    prev_month = prev_dt.strftime('%Y-%m')
    prev_year_month = f'{today.year - 1}-{today.strftime("%m")}'

    # Dealer stats
    stats = defaultdict(lambda: {
        'market': '', 'mtd': 0, 'cvp': 0, 'sw': 0, 'qm': 0, 'svo': 0,
        'pm': 0, 'py': 0, 'og': 0, 'og_sw': 0, 'og_qm': 0,
    })

    for r in export_rows:
        if r['country_code'] not in ('US', 'CA', 'MX'):
            continue

        dk = r['dealer_upper']
        if not stats[dk]['market']:
            stats[dk]['market'] = r['market']

        # On-ground (dealer stock, retail channels)
        if ('dealer stock' in r['status'] or '7.' in r['status']):
            if r['channel'] in ('STOCK', 'PRIVATE - RETAILER'):
                stats[dk]['og'] += 1
                if r['body'] == 'sw':
                    stats[dk]['og_sw'] += 1
                else:
                    stats[dk]['og_qm'] += 1

        # Sales
        if r['ho_date']:
            ho_ym = r['ho_date'].strftime('%Y-%m')
            if ho_ym == cur_month:
                stats[dk]['mtd'] += 1
                stats[dk][r['body']] += 1
                if r['is_cvp']:
                    stats[dk]['cvp'] += 1
            if ho_ym == prev_month:
                stats[dk]['pm'] += 1
            if ho_ym == prev_year_month:
                stats[dk]['py'] += 1

    # Header rows
    ws.append(['Market', 'Dealer', 'Handovers', 'CVP', 'Wholesale', 'Gap',
               'On Ground', 'Dollar', 'Count'] + [''] * 20)
    ws.append([''] * 30)
    ws.append([''] * 30)

    # Data rows (sorted by market then dealer)
    region_order = ['Central', 'Southeast', 'Northeast', 'Western', 'Canada', 'Mexico']
    for region in region_order:
        dealers = [(dk, s) for dk, s in stats.items() if s['market'] == region]
        dealers.sort(key=lambda x: -x[1]['mtd'])
        for dk, s in dealers:
            row = [''] * 30
            row[0] = s['market']
            row[1] = dk.title()
            row[2] = s['mtd']
            row[3] = s['cvp']
            row[4] = 0  # wholesale
            row[5] = 0  # gap
            row[6] = s['og']
            # Monthly history placeholder
            row[9] = s['pm']   # recent month
            row[10] = s['py']  # prior year
            ws.append(row)


# ═══════════════════════════════════════════════════════════════════════
# Dealer Inventory Report
# ═══════════════════════════════════════════════════════════════════════

def build_inventory_sheet(ws, export_rows, mkt_map):
    """Populate the Dealer Inventory Report sheet.

    Note: The processor RECOMPUTES INV from Export data (lines 3090-3278),
    so this sheet mainly needs dealer names and markets for the initial
    INV build. The Export-based recomputation will override most values.
    """
    today = datetime.now()
    cur_month = today.strftime('%Y-%m')

    # Collect unique dealers with their markets
    dealers = {}
    for r in export_rows:
        if r['country_code'] not in ('US', 'CA', 'MX'):
            continue
        dk = r['dealer_upper']
        if dk not in dealers:
            dealers[dk] = {'name': r['dealer'], 'market': r['market']}

    # Header rows (0-2)
    ws.append(['', '', 'Dealer', 'MY24 SW', 'MY24 QM', 'MY25 SW', 'MY25 QM',
               'MY26 SW', 'MY26 QM', '', '', 'OG SW', 'OG QM'] + [''] * 32)
    ws.append([''] * 45)
    ws.append([''] * 45)

    # Data rows (row 3+)
    region_order = ['Central', 'Southeast', 'Northeast', 'Western', 'Canada', 'Mexico']
    for region in region_order:
        region_dealers = [(dk, d) for dk, d in dealers.items() if d['market'] == region]
        region_dealers.sort(key=lambda x: x[1]['name'])
        for dk, d in region_dealers:
            row = [''] * 45
            row[1] = d['market']
            row[2] = d['name']
            ws.append(row)


# ═══════════════════════════════════════════════════════════════════════
# Objectives
# ═══════════════════════════════════════════════════════════════════════

def build_objectives_sheet(ws, template_path=None):
    """Populate the Objectives sheet.

    Objectives are manually entered business targets. We extract them
    from the existing Dashboard template HTML (which has OBJ constant
    from the last Master File upload) if available.

    Layout: row 2=US, row 4=Retailer, row 5=Rental, row 6=Fleet,
            row 7=IECP, row 8=Total. Cols 9-20 = 12 monthly values.
    """
    obj_data = {}

    # Try to extract OBJ from template HTML
    if template_path and os.path.exists(template_path):
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                html = f.read()
            # Find const OBJ = {...};
            m = re.search(r'const\s+OBJ\s*=\s*(\{.*?\});', html, re.DOTALL)
            if m:
                obj_data = json.loads(m.group(1))
        except Exception as e:
            print(f"  [Objectives] Could not extract from template: {e}")

    # Row mapping: category → row index
    cat_rows = {'US': 2, 'Retailer': 4, 'Rental': 5, 'Fleet': 6, 'IECP': 7, 'Total': 8}

    # Write rows 0-14
    for i in range(15):
        row = [''] * 25
        # Find if this row maps to a category
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
    """Populate the Historical Sales sheet.

    Layout: Row 1=month dates (serial), Row 2=total retail, Row 3=SW,
    Row 4=QM, Row 5=SVO, Row 10=wholesale, Row 11=WS SW, Row 12=WS QM
    Rows 30-68 = retail by dealer, Rows 70-102 = wholesale by dealer
    """
    # Collect monthly sales from handover dates
    monthly = defaultdict(lambda: {'sw': 0, 'qm': 0, 'svo': 0, 'total': 0})
    dealer_monthly = defaultdict(lambda: defaultdict(int))

    for r in export_rows:
        if not r['ho_date']:
            continue
        if r['country_code'] not in ('US', 'CA', 'MX'):
            continue
        ym = r['ho_date'].strftime('%Y-%m')
        monthly[ym][r['body']] += 1
        monthly[ym]['total'] += 1
        dealer_monthly[r['dealer_upper']][ym] += 1

    if not monthly:
        for i in range(15):
            ws.append([''] * 15)
        return

    # Sort months
    all_months = sorted(monthly.keys())

    # Row 0: header
    ws.append([''] * (len(all_months) + 2))

    # Row 1: dates (1st of each month as serial)
    date_row = ['', 'Month']
    for ym in all_months:
        y, m = ym.split('-')
        dt = datetime(int(y), int(m), 1)
        date_row.append(_date_to_serial(dt))
    ws.append(date_row)

    # Row 2: total retail
    total_row = ['', 'Total Retail']
    for ym in all_months:
        total_row.append(monthly[ym]['total'])
    ws.append(total_row)

    # Row 3: SW
    sw_row = ['', 'SW']
    for ym in all_months:
        sw_row.append(monthly[ym]['sw'])
    ws.append(sw_row)

    # Row 4: QM
    qm_row = ['', 'QM']
    for ym in all_months:
        qm_row.append(monthly[ym]['qm'])
    ws.append(qm_row)

    # Row 5: SVO
    svo_row = ['', 'SVO']
    for ym in all_months:
        svo_row.append(monthly[ym]['svo'])
    ws.append(svo_row)

    # Rows 6-9: padding
    for _ in range(4):
        ws.append([''] * (len(all_months) + 2))

    # Row 10-12: wholesale (no data from our sources, zeros)
    for label in ['Wholesale Total', 'WS SW', 'WS QM']:
        ws.append(['', label] + [0] * len(all_months))

    # Padding to row 29
    while ws.max_row < 30:
        ws.append([''] * (len(all_months) + 2))

    # Rows 30+: retail by dealer
    all_dealers = sorted(dealer_monthly.keys())
    for dk in all_dealers[:38]:  # cap at 38 dealers (rows 30-67)
        mkt = mkt_map.get(dk, '')
        row = [mkt, dk.title()]
        for ym in all_months:
            row.append(dealer_monthly[dk].get(ym, 0))
        ws.append(row)


# ═══════════════════════════════════════════════════════════════════════
# Lead Handling KPIs
# ═══════════════════════════════════════════════════════════════════════

def build_lead_kpis_sheet(ws, leads, mkt_map):
    """Populate Lead Handling KPIs sheet.

    Layout: rows 0-3 = header, rows 4+ = dealer rows
    Cols: [0]=market, [1]=dealer, [2]=RBM, [3]=leads, [4]=contacted,
          [5]=contact%, [6]=UTC, [7]=UTC%, [8]=TD booked, [9]=TD completed,
          [10]=show rate, [11]=lead-to-sale%, [12]=won, [13]=lost,
          [14]=loss rate, [15]=MB30%, [16]=MB60%, [17]=MB90%
    """
    if leads is None or len(leads) == 0:
        for i in range(5):
            ws.append([''] * 20)
        return

    # Header rows
    ws.append(['Market', 'Dealer', 'RBM', 'Leads', 'Contacted', 'Contact %',
               'Under Control', 'UTC %', 'TD Booked', 'TD Completed',
               'Show Rate', 'Lead to Sale %', 'Won', 'Lost', 'Loss Rate',
               'MB 30%', 'MB 60%', 'MB 90%'])
    for _ in range(3):
        ws.append([''] * 20)

    # Compute per-dealer KPIs from leads data
    dealer_kpis = defaultdict(lambda: {
        'market': '', 'leads': 0, 'contacted': 0, 'td_booked': 0,
        'td_completed': 0, 'won': 0, 'lost': 0
    })

    for _, lr in leads.iterrows():
        dealer = _safe_str(lr.get('retailer_name', ''))
        if not dealer:
            continue
        dealer = _norm_dealer(dealer)
        dk = dealer.upper()

        dealer_kpis[dk]['leads'] += 1
        if not dealer_kpis[dk]['market']:
            mkt = _safe_str(lr.get('marketing_unit', ''))
            if not mkt:
                mkt = mkt_map.get(dk, '')
            dealer_kpis[dk]['market'] = mkt

        status = _safe_str(lr.get('retailer_status', lr.get('lead_status', ''))).lower()
        if status in ('contacted', 'won', 'lost', 'qualified'):
            dealer_kpis[dk]['contacted'] += 1

        # TD booked
        td_book = lr.get('td_booking_date', None)
        if td_book is not None:
            try:
                if not pd.isna(td_book):
                    dealer_kpis[dk]['td_booked'] += 1
            except (TypeError, ValueError):
                pass

        # TD completed
        td_flag = _safe_str(lr.get('td_completed_flag', '')).lower()
        if td_flag in ('yes', 'true', '1', 'completed'):
            dealer_kpis[dk]['td_completed'] += 1

        # Won/Lost
        if 'won' in status:
            dealer_kpis[dk]['won'] += 1
        elif 'lost' in status:
            dealer_kpis[dk]['lost'] += 1

    # Network totals
    net = {'leads': 0, 'contacted': 0, 'td_booked': 0, 'td_completed': 0, 'won': 0, 'lost': 0}

    # Write dealer rows (row 4+)
    region_order = ['Central', 'Southeast', 'Northeast', 'Western', 'Canada', 'Mexico']
    for region in region_order:
        dealers = [(dk, kpi) for dk, kpi in dealer_kpis.items() if kpi['market'] == region]
        dealers.sort(key=lambda x: -x[1]['leads'])

        for dk, kpi in dealers:
            leads_n = kpi['leads']
            contacted = kpi['contacted']
            td_b = kpi['td_booked']
            td_c = kpi['td_completed']
            won = kpi['won']
            lost = kpi['lost']

            cp = round(contacted / leads_n, 3) if leads_n > 0 else 0
            show = round(td_c / td_b, 3) if td_b > 0 else 0
            lts = round(won / leads_n, 3) if leads_n > 0 else 0
            loss = round(lost / leads_n, 3) if leads_n > 0 else 0

            row = [''] * 20
            row[0] = region
            row[1] = dk.title()
            row[2] = ''  # RBM
            row[3] = leads_n
            row[4] = contacted
            row[5] = cp
            row[6] = contacted  # UTC ≈ contacted for now
            row[7] = cp
            row[8] = td_b
            row[9] = td_c
            row[10] = show
            row[11] = lts
            row[12] = won
            row[13] = lost
            row[14] = loss
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
    """Populate Santander Report sheets from cached JSON data.

    Creates: "Santander Report " (monthly pivot), "Santander Report Finance",
    "Santander Report Lease", "App Report MoM", "App Report Finance",
    "App Report Lease"
    """
    sant_path = os.path.join(cache_dir, 'santander_latest.json')
    if not os.path.exists(sant_path):
        return

    with open(sant_path) as f:
        sant_data = json.load(f)

    # "Santander Report " - monthly pivot (rows 9+)
    ws_pivot = wb['Santander Report '] if 'Santander Report ' in wb.sheetnames else None
    if ws_pivot:
        # Clear and rebuild
        # Monthly summary data
        monthly = sant_data.get('monthly', sant_data.get('pivot', {}))
        if isinstance(monthly, dict):
            # Write 9 header rows
            for i in range(9):
                ws_pivot.append([''] * 10)
            # Monthly rows: col[0]=date serial, col[1]=volume
            for date_str, volume in sorted(monthly.items()):
                serial = _date_to_serial(date_str)
                if serial:
                    ws_pivot.append([serial, volume])

    # Daily sheets
    for sheet_name, data_key in [
        ('App Report MoM', 'all'),
        ('App Report Finance', 'finance'),
        ('App Report Lease', 'lease'),
    ]:
        # Create sheet if needed
        if sheet_name not in wb.sheetnames:
            ws = wb.create_sheet(sheet_name)
        else:
            ws = wb[sheet_name]

        ws.append(['Date', 'Volume'])  # Header row

        daily = sant_data.get(data_key, sant_data.get('daily_' + data_key, []))
        if isinstance(daily, list):
            for entry in daily:
                if isinstance(entry, dict):
                    serial = _date_to_serial(entry.get('date', ''))
                    vol = entry.get('volume', entry.get('count', 0))
                    if serial:
                        ws.append([serial, vol])
                elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                    serial = _date_to_serial(entry[0])
                    if serial:
                        ws.append([serial, entry[1]])
        elif isinstance(daily, dict):
            for date_str, vol in sorted(daily.items()):
                serial = _date_to_serial(date_str)
                if serial:
                    ws.append([serial, vol])


# ═══════════════════════════════════════════════════════════════════════
# GA4 Sheets
# ═══════════════════════════════════════════════════════════════════════

def build_ga4_sheet_formatted(ws, ga4_df, ga4_type):
    """Write GA4 data in the format the processor expects.

    Each GA4 sheet has 9 metadata/header rows, then data rows.
    The data format depends on the report type:
    - Engagement: col[0]=day_index, col[1]=all, col[2]=organic, col[3]=paid, col[4]=direct
    - Acquisition: similar to engagement
    - User Attributes: sections with "Country", "City", etc. headers
    - Demographics: 4 sections (All/Direct/Organic/Paid) with country data
    - Tech: sections with "Operating system", "Browser", etc.
    - Audiences: sections with audience data
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
        # Daily time series: need day_index from start_date (2025-01-01)
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

                    # Get metric columns (take first 4 numeric after date)
                    metrics = []
                    for c in cols:
                        if c != date_col:
                            try:
                                metrics.append(float(r[c]) if not pd.isna(r[c]) else 0)
                            except (ValueError, TypeError):
                                metrics.append(0)
                    # Pad to 4 metrics
                    while len(metrics) < 4:
                        metrics.append(0)

                    ws.append([day_idx] + metrics[:4])
                except Exception:
                    continue
        else:
            # No date column — write raw data
            for _, r in ga4_df.iterrows():
                ws.append([r.get(c, '') for c in cols])

    elif ga4_type == 'ga4_user_attributes':
        # Sections: Country, City, Language, Gender, Age, Interests
        # Detect dimension column
        dim_col = cols[0] if cols else None
        val_col = cols[1] if len(cols) > 1 else None
        if dim_col and val_col:
            for _, r in ga4_df.iterrows():
                ws.append([r.get(dim_col, ''), r.get(val_col, 0)])

    else:
        # Generic: write columns as-is
        for _, r in ga4_df.iterrows():
            ws.append([r.get(c, '') for c in cols])
