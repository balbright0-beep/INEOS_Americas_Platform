"""Compute Engine — replaces 20+ formula sheets from the Master File.
Each function takes enriched DataFrames and returns JSON-serializable dicts."""
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
from data_hub.utils import clean_dealer_name, is_retail_dealer, safe_int, safe_float, pct, OG_CHANNELS


def compute_retail_sales(enriched_df, ref_db_path, today=None):
    """Replaces: Retail Sales Report sheet."""
    if today is None:
        today = datetime.now()
    cur_month = today.strftime('%Y-%m')
    df = enriched_df.copy()

    # Filter: US, retail channel, handover in current month
    mask = (
        df['country'].str.upper().str.contains('UNITED STATES', na=False) &
        df.get('handover_date', pd.Series(dtype='object')).notna() &
        df.get('handover_date', pd.Series(dtype='object')).astype(str).str.startswith(cur_month)
    )
    sales = df[mask]

    # Market summary
    market_summary = []
    for market, group in sales.groupby('market_area'):
        if not market:
            continue
        sw = len(group[group['body_type'] == 'SW'])
        qm = len(group[group['body_type'] == 'QM'])
        svo = len(group[group['body_type'] == 'SVO'])
        market_summary.append({
            'r': market, 'sw': sw, 'qm': qm, 'svo': svo, 't': sw + qm + svo,
        })

    # Dealer detail
    dealer_detail = []
    for dealer, group in sales.groupby('normalized_dealer'):
        if not dealer or not is_retail_dealer(dealer):
            continue
        dealer_detail.append({
            'd': dealer, 'mkt': group['market_area'].iloc[0] if len(group) > 0 else '',
            'sw': len(group[group['body_type'] == 'SW']),
            'qm': len(group[group['body_type'] == 'QM']),
            'svo': len(group[group['body_type'] == 'SVO']),
            't': len(group),
        })

    # Individual units
    units = []
    for _, r in sales.iterrows():
        units.append({
            'd': r.get('normalized_dealer', ''), 'mkt': r.get('market_area', ''),
            'vin': str(r.get('vin', ''))[-6:], 'vinFull': str(r.get('vin', '')),
            'body': r.get('body_type', ''), 'my': r.get('model_year', ''),
            'trim': r.get('trim', ''), 'ext': r.get('ext_color', ''),
            'int': r.get('seats', ''), 'whl': r.get('wheels', ''),
            'ch': r.get('channel', ''), 'msrp': safe_int(r.get('msrp', 0)),
            'dts': safe_int(r.get('days_to_sell', 0)),
            'cvp': r.get('cvp', 'No'),
            'ho': str(r.get('handover_date', ''))[:10],
        })

    return {'market_summary': market_summary, 'dealer_detail': dealer_detail, 'units': units}


def compute_dpd(enriched_df, leads_df=None, today=None):
    """Replaces: Dealer Performance Dashboard sheet."""
    if today is None:
        today = datetime.now()
    df = enriched_df.copy()
    results = []

    for dealer, group in df.groupby('normalized_dealer'):
        if not dealer or not is_retail_dealer(dealer):
            continue
        market = group['market_area'].iloc[0] if len(group) > 0 else ''
        ho = group[group.get('handover_date', pd.Series(dtype='object')).notna()]
        og = group[group.get('status_enriched') == 'Dealer Stock']
        ds = group[(group.get('status_enriched') == 'Dealer Stock') & (group['channel'].isin(OG_CHANNELS))]

        r = {
            'd': dealer, 'm': market,
            'ho': len(ho), 'og': len(og), 'ds': len(ds),
        }

        # Lead metrics (if leads available)
        if leads_df is not None and len(leads_df) > 0:
            dlr_leads = leads_df[leads_df.get('retailer_name', pd.Series(dtype='str')).str.upper().str.contains(dealer, na=False)]
            r['leads'] = len(dlr_leads)
            r['td'] = len(dlr_leads[dlr_leads.get('td_completed_flag', '').astype(str) == 'Yes'])
        else:
            r['leads'] = 0
            r['td'] = 0

        results.append(r)

    return sorted(results, key=lambda x: x.get('ho', 0), reverse=True)


def compute_pipeline(enriched_df):
    """Replaces: Pipeline constants."""
    df = enriched_df[enriched_df['country'].str.upper().str.contains('UNITED STATES', na=False)].copy()
    result = {}
    for my in ['MY25', 'MY26', 'MY27']:
        my_df = df[df['model_year'] == my]
        result[my.lower()] = {
            'og': len(my_df[my_df.get('status_enriched') == 'Dealer Stock']),
            'it': len(my_df[my_df.get('status_enriched') == 'In-Transit to Dealer']),
            'ap': len(my_df[my_df.get('status_enriched') == 'At Americas Port']),
            'ow': len(my_df[my_df.get('status_enriched') == 'On Water']),
            'ip': len(my_df[my_df.get('status_enriched') == 'In Production']),
            'pl': len(my_df[my_df.get('status_enriched') == 'Planned']),
        }
    return result


def compute_inventory(enriched_df):
    """Replaces: Retailer Inventory Report + INV arrays."""
    df = enriched_df.copy()
    og = df[df.get('status_enriched') == 'Dealer Stock']
    by_dealer = []

    for dealer, group in og.groupby('normalized_dealer'):
        if not dealer or not is_retail_dealer(dealer):
            continue
        by_dealer.append({
            'n': dealer, 'm': group['market_area'].iloc[0] if len(group) > 0 else '',
            'ogS': len(group[group['body_type'] == 'SW']),
            'ogQ': len(group[group['body_type'] == 'QM']),
            'my25': len(group[group['model_year'] == 'MY25']),
            'my26': len(group[group['model_year'] == 'MY26']),
        })

    return {'by_dealer': by_dealer, 'total': len(og)}


def compute_historical_sales(enriched_df):
    """Replaces: Historical Sales sheet. Monthly handovers rolling 18 months."""
    df = enriched_df[
        enriched_df['country'].str.upper().str.contains('UNITED STATES', na=False) &
        enriched_df.get('handover_date', pd.Series(dtype='object')).notna()
    ].copy()
    df['ho_month'] = pd.to_datetime(df['handover_date']).dt.strftime('%Y-%m')
    monthly = df.groupby('ho_month').size().to_dict()
    months = sorted(monthly.keys())[-18:]
    return {'months': months, 'totals': [monthly.get(m, 0) for m in months]}


def compute_vex(enriched_df):
    """Replaces: VEX dashboard tab. Full vehicle export with all enriched columns."""
    records = []
    for _, r in enriched_df.iterrows():
        records.append({
            'vin': str(r.get('vin', '')),
            'dealer': str(r.get('normalized_dealer', '')),
            'market': str(r.get('market_area', '')),
            'country': str(r.get('country', '')),
            'body': str(r.get('body_type', '')),
            'my': str(r.get('model_year', '')),
            'status': str(r.get('status_enriched', r.get('status', ''))),
            'msrp': safe_int(r.get('msrp', 0)),
            'trim': str(r.get('trim', '')),
            'ext': str(r.get('ext_color', '')),
            'seats': str(r.get('seats', '')),
            'wheels': str(r.get('wheels', '')),
            'channel': str(r.get('channel', '')),
            'plant': str(r.get('plant_code', '')),
            'ho': str(r.get('handover_date', ''))[:10] if pd.notna(r.get('handover_date')) else '',
            'eta': str(r.get('shipping_eta', ''))[:10] if pd.notna(r.get('shipping_eta')) else '',
            'vessel': str(r.get('vessel', '')),
            'dis': safe_int(r.get('days_in_stock', 0)),
            'dts': safe_int(r.get('days_to_sell', 0)),
            'so': str(r.get('order_no', '')),
        })
    return records


def compute_lead_kpis(leads_df, today=None):
    """Replaces: Lead Handling KPIs sheet."""
    if today is None:
        today = datetime.now()
    if leads_df is None or len(leads_df) == 0:
        return {}

    us = leads_df[leads_df.get('retailer_country', '').astype(str).str.contains('United States', na=False)]

    def kpis_for_window(df):
        total = len(df)
        if total == 0:
            return {'total': 0}
        td_requested = df.get('td_requested', pd.Series(dtype='object')).notna().sum()
        td_completed = df.get('td_completed_flag', '').astype(str).eq('Yes').sum()
        td_show = round(td_completed / td_requested * 100, 1) if td_requested > 0 else 0
        return {'total': total, 'td_requested': int(td_requested), 'td_completed': int(td_completed), 'td_show_pct': td_show}

    result = {'all_time': kpis_for_window(us)}

    for days, label in [(120, 'r120'), (90, 'r90'), (30, 'r30')]:
        cutoff = today - timedelta(days=days)
        window = us[pd.to_datetime(us.get('created_on', pd.Series(dtype='object')), errors='coerce') >= cutoff]
        result[label] = kpis_for_window(window)

    return result


def compute_brand_leads(leads_df, today=None):
    """Replaces: Brand Lead Volume sheet. Daily lead counts."""
    if today is None:
        today = datetime.now()
    if leads_df is None or len(leads_df) == 0:
        return {'dates': [], 'total': [], 'rolling_7': []}

    us = leads_df[leads_df.get('retailer_country', '').astype(str).str.contains('United States', na=False)].copy()
    us['created_date'] = pd.to_datetime(us.get('created_on'), errors='coerce').dt.date
    daily = us.groupby('created_date').size().reset_index(name='total')
    daily = daily.sort_values('created_date')
    daily['rolling_7'] = daily['total'].rolling(7, min_periods=1).mean().round(1)

    return {
        'dates': [str(d) for d in daily['created_date']],
        'total': daily['total'].tolist(),
        'rolling_7': daily['rolling_7'].tolist(),
    }


def compute_santander(santander_data, santander_cache=None):
    """Replaces: Santander dashboard arrays."""
    result = {'monthly': [], 'daily_cache': santander_cache or {}}
    if santander_data:
        for key in ['applications', 'approvals', 'fundings']:
            if key in santander_data:
                result[key] = santander_data[key]
    return result


def compute_scorecard(enriched_df, leads_df=None, ref_db_path=None, today=None):
    """Replaces: SC_DATA dashboard array. Composite dealer scoring."""
    if today is None:
        today = datetime.now()
    df = enriched_df[
        enriched_df['country'].str.upper().str.contains('UNITED STATES', na=False)
    ].copy()

    results = []
    for dealer, group in df.groupby('normalized_dealer'):
        if not dealer or not is_retail_dealer(dealer):
            continue
        og = group[group.get('status_enriched') == 'Dealer Stock']
        sold = group[group.get('handover_date', pd.Series(dtype='object')).notna()]
        dol_vals = og['days_in_stock'].dropna()
        avg_dol = round(dol_vals.mean()) if len(dol_vals) > 0 else 0

        r = {
            'd': dealer, 'mkt': group['market_area'].iloc[0] if len(group) > 0 else '',
            'og': len(og), 'dol': avg_dol,
            'mtd': len(sold[pd.to_datetime(sold['handover_date']).dt.strftime('%Y-%m') == today.strftime('%Y-%m')]),
            'ho': len(sold),
        }

        # R90 sales
        r90_start = today - timedelta(days=90)
        r90_sold = sold[pd.to_datetime(sold['handover_date']) >= r90_start]
        r['r90'] = round(len(r90_sold) / 3, 1)
        r['turn'] = avg_dol

        results.append(r)

    return sorted(results, key=lambda x: x.get('ho', 0), reverse=True)


def compute_objectives(ref_db_path, today=None):
    """Read objectives from reference database."""
    if today is None:
        today = datetime.now()
    try:
        conn = sqlite3.connect(ref_db_path)
        df = pd.read_sql('SELECT * FROM objectives', conn)
        conn.close()
        return df.to_dict('records')
    except Exception:
        return []
