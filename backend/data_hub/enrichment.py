"""Enrichment Engine — replaces 26 VLOOKUP columns from the Master File."""
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from data_hub.utils import (
    clean_dealer_name, normalize_dealer_via_c4c, haversine_miles,
    map_vehicle_status, safe_int, safe_float, OG_CHANNELS
)


def enrich(sap_df, handover_df=None, stock_pipeline_df=None,
           urban_science_df=None, sales_order_df=None,
           campaign_codes_df=None, incentive_spend_df=None,
           qm_leads_df=None, ref_db_path='reference/reference.db'):
    """
    Enrich SAP Export DataFrame with data from all other sources.
    Replaces Export sheet columns 51-76.
    """
    df = sap_df.copy()
    today = pd.Timestamp.now().normalize()

    # Open reference DB if it exists, otherwise skip DB-dependent enrichments
    import os
    conn = None
    if os.path.exists(ref_db_path):
        conn = sqlite3.connect(ref_db_path)
    else:
        print(f"  Warning: Reference DB not found at {ref_db_path}, skipping DB-dependent enrichments")

    # ═══ 1. HANDOVER DATE + REV REC (cols 51, 55) ═══
    if handover_df is not None and len(handover_df) > 0:
        ho_cols = ['vin']
        for c in ['handover_date', 'rev_rec_date', 'retail_date', 'registration_date']:
            if c in handover_df.columns:
                ho_cols.append(c)
        ho = handover_df[ho_cols].drop_duplicates('vin')
        df = df.merge(ho, on='vin', how='left', suffixes=('', '_ho'))

    # ═══ 2. VESSEL ETA + VESSEL NAME (cols 52, 53) ═══
    if stock_pipeline_df is not None and len(stock_pipeline_df) > 0:
        sp_cols = ['order_no']
        for c in ['shipping_eta', 'vessel']:
            if c in stock_pipeline_df.columns:
                sp_cols.append(c)
        vessel = stock_pipeline_df[sp_cols].drop_duplicates('order_no')
        if 'order_no' in df.columns:
            df = df.merge(vessel, on='order_no', how='left', suffixes=('', '_sp'))

    # ═══ 3. MARKET AREA (col 54) ═══
    try:
        c4c = pd.read_sql('SELECT c4c_name, normalized_name FROM c4c_key', conn)
        c4c_lookup = dict(zip(c4c['c4c_name'], c4c['normalized_name']))
    except Exception:
        c4c_lookup = {}

    try:
        rbm = pd.read_sql('SELECT dealer_name, market_area FROM rbm_assignments', conn)
        rbm_lookup = dict(zip(rbm['dealer_name'].str.upper(), rbm['market_area']))
    except Exception:
        rbm_lookup = {}

    if 'customer_name' in df.columns:
        df['normalized_dealer'] = df['customer_name'].apply(
            lambda x: normalize_dealer_via_c4c(x, c4c_lookup))
        df['market_area'] = df['normalized_dealer'].str.upper().map(rbm_lookup).fillna('')

    # ═══ 4. VEHICLE STATUS (enriched) ═══
    if 'status' in df.columns and 'channel' in df.columns:
        has_ho = df.get('handover_date', pd.Series(dtype='object')).notna()
        df['status_enriched'] = df.apply(
            lambda r: map_vehicle_status(r.get('status', ''), r.get('channel', ''), has_ho.get(r.name, False)),
            axis=1
        )

    # ═══ 5. DAYS TO SELL (col 56) ═══
    if 'handover_date' in df.columns:
        df['days_to_sell'] = df.apply(
            lambda r: (pd.Timestamp(r['handover_date']) - pd.Timestamp(r.get('invoice_date', r.get('order_date'))))
            .days if pd.notna(r.get('handover_date')) and pd.notna(r.get('invoice_date', r.get('order_date')))
            else None, axis=1
        )

    # ═══ 6. DAYS IN STOCK (col 57) ═══
    if 'order_date' in df.columns:
        df['days_in_stock'] = df.apply(
            lambda r: (today - pd.Timestamp(r['order_date'])).days
            if r.get('status_enriched') == 'Dealer Stock' and pd.notna(r.get('order_date'))
            else None, axis=1
        )

    # ═══ 7. HANDOVER BILL TO DEALER (col 58) ═══
    # Priority: Sales Order bill-to > Handover bill-to > 'Not Handed Over'
    df['bill_to_dealer'] = 'Not Handed Over'
    if handover_df is not None and 'customer_name' in handover_df.columns:
        ho_bill = handover_df[['vin', 'customer_name']].drop_duplicates(subset='vin', keep='last')
        ho_bill.columns = ['vin', 'bill_to_dealer_ho']
        df = df.merge(ho_bill, on='vin', how='left')
        df['bill_to_dealer'] = df['bill_to_dealer_ho'].fillna(df['bill_to_dealer'])
        df = df.drop(columns=['bill_to_dealer_ho'], errors='ignore')
    if sales_order_df is not None and 'bill_to_dealer' in sales_order_df.columns:
        so_bill = sales_order_df[['vin', 'bill_to_dealer']].drop_duplicates(subset='vin', keep='last')
        so_bill.columns = ['vin', 'bill_to_dealer_so']
        df = df.merge(so_bill, on='vin', how='left')
        mask = df['bill_to_dealer_so'].notna() & (df['bill_to_dealer_so'].str.strip() != '')
        df.loc[mask, 'bill_to_dealer'] = df.loc[mask, 'bill_to_dealer_so']
        df = df.drop(columns=['bill_to_dealer_so'], errors='ignore')

    # ═══ 8. RETAILER ZIP/LAT/LONG (cols 59-61) ═══
    try:
        addr = pd.read_sql('SELECT dealer_name, zip, latitude, longitude FROM dealer_address', conn)
        addr_lookup = {r['dealer_name'].upper(): r for _, r in addr.iterrows()}
    except Exception:
        addr_lookup = {}

    if 'normalized_dealer' in df.columns:
        df['retailer_zip'] = df['normalized_dealer'].str.upper().map(
            lambda x: addr_lookup.get(x, {}).get('zip', ''))
        df['retailer_lat'] = df['normalized_dealer'].str.upper().map(
            lambda x: addr_lookup.get(x, {}).get('latitude'))
        df['retailer_lon'] = df['normalized_dealer'].str.upper().map(
            lambda x: addr_lookup.get(x, {}).get('longitude'))

    # ═══ 9. CVP FLAG (col 62) ═══
    cvp_vins = set()
    demo_vins = set()
    if campaign_codes_df is not None and len(campaign_codes_df) > 0:
        # Use uploaded campaign codes file
        if 'campaign_type' in campaign_codes_df.columns:
            cvp_vins = set(campaign_codes_df[campaign_codes_df['campaign_type'] == 'CVP']['vin'].dropna())
            demo_vins = set(campaign_codes_df[campaign_codes_df['campaign_type'] == 'Demo']['vin'].dropna())
        elif 'is_cvp' in campaign_codes_df.columns:
            cvp_vins = set(campaign_codes_df[campaign_codes_df['is_cvp'].astype(str).str.upper() == 'YES']['vin'].dropna())
        if 'is_demo' in campaign_codes_df.columns:
            demo_vins = set(campaign_codes_df[campaign_codes_df['is_demo'].astype(str).str.upper() == 'YES']['vin'].dropna())
    else:
        # Fallback: reference database
        try:
            cvp_vins = set(pd.read_sql("SELECT vin FROM campaign_codes WHERE campaign_type='CVP'", conn)['vin'])
        except Exception:
            pass
    df['cvp'] = df['vin'].isin(cvp_vins).map({True: 'Yes', False: 'No'})

    # ═══ 10. DEMO FLAG (col 63) ═══
    if not demo_vins:
        try:
            demo_vins = set(pd.read_sql("SELECT vin FROM campaign_codes WHERE campaign_type='Demo'", conn)['vin'])
        except Exception:
            pass
    df['demo'] = df['vin'].isin(demo_vins).map({True: 'Yes', False: 'No'})

    # ═══ 11. CUSTOMER GEOGRAPHY (cols 64-68) ═══
    if urban_science_df is not None and len(urban_science_df) > 0:
        us_cols = ['vin']
        for c in ['customer_city', 'customer_state', 'customer_zip']:
            if c in urban_science_df.columns:
                us_cols.append(c)
        us = urban_science_df[us_cols].drop_duplicates('vin')
        df = df.merge(us, on='vin', how='left', suffixes=('', '_cust'))

        # Customer lat/long from zip
        try:
            zips = pd.read_sql('SELECT zip, latitude, longitude FROM zip_lat_long', conn)
            zip_lookup = dict(zip(zips['zip'], zip(zips['latitude'], zips['longitude'])))
        except Exception:
            zip_lookup = {}

        if 'customer_zip' in df.columns:
            df['customer_lat'] = df['customer_zip'].map(lambda z: zip_lookup.get(str(z), (None, None))[0])
            df['customer_lon'] = df['customer_zip'].map(lambda z: zip_lookup.get(str(z), (None, None))[1])

    # ═══ 12. SPEND COLUMNS (cols 70-75) ═══
    df['incentive_spend'] = 0
    df['subvention_spend'] = 0
    if incentive_spend_df is not None and len(incentive_spend_df) > 0:
        # Use uploaded incentive/subvention spend file
        if 'incentive_amount' in incentive_spend_df.columns:
            inc = incentive_spend_df.groupby('vin')['incentive_amount'].sum()
            df['incentive_spend'] = df['vin'].map(inc).fillna(0)
        if 'subvention_amount' in incentive_spend_df.columns:
            sub = incentive_spend_df.groupby('vin')['subvention_amount'].sum()
            df['subvention_spend'] = df['vin'].map(sub).fillna(0)
        if 'amount' in incentive_spend_df.columns and 'incentive_amount' not in incentive_spend_df.columns:
            # Single amount column — use spend_type to split
            if 'spend_type' in incentive_spend_df.columns:
                inc = incentive_spend_df[incentive_spend_df['spend_type'].str.contains('incentive', case=False, na=False)].groupby('vin')['amount'].sum()
                sub = incentive_spend_df[incentive_spend_df['spend_type'].str.contains('subvention', case=False, na=False)].groupby('vin')['amount'].sum()
                df['incentive_spend'] = df['vin'].map(inc).fillna(0)
                df['subvention_spend'] = df['vin'].map(sub).fillna(0)
            else:
                total = incentive_spend_df.groupby('vin')['amount'].sum()
                df['incentive_spend'] = df['vin'].map(total).fillna(0)
    else:
        # Fallback: reference database
        try:
            spend = pd.read_sql(
                "SELECT vin, campaign_type, SUM(amount) as amount FROM campaign_codes GROUP BY vin, campaign_type", conn)
            incentive = spend[spend['campaign_type'] == 'Incentive'].set_index('vin')['amount']
            subvention = spend[spend['campaign_type'] == 'Subvention'].set_index('vin')['amount']
            df['incentive_spend'] = df['vin'].map(incentive).fillna(0)
            df['subvention_spend'] = df['vin'].map(subvention).fillna(0)
        except Exception:
            pass
    df['total_variable_spend'] = df['incentive_spend'] + df['subvention_spend']
    df['msrp_less_vie'] = pd.to_numeric(df.get('msrp', 0), errors='coerce').fillna(0) - df['total_variable_spend']

    # ═══ 13. MILES TO CUSTOMER (col 76) ═══
    if all(c in df.columns for c in ['retailer_lat', 'retailer_lon', 'customer_lat', 'customer_lon']):
        df['miles_to_customer'] = df.apply(
            lambda r: haversine_miles(r['retailer_lat'], r['retailer_lon'],
                                     r['customer_lat'], r['customer_lon']), axis=1)

    if conn:
        conn.close()
    return df
