"""Sales Order Report ingest handler — provides Bill-to-Dealer for true retail attribution."""
import pandas as pd


def ingest_sales_order(filepath):
    """Parse Sales Order Report. Key column: Bill-to-Dealer linked by SO/VIN."""
    df = pd.read_excel(filepath, engine='openpyxl', dtype=str)

    # Rename columns (partial match)
    rename_map = {
        'Sales Order': 'order_no',
        'SO Sales Order': 'order_no',
        'Vehicle VIN': 'vin',
        'VIN': 'vin',
        'Bill-to': 'bill_to_dealer',
        'Bill to': 'bill_to_dealer',
        'Bill To Party': 'bill_to_dealer',
        'Bill-To Party': 'bill_to_dealer',
        'Ship to Party': 'ship_to_party',
        'Customer': 'customer_name',
        'Customer Full Name': 'customer_name',
        'Material': 'material',
        'Material Desc': 'material',
        'Channel': 'channel',
        'SO Channel': 'channel',
        'Handover': 'handover_flag',
        'Handover Complete': 'handover_flag',
        'Status': 'status',
        'Retail Date': 'retail_date',
    }

    rename = {}
    for actual_col in df.columns:
        for key, val in rename_map.items():
            if key.lower() in str(actual_col).lower():
                if val not in rename.values():
                    rename[actual_col] = val
                    break
    df = df.rename(columns=rename)

    # Parse dates
    for col in ['retail_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # VIN uppercase
    if 'vin' in df.columns:
        df['vin'] = df['vin'].astype(str).str.strip().str.upper()
        df = df[df['vin'].str.len() > 3]

    print(f"  Sales Order: {len(df)} rows, cols: {list(df.columns)[:8]}")
    return df
