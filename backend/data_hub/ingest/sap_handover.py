"""SAP Handover Report ingest handler."""
import pandas as pd

HANDOVER_COLUMNS = {
    'Ship to Party No': 'ship_to_party',
    'Customer Full Name': 'customer_name',
    'SO Sales Order No': 'order_no',
    'Material Desc': 'material',
    'Count of Vehicle VIN': 'vin_count',
    'Vehicle VIN': 'vin',
    'Stock Category': 'stock_category',
    'SO Channel Desc': 'channel',
    'Vehicle Current Primary Status Code': 'status_code',
    'Vehicle Current Primary Status Text': 'status',
    'SO ZRTL Retail Date': 'retail_date',
    'Vehicle Registration Date': 'registration_date',
    'SO Vehicle Handover Complete Flag': 'handover_complete',
    'SO Vehicle Handover Status Date': 'handover_date',
    '#Revenue Recognition Date': 'rev_rec_date',
}

DATE_COLS = ['retail_date', 'registration_date', 'handover_date', 'rev_rec_date']


def ingest_handover(filepath):
    """Parse SAP Handover Report into normalized DataFrame."""
    # Read all data as strings first to avoid type conversion issues
    df = pd.read_excel(filepath, engine='openpyxl', dtype=str)

    print(f"  Handover raw: {len(df)} rows, {len(df.columns)} cols")
    print(f"  Columns: {list(df.columns)[:10]}...")

    # Rename columns (partial match)
    rename = {}
    for src_col, int_col in HANDOVER_COLUMNS.items():
        for actual_col in df.columns:
            if src_col in str(actual_col):
                rename[actual_col] = int_col
                break
    df = df.rename(columns=rename)

    # Parse date columns safely
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # VIN to uppercase string — keep ALL rows, don't deduplicate
    # (deduplication can happen later during enrichment)
    if 'vin' in df.columns:
        df['vin'] = df['vin'].astype(str).str.strip().str.upper()
        # Only drop rows where VIN is truly empty
        df = df[df['vin'].str.len() > 3]

    print(f"  Handover processed: {len(df)} rows")
    return df
