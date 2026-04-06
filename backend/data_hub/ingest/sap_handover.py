"""SAP Handover Report / Sales Order Handover ingest handler.
Handles both traditional SAP Handover format and ListOfSalesOrders format."""
import pandas as pd
from data_hub.utils import parse_xlsx_raw, excel_serial_to_date

HANDOVER_COLUMNS = {
    'Ship to Party No': 'ship_to_party',
    'Customer Full Name': 'customer_name',
    'SO Sales Order No': 'order_no',
    'Material Desc': 'material',
    'Vehicle VIN': 'vin',
    'SO Channel Desc': 'channel',
    'Vehicle Current Primary Status Text': 'status',
    'SO ZRTL Retail Date': 'retail_date',
    'Vehicle Registration Date': 'registration_date',
    'SO Vehicle Handover Complete Flag': 'handover_complete',
    'SO Vehicle Handover Status Date': 'handover_date',
    '#Revenue Recognition Date': 'rev_rec_date',
}

# Alternative column names for ListOfSalesOrders format
SALES_ORDER_COLUMNS = {
    'ID': 'order_no',
    'External ID': 'external_id',
    'Account ID': 'account_id',
    'Account': 'customer_name',
    'Document Type': 'doc_type',
    'Status': 'status',
    'Delivery Status': 'delivery_status',
    'Requested Date': 'requested_date',
    'Ship-To': 'ship_to',
    'Total': 'total_amount',
    'Handover Complete': 'handover_complete',
    'Hand Over Date': 'handover_date',
    'Changed On': 'changed_on',
}


def ingest_handover(filepath):
    """Parse SAP Handover or Sales Order report."""
    # Try pandas first
    try:
        df = pd.read_excel(filepath, engine='openpyxl', dtype=str)
        if len(df) > 10:
            return _process(df)
    except Exception:
        pass

    # Fallback: shared raw XML parser
    df = parse_xlsx_raw(filepath)
    return _process(df)


def _process(df):
    """Normalize — handles both SAP Handover and Sales Order formats."""
    cols = set(str(c) for c in df.columns)

    # Detect format
    if 'Vehicle VIN' in cols or any('Vehicle VIN' in str(c) for c in cols):
        # Traditional SAP Handover format — use exact matching first
        rename = {}
        # First pass: exact matches
        for actual_col in df.columns:
            col_str = str(actual_col).strip()
            if col_str == 'Vehicle VIN':
                rename[actual_col] = 'vin'
            elif col_str == 'Count of Vehicle VIN':
                rename[actual_col] = 'vin_count'
        # Second pass: partial matches for remaining columns
        for src_col, int_col in HANDOVER_COLUMNS.items():
            if int_col in rename.values():
                continue  # Already mapped
            for actual_col in df.columns:
                if actual_col in rename:
                    continue  # Already mapped
                if src_col in str(actual_col):
                    rename[actual_col] = int_col
                    break
        df = df.rename(columns=rename)
    else:
        # ListOfSalesOrders format
        rename = {}
        for src_col, int_col in SALES_ORDER_COLUMNS.items():
            for actual_col in df.columns:
                if src_col == str(actual_col).strip():
                    rename[actual_col] = int_col
                    break
        df = df.rename(columns=rename)

    # Parse handover date (could be Excel serial)
    if 'handover_date' in df.columns:
        df['handover_date'] = df['handover_date'].apply(
            lambda v: excel_serial_to_date(v) if v and str(v).replace('.', '').isdigit()
            else pd.to_datetime(v, errors='coerce'))

    # Parse other dates
    for col in ['retail_date', 'registration_date', 'rev_rec_date', 'requested_date', 'changed_on']:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: excel_serial_to_date(v) if v and str(v).replace('.', '').isdigit()
                else pd.to_datetime(v, errors='coerce'))

    # VIN handling
    if 'vin' in df.columns:
        df['vin'] = df['vin'].astype(str).str.strip().str.upper()
        print(f"  VIN sample before filter: {df['vin'].head(5).tolist()}")
        print(f"  VIN lengths: {df['vin'].str.len().value_counts().head(5).to_dict()}")
        # Filter out empty/nan VINs but keep real ones
        df = df[~df['vin'].isin(['', 'NAN', 'NONE', 'NULL'])]
        df = df[df['vin'].str.len() > 3]
    elif 'order_no' in df.columns:
        # No VIN column — keep all rows, they'll be joined by order_no
        df['order_no'] = df['order_no'].astype(str).str.strip()
        df = df[df['order_no'].str.len() > 3]

    # Normalize handover_complete to Yes/No
    if 'handover_complete' in df.columns:
        df['handover_complete'] = df['handover_complete'].astype(str).str.strip().str.capitalize()

    print(f"  Handover: {len(df)} rows, {len(df.columns)} cols")
    return df
