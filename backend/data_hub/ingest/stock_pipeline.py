"""Stock & Pipeline Report ingest handler."""
import pandas as pd
from data_hub.utils import excel_serial_to_date, safe_str


def ingest_stock_pipeline(filepath):
    """Parse Stock & Pipeline Report. Extracts Americas subset with vessel/ETA data."""
    ext = filepath.lower().rsplit('.', 1)[-1]

    if ext == 'xlsb':
        return _ingest_xlsb(filepath)
    else:
        return _ingest_xlsx(filepath)


def _ingest_xlsb(filepath):
    """Parse .xlsb format using pyxlsb."""
    from pyxlsb import open_workbook
    wb = open_workbook(filepath)
    rows = []
    with wb.get_sheet("Data") as sheet:
        headers = None
        for i, row in enumerate(sheet.rows()):
            vals = [c.v for c in row]
            if i == 1:
                headers = [safe_str(v) for v in vals]
            elif i >= 2:
                rows.append(vals)

    df = pd.DataFrame(rows, columns=headers if headers else None)
    return _process_sp(df)


def _ingest_xlsx(filepath):
    """Parse .xlsx format using openpyxl. Handles duplicate column names."""
    df = pd.read_excel(filepath, sheet_name='Data', header=1, engine='openpyxl')
    # Deduplicate column names by appending suffix
    cols = list(df.columns)
    seen = {}
    new_cols = []
    for c in cols:
        c_str = str(c)
        if c_str in seen:
            seen[c_str] += 1
            new_cols.append(f"{c_str}.{seen[c_str]}")
        else:
            seen[c_str] = 0
            new_cols.append(c_str)
    df.columns = new_cols
    return _process_sp(df)


def _process_sp(df):
    """Filter to Americas and extract key columns."""
    # Find region column (typically col 22 = 'REGION')
    region_col = None
    for col in df.columns:
        if 'REGION' in str(col).upper():
            region_col = col
            break

    if region_col:
        df = df[df[region_col].astype(str).str.upper().str.contains('AMERICA', na=False)]

    # Normalize column names
    rename = {}
    col_map = {
        'ORDER NO': 'order_no',
        'VIN': 'vin',
        'SHIPPING ETA': 'shipping_eta',
        'VESSEL': 'vessel',
        'MARKET': 'market',
        'VEHICLE STATUS DESC': 'vehicle_status',
        'MODEL YEAR': 'model_year',
        'DERIVATIVE': 'derivative',
        'STOCK AGE FROM BUILD': 'stock_age',
        'Handover Complete Date': 'handover_complete_date',
    }
    for actual_col in df.columns:
        col_upper = str(actual_col).upper().strip()
        for key, val in col_map.items():
            if key.upper() in col_upper:
                rename[actual_col] = val
                break
    df = df.rename(columns=rename)

    # Parse shipping ETA
    if 'shipping_eta' in df.columns:
        df['shipping_eta'] = df['shipping_eta'].apply(excel_serial_to_date)

    # VIN to uppercase
    if 'vin' in df.columns:
        df['vin'] = df['vin'].astype(str).str.strip().str.upper()

    if 'order_no' in df.columns:
        df['order_no'] = df['order_no'].astype(str).str.strip()

    if 'vessel' in df.columns:
        df['vessel'] = df['vessel'].astype(str).str.strip()

    return df
