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
    headers = None
    with wb.get_sheet("Data") as sheet:
        for i, row in enumerate(sheet.rows()):
            vals = [c.v for c in row]
            if i == 1:
                headers = [safe_str(v) for v in vals]
            elif i >= 2:
                rows.append(vals)

    if headers:
        headers = _dedup_columns(headers)
    df = pd.DataFrame(rows, columns=headers if headers else None)
    return _process_sp(df)


def _ingest_xlsx(filepath):
    """Parse .xlsx format using openpyxl. Handles duplicate column names."""
    # Read without header first to get raw column names
    try:
        df_raw = pd.read_excel(filepath, sheet_name='Data', header=None, engine='openpyxl', nrows=3)
        # Row 1 typically has headers
        headers = [safe_str(v) for v in df_raw.iloc[1].values]
        headers = _dedup_columns(headers)

        # Read data starting from row 2
        df = pd.read_excel(filepath, sheet_name='Data', header=None, skiprows=2, engine='openpyxl')
        df.columns = headers[:len(df.columns)]
    except Exception:
        # Fallback: let pandas handle it
        df = pd.read_excel(filepath, sheet_name='Data', header=1, engine='openpyxl')
        df.columns = _dedup_columns([str(c) for c in df.columns])

    return _process_sp(df)


def _dedup_columns(cols):
    """Deduplicate column names by appending .1, .2 suffix."""
    seen = {}
    result = []
    for c in cols:
        c_str = str(c).strip()
        if c_str in seen:
            seen[c_str] += 1
            result.append(f"{c_str}.{seen[c_str]}")
        else:
            seen[c_str] = 0
            result.append(c_str)
    return result


def _process_sp(df):
    """Filter to Americas and extract key columns."""
    # Find region column
    region_col = None
    for col in df.columns:
        if 'REGION' in str(col).upper():
            region_col = col
            break

    if region_col:
        df = df[df[region_col].astype(str).str.upper().str.contains('AMERICA', na=False)]

    # Normalize column names
    # NOTE: Priority order matters — more specific keys must appear before
    # generic ones so e.g. 'SHIPPING ETA' wins over a bare 'ETA' match.
    rename = {}
    col_map = [
        ('ORDER NO', 'order_no'),
        ('VIN', 'vin'),
        ('SHIPPING ETA', 'shipping_eta'),
        ('ETA DATE', 'shipping_eta'),
        ('ARRIVAL DATE', 'shipping_eta'),
        ('DESTINATION ETA', 'shipping_eta'),
        ('PORT ETA', 'shipping_eta'),
        ('ETA', 'shipping_eta'),  # generic fallback — last resort
        ('VESSEL', 'vessel'),
        ('MARKET', 'market'),
        ('VEHICLE STATUS DESC', 'vehicle_status'),
        ('MODEL YEAR', 'model_year'),
        ('DERIVATIVE', 'derivative'),
        ('STOCK AGE FROM BUILD', 'stock_age'),
        ('HANDOVER COMPLETE DATE', 'handover_complete_date'),
        ('FOK DATE', 'fok_date'),
    ]
    for actual_col in df.columns:
        col_upper = str(actual_col).upper().strip()
        for key, val in col_map:
            if key in col_upper:
                if val not in rename.values():  # Avoid duplicate mappings
                    rename[actual_col] = val
                    break
    df = df.rename(columns=rename)
    print(f"  [stock_pipeline] renamed columns: {rename}")

    # Parse shipping ETA (may arrive as serial number, datetime, or string)
    if 'shipping_eta' in df.columns:
        def _parse_eta(v):
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return excel_serial_to_date(v)
            if isinstance(v, pd.Timestamp):
                return v.to_pydatetime() if not pd.isna(v) else None
            if isinstance(v, str):
                s = v.strip()
                if not s or s.lower() in ('nan', 'none', 'nat'):
                    return None
                try:
                    ts = pd.to_datetime(s, errors='coerce')
                    return ts.to_pydatetime() if not pd.isna(ts) else None
                except Exception:
                    return None
            # Already a datetime-like
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass
            return v
        df['shipping_eta'] = df['shipping_eta'].apply(_parse_eta)
        non_null_eta = df['shipping_eta'].notna().sum()
        print(f"  [stock_pipeline] shipping_eta parsed: {non_null_eta}/{len(df)} non-null")

    # VIN to uppercase
    if 'vin' in df.columns:
        df['vin'] = df['vin'].astype(str).str.strip().str.upper()

    if 'order_no' in df.columns:
        df['order_no'] = df['order_no'].astype(str).str.strip()

    if 'vessel' in df.columns:
        df['vessel'] = df['vessel'].astype(str).str.strip()

    return df
