"""SAP Handover Report ingest handler — uses raw XML to avoid openpyxl errors."""
import pandas as pd
import zipfile
import xml.etree.ElementTree as ET
import re

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
    """Parse SAP Handover Report. Uses raw XML if openpyxl fails."""
    # Try pandas first (faster for valid files)
    try:
        df = pd.read_excel(filepath, engine='openpyxl', dtype=str)
        if len(df) > 10:
            return _process(df)
    except Exception:
        pass

    # Fallback: raw XML extraction
    try:
        return _parse_xlsx_raw(filepath)
    except Exception as e:
        raise RuntimeError(f"Could not parse Handover Report: {e}")


def _parse_xlsx_raw(filepath):
    """Parse xlsx by reading XML directly from the ZIP archive."""
    with zipfile.ZipFile(filepath) as z:
        shared_strings = []
        if 'xl/sharedStrings.xml' in z.namelist():
            root = ET.fromstring(z.read('xl/sharedStrings.xml'))
            ns = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
            for si in root.findall('.//s:si', ns):
                texts = si.findall('.//s:t', ns)
                shared_strings.append(''.join(t.text or '' for t in texts))

        # Find largest sheet
        sheet_path = None
        max_rows = 0
        ns_check = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
        for name in sorted(z.namelist()):
            if name.startswith('xl/worksheets/sheet') and name.endswith('.xml'):
                try:
                    root_check = ET.fromstring(z.read(name))
                    row_count = len(root_check.findall('.//s:sheetData/s:row', ns_check))
                    if row_count > max_rows:
                        max_rows = row_count
                        sheet_path = name
                except:
                    pass

        if not sheet_path:
            raise RuntimeError("No worksheet found")

        sheet_xml = z.read(sheet_path)
        ns = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
        root = ET.fromstring(sheet_xml)

        rows = []
        for row_el in root.findall('.//s:sheetData/s:row', ns):
            row_data = {}
            for cell in row_el.findall('s:c', ns):
                ref = cell.get('r', '')
                cell_type = cell.get('t', '')
                val_el = cell.find('s:v', ns)
                val = val_el.text if val_el is not None else ''
                if cell_type == 's' and val:
                    try:
                        val = shared_strings[int(val)]
                    except (ValueError, IndexError):
                        pass
                col = re.match(r'([A-Z]+)', ref)
                if col:
                    row_data[col.group(1)] = val
            rows.append(row_data)

    if not rows:
        return pd.DataFrame()

    all_cols = sorted(set(c for r in rows for c in r.keys()), key=lambda x: (len(x), x))
    data = [[r.get(c, '') for c in all_cols] for r in rows]

    # Find header row (contains 'Vehicle VIN' or 'Handover')
    header_idx = 0
    for i, row in enumerate(data[:10]):
        row_str = ' '.join(str(v) for v in row)
        if 'Vehicle VIN' in row_str or 'Handover' in row_str:
            header_idx = i
            break

    headers = data[header_idx]
    data_rows = data[header_idx + 1:]

    # Clean headers
    seen = {}
    clean_headers = []
    for i, h in enumerate(headers):
        h = h.strip() if h else ''
        if not h:
            h = f'_col_{i}'
        if h in seen:
            seen[h] += 1
            h = f'{h}.{seen[h]}'
        else:
            seen[h] = 0
        clean_headers.append(h)

    data_rows = [r[:len(clean_headers)] for r in data_rows]
    df = pd.DataFrame(data_rows, columns=clean_headers)
    df = df[[c for c in df.columns if not c.startswith('_col_')]]

    return _process(df)


def _process(df):
    """Normalize handover DataFrame."""
    rename = {}
    for src_col, int_col in HANDOVER_COLUMNS.items():
        for actual_col in df.columns:
            if src_col in str(actual_col):
                rename[actual_col] = int_col
                break
    df = df.rename(columns=rename)

    # Parse date columns
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # VIN to uppercase
    if 'vin' in df.columns:
        df['vin'] = df['vin'].astype(str).str.strip().str.upper()
        df = df[df['vin'].str.len() > 3]

    print(f"  Handover: {len(df)} rows")
    return df
