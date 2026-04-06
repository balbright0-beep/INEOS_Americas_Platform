"""Campaign Code Extract ingest handler.
Determines which campaign codes are applied to which VINs.
Critical for: CVP counting, Demo flagging, Incentive Dashboard tracking."""
import pandas as pd
import zipfile
import xml.etree.ElementTree as ET
import re


def ingest_campaign_codes(filepath):
    """Parse Campaign Code Extract file."""
    try:
        return _parse_xlsx_raw(filepath)
    except Exception as e1:
        try:
            df = pd.read_excel(filepath, engine='openpyxl', dtype=str)
            return _process(df)
        except Exception as e2:
            raise RuntimeError(f"Could not parse Campaign Codes: {e1} / {e2}")


def _parse_xlsx_raw(filepath):
    """Parse xlsx by reading XML directly from the ZIP archive."""
    with zipfile.ZipFile(filepath) as z:
        shared_strings = []
        if 'xl/sharedStrings.xml' in z.namelist():
            ss_xml = z.read('xl/sharedStrings.xml')
            ns = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
            root = ET.fromstring(ss_xml)
            for si in root.findall('.//s:si', ns):
                texts = si.findall('.//s:t', ns)
                shared_strings.append(''.join(t.text or '' for t in texts))

        # Find the sheet with the most rows (the detail data, not summary)
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

    # Find header row
    header_idx = 0
    for i, row in enumerate(data[:10]):
        row_str = ' '.join(str(v) for v in row).lower()
        if 'vin' in row_str or 'campaign' in row_str or 'code' in row_str:
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
    """Normalize Campaign Codes DataFrame."""
    rename = {}
    # Priority order: most specific patterns first
    col_patterns = [
        ('Vehicle VIN', 'vin'),
        ('Campaign code', 'campaign_code'),
        ('Campaign Code', 'campaign_code'),
        ('SO Sales Order', 'order_no'),
        ('SO Channel', 'channel'),
        ('Country Name', 'country'),
        ('Ship to Party', 'ship_to_party'),
        ('Ship To Party', 'dealer'),
        ('Handover Status Date', 'handover_date'),
        ('Registration Date', 'registration_date'),
        ('Retail Date', 'retail_date'),
        ('Region Group', 'region'),
    ]
    for actual_col in df.columns:
        for pattern, internal in col_patterns:
            if pattern.lower() in str(actual_col).lower():
                if internal not in rename.values():
                    rename[actual_col] = internal
                    break
    df = df.rename(columns=rename)

    # Determine campaign type from the "Campaign code applied?" column
    if 'campaign_code' in df.columns:
        # The campaign_code column contains values like "YES", "NO", "BLANK"
        # This indicates whether a campaign code was applied
        df['has_campaign'] = df['campaign_code'].astype(str).str.upper().isin(['YES', 'TRUE', '1'])

    # Try to classify based on channel or other columns
    if 'channel' in df.columns:
        df['campaign_type'] = df['channel'].apply(_classify_campaign)
    elif 'campaign_code' not in df.columns:
        df['campaign_type'] = 'Other'

    # Convert amount to numeric
    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)

    # Parse dates
    if 'effective_date' in df.columns:
        df['effective_date'] = pd.to_datetime(df['effective_date'], errors='coerce')

    # VIN uppercase
    if 'vin' in df.columns:
        df['vin'] = df['vin'].astype(str).str.strip().str.upper()
        df = df[df['vin'].str.len() > 3]

    print(f"  Campaign Codes: {len(df)} rows")
    return df


def _classify_campaign(val):
    """Classify campaign type from code or description text."""
    if not val:
        return 'Other'
    val_upper = str(val).upper()
    if 'CVP' in val_upper or 'CO-DEVELOPMENT' in val_upper or 'EMPLOYEE' in val_upper:
        return 'CVP'
    if 'DEMO' in val_upper or 'DEMONSTRATION' in val_upper:
        return 'Demo'
    if 'SUBVENTION' in val_upper or 'RATE' in val_upper:
        return 'Subvention'
    if 'INCENTIVE' in val_upper or 'BONUS' in val_upper or 'LOYALTY' in val_upper:
        return 'Incentive'
    if 'FLEET' in val_upper or 'RENTAL' in val_upper:
        return 'Fleet'
    return 'Other'
