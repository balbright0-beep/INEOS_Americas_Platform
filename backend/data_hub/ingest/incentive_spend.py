"""Incentive & Subvention Spend ingest handler.
Library of incentive and subvention spend by VIN.
Used for dashboard variable spend columns and incentive dashboard tracking."""
import pandas as pd
import zipfile
import xml.etree.ElementTree as ET
import re


def ingest_incentive_spend(filepath):
    """Parse Incentive & Subvention Spend file."""
    # Try raw XML first (handles problematic xlsx)
    try:
        return _parse_xlsx_raw(filepath)
    except Exception as e1:
        try:
            df = pd.read_excel(filepath, engine='openpyxl', dtype=str)
            return _process(df)
        except Exception as e2:
            raise RuntimeError(f"Could not parse Incentive Spend: {e1} / {e2}")


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

        # Find the sheet with the most rows (detail data)
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

    # Find header row (contains VIN or Vehicle)
    header_idx = 0
    for i, row in enumerate(data[:10]):
        row_str = ' '.join(str(v) for v in row).lower()
        if 'vin' in row_str or 'vehicle' in row_str or 'incentive' in row_str or 'subvention' in row_str:
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
    """Normalize Incentive Spend DataFrame."""
    rename = {}
    col_patterns = {
        'VIN': 'vin',
        'Vehicle VIN': 'vin',
        'Incentive': 'incentive_amount',
        'Subvention': 'subvention_amount',
        'Total': 'total_spend',
        'Amount': 'amount',
        'Dealer': 'dealer',
        'Campaign': 'campaign_code',
        'Program': 'program',
        'Type': 'spend_type',
    }
    for actual_col in df.columns:
        for pattern, internal in col_patterns.items():
            if pattern.lower() in str(actual_col).lower():
                if internal not in rename.values():
                    rename[actual_col] = internal
                    break
    df = df.rename(columns=rename)

    # Convert amounts to numeric
    for col in ['incentive_amount', 'subvention_amount', 'total_spend', 'amount']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # VIN uppercase
    if 'vin' in df.columns:
        df['vin'] = df['vin'].astype(str).str.strip().str.upper()
        df = df[df['vin'].str.len() > 3]

    print(f"  Incentive Spend: {len(df)} rows")
    return df
