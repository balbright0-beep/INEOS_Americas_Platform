"""QM Leads (ALL LEADS) ingest handler — Quartermaster-specific lead list.
Cross-referenced with C4C leads to determine SW vs QM lead volume split."""
import pandas as pd
import zipfile
import xml.etree.ElementTree as ET
import re


def ingest_qm_leads(filepath):
    """Parse QM Leads file. Uses raw XML extraction like C4C handler."""
    # Try raw XML approach first (handles problematic xlsx files)
    try:
        return _parse_xlsx_raw(filepath)
    except Exception as e1:
        # Fallback: pandas with dtype=str
        try:
            df = pd.read_excel(filepath, engine='openpyxl', dtype=str)
            return _process_qm(df)
        except Exception as e2:
            raise RuntimeError(f"Could not parse QM Leads: {e1} / {e2}")


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

        sheet_path = None
        for name in z.namelist():
            if name.startswith('xl/worksheets/sheet') and name.endswith('.xml'):
                sheet_path = name
                break

        if not sheet_path:
            raise RuntimeError("No worksheet found in xlsx")

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

    all_cols = sorted(set(c for r in rows for c in r.keys()),
                      key=lambda x: (len(x), x))
    data = [[r.get(c, '') for c in all_cols] for r in rows]

    # Find header row
    header_idx = 0
    for i, row in enumerate(data[:10]):
        row_str = ' '.join(str(v) for v in row).lower()
        if 'lead' in row_str or 'name' in row_str or 'retailer' in row_str:
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

    return _process_qm(df)


def _process_qm(df):
    """Process QM leads DataFrame."""
    # Try to identify key columns. Order matters: more specific patterns first
    # so 'Lead Name' isn't accidentally renamed to lead_id by the 'Name' rule.
    col_patterns = [
        ('lead id', 'lead_id'),
        ('lead name', 'lead_name'),
        ('retailer', 'retailer_name'),
        ('status', 'lead_status'),
        ('created', 'created_on'),
        ('country', 'country'),
        ('source', 'source'),
    ]
    rename = {}
    used_internal = set()
    for actual_col in df.columns:
        col_lower = str(actual_col).lower()
        for pattern, internal in col_patterns:
            if pattern in col_lower and internal not in used_internal:
                rename[actual_col] = internal
                used_internal.add(internal)
                break
    df = df.rename(columns=rename)

    # Parse dates
    if 'created_on' in df.columns:
        df['created_on'] = pd.to_datetime(df['created_on'], errors='coerce')

    # Tag as QM
    df['body_type'] = 'QM'

    # Drop empty rows
    if 'lead_id' in df.columns:
        df['lead_id'] = df['lead_id'].astype(str).str.strip()
        df = df[df['lead_id'] != '']
        df = df[~df['lead_id'].isin(['nan', 'None', 'NaT'])]
    else:
        print("  [WARN] QM Leads file has no recognizable Lead ID column — "
              f"available columns: {list(df.columns)[:10]}")

    print(f"  QM Leads: {len(df)} rows")
    if 'lead_id' in df.columns and len(df) > 0:
        print(f"    Sample QM lead IDs from source: {df['lead_id'].head(3).tolist()}")
    return df
