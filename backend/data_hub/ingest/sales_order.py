"""Sales Order Report ingest handler — uses raw XML to avoid openpyxl type errors."""
import pandas as pd
import zipfile
import xml.etree.ElementTree as ET
import re


def ingest_sales_order(filepath):
    """Parse Sales Order Report using shared raw XML parser."""
    from data_hub.utils import parse_xlsx_raw

    def _find_header(rows):
        for i, row in enumerate(rows[:10]):
            row_str = ' '.join(str(v) for v in row if v).lower()
            if 'sales order' in row_str or 'vehicle vin' in row_str or 'bill' in row_str:
                return i
        return 0

    try:
        df = parse_xlsx_raw(filepath, find_header_fn=_find_header)
        return _process(df)
    except Exception as e1:
        # Fallback: pandas with dtype=str
        try:
            df = pd.read_excel(filepath, engine='openpyxl', dtype=str)
            return _process(df)
        except Exception as e2:
            raise RuntimeError(f"Could not parse Sales Order: {e1} / {e2}")


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
                elif cell_type == 'inlineStr':
                    # Inline string: value is in <is><t> not <v>
                    is_el = cell.find('.//s:is/s:t', ns)
                    if is_el is not None and is_el.text:
                        val = is_el.text
                    else:
                        is_el = cell.find('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}is/{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t')
                        if is_el is not None and is_el.text:
                            val = is_el.text
                elif cell_type == 'str':
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
        if 'sales order' in row_str or 'vehicle vin' in row_str or 'bill' in row_str or 'ship to' in row_str:
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
    """Normalize Sales Order DataFrame."""
    rename = {}
    col_patterns = {
        'Sales Order': 'order_no',
        'SO Sales Order': 'order_no',
        'Vehicle VIN': 'vin',
        'VIN': 'vin',
        'Bill-to': 'bill_to_dealer',
        'Bill to': 'bill_to_dealer',
        'Bill To': 'bill_to_dealer',
        'Ship to Party': 'ship_to_party',
        'Ship To Party': 'ship_to_party',
        'Customer Full Name': 'customer_name',
        'Customer': 'customer_name',
        'Material': 'material',
        'Channel': 'channel',
        'Handover': 'handover_flag',
        'Status': 'status',
        'Retail Date': 'retail_date',
    }

    for actual_col in df.columns:
        for pattern, internal in col_patterns.items():
            if pattern.lower() in str(actual_col).lower():
                if internal not in rename.values():
                    rename[actual_col] = internal
                    break
    df = df.rename(columns=rename)

    # Parse dates
    if 'retail_date' in df.columns:
        df['retail_date'] = pd.to_datetime(df['retail_date'], errors='coerce')

    # VIN uppercase
    if 'vin' in df.columns:
        df['vin'] = df['vin'].astype(str).str.strip().str.upper()
        df = df[df['vin'].str.len() > 3]

    print(f"  Sales Order: {len(df)} rows")
    return df
