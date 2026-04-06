"""C4C Leads Created in Marketing ingest handler."""
import pandas as pd
import zipfile
import xml.etree.ElementTree as ET
import io
import re

LEADS_COLUMNS = {
    'Lead ID': 'lead_id',
    'Name': 'lead_name',
    'Retailer Name': 'retailer_name',
    'Company/Customer': 'customer_name',
    'Customer Phone': 'customer_phone',
    'Customer Mobile': 'customer_mobile',
    'Customer E-Mail': 'customer_email',
    'Status': 'lead_status',
    'Reason Code': 'reason_code',
    'Retailer Status': 'retailer_status',
    'Retailer Country Name': 'retailer_country',
    'Country/Region': 'country_region',
    'Marketing Unit': 'marketing_unit',
    'Source': 'source',
    'Qualified': 'qualified_date',
    'Closed': 'closed_date',
    'Start Date': 'start_date',
    'End Date': 'end_date',
    'Category': 'category',
    'Owner': 'owner',
    'Created On': 'created_on',
    'Model of Interest': 'model_interest',
    'Test Drive Requested Date': 'td_requested',
    'First contact attempt': 'first_contact',
    'Retailer First Status Changed On': 'first_status_change',
    'Test drive booking date': 'td_booking_date',
    'Test drive booking time': 'td_booking_time',
    'Booking ID': 'booking_id',
    'Test Drive Completed Date': 'td_completed_date',
    'Test drive completed': 'td_completed_flag',
    'Note Exists': 'note_exists',
}


def ingest_c4c_leads(filepath):
    """Parse C4C Leads file. Uses raw XML extraction to avoid openpyxl type conversion bugs."""
    # Approach: extract shared strings + sheet data directly from the xlsx ZIP
    # This bypasses openpyxl's cell type inference entirely
    try:
        return _parse_xlsx_raw(filepath)
    except Exception as e1:
        # Fallback: try pandas with different engines
        for engine in ['openpyxl']:
            for skip in range(6):
                try:
                    df = pd.read_excel(filepath, skiprows=skip, engine=engine, dtype=str)
                    cols_str = ' '.join(str(c) for c in df.columns)
                    if 'Lead' in cols_str or 'lead' in cols_str:
                        return _process_leads(df)
                except Exception:
                    continue
        raise RuntimeError(f"Could not parse C4C Leads file: {e1}")


def _parse_xlsx_raw(filepath):
    """Parse xlsx by reading XML directly from the ZIP archive."""
    with zipfile.ZipFile(filepath) as z:
        # Read shared strings
        shared_strings = []
        if 'xl/sharedStrings.xml' in z.namelist():
            ss_xml = z.read('xl/sharedStrings.xml')
            ns = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
            root = ET.fromstring(ss_xml)
            for si in root.findall('.//s:si', ns):
                texts = si.findall('.//s:t', ns)
                shared_strings.append(''.join(t.text or '' for t in texts))

        # Read first sheet
        sheet_path = 'xl/worksheets/sheet1.xml'
        if sheet_path not in z.namelist():
            # Find first sheet
            for name in z.namelist():
                if name.startswith('xl/worksheets/sheet') and name.endswith('.xml'):
                    sheet_path = name
                    break

        sheet_xml = z.read(sheet_path)
        ns = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
        root = ET.fromstring(sheet_xml)

        rows = []
        for row_el in root.findall('.//s:sheetData/s:row', ns):
            row_data = {}
            for cell in row_el.findall('s:c', ns):
                ref = cell.get('r', '')  # e.g., 'A1', 'B1'
                cell_type = cell.get('t', '')
                val_el = cell.find('s:v', ns)
                val = val_el.text if val_el is not None else ''

                if cell_type == 's' and val:
                    # Shared string reference
                    try:
                        val = shared_strings[int(val)]
                    except (ValueError, IndexError):
                        pass

                # Extract column letter
                col = re.match(r'([A-Z]+)', ref)
                if col:
                    row_data[col.group(1)] = val

            rows.append(row_data)

    if not rows:
        return pd.DataFrame()

    # Convert to DataFrame
    all_cols = sorted(set(c for r in rows for c in r.keys()),
                      key=lambda x: (len(x), x))  # Sort A, B, ..., Z, AA, AB...
    data = [[r.get(c, '') for c in all_cols] for r in rows]

    # Find header row (contains 'Lead ID')
    header_idx = 0
    for i, row in enumerate(data[:10]):
        if any('Lead ID' in str(v) for v in row):
            header_idx = i
            break

    headers = data[header_idx]
    data_rows = data[header_idx + 1:]

    df = pd.DataFrame(data_rows, columns=headers)
    return _process_leads(df)


def _process_leads(df):
    """Normalize leads DataFrame."""
    rename = {}
    for src_col, int_col in LEADS_COLUMNS.items():
        for actual_col in df.columns:
            if src_col in str(actual_col):
                rename[actual_col] = int_col
                break
    df = df.rename(columns=rename)

    # Parse date columns safely
    date_cols = [
        'qualified_date', 'closed_date', 'start_date', 'end_date', 'created_on',
        'td_requested', 'first_contact', 'first_status_change',
        'td_booking_date', 'td_completed_date',
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Drop empty rows
    if 'lead_id' in df.columns:
        df = df[df['lead_id'].astype(str).str.strip() != '']
        df = df[df['lead_id'].astype(str) != 'nan']
        df = df[df['lead_id'].astype(str) != 'None']
        df = df[df['lead_id'].astype(str) != '']

    return df
