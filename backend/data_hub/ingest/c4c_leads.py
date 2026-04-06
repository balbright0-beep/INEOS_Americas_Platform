"""C4C Leads Created in Marketing ingest handler."""
import pandas as pd
import openpyxl

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
    """Parse C4C Leads file using raw openpyxl to avoid pandas parsing errors."""
    # Read directly with openpyxl to bypass pandas cell type conversion issues
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb.active

    # Read all rows as strings to avoid type conversion errors
    all_rows = []
    for row in ws.iter_rows(values_only=True):
        all_rows.append([str(v).strip() if v is not None else '' for v in row])
    wb.close()

    if not all_rows:
        return pd.DataFrame()

    # Find the header row (contains 'Lead ID')
    header_idx = None
    for i, row in enumerate(all_rows[:10]):
        row_str = ' '.join(row)
        if 'Lead ID' in row_str:
            header_idx = i
            break

    if header_idx is None:
        # No header found — use first row as header
        header_idx = 0

    headers = all_rows[header_idx]
    data_rows = all_rows[header_idx + 1:]

    # Filter out empty rows
    data_rows = [r for r in data_rows if any(v for v in r)]

    # Create DataFrame
    df = pd.DataFrame(data_rows, columns=headers[:len(data_rows[0])] if data_rows else headers)

    return _process_leads(df)


def _process_leads(df):
    """Normalize leads DataFrame."""
    # Rename columns (partial match)
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

    # Drop rows without lead_id or where lead_id is empty
    if 'lead_id' in df.columns:
        df = df[df['lead_id'].astype(str).str.strip() != '']
        df = df[df['lead_id'].astype(str) != 'nan']

    return df
