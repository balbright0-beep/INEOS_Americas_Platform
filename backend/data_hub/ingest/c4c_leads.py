"""C4C Leads Created in Marketing ingest handler."""
import pandas as pd

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
    """Parse C4C Leads file. Tries multiple approaches to find the data."""
    # Try 1: Read with different skiprows to find the header row
    for skip in [0, 1, 2, 3, 4, 5]:
        try:
            df = pd.read_excel(filepath, skiprows=skip, engine='openpyxl')
            # Check if we found real column names (not metadata)
            cols_str = ' '.join(str(c) for c in df.columns)
            if 'Lead ID' in cols_str or 'lead' in cols_str.lower():
                return _process_leads(df)
            # Check if any row contains Lead ID
            for i in range(min(5, len(df))):
                row_str = ' '.join(str(v) for v in df.iloc[i].values if pd.notna(v))
                if 'Lead ID' in row_str:
                    # This row is the header — re-read with correct skiprows
                    df2 = pd.read_excel(filepath, skiprows=skip + i + 1, engine='openpyxl')
                    # Use this row as headers
                    headers = [str(v).strip() for v in df.iloc[i].values if pd.notna(v)]
                    if len(headers) >= len(df2.columns):
                        df2.columns = headers[:len(df2.columns)]
                    return _process_leads(df2)
        except Exception:
            continue

    # Try 2: Just read everything and hope for the best
    df = pd.read_excel(filepath, engine='openpyxl')
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

    # Parse date columns safely (don't force conversion that could fail)
    date_cols = [
        'qualified_date', 'closed_date', 'start_date', 'end_date', 'created_on',
        'td_requested', 'first_contact', 'first_status_change',
        'td_booking_date', 'td_completed_date',
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Drop rows without lead_id
    if 'lead_id' in df.columns:
        df = df.dropna(subset=['lead_id'])

    return df
