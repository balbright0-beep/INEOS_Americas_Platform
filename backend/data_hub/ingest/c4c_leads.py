"""C4C Leads Created in Marketing ingest handler."""
import pandas as pd
import subprocess
import tempfile
import os
from data_hub.utils import parse_date_flexible

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

DATE_COLS = [
    'qualified_date', 'closed_date', 'start_date', 'end_date', 'created_on',
    'td_requested', 'first_contact', 'first_status_change',
    'td_booking_date', 'td_completed_date',
]


def ingest_c4c_leads(filepath):
    """Parse C4C Leads file. Tries openpyxl first, falls back to libreoffice CSV conversion."""
    # Try direct pandas read first
    try:
        df = pd.read_excel(filepath, engine='openpyxl', skiprows=4)
        if len(df) > 10:
            return _process_leads(df)
    except Exception:
        pass

    # Fallback: libreoffice CSV conversion
    try:
        tmpdir = tempfile.mkdtemp()
        subprocess.run(
            ['libreoffice', '--headless', '--convert-to', 'csv', filepath, '--outdir', tmpdir],
            capture_output=True, timeout=120
        )
        csv_files = [f for f in os.listdir(tmpdir) if f.endswith('.csv')]
        if csv_files:
            df = pd.read_csv(os.path.join(tmpdir, csv_files[0]), skiprows=4)
            return _process_leads(df)
    except Exception:
        pass

    # Last resort: try reading without skiprows
    df = pd.read_excel(filepath, engine='openpyxl')
    return _process_leads(df)


def _process_leads(df):
    """Normalize leads DataFrame."""
    # Rename columns
    rename = {}
    for src_col, int_col in LEADS_COLUMNS.items():
        for actual_col in df.columns:
            if src_col in str(actual_col):
                rename[actual_col] = int_col
                break
    df = df.rename(columns=rename)

    # Parse date columns
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = df[col].apply(parse_date_flexible)

    # Drop rows without lead_id
    if 'lead_id' in df.columns:
        df = df.dropna(subset=['lead_id'])

    return df
