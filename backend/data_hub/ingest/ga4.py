"""GA4 Analytics CSV ingest handler."""
import pandas as pd


GA4_REPORT_MAP = {
    'engagement overview': 'engagement',
    'acquisition overview': 'acquisition',
    'user attributes overview': 'user_attributes',
    'demographic details': 'demographics',
    'audiences': 'audiences',
    'tech overview': 'tech',
}


def ingest_ga4(filepath):
    """Parse GA4 analytics CSV export."""
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Extract metadata from comment header
    report_name = ""
    start_date = ""
    end_date = ""
    data_start = 0

    for i, line in enumerate(lines):
        if not line.startswith('#'):
            data_start = i
            break
        line_clean = line.lstrip('# ').strip()
        if i == 1:
            report_name = line_clean
        if 'Start date:' in line:
            start_date = line.split(':', 1)[1].strip()
        if 'End date:' in line:
            end_date = line.split(':', 1)[1].strip()

    # Detect report type
    report_type = 'unknown'
    for key, val in GA4_REPORT_MAP.items():
        if key in report_name.lower():
            report_type = val
            break

    # Parse data
    df = pd.read_csv(filepath, skiprows=data_start)

    # Clean column names
    df.columns = [c.strip() for c in df.columns]

    return {
        'report_type': report_type,
        'report_name': report_name,
        'start_date': start_date,
        'end_date': end_date,
        'data': df,
    }
