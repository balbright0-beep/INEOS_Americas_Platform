"""Urban Science Extract ingest handler."""
import pandas as pd
from data_hub.utils import parse_date_flexible

URBAN_SCIENCE_COLUMNS = {
    'INEOS DEALER': 'dealer_id',
    'DEALER NAME': 'dealer_name',
    'ORDER NO': 'order_no',
    'VIN': 'vin',
    'SALE DATE': 'sale_date',
    'CUSTOMER ID': 'customer_id',
    'LAST NAME': 'customer_last_name',
    'STREET': 'customer_street',
    'CITY': 'customer_city',
    'STATE CODE': 'customer_state',
    'POSTAL CODE': 'customer_zip',
}


def ingest_urban_science(filepath):
    """Parse Urban Science extract into normalized DataFrame."""
    df = pd.read_excel(filepath, engine='openpyxl')

    # Rename columns (partial match)
    rename = {}
    for src_col, int_col in URBAN_SCIENCE_COLUMNS.items():
        for actual_col in df.columns:
            if src_col in str(actual_col).upper():
                rename[actual_col] = int_col
                break
    df = df.rename(columns=rename)

    # Parse sale date
    if 'sale_date' in df.columns:
        df['sale_date'] = df['sale_date'].apply(parse_date_flexible)

    # VIN uppercase
    if 'vin' in df.columns:
        df['vin'] = df['vin'].astype(str).str.strip().str.upper()
        df = df.drop_duplicates(subset='vin', keep='last')

    # Clean zip code
    if 'customer_zip' in df.columns:
        df['customer_zip'] = df['customer_zip'].astype(str).str.strip().str[:5]

    return df
