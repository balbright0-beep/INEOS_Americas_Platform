"""SAP Vehicle Export ingest handler."""
import pandas as pd
from data_hub.utils import extract_model_year, extract_body_type, parse_date_flexible, safe_str

SAP_EXPORT_COLUMNS = {
    'Ship to Party No': 'ship_to_party',
    'Customer Full Name': 'customer_name',
    'SO Sales Order No': 'order_no',
    'SO Creation Date': 'order_date',
    'SO BD Customer Invoice No': 'invoice_no',
    'SO BD Date (Invoice Date)': 'invoice_date',
    'Material Desc': 'material',
    'Vehicle VIN': 'vin',
    'Stock Category': 'stock_category',
    'Country Region Group': 'region_group',
    'Country Name': 'country',
    'Vehicle Current Primary Status Code': 'status_code',
    'Vehicle Current Primary Status Text (groups)': 'status',
    'SO Channel Desc': 'channel',
    'SO Condition Value': 'condition_value',
    'SO Condition Type': 'condition_type',
    'SO Condition Local Currency': 'currency',
    'MSRP (US$)': 'msrp',
    'Trim Levels Groups': 'trim',
    'Rough Pack Desc': 'rough_pack',
    'Exterior Paint Colour Desc': 'ext_color',
    'Seats Material Desc': 'seats',
    'Exterior Contrast Roof Colour Desc': 'roof_color',
    'Safari Windows Desc': 'safari_windows',
    'Wheels Desc': 'wheels',
    'Std Tyre  Opt Tyre Desc': 'tyres',
    'Exterior Ladder Frame Colour Desc': 'frame_color',
    'Fixed Tow Ball Desc': 'tow_ball',
    'Seat Heating Driver Codriver Desc': 'seat_heating',
    'Differential Locks Rear & Front Desc': 'diff_locks_rf',
    'Access Ladder Desc': 'access_ladder',
    'Auxiliary Battery Desc': 'aux_battery',
    'Auxiliary Switchbar Desc': 'aux_switchbar',
    'Carpet Floor Mats Desc': 'carpet_mats',
    'Compass Centre Console Desc': 'compass',
    'Differential Lock Central Desc': 'diff_lock_central',
    'Enhanced Emergency Safety Package Desc': 'safety_package',
    'Floor Trim Desc': 'floor_trim',
    'Integrated Front Winch Desc': 'front_winch',
    'Interior Utility Rail System Desc': 'utility_rails',
    'Privacy Glass Desc': 'privacy_glass',
    'Raised Air Intake Desc': 'raised_air_intake',
    'Smokers Pack Desc': 'smokers_pack',
    'Spare Wheel Storage Container Desc': 'spare_wheel_container',
    'Towing Mounting Plate Front Desc': 'tow_plate_front',
    'Utility Rails  Rubber Bump Strips Desc': 'rubber_bump_strips',
    'Steering Wheel, Handbrake Grip, Grab handle Desc': 'steering_wheel',
    'Wheel Locks Desc': 'wheel_locks',
    'Advanced Speaker System Desc': 'speaker_system',
    'Plant Code': 'plant_code',
}


def ingest_sap_export(filepath):
    """Parse SAP Vehicle Export into normalized DataFrame."""
    df = pd.read_excel(filepath, engine='openpyxl')

    # Rename columns using mapping (match by partial name if exact fails)
    rename = {}
    for src_col, int_col in SAP_EXPORT_COLUMNS.items():
        for actual_col in df.columns:
            if src_col in str(actual_col):
                rename[actual_col] = int_col
                break
    df = df.rename(columns=rename)

    # Parse dates
    for col in ['order_date', 'invoice_date']:
        if col in df.columns:
            df[col] = df[col].apply(parse_date_flexible)

    # Extract model year and body type from material description
    if 'material' in df.columns:
        df['model_year'] = df['material'].apply(extract_model_year)
        df['body_type'] = df['material'].apply(extract_body_type)

    # Ensure VIN is string and uppercase
    if 'vin' in df.columns:
        df['vin'] = df['vin'].astype(str).str.strip().str.upper()

    # MSRP to numeric
    if 'msrp' in df.columns:
        df['msrp'] = pd.to_numeric(df['msrp'], errors='coerce').fillna(0).astype(int)

    # Clean dealer name
    if 'customer_name' in df.columns:
        df['customer_name'] = df['customer_name'].astype(str).str.strip()

    return df
