"""Auto-detect uploaded file type by column signature and content patterns."""
import os
import pandas as pd
from pyxlsb import open_workbook as open_xlsb


def detect_file_type(filepath):
    """
    Detect which data source a file belongs to based on content signatures.
    Returns one of: 'sap_export', 'sap_handover', 'stock_pipeline', 'c4c_leads',
                     'santander', 'urban_science', 'ga4_engagement', 'ga4_acquisition',
                     'ga4_user_attributes', 'ga4_demographics', 'ga4_audiences', 'ga4_tech',
                     or 'unknown'
    """
    ext = os.path.splitext(filepath)[1].lower()

    # CSV files -> GA4
    if ext == '.csv':
        return _detect_ga4(filepath)

    # XLSB -> Stock & Pipeline
    if ext == '.xlsb':
        return _detect_xlsb(filepath)

    # XLSX -> check signatures
    if ext in ('.xlsx', '.xls'):
        return _detect_xlsx(filepath)

    return 'unknown'


def _detect_ga4(filepath):
    """Detect GA4 report type from CSV header comments."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = [f.readline() for _ in range(3)]
        if not lines[0].startswith('# '):
            return 'unknown'
        report_name = lines[1].strip().lstrip('# ').lower()
        ga4_map = {
            'engagement overview': 'ga4_engagement',
            'acquisition overview': 'ga4_acquisition',
            'user attributes overview': 'ga4_user_attributes',
            'demographic details': 'ga4_demographics',
            'audiences': 'ga4_audiences',
            'tech overview': 'ga4_tech',
        }
        for key, val in ga4_map.items():
            if key in report_name:
                return val
        return 'unknown'
    except Exception:
        return 'unknown'


def _detect_xlsb(filepath):
    """XLSB files are typically Stock & Pipeline Reports."""
    try:
        wb = open_xlsb(filepath)
        sheets = wb.sheets
        if 'Data' in sheets:
            return 'stock_pipeline'
        return 'unknown'
    except Exception:
        return 'unknown'


def _detect_xlsx(filepath):
    """Detect XLSX file type by column signatures."""
    try:
        # Read first few rows to check signatures
        # Try reading sheet names first
        xl = pd.ExcelFile(filepath, engine='openpyxl')
        sheet_names = xl.sheet_names

        # Santander: has 'Applications' sheet
        if 'Applications' in sheet_names:
            return 'santander'

        # Stock & Pipeline: has 'Data' sheet
        if 'Data' in sheet_names:
            return 'stock_pipeline'

        # Read first sheet headers
        df = pd.read_excel(filepath, nrows=5, header=None, engine='openpyxl')

        # Check row 0 for title patterns
        row0 = ' '.join(str(v) for v in df.iloc[0].dropna().values).lower()

        # C4C Leads
        if 'leads' in row0 and 'americas' in row0:
            return 'c4c_leads'

        # Santander fallback
        if 'apps_fundings_ineos' in row0:
            return 'santander'

        # Try reading with headers
        df = pd.read_excel(filepath, nrows=2, engine='openpyxl')
        cols = set(str(c).strip() for c in df.columns)

        # SAP Export: has 'Vehicle VIN', 'MSRP (US$)', 'Plant Code'
        if 'Vehicle VIN' in cols and 'Plant Code' in cols:
            return 'sap_export'

        # SAP Handover: has 'SO Vehicle Handover Complete Flag'
        if 'SO Vehicle Handover Complete Flag' in cols:
            return 'sap_handover'

        # Urban Science: has 'INEOS DEALER', 'SALE DATE (HANDOVER DATE)'
        if 'INEOS DEALER' in cols or any('SALE DATE' in c for c in cols):
            return 'urban_science'

        # Check for MSRP column variants
        if any('MSRP' in c for c in cols):
            if any('Handover' in c for c in cols):
                return 'sap_handover'
            return 'sap_export'

        return 'unknown'

    except Exception:
        return 'unknown'


# Source metadata
SOURCE_INFO = {
    'sap_export': {'label': 'SAP Vehicle Export', 'cadence': 'Daily', 'file_type': '.xlsx'},
    'sap_handover': {'label': 'SAP Handover Report', 'cadence': 'Daily', 'file_type': '.xlsx'},
    'stock_pipeline': {'label': 'Stock & Pipeline Report', 'cadence': 'Daily', 'file_type': '.xlsb/.xlsx'},
    'c4c_leads': {'label': 'C4C Leads (Marketing)', 'cadence': 'Daily', 'file_type': '.xlsx'},
    'santander': {'label': 'Santander Daily Report', 'cadence': 'Daily', 'file_type': '.xlsx'},
    'urban_science': {'label': 'Urban Science Extract', 'cadence': 'Monthly', 'file_type': '.xlsx'},
    'ga4_engagement': {'label': 'GA4 Engagement', 'cadence': 'Weekly', 'file_type': '.csv'},
    'ga4_acquisition': {'label': 'GA4 Acquisition', 'cadence': 'Weekly', 'file_type': '.csv'},
    'ga4_user_attributes': {'label': 'GA4 User Attributes', 'cadence': 'Weekly', 'file_type': '.csv'},
    'ga4_demographics': {'label': 'GA4 Demographics', 'cadence': 'Weekly', 'file_type': '.csv'},
    'ga4_audiences': {'label': 'GA4 Audiences', 'cadence': 'Weekly', 'file_type': '.csv'},
    'ga4_tech': {'label': 'GA4 Tech', 'cadence': 'Weekly', 'file_type': '.csv'},
}
