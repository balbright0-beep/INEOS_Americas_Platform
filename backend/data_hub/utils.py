"""Shared utilities for INEOS Data Hub."""
import re
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt

import pandas as pd
import numpy as np


# ═══════════════════════════════════════════════════
# Date Conversion
# ═══════════════════════════════════════════════════

def excel_serial_to_date(serial):
    """Convert Excel serial date number to Python datetime."""
    if serial is None or (isinstance(serial, float) and np.isnan(serial)):
        return None
    try:
        s = int(float(serial))
        if s < 1:
            return None
        return datetime(1899, 12, 30) + timedelta(days=s)
    except (ValueError, TypeError, OverflowError):
        return None


def parse_date_flexible(val):
    """Parse date from various formats: serial, MM/DD/YYYY, datetime64, string."""
    if val is None:
        return None
    if isinstance(val, (pd.Timestamp, datetime)):
        return val
    if isinstance(val, (int, float)) and not np.isnan(val):
        if val > 30000:  # Excel serial
            return excel_serial_to_date(val)
    if isinstance(val, str):
        val = val.strip()
        for fmt in ("%m/%d/%Y", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(val, fmt)
            except ValueError:
                continue
    return None


# ═══════════════════════════════════════════════════
# Model Year / Body Type Extraction
# ═══════════════════════════════════════════════════

MY_PATTERN = re.compile(r'MY(\d{2})')

def extract_model_year(material_desc):
    """Extract model year from material description. Returns 'MY25', 'MY26', etc."""
    if not material_desc:
        return ""
    m = MY_PATTERN.search(str(material_desc))
    if m:
        return f"MY{m.group(1)}"
    # Fallback: look for bare year numbers
    s = str(material_desc)
    for y in ("27", "26", "25", "24"):
        if y in s:
            return f"MY{y}"
    return ""


def extract_body_type(material_desc):
    """Extract body type from material description. Returns 'SW', 'QM', or 'SVO'."""
    if not material_desc:
        return "SW"
    s = str(material_desc).upper()
    if "SVO" in s or "PICK-UP" in s or "PICK UP" in s:
        return "SVO"
    if "QUARTERMASTER" in s:
        return "QM"
    return "SW"


# ═══════════════════════════════════════════════════
# Dealer Name Normalization
# ═══════════════════════════════════════════════════

INTERNAL_ENTITIES = {
    'INEOS US STOCK', 'IN_US_STK', 'INEOS AUTOMOTIVE USA',
    'INEOS AUTOMOTIVE (SHANGHAI)', 'INEOS AUTOMOTIVE AMERICAS',
    'IN_US_STK1', 'IN_US_STK2', 'IN_US_STK3',
}

STRIP_SUFFIXES = [
    " INEOS Grenadier", " INEOS GRENADIER", " INEOS",
    " Grenadier", " GRENADIER", " LLC", " Inc", " Inc.", " LP",
]


def clean_dealer_name(raw_name):
    """Normalize dealer name: strip suffixes, remove 'Grenadier' standalone word."""
    if not raw_name:
        return ""
    d = str(raw_name).strip()
    for suffix in STRIP_SUFFIXES:
        d = d.replace(suffix, "")
    d = " ".join(w for w in d.split() if w.upper() != "GRENADIER")
    return d.strip().upper()


def is_retail_dealer(name):
    """Returns True if name is a retail dealer (not internal/fleet)."""
    if not name:
        return False
    upper = str(name).upper()
    return not any(ie in upper for ie in INTERNAL_ENTITIES)


def normalize_dealer_via_c4c(raw_name, c4c_lookup):
    """Normalize via C4C key lookup, fallback to clean_dealer_name."""
    if not raw_name:
        return ""
    raw = str(raw_name).strip()
    if raw in c4c_lookup:
        return c4c_lookup[raw]
    cleaned = clean_dealer_name(raw)
    if cleaned in c4c_lookup:
        return c4c_lookup[cleaned]
    return cleaned


# ═══════════════════════════════════════════════════
# Geography
# ═══════════════════════════════════════════════════

def haversine_miles(lat1, lon1, lat2, lon2):
    """Calculate distance in miles between two lat/long points."""
    if any(v is None or (isinstance(v, float) and np.isnan(v)) for v in [lat1, lon1, lat2, lon2]):
        return None
    R = 3959  # Earth radius in miles
    lat1, lon1, lat2, lon2 = map(radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return R * 2 * asin(sqrt(a))


# ═══════════════════════════════════════════════════
# Status Mapping
# ═══════════════════════════════════════════════════

OG_STATUSES = {
    "Delivered to Retail Partner", "Ready for pick-up", "Handover complete",
    "7.Delivered to Retail Partner", "7.Ready for Pick Up",
}

PIPELINE_STATUS_MAP = {
    "Dealer Stock": ["Delivered to Retail Partner", "Ready for pick-up", "7.Delivered to Retail Partner", "7.Ready for Pick Up"],
    "In-Transit to Dealer": ["Goods issue", "In transit", "6.Goods Issue"],
    "At Americas Port": ["Delivered to port", "5.Delivered to Port"],
    "On Water": ["Order confirmed", "Awaiting collection", "4.Order Confirmed"],
    "In Production": ["In production", "Built", "3.In Production", "3.Built"],
    "Planned": ["Planned", "Draft", "1.Planned", "2.Draft"],
    "Sold": [],  # Determined by handover date
}

OG_CHANNELS = {"STOCK", "PRIVATE - RETAILER"}


def map_vehicle_status(status_text, channel, has_handover):
    """Map raw status to standardized status category."""
    if has_handover:
        return "Sold"
    if channel in OG_CHANNELS:
        for category, statuses in PIPELINE_STATUS_MAP.items():
            if any(s.lower() in str(status_text).lower() for s in statuses):
                return category
        return "Dealer Stock"  # Default for retail channel
    for category, statuses in PIPELINE_STATUS_MAP.items():
        if any(s.lower() in str(status_text).lower() for s in statuses):
            return category
    return "Unknown"


# ═══════════════════════════════════════════════════
# Formatting
# ═══════════════════════════════════════════════════

def safe_int(x):
    """Convert to int, return 0 on failure."""
    if x is None:
        return 0
    try:
        return int(float(x))
    except (ValueError, TypeError):
        return 0


def safe_float(x):
    """Convert to float, return 0.0 on failure."""
    if x is None:
        return 0.0
    try:
        return float(x)
    except (ValueError, TypeError):
        return 0.0


def safe_str(x):
    """Convert to stripped string."""
    return str(x).strip() if x is not None and not (isinstance(x, float) and np.isnan(x)) else ""


def pct(val):
    """Format as percentage string: 0.75 -> '75.0'"""
    return f"{safe_float(val) * 100:.1f}"


# ═══════════════════════════════════════════════════
# Raw XLSX XML Parser (handles inlineStr, shared strings, etc.)
# ═══════════════════════════════════════════════════

def parse_xlsx_raw(filepath, find_header_fn=None):
    """
    Parse xlsx by reading XML directly from the ZIP archive.
    Handles: shared strings (type=s), inline strings (type=inlineStr),
    regular values, and booleans.
    Returns a pandas DataFrame.

    find_header_fn: optional function(row_str) -> bool to identify header row
    """
    import pandas as pd

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

        # Find largest sheet
        sheet_path = None
        max_rows = 0
        ns = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
        for name in sorted(z.namelist()):
            if name.startswith('xl/worksheets/sheet') and name.endswith('.xml'):
                try:
                    root_check = ET.fromstring(z.read(name))
                    row_count = len(root_check.findall('.//s:sheetData/s:row', ns))
                    if row_count > max_rows:
                        max_rows = row_count
                        sheet_path = name
                except:
                    pass

        if not sheet_path:
            raise RuntimeError("No worksheet found in xlsx")

        sheet_xml = z.read(sheet_path)
        root = ET.fromstring(sheet_xml)

        rows = []
        for row_el in root.findall('.//s:sheetData/s:row', ns):
            row_values = []
            for idx, cell in enumerate(row_el.findall('s:c', ns)):
                ref = cell.get('r', '')
                cell_type = cell.get('t', '')

                # Extract value based on cell type
                val = ''
                if cell_type == 'inlineStr':
                    is_el = cell.find('s:is', ns)
                    if is_el is not None:
                        t_el = is_el.find('s:t', ns)
                        if t_el is not None and t_el.text:
                            val = t_el.text
                elif cell_type == 's':
                    val_el = cell.find('s:v', ns)
                    if val_el is not None and val_el.text:
                        try:
                            val = shared_strings[int(val_el.text)]
                        except (ValueError, IndexError):
                            val = val_el.text
                else:
                    val_el = cell.find('s:v', ns)
                    val = val_el.text if val_el is not None else ''

                row_values.append(val)

            rows.append(row_values)

    if not rows:
        return pd.DataFrame()

    # Normalize row lengths (pad shorter rows)
    max_cols = max(len(r) for r in rows) if rows else 0
    data = [r + [''] * (max_cols - len(r)) for r in rows]

    # Find header row — prefer rows with many non-empty cells that contain keywords
    header_idx = 0
    best_score = 0
    for i, row in enumerate(data[:15]):
        non_empty = sum(1 for v in row if str(v).strip())
        if non_empty < 3:
            continue  # Skip metadata rows with few cells
        row_str = ' '.join(str(v) for v in row)
        score = non_empty  # More columns = more likely to be header
        if find_header_fn and find_header_fn(row_str):
            score += 100
        # Bonus for common header keywords
        for kw in ['VIN', 'Vehicle', 'Lead ID', 'Campaign', 'Order', 'Dealer', 'Date', 'Status', 'Channel']:
            if kw in row_str:
                score += 10
        if score > best_score:
            best_score = score
            header_idx = i

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

    # Filter out completely empty rows
    df = df[df.astype(str).apply(lambda r: r.str.strip().str.len().sum() > 0, axis=1)]

    return df
