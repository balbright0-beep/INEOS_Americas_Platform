"""Campaign Code Extract ingest handler.
Determines which campaign codes are applied to which VINs.
Critical for: CVP counting, Demo flagging, Incentive Dashboard tracking."""
import pandas as pd
import zipfile
import xml.etree.ElementTree as ET
import re


def ingest_campaign_codes(filepath):
    """Parse Campaign Code Extract file."""
    try:
        return _parse_xlsx_raw(filepath)
    except Exception as e1:
        try:
            df = pd.read_excel(filepath, engine='openpyxl', dtype=str)
            return _process(df)
        except Exception as e2:
            raise RuntimeError(f"Could not parse Campaign Codes: {e1} / {e2}")


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

        # Find the sheet with the most rows (the detail data, not summary)
        sheet_path = None
        max_rows = 0
        ns_check = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
        for name in sorted(z.namelist()):
            if name.startswith('xl/worksheets/sheet') and name.endswith('.xml'):
                try:
                    root_check = ET.fromstring(z.read(name))
                    row_count = len(root_check.findall('.//s:sheetData/s:row', ns_check))
                    if row_count > max_rows:
                        max_rows = row_count
                        sheet_path = name
                except:
                    pass

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
                col = re.match(r'([A-Z]+)', ref)
                if col:
                    row_data[col.group(1)] = val
            rows.append(row_data)

    if not rows:
        return pd.DataFrame()

    all_cols = sorted(set(c for r in rows for c in r.keys()), key=lambda x: (len(x), x))
    data = [[r.get(c, '') for c in all_cols] for r in rows]

    # Find header row
    header_idx = 0
    for i, row in enumerate(data[:10]):
        row_str = ' '.join(str(v) for v in row).lower()
        if 'vin' in row_str or 'campaign' in row_str or 'code' in row_str:
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
    """Normalize Campaign Codes DataFrame.

    The SAP Campaign Code Extract has TWO columns that look the same:
      - "Campaign Code Applied?" → YES / NO flag
      - "Campaign Code"          → actual code text e.g. USCVP / CACVP / MXDEMO

    Older versions of this loader collapsed both into one column and lost
    the actual code text, which made YTD CVP read zero across the board.
    We now keep them separate and classify CVP rows by matching the actual
    code text against USCVP / CACVP / MXCVP (and DEMO variants).
    """
    rename = {}
    # Priority order: most specific patterns FIRST (so "Applied?" wins
    # over plain "Campaign Code"). After matching, the same target name
    # cannot be reused.
    col_patterns = [
        ('Vehicle VIN', 'vin'),
        ('Campaign Code Applied', 'campaign_flag'),
        ('Campaign code applied', 'campaign_flag'),
        ('Campaign Code', 'campaign_code'),
        ('Campaign code', 'campaign_code'),
        ('SO Sales Order', 'order_no'),
        ('SO Channel', 'channel'),
        ('Country Name', 'country'),
        ('Ship to Party', 'ship_to_party'),
        ('Ship To Party', 'dealer'),
        ('Handover Status Date', 'handover_date'),
        ('Registration Date', 'registration_date'),
        ('Retail Date', 'retail_date'),
        ('Region Group', 'region'),
    ]
    for actual_col in df.columns:
        col_lower = str(actual_col).lower()
        for pattern, internal in col_patterns:
            if pattern.lower() in col_lower:
                if internal not in rename.values():
                    rename[actual_col] = internal
                    break
    df = df.rename(columns=rename)

    # has_campaign flag from the YES/NO column
    if 'campaign_flag' in df.columns:
        df['has_campaign'] = df['campaign_flag'].astype(str).str.upper().isin(['YES', 'TRUE', '1'])
    elif 'campaign_code' in df.columns:
        # Fallback: if there's only one column and it contains YES/NO it's the flag,
        # otherwise the presence of a non-empty code value means the campaign applied.
        sample = df['campaign_code'].astype(str).str.upper().head(50).tolist()
        if any(v in ('YES', 'NO', 'BLANK', 'TRUE', 'FALSE') for v in sample):
            df['has_campaign'] = df['campaign_code'].astype(str).str.upper().isin(['YES', 'TRUE', '1'])
        else:
            df['has_campaign'] = df['campaign_code'].astype(str).str.strip().ne('') & \
                                 ~df['campaign_code'].astype(str).str.upper().isin(['NAN', 'NONE', 'BLANK'])

    # Classify campaign_type from the actual campaign code text (USCVP / CACVP / etc).
    # Fall back to channel only if there is no code column.
    if 'campaign_code' in df.columns:
        df['campaign_type'] = df['campaign_code'].apply(_classify_campaign)
    elif 'channel' in df.columns:
        df['campaign_type'] = df['channel'].apply(_classify_campaign)
    else:
        df['campaign_type'] = 'Other'

    # NOTE: previously this defaulted any has_campaign=True row with
    # campaign_type='Other' to 'CVP', but that inflated CVP counts with
    # unrelated campaign codes (Fleet, Subvention, etc.). CVP and Demo are
    # now driven strictly by the explicit USCVP/CACVP/MXCVP and
    # USDEMO/CADEMO/MXDEMO code patterns inside _classify_campaign.

    # Convert amount to numeric
    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)

    # Parse dates
    if 'effective_date' in df.columns:
        df['effective_date'] = pd.to_datetime(df['effective_date'], errors='coerce')

    # VIN uppercase
    if 'vin' in df.columns:
        df['vin'] = df['vin'].astype(str).str.strip().str.upper()
        df = df[df['vin'].str.len() > 3]

    # Diagnostic — surface the distribution so we can verify CVP/Demo are
    # actually being detected on the current Campaign Code Extract file.
    try:
        if 'campaign_type' in df.columns:
            type_counts = df['campaign_type'].value_counts().to_dict()
            print(f"  Campaign Codes: {len(df)} rows, type_counts={type_counts}")
            if 'campaign_code' in df.columns:
                sample_codes = df[df['campaign_type'] == 'CVP']['campaign_code'].astype(str).head(5).tolist()
                print(f"  Campaign Codes: sample CVP codes={sample_codes}")
                if not sample_codes:
                    uniq_codes = df['campaign_code'].astype(str).str.strip().str.upper()
                    uniq_codes = uniq_codes[uniq_codes.ne('') & ~uniq_codes.isin(['NAN', 'NONE', 'BLANK'])]
                    print(f"  Campaign Codes: unique codes seen={sorted(set(uniq_codes.head(100)))[:20]}")
        else:
            print(f"  Campaign Codes: {len(df)} rows (no campaign_type column!)")
    except Exception as _diag_err:
        print(f"  Campaign Codes: {len(df)} rows (diag err: {_diag_err})")
    return df


def _classify_campaign(val):
    """Classify campaign type from a campaign code value.

    INEOS Americas SAP campaign codes follow the pattern:
      USCVP, CACVP, MXCVP  → Customer Value Programme (CVP)
      USDEMO, CADEMO       → Demo / press fleet
      USFLT, CAFLT         → Fleet
    Plus the older descriptive variants (CO-DEVELOPMENT, EMPLOYEE,
    DEMONSTRATION, SUBVENTION, INCENTIVE, etc.) we still want to match.
    """
    if val is None:
        return 'Other'
    s = str(val).strip()
    if not s or s.upper() in ('NAN', 'NONE', 'BLANK', 'YES', 'NO', 'TRUE', 'FALSE'):
        return 'Other'
    val_norm = s.upper().replace(' ', '').replace('-', '').replace('_', '')
    # Reject non-CVP SAP codes that happen to share letters. These used to
    # be over-classified as CVP because the prior loose matcher included
    # 'EMPLOYEE' and 'CO-DEVELOPMENT'.
    if 'EMPLOYEE' in val_norm or 'CODEVELOPMENT' in val_norm:
        return 'Other'
    if 'SUBVENTION' in val_norm:
        return 'Subvention'
    if 'FLEET' in val_norm or 'RENTAL' in val_norm:
        return 'Fleet'
    # CVP — canonical SAP codes are USCVP / CACVP / MXCVP, but accept any
    # value containing "CVP" after the above exclusions so we don't miss
    # minor formatting variants.
    if 'CVP' in val_norm:
        return 'CVP'
    # Demo / press-fleet — USDEMO / CADEMO / MXDEMO. 'DEMO' substring is
    # specific enough after the non-CVP exclusions above.
    if 'DEMO' in val_norm:
        return 'Demo'
    if 'INCENTIVE' in val_norm or 'BONUS' in val_norm or 'LOYALTY' in val_norm:
        return 'Incentive'
    return 'Other'
