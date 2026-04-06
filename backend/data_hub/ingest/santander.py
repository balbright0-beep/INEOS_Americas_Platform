"""Santander Daily Report ingest handler."""
import pandas as pd
import json
import os
from datetime import datetime
from data_hub.utils import safe_int


def ingest_santander(filepath):
    """Parse Santander pivot sheets and extract monthly totals."""
    sheets = ['Applications', 'Approvals', 'Fundings']
    results = {}

    for sheet in sheets:
        try:
            df = pd.read_excel(filepath, sheet_name=sheet, header=None, engine='openpyxl')
            monthly = _extract_monthly(df)
            results[sheet.lower()] = monthly
        except Exception:
            results[sheet.lower()] = []

    return results


def _extract_monthly(df):
    """Extract monthly totals from a Santander pivot sheet."""
    # Find 'Row Labels' row to locate data start
    data_start = None
    total_col = None

    for i in range(len(df)):
        row_vals = [str(v).strip() for v in df.iloc[i].values if pd.notna(v)]
        if 'Row Labels' in row_vals:
            data_start = i + 1
            # Find 'Total Applications' or 'Total' column in the header above
            for j in range(len(df.columns)):
                val = str(df.iloc[i - 1, j]).strip() if i > 0 and pd.notna(df.iloc[i - 1, j]) else ""
                if 'Total' in val and ('Application' in val or 'Approval' in val or 'Funding' in val):
                    total_col = j
                    break
            break

    if data_start is None:
        return []

    # If total column not found, try last numeric column before separator
    if total_col is None:
        total_col = 10  # Default fallback

    monthly = []
    for i in range(data_start, len(df)):
        label = df.iloc[i, 1] if pd.notna(df.iloc[i, 1]) else None
        if label is None or str(label).strip() == 'Grand Total':
            break
        try:
            date = pd.to_datetime(label)
            total = safe_int(df.iloc[i, total_col]) if total_col < len(df.columns) else 0
            monthly.append({'month': date.strftime('%Y-%m'), 'total': total})
        except (ValueError, TypeError):
            continue

    return monthly


def update_santander_cache(cache_path, today_data, today_str=None):
    """Update daily Santander volume cache."""
    if today_str is None:
        today_str = datetime.now().strftime('%Y-%m-%d')

    cache = {}
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            cache = json.load(f)

    # Store today's running total
    apps_total = 0
    if 'applications' in today_data and today_data['applications']:
        current_month = datetime.now().strftime('%Y-%m')
        for m in today_data['applications']:
            if m['month'] == current_month:
                apps_total = m['total']
                break

    fund_total = 0
    if 'fundings' in today_data and today_data['fundings']:
        current_month = datetime.now().strftime('%Y-%m')
        for m in today_data['fundings']:
            if m['month'] == current_month:
                fund_total = m['total']
                break

    # Compute daily delta
    yesterday = sorted(cache.keys())[-1] if cache else None
    yesterday_apps = cache[yesterday].get('applications_cumulative', 0) if yesterday else 0
    yesterday_fund = cache[yesterday].get('fundings_cumulative', 0) if yesterday else 0

    cache[today_str] = {
        'applications': max(0, apps_total - yesterday_apps),
        'fundings': max(0, fund_total - yesterday_fund),
        'applications_cumulative': apps_total,
        'fundings_cumulative': fund_total,
    }

    with open(cache_path, 'w') as f:
        json.dump(cache, f, indent=2)

    return cache
