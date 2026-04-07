"""Santander Daily Report ingest handler.

Reads the Apps_Fundings_Ineos pivot file:
- Applications sheet: monthly totals + daily breakdown, col 50 (AY) = Total Applications
- Approvals sheet: same structure
- Fundings sheet: same structure
- Product filter: (All) combined, or Retail/Lease if filtered

The processor needs:
- SAN_DAYS: date strings for daily chart
- SAN_ALL: daily total application counts
- SAN_FIN: daily finance (retail) counts
- SAN_LEASE: daily lease counts
- SAN_MO: {YYYY-MM: monthly_total}
"""
import pandas as pd
import json
import os
from datetime import datetime, timedelta
from data_hub.utils import safe_int


def detect_product_filter(filepath):
    """Detect the Product filter value from the Santander pivot file."""
    import openpyxl
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb.active
    product = '(All)'
    for row in ws.iter_rows(min_row=1, max_row=12, max_col=6, values_only=True):
        vals = [str(v).strip() for v in row if v]
        if len(vals) >= 2 and vals[0] == 'Product':
            product = vals[1]
            break
    wb.close()
    return product


def ingest_santander(filepath, product_override=None):
    """Parse Santander pivot file and extract daily + monthly data.

    Auto-detects Product filter from file: (All), Retail, or Lease.
    product_override forces a specific product type.
    """
    product = product_override or detect_product_filter(filepath)
    print(f"  Santander Product filter: {product}")

    result = {
        'product': product,
        'monthly': {},    # YYYY-MM → total apps
        'daily': {},      # YYYY-MM-DD → daily count
        'daily_finance': {},  # populated if product=Retail
        'daily_lease': {},    # populated if product=Lease
    }

    try:
        apps = _parse_pivot_sheet(filepath, 'Applications')
        result['monthly'] = apps.get('monthly', {})
        result['daily'] = apps.get('daily', {})
    except Exception as e:
        print(f"  Santander Applications error: {e}")

    try:
        fundings = _parse_pivot_sheet(filepath, 'Fundings')
        result['fundings_monthly'] = fundings.get('monthly', {})
    except Exception as e:
        print(f"  Santander Fundings error: {e}")

    try:
        approvals = _parse_pivot_sheet(filepath, 'Approvals')
        result['approvals_monthly'] = approvals.get('monthly', {})
    except Exception as e:
        print(f"  Santander Approvals error: {e}")

    total_entries = len(result['monthly']) + len(result['daily'])
    print(f"  Santander: {len(result['monthly'])} months, {len(result['daily'])} daily entries")
    return result


def _parse_pivot_sheet(filepath, sheet_name):
    """Parse a Santander pivot sheet.

    The total column shifts based on the Product filter:
    - Product=(All)    → col AY (50) = Total Applications
    - Product=Retail   → col AS (44) = Total Applications
    - Product=Lease    → col AM (38) = Total Applications

    This is because filtering collapses/shifts columns in the pivot layout.
    """
    df = pd.read_excel(filepath, sheet_name=sheet_name, header=None, engine='openpyxl', dtype=str)

    # Detect product filter from row 9 to determine which column to read
    product = '(All)'
    for i in range(len(df)):
        v1 = str(df.iloc[i, 1]).strip() if pd.notna(df.iloc[i, 1]) else ''
        if v1 == 'Product':
            v2 = str(df.iloc[i, 2]).strip() if pd.notna(df.iloc[i, 2]) else ''
            if v2:
                product = v2
            break

    # Map product → total column (0-indexed)
    # Excel: A=0, ..., AM=38, AS=44, AY=50
    TOTAL_COL_MAP = {
        '(All)': 50,
        'Retail': 44,
        'Lease': 38,
    }
    total_col = TOTAL_COL_MAP.get(product, 50)
    print(f"  Santander pivot product={product}, total_col={total_col}")

    def _read_count(row_idx):
        """Get count from the product-specific total column."""
        if len(df.columns) > total_col:
            try:
                val = str(df.iloc[row_idx, total_col]).strip()
                if val and val not in ('nan', 'None', ''):
                    return int(float(val))
            except (ValueError, TypeError):
                pass
        return 0

    # Find data start (row after "Row Labels")
    data_start = None
    for i in range(len(df)):
        val = str(df.iloc[i, 1]).strip() if pd.notna(df.iloc[i, 1]) else ''
        if val == 'Row Labels':
            data_start = i + 1
            break

    if data_start is None:
        return {'monthly': {}, 'daily': {}}

    monthly = {}
    daily = {}
    current_month = None

    for i in range(data_start, len(df)):
        label = df.iloc[i, 1] if pd.notna(df.iloc[i, 1]) else None
        if label is None or str(label).strip() in ('', 'nan'):
            continue

        label_str = str(label).strip()
        if label_str == 'Grand Total':
            break

        count = _read_count(i)

        # Check if this is a monthly row (date-like) or daily row (number)
        try:
            dt = pd.to_datetime(label)
            ym = dt.strftime('%Y-%m')
            monthly[ym] = count
            current_month = dt
            continue
        except (ValueError, TypeError):
            pass

        # Daily row (day number within current month)
        try:
            day_num = int(float(label_str))
            if current_month and 1 <= day_num <= 31:
                try:
                    day_date = current_month.replace(day=day_num)
                    date_str = day_date.strftime('%Y-%m-%d')
                    daily[date_str] = count
                except ValueError:
                    pass
        except (ValueError, TypeError):
            pass

    return {'monthly': monthly, 'daily': daily}


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
    monthly = today_data.get('monthly', {})
    current_month = datetime.now().strftime('%Y-%m')
    apps_total = monthly.get(current_month, 0)

    fund_total = 0
    fundings_monthly = today_data.get('fundings_monthly', {})
    fund_total = fundings_monthly.get(current_month, 0)

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
