"""GA4 Analytics API integration using OAuth2 Desktop flow."""
import os
import json
import pickle
from datetime import datetime, timedelta

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Dimension, Metric, OrderBy
)
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
PROPERTY_ID = '256465380'
TOKEN_PATH = os.environ.get('GA4_TOKEN_PATH', 'ga4_token.pickle')


def get_credentials(client_secret_path=None):
    """Get or refresh OAuth2 credentials. Supports env var GA4_TOKEN_B64."""
    import base64
    creds = None

    # Try loading from env var first (for Render/headless deployment)
    token_b64 = os.environ.get('GA4_TOKEN_B64')
    if token_b64:
        try:
            creds = pickle.loads(base64.b64decode(token_b64))
        except Exception:
            pass

    # Try loading from file
    if not creds and os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'rb') as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            # Save refreshed token
            try:
                os.makedirs(os.path.dirname(TOKEN_PATH) if os.path.dirname(TOKEN_PATH) else '.', exist_ok=True)
                with open(TOKEN_PATH, 'wb') as f:
                    pickle.dump(creds, f)
            except Exception:
                pass
        elif client_secret_path and os.path.exists(client_secret_path):
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
            creds = flow.run_local_server(port=0)
            try:
                os.makedirs(os.path.dirname(TOKEN_PATH) if os.path.dirname(TOKEN_PATH) else '.', exist_ok=True)
                with open(TOKEN_PATH, 'wb') as f:
                    pickle.dump(creds, f)
            except Exception:
                pass
        else:
            raise RuntimeError("No GA4 credentials available. Set GA4_TOKEN_B64 env var or provide client_secret_path.")

    return creds


def get_client(client_secret_path):
    """Get authenticated GA4 client."""
    creds = get_credentials(client_secret_path)
    return BetaAnalyticsDataClient(credentials=creds)


# ═══════════════════════════════════════════════════
# Report Definitions — maps to the 6 GA4 CSV exports
# ═══════════════════════════════════════════════════

REPORTS = {
    'engagement': {
        'dimensions': ['date'],
        'metrics': [
            'sessions', 'engagedSessions', 'averageSessionDuration',
            'screenPageViews', 'screenPageViewsPerSession',
            'bounceRate', 'engagementRate',
        ],
    },
    'acquisition': {
        'dimensions': ['date', 'sessionDefaultChannelGroup'],
        'metrics': [
            'sessions', 'newUsers', 'totalUsers',
            'engagedSessions', 'engagementRate',
        ],
    },
    'user_attributes': {
        'dimensions': ['country', 'city', 'language'],
        'metrics': ['totalUsers', 'newUsers', 'sessions'],
    },
    'demographics': {
        'dimensions': ['country'],
        'metrics': [
            'totalUsers', 'newUsers', 'sessions',
            'engagedSessions', 'engagementRate',
        ],
    },
    'audiences': {
        'dimensions': ['audienceName'],
        'metrics': ['totalUsers', 'sessions'],
    },
    'tech': {
        'dimensions': ['deviceCategory', 'operatingSystem', 'browser'],
        'metrics': ['totalUsers', 'sessions', 'engagedSessions'],
    },
}


def fetch_report(client, report_name, start_date=None, end_date=None):
    """Fetch a single GA4 report."""
    if report_name not in REPORTS:
        raise ValueError(f"Unknown report: {report_name}. Available: {list(REPORTS.keys())}")

    config = REPORTS[report_name]

    if start_date is None:
        start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    if end_date is None:
        end_date = 'today'

    request = RunReportRequest(
        property=f'properties/{PROPERTY_ID}',
        dimensions=[Dimension(name=d) for d in config['dimensions']],
        metrics=[Metric(name=m) for m in config['metrics']],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        limit=100000,
    )

    response = client.run_report(request)

    # Convert to list of dicts
    rows = []
    dim_names = [d for d in config['dimensions']]
    met_names = [m for m in config['metrics']]

    for row in response.rows:
        record = {}
        for i, dim in enumerate(row.dimension_values):
            record[dim_names[i]] = dim.value
        for i, met in enumerate(row.metric_values):
            try:
                record[met_names[i]] = float(met.value)
            except ValueError:
                record[met_names[i]] = met.value
        rows.append(record)

    return {
        'report_type': report_name,
        'start_date': start_date,
        'end_date': end_date,
        'row_count': len(rows),
        'data': rows,
    }


def fetch_all_reports(client_secret_path, start_date=None, end_date=None):
    """Fetch all 6 GA4 reports."""
    client = get_client(client_secret_path)
    results = {}

    for name in REPORTS:
        try:
            result = fetch_report(client, name, start_date, end_date)
            results[name] = result
            print(f"  GA4 {name}: {result['row_count']} rows")
        except Exception as e:
            print(f"  GA4 {name} ERROR: {e}")
            results[name] = {'report_type': name, 'error': str(e), 'data': []}

    return results


def save_reports_to_cache(results, cache_dir='cache'):
    """Save fetched reports to cache as JSON."""
    import pandas as pd
    os.makedirs(os.path.join(cache_dir, 'data'), exist_ok=True)

    for name, result in results.items():
        if result.get('data'):
            df = pd.DataFrame(result['data'])
            path = os.path.join(cache_dir, 'data', f'ga4_{name}.parquet')
            df.to_parquet(path, index=False)

    # Save metadata
    meta = {name: {k: v for k, v in r.items() if k != 'data'}
            for name, r in results.items()}
    with open(os.path.join(cache_dir, 'ga4_meta.json'), 'w') as f:
        json.dump(meta, f, indent=2, default=str)

    return meta


if __name__ == '__main__':
    import sys
    secret = sys.argv[1] if len(sys.argv) > 1 else r"C:\Users\bxa68077\Downloads\client_secret_696300880247-t1qpfiin3nqg728p9jsh2kth9jj9h4bm.apps.googleusercontent.com.json"
    results = fetch_all_reports(secret)
    meta = save_reports_to_cache(results)
    print("\nDone. Reports fetched:")
    for name, info in meta.items():
        print(f"  {name}: {info.get('row_count', 0)} rows")
