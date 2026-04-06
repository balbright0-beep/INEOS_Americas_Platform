"""Dashboard HTML Generator — injects computed data into the Dashboard template.
Uses the same replace_const() pattern as dashboard_refresh_all_in_one.py."""
import re
import json
import os
from datetime import datetime


def replace_const(html, name, value):
    """Replace a JavaScript constant in the HTML template."""
    json_val = json.dumps(value, default=str)
    pattern = rf'(const\s+{name}\s*=)\s*.*?;'
    replacement = rf'\g<1>{json_val};'
    new_html, count = re.subn(pattern, replacement, html, count=1, flags=re.DOTALL)
    if count == 0:
        print(f"  Warning: const {name} not found in template")
    return new_html


def generate_dashboard(compute_results, template_path, output_path):
    """
    Generate the Americas Dashboard HTML from compute results.

    Args:
        compute_results: dict of {metric_name: data} from compute engine
        template_path: path to the Dashboard HTML template
        output_path: path to write the generated HTML
    """
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Dashboard template not found: {template_path}")

    with open(template_path, 'r', encoding='utf-8') as f:
        html = f.read()

    now = datetime.now()
    injected = []

    # ═══ Map compute results to Dashboard JavaScript constants ═══

    # Retail Sales
    rs = compute_results.get('retail_sales', {})
    if rs:
        if 'market_summary' in rs:
            html = replace_const(html, 'RS', rs['market_summary'])
            injected.append('RS')
        if 'dealer_detail' in rs:
            html = replace_const(html, 'MTD_DLR', rs['dealer_detail'])
            injected.append('MTD_DLR')
        if 'units' in rs:
            html = replace_const(html, 'RSR_RET', rs['units'])
            injected.append('RSR_RET')

    # Dealer Performance Dashboard
    dpd = compute_results.get('dpd', [])
    if dpd:
        html = replace_const(html, 'DPD', dpd)
        injected.append('DPD')

    # Pipeline
    pipeline = compute_results.get('pipeline', {})
    if pipeline:
        for key in ['my25', 'my26', 'my27']:
            if key in pipeline:
                html = replace_const(html, f'P_{key.upper()}', pipeline[key])
                injected.append(f'P_{key.upper()}')

    # Inventory
    inv = compute_results.get('inventory', {})
    if inv:
        if 'by_dealer' in inv:
            html = replace_const(html, 'INV', inv['by_dealer'])
            injected.append('INV')

    # Historical Sales
    hist = compute_results.get('historical', {})
    if hist:
        html = replace_const(html, 'HIST', hist)
        injected.append('HIST')

    # Vehicle Export (VEX)
    vex = compute_results.get('vex', [])
    if vex:
        html = replace_const(html, 'VEX', vex)
        injected.append('VEX')

    # Scorecard
    sc = compute_results.get('scorecard', [])
    if sc:
        html = replace_const(html, 'SC_DATA', sc)
        injected.append('SC_DATA')

    # Lead KPIs
    lk = compute_results.get('lead_kpis', {})
    if lk:
        if 'all_time' in lk:
            html = replace_const(html, 'LK_ALL', lk.get('all_time', {}))
            injected.append('LK_ALL')

    # Brand Leads / Test Drive Daily
    bl = compute_results.get('brand_leads', {})
    if bl:
        html = replace_const(html, 'TDD', bl)
        injected.append('TDD')

    # Santander
    san = compute_results.get('santander', {})
    if san:
        for key in ['applications', 'approvals', 'fundings']:
            if key in san:
                html = replace_const(html, f'SAN_{key[:3].upper()}', san[key])

    # Objectives
    obj = compute_results.get('objectives', [])
    if obj:
        html = replace_const(html, 'OBJ', obj)
        injected.append('OBJ')

    # Inject timestamp
    html = replace_const(html, 'DATA_TS', now.strftime('%Y-%m-%d %H:%M'))

    # Write output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    return {
        'output_path': output_path,
        'injected_constants': injected,
        'file_size': len(html),
        'timestamp': now.isoformat(),
    }
