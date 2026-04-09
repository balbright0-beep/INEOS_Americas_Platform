"""Render a parsed FO Performance report as a self-contained HTML fragment.

The output mirrors the Excel layout:
  - Report title banner
  - One big data table grouped by month (monthly rows inside a coloured band,
    monthly total row bolded with the mushroom fill the Excel uses)
  - Grand Total row
  - Summary blocks (Anticipated Wholesales, FO Pacing to Objective)
  - Column Definitions reference

The rendered HTML includes its own `<style>` block so it can be embedded as a
fragment anywhere — the React Logistics page drops it straight into a div with
`dangerouslySetInnerHTML`.
"""

from __future__ import annotations

import html
from typing import Any

# Columns that represent compliance fractions (0-1) and should be rendered
# as percentages with good/warn/bad colour bands.
COMPLIANCE_KEYS = {'fo_disp_compliance', 'disp_pu_compliance'}

# Columns where a number should be right-aligned and formatted as a plain
# integer (if whole) or a one-decimal float.
NUMERIC_KEYS = {
    'fos_created', 'dispatched', 'picked_up', 'delivered',
    'avg_fo_to_disp_bd', 'sla_fo_to_disp', 'avg_disp_to_pu_bd',
    'sla_disp_to_pu', 'avg_e2e_bd', 'prior_month_fo_pickups',
    'cumulative_awaiting_pu', 'mtd_pickups',
}


def _fmt_number(value: Any) -> str:
    if value is None or value == '':
        return ''
    if isinstance(value, bool):
        return html.escape(str(value))
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not value.is_integer():
            return f"{value:,.1f}"
        return f"{int(value):,}"
    s = str(value).strip()
    # Try to coerce numeric-looking strings
    try:
        f = float(s.replace(',', ''))
        if f.is_integer():
            return f"{int(f):,}"
        return f"{f:,.1f}"
    except (ValueError, TypeError):
        return html.escape(s)


def _fmt_compliance(value: Any) -> tuple[str, str]:
    """Return (text, css_class) for a compliance fraction."""
    if value is None or value == '':
        return '', ''
    try:
        f = float(value)
    except (ValueError, TypeError):
        return html.escape(str(value)), ''
    # Excel stores these as 0..1 fractions when populated.
    if 0 <= f <= 1:
        pct = f * 100
    else:
        pct = f
    text = f"{pct:.0f}%"
    if pct >= 80:
        cls = 'fo-good'
    elif pct >= 50:
        cls = 'fo-warn'
    else:
        cls = 'fo-bad'
    return text, cls


def _cell_html(key: str, value: Any) -> str:
    if value is None or value == '':
        return '<td class="fo-num"></td>'
    if key == 'date':
        return f'<td class="fo-date">{html.escape(str(value))}</td>'
    if key in COMPLIANCE_KEYS:
        text, cls = _fmt_compliance(value)
        return f'<td class="fo-num {cls}">{text}</td>'
    if key in NUMERIC_KEYS:
        return f'<td class="fo-num">{_fmt_number(value)}</td>'
    return f'<td>{html.escape(str(value))}</td>'


def _row_html(columns: list[dict[str, str]], entry: dict[str, Any], row_class: str = '') -> str:
    cells = ''.join(_cell_html(c['key'], entry.get(c['key'])) for c in columns)
    cls = f' class="{row_class}"' if row_class else ''
    return f'<tr{cls}>{cells}</tr>'


def _render_sections(sections: list[dict[str, Any]]) -> str:
    if not sections:
        return ''
    out: list[str] = ['<div class="fo-sections">']
    for s in sections:
        title = html.escape(s.get('title', ''))
        rows = s.get('rows', [])
        # Column Definitions has a different shape (label, header, description)
        is_glossary = 'COLUMN DEFINITIONS' in title.upper()
        out.append(f'<section class="fo-section{" fo-section-glossary" if is_glossary else ""}">')
        out.append(f'<h3>{title}</h3>')
        out.append('<table class="fo-summary"><tbody>')
        for row in rows:
            cells = list(row) + [None] * (15 - len(row))
            label = cells[0] or ''
            if is_glossary:
                # Col A (cells[1]), short-name (cells[2]), description (cells[3] if present)
                name = cells[1] or ''
                desc = cells[2] or ''
                out.append(
                    '<tr>'
                    f'<td class="fo-def-key">{html.escape(str(label))}</td>'
                    f'<td class="fo-def-name">{html.escape(str(name))}</td>'
                    f'<td class="fo-def-desc">{html.escape(str(desc))}</td>'
                    '</tr>'
                )
            else:
                # Summary block: label, numeric value (col 11 = index 11), and an optional note (col 13).
                value = cells[11]
                note = cells[13] or ''
                is_header = not any(c is not None and str(c).strip() for c in cells[1:])
                out.append(
                    f'<tr class="{"fo-summary-header" if is_header else ""}">'
                    f'<td class="fo-summary-label">{html.escape(str(label))}</td>'
                    f'<td class="fo-summary-value">{_fmt_number(value)}</td>'
                    f'<td class="fo-summary-note">{html.escape(str(note))}</td>'
                    '</tr>'
                )
        out.append('</tbody></table>')
        out.append('</section>')
    out.append('</div>')
    return '\n'.join(out)


def _month_slug(label: str) -> str:
    return ''.join(ch.lower() if ch.isalnum() else '-' for ch in label)


def render_fo_performance(data: dict[str, Any], *, standalone: bool = False) -> str:
    """Return an HTML rendering of the FO report.

    When ``standalone=True`` a complete <!doctype html> document is returned,
    suitable for download and offline viewing. Otherwise a fragment is
    returned for embedding via dangerouslySetInnerHTML in the React page.
    """
    title = html.escape(data.get('title') or 'Daily Freight Order Activity')
    columns = data.get('columns') or []
    months = data.get('months') or []
    grand_total = data.get('grand_total')
    sections = data.get('sections') or []
    generated = html.escape(data.get('generated_at', ''))
    ncols = len(columns)

    # Header row
    header_cells = ''.join(
        f'<th class="fo-th" scope="col">{html.escape(c.get("label", ""))}</th>'
        for c in columns
    )

    # Build one <tbody> per month so a click on the month header can toggle
    # the daily+total rows inside that tbody only. This keeps the collapse
    # state purely CSS-driven with a single class on the tbody.
    tbody_parts: list[str] = []
    for m in months:
        label = m.get('label') or ''
        slug = _month_slug(label)
        esc_label = html.escape(label)
        rows_html: list[str] = [
            (
                f'<tr class="fo-month-header" data-month="{slug}" role="button" tabindex="0" '
                f'aria-expanded="true" aria-controls="fo-month-{slug}">'
                f'<td colspan="{ncols}"><span class="fo-caret" aria-hidden="true">\u25be</span>{esc_label}</td></tr>'
            )
        ]
        for day in m.get('days', []):
            rows_html.append(_row_html(columns, day))
        if m.get('total'):
            rows_html.append(_row_html(columns, m['total'], row_class='fo-month-total'))
        tbody_parts.append(
            f'<tbody class="fo-month" id="fo-month-{slug}" data-month="{slug}">'
            + ''.join(rows_html)
            + '</tbody>'
        )

    # Grand total lives in its own tbody so collapse never hides it
    grand_tbody = ''
    if grand_total:
        grand_tbody = (
            '<tbody class="fo-grand"><tr class="fo-grand-total">'
            + ''.join(_cell_html(c['key'], grand_total.get(c['key'])) for c in columns).replace(
                '<td class="fo-num">', '<td class="fo-num fo-grand-cell">'
            )
            + '</tr></tbody>'
        )
        # Simpler: just render the row via _row_html and wrap
        grand_tbody = (
            '<tbody class="fo-grand">'
            + _row_html(columns, grand_total, row_class='fo-grand-total')
            + '</tbody>'
        )

    table_html = (
        '<table class="fo-table"><thead><tr>'
        + header_cells
        + '</tr></thead>'
        + ''.join(tbody_parts)
        + grand_tbody
        + '</table>'
    )

    sections_html = _render_sections(sections)

    # Inline click handler — works whether the HTML is injected via
    # dangerouslySetInnerHTML or loaded directly from a file. Uses a
    # delegated listener so new tbody elements on re-render keep working.
    toggle_js = """
<script>
(function(){
  function bind(root){
    root.querySelectorAll('.fo-month-header').forEach(function(hdr){
      if(hdr.dataset.foBound)return;
      hdr.dataset.foBound='1';
      hdr.addEventListener('click',function(){
        var tb=hdr.closest('tbody.fo-month');
        if(!tb)return;
        var collapsed=tb.classList.toggle('fo-collapsed');
        hdr.setAttribute('aria-expanded',collapsed?'false':'true');
      });
      hdr.addEventListener('keydown',function(e){
        if(e.key==='Enter'||e.key===' '){e.preventDefault();hdr.click();}
      });
    });
  }
  if(document.readyState==='loading')
    document.addEventListener('DOMContentLoaded',function(){bind(document);});
  else bind(document);
  // Re-bind if React re-renders the fragment
  var obs=new MutationObserver(function(m){m.forEach(function(r){bind(document);});});
  if(document.body)obs.observe(document.body,{childList:true,subtree:true});
  window.foCollapseAll=function(){document.querySelectorAll('tbody.fo-month').forEach(function(tb){tb.classList.add('fo-collapsed');});document.querySelectorAll('.fo-month-header').forEach(function(h){h.setAttribute('aria-expanded','false');});};
  window.foExpandAll=function(){document.querySelectorAll('tbody.fo-month').forEach(function(tb){tb.classList.remove('fo-collapsed');});document.querySelectorAll('.fo-month-header').forEach(function(h){h.setAttribute('aria-expanded','true');});};
})();
</script>
"""

    css = """
<style>
  .fo-report { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; color: #2A1F0F; }
  .fo-report h2.fo-title { font-size: 18px; font-weight: 700; margin: 0 0 4px; color: #2A1F0F;
    padding: 14px 18px; background: #E5DDD3; border-left: 4px solid #A84E1F; border-radius: 2px; }
  .fo-report .fo-generated { font-size: 11px; color: #6F6558; margin: 0 0 16px; padding-left: 18px; }
  .fo-table { width: 100%; border-collapse: collapse; font-size: 12px; background: #FFFFFF;
    border: 1px solid #D6CEC4; box-shadow: 0 1px 2px rgba(42,31,15,.05); }
  .fo-table thead tr { background: #D6CEC4; }
  .fo-table th.fo-th { text-align: left; padding: 8px 10px; font-size: 10.5px; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.4px; color: #6F6558; border-bottom: 2px solid #A84E1F;
    white-space: nowrap; }
  .fo-table td { padding: 6px 10px; border-bottom: 1px solid #F2ECE3; vertical-align: middle; }
  .fo-table td.fo-date { font-weight: 500; color: #3C342C; white-space: nowrap; }
  .fo-table td.fo-num { text-align: right; font-variant-numeric: tabular-nums; }
  .fo-table tr.fo-month-header { cursor: pointer; user-select: none; }
  .fo-table tr.fo-month-header td { background: #F2ECE3; font-weight: 700; color: #A84E1F;
    padding: 8px 12px; border-top: 2px solid #D6CEC4; text-transform: uppercase;
    font-size: 11px; letter-spacing: 0.6px; transition: background 120ms ease; }
  .fo-table tr.fo-month-header:hover td { background: #E8DFD0; }
  .fo-table tr.fo-month-header:focus { outline: none; }
  .fo-table tr.fo-month-header:focus td { box-shadow: inset 0 0 0 2px #A84E1F; }
  .fo-table .fo-caret { display: inline-block; width: 14px; margin-right: 6px; color: #A84E1F;
    font-size: 10px; transition: transform 160ms ease; }
  .fo-table tbody.fo-month.fo-collapsed .fo-caret { transform: rotate(-90deg); }
  .fo-table tbody.fo-month.fo-collapsed tr:not(.fo-month-header) { display: none; }
  .fo-table tr.fo-month-total td { background: #D6CEC4; font-weight: 700; color: #2A1F0F; }
  .fo-table tr.fo-grand-total td { background: #2A1F0F; color: #F5F1EA; font-weight: 700;
    padding: 10px; border-top: 3px double #A84E1F; }
  .fo-table td.fo-good { background: rgba(44,147,30,.10); color: #1F6913; font-weight: 600; }
  .fo-table td.fo-warn { background: rgba(234,179,8,.12); color: #8A6212; font-weight: 600; }
  .fo-table td.fo-bad  { background: rgba(200,43,43,.10); color: #A82727; font-weight: 600; }

  .fo-toolbar { display: flex; gap: 8px; margin: 0 0 10px; }
  .fo-toolbar button { background: #FFFFFF; border: 1px solid #D6CEC4; color: #6F6558;
    font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.4px;
    padding: 6px 10px; border-radius: 3px; cursor: pointer; }
  .fo-toolbar button:hover { background: #F2ECE3; color: #A84E1F; border-color: #A84E1F; }

  .fo-sections { margin-top: 24px; display: flex; flex-direction: column; gap: 16px; }
  .fo-section { background: #FFFFFF; border: 1px solid #D6CEC4; border-left: 4px solid #A84E1F;
    border-radius: 2px; padding: 14px 18px; }
  .fo-section h3 { font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px;
    color: #A84E1F; margin: 0 0 10px; }
  .fo-summary { width: 100%; border-collapse: collapse; font-size: 12px; }
  .fo-summary td { padding: 5px 8px; border-bottom: 1px solid #F2ECE3; }
  .fo-summary td.fo-summary-label { color: #3C342C; }
  .fo-summary td.fo-summary-value { text-align: right; font-weight: 700; font-variant-numeric: tabular-nums;
    width: 120px; color: #2A1F0F; }
  .fo-summary td.fo-summary-note { color: #6F6558; font-size: 11px; font-style: italic; width: 260px; }
  .fo-summary tr.fo-summary-header td { font-weight: 700; color: #A84E1F; text-transform: uppercase;
    font-size: 11px; letter-spacing: 0.4px; border-bottom: 2px solid #D6CEC4; }

  .fo-section-glossary .fo-summary td.fo-def-key { width: 60px; font-weight: 600; color: #6F6558; }
  .fo-section-glossary .fo-summary td.fo-def-name { width: 180px; font-weight: 600; color: #2A1F0F; }
  .fo-section-glossary .fo-summary td.fo-def-desc { color: #3C342C; font-size: 11.5px; }
</style>
"""

    toolbar_html = (
        '<div class="fo-toolbar fo-standalone-only">'
        '<button type="button" onclick="window.foExpandAll&&window.foExpandAll()">Expand all</button>'
        '<button type="button" onclick="window.foCollapseAll&&window.foCollapseAll()">Collapse all</button>'
        '</div>'
    )

    # When embedded in the React page, all the interactive behaviour is
    # driven from React (click delegation + toolbar buttons), so we omit
    # the <script> and the standalone-only toolbar from the fragment.
    body = (
        '<div class="fo-report">'
        + f'<h2 class="fo-title">{title}</h2>'
        + (f'<p class="fo-generated">Generated {generated}</p>' if generated else '')
        + (toolbar_html if standalone else '')
        + table_html
        + sections_html
        + '</div>'
    )

    if not standalone:
        return css + body

    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        f'<title>{title}</title>'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        + css
        + toggle_js
        + '</head><body style="margin:0;padding:24px;background:#F5F1EA">'
        + body
        + '</body></html>'
    )
