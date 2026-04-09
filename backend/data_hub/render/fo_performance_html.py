"""Render a parsed FO Performance report as a self-contained HTML fragment.

Quiet Luxury palette (per the render spec):
  - Canvas   #F2ECE3  (report background)
  - Surface  #E5DDD3  (alt row fill)
  - Tertiary #D6CEC4  (header + month-total fill)
  - Rule     #C9BFB3  (hair-weight rules)
  - H1       #2A1F0F
  - Body     #3C342C
  - Caption  #6F6558
  - Muted    #8A7F72
  - Burgundy #6B2D3E  (Grand Total, Anticipated Remaining, pacing result; max 5% area)
  - Green    #4A7C59  (compliance ≥95%)
  - Amber    #8B6914  (compliance 75–94%)
  - Red      #8B3A3A  (compliance <75%, Avg BD >3 highlight)

Font: PP Neue Montreal (with system fallbacks).
No bold in data rows. Hair-weight horizontal rules only. No vertical rules.

The renderer supports two output modes:
  - fragment  (default): self-contained HTML fragment (CSS + markup) suitable
                         for React's dangerouslySetInnerHTML. React attaches
                         click handlers for the collapsible months.
  - standalone (export): full <!doctype html> document with inline CSS + the
                         collapsible-month toggle <script>, so the downloaded
                         HTML works offline.
"""

from __future__ import annotations

import html
from typing import Any

COMPLIANCE_KEYS = {'fo_disp_compliance', 'disp_pu_compliance'}
NUMERIC_KEYS = {
    'fos_created', 'dispatched', 'picked_up', 'delivered',
    'avg_fo_to_disp_bd', 'sla_fo_to_disp', 'avg_disp_to_pu_bd',
    'sla_disp_to_pu', 'avg_e2e_bd', 'prior_month_fo_pickups',
    'cumulative_awaiting_pu', 'mtd_pickups',
}

# BD columns that get a red highlight when the average exceeds 3.
BD_KEYS_WITH_RED_HIGHLIGHT = {'avg_fo_to_disp_bd', 'avg_disp_to_pu_bd', 'avg_e2e_bd'}

COMPLIANCE_GOOD = 0.95
COMPLIANCE_WARN = 0.75


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
    if 0 <= f <= 1:
        pct = f * 100
    else:
        pct = f
    text = f"{pct:.0f}%"
    if pct >= COMPLIANCE_GOOD * 100:
        cls = 'fo-good'
    elif pct >= COMPLIANCE_WARN * 100:
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
    if key in BD_KEYS_WITH_RED_HIGHLIGHT:
        try:
            f = float(value)
        except (ValueError, TypeError):
            return f'<td class="fo-num">{_fmt_number(value)}</td>'
        cls = ' fo-bd-over' if f > 3 else ''
        return f'<td class="fo-num{cls}">{_fmt_number(value)}</td>'
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
        section_id = s.get('id', '')
        is_glossary = section_id == 'column_definitions' or 'COLUMN DEFINITIONS' in title.upper()
        is_anticipated = section_id == 'anticipated_wholesales'
        is_pacing = section_id == 'fo_pacing'

        section_class = 'fo-section'
        if is_glossary:
            section_class += ' fo-section-glossary'
        if is_anticipated:
            section_class += ' fo-section-anticipated'
        if is_pacing:
            section_class += ' fo-section-pacing'

        out.append(f'<section class="{section_class}">')
        out.append(f'<h3>{title}</h3>')
        out.append('<table class="fo-summary"><tbody>')
        for row in rows:
            cells = list(row) + [None] * (15 - len(row))
            label = cells[0] or ''
            if is_glossary:
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
                value = cells[11]
                note = cells[13] or ''
                label_upper = str(label).upper()
                # Tag rows so CSS can highlight the objective + FOs/day rows
                # in burgundy (the "yellow box" equivalents).
                tr_class = ''
                value_class = 'fo-summary-value'
                if 'MONTHLY WHOLESALE OBJECTIVE' in label_upper:
                    tr_class = 'fo-summary-objective'
                    value_class += ' fo-accent-edit'
                elif 'NEW FOS NEEDED PER DAY' in label_upper:
                    tr_class = 'fo-summary-result'
                    value_class += ' fo-accent-burgundy'
                elif 'ANTICIPATED REMAINING WHOLESALES' in label_upper:
                    tr_class = 'fo-summary-highlight'
                    value_class += ' fo-accent-burgundy'
                elif label_upper.startswith('MTD WHOLESALES'):
                    tr_class = 'fo-summary-highlight'
                    value_class += ' fo-accent-good'
                out.append(
                    f'<tr class="{tr_class}">'
                    f'<td class="fo-summary-label">{html.escape(str(label))}</td>'
                    f'<td class="{value_class}">{_fmt_number(value)}</td>'
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
    title = html.escape(data.get('title') or 'Daily Freight Order Activity')
    columns = data.get('columns') or []
    months = data.get('months') or []
    grand_total = data.get('grand_total')
    sections = data.get('sections') or []
    generated = html.escape(data.get('generated_at', ''))
    ncols = len(columns)

    header_cells = ''.join(
        f'<th class="fo-th" scope="col">{html.escape(c.get("label", ""))}</th>'
        for c in columns
    )

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

    grand_tbody = ''
    if grand_total:
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
  var obs=new MutationObserver(function(){bind(document);});
  if(document.body)obs.observe(document.body,{childList:true,subtree:true});
  window.foCollapseAll=function(){document.querySelectorAll('tbody.fo-month').forEach(function(tb){tb.classList.add('fo-collapsed');});document.querySelectorAll('.fo-month-header').forEach(function(h){h.setAttribute('aria-expanded','false');});};
  window.foExpandAll=function(){document.querySelectorAll('tbody.fo-month').forEach(function(tb){tb.classList.remove('fo-collapsed');});document.querySelectorAll('.fo-month-header').forEach(function(h){h.setAttribute('aria-expanded','true');});};
})();
</script>
"""

    css = """
<style>
  .fo-report {
    font-family: "PP Neue Montreal", "Inter", -apple-system, BlinkMacSystemFont,
                 "Segoe UI", Roboto, sans-serif;
    color: #3C342C;
    background: #F2ECE3;
    padding: 4px 0;
    font-feature-settings: "ss01", "tnum";
  }
  .fo-report h2.fo-title {
    font-size: 16px; font-weight: 500; letter-spacing: 0.2px;
    margin: 0 0 4px; color: #2A1F0F;
    padding: 16px 20px; background: #E5DDD3;
    border-bottom: 1px solid #C9BFB3;
  }
  .fo-report .fo-generated {
    font-size: 10.5px; color: #8A7F72; margin: 0 0 20px;
    padding: 6px 20px 0; font-style: italic;
  }

  .fo-table {
    width: 100%; border-collapse: collapse; font-size: 11.5px;
    background: #F2ECE3;
  }
  .fo-table thead tr { background: #D6CEC4; }
  .fo-table th.fo-th {
    text-align: left; padding: 10px 10px; font-size: 9.5px;
    font-weight: 500; text-transform: uppercase; letter-spacing: 0.8px;
    color: #6F6558; border-bottom: 1px solid #C9BFB3;
    white-space: nowrap;
  }
  .fo-table td {
    padding: 7px 10px; border-bottom: 1px solid #C9BFB3;
    vertical-align: middle; font-weight: 400; color: #3C342C;
  }
  .fo-table td.fo-date { color: #3C342C; white-space: nowrap; }
  .fo-table td.fo-num { text-align: right; font-variant-numeric: tabular-nums; }

  /* Alternating row fills — header is always position 1, so daily rows
     alternate even/odd starting from position 2. The .fo-month-header and
     .fo-month-total rules below use !important to override these stripes. */
  .fo-table tbody.fo-month tr:nth-child(even) td { background: #E5DDD3; }
  .fo-table tbody.fo-month tr:nth-child(odd) td { background: #F2ECE3; }

  /* Month header — clickable, no border weight on the right */
  .fo-table tr.fo-month-header { cursor: pointer; user-select: none; }
  .fo-table tr.fo-month-header td {
    background: #E5DDD3 !important; font-weight: 500; color: #6F6558;
    padding: 12px 14px; border-top: 1px solid #C9BFB3;
    text-transform: uppercase; font-size: 10.5px; letter-spacing: 1px;
  }
  .fo-table tr.fo-month-header:hover td { background: #D6CEC4 !important; color: #2A1F0F; }
  .fo-table tr.fo-month-header:focus { outline: none; }
  .fo-table tr.fo-month-header:focus td { box-shadow: inset 2px 0 0 #6B2D3E; }
  .fo-table .fo-caret {
    display: inline-block; width: 14px; margin-right: 8px;
    color: #8A7F72; font-size: 9px;
    transition: transform 160ms ease;
  }
  .fo-table tbody.fo-month.fo-collapsed .fo-caret { transform: rotate(-90deg); }
  .fo-table tbody.fo-month.fo-collapsed tr:not(.fo-month-header) { display: none; }

  /* Month subtotal row */
  .fo-table tr.fo-month-total td {
    background: #D6CEC4 !important; font-weight: 500; color: #2A1F0F;
    border-top: 1px solid #C9BFB3; border-bottom: 1px solid #C9BFB3;
  }

  /* Grand total — the one Burgundy accent */
  .fo-table tr.fo-grand-total td {
    background: #6B2D3E !important; color: #F2ECE3 !important;
    font-weight: 500; padding: 12px 10px;
    border-top: 1px solid #6B2D3E; border-bottom: none;
    letter-spacing: 0.3px;
  }

  /* Compliance bands */
  .fo-table td.fo-good {
    color: #4A7C59; font-weight: 500;
  }
  .fo-table td.fo-warn {
    color: #8B6914; font-weight: 500;
  }
  .fo-table td.fo-bad {
    color: #8B3A3A; font-weight: 500;
    background: #E8D5D5 !important;
  }
  /* Red highlight when average BD exceeds the 3-day SLA */
  .fo-table td.fo-bd-over {
    color: #8B3A3A; font-weight: 500;
  }

  /* Spacer toolbar (Expand / Collapse) — only rendered in standalone */
  .fo-toolbar {
    display: flex; gap: 8px; margin: 16px 0 12px; padding: 0 20px;
  }
  .fo-toolbar button {
    background: #F2ECE3; border: 1px solid #C9BFB3; color: #6F6558;
    font-family: inherit; font-size: 10px; font-weight: 500;
    text-transform: uppercase; letter-spacing: 0.8px;
    padding: 7px 12px; cursor: pointer;
  }
  .fo-toolbar button:hover { background: #E5DDD3; color: #2A1F0F; border-color: #8A7F72; }

  /* Summary sections */
  .fo-sections {
    margin-top: 28px; display: flex; flex-direction: column; gap: 16px;
    padding: 0 0;
  }
  .fo-section {
    background: #F2ECE3; border-top: 1px solid #C9BFB3;
    padding: 16px 20px;
  }
  .fo-section h3 {
    font-size: 10px; font-weight: 500; text-transform: uppercase;
    letter-spacing: 1.2px; color: #8A7F72; margin: 0 0 12px;
  }
  .fo-summary { width: 100%; border-collapse: collapse; font-size: 11.5px; }
  .fo-summary td {
    padding: 7px 10px; border-bottom: 1px solid #C9BFB3;
    font-weight: 400; color: #3C342C;
  }
  .fo-summary tr:last-child td { border-bottom: none; }
  .fo-summary td.fo-summary-label { color: #3C342C; }
  .fo-summary td.fo-summary-value {
    text-align: right; font-weight: 500; font-variant-numeric: tabular-nums;
    width: 130px; color: #2A1F0F;
  }
  .fo-summary td.fo-summary-note {
    color: #8A7F72; font-size: 10.5px; font-style: italic; width: 260px;
  }

  /* The two "yellow box" highlights — burgundy accent so the eye lands on
     the editable objective and the computed FOs/day result. */
  .fo-summary tr.fo-summary-objective td {
    background: #FBF4E3; border-bottom: 1px solid #E8D5A8;
  }
  .fo-summary tr.fo-summary-objective td.fo-accent-edit {
    color: #6B2D3E; font-weight: 500;
  }
  .fo-summary tr.fo-summary-result td {
    background: #FBF4E3; border-bottom: none; border-top: 1px solid #E8D5A8;
  }
  .fo-summary tr.fo-summary-result td.fo-accent-burgundy {
    color: #6B2D3E; font-weight: 500;
  }
  .fo-summary tr.fo-summary-highlight td.fo-accent-burgundy { color: #6B2D3E; font-weight: 500; }
  .fo-summary tr.fo-summary-highlight td.fo-accent-good { color: #4A7C59; font-weight: 500; }

  /* Glossary column definitions */
  .fo-section-glossary .fo-summary td.fo-def-key {
    width: 60px; font-weight: 500; color: #8A7F72;
    text-transform: uppercase; letter-spacing: 0.6px; font-size: 10px;
  }
  .fo-section-glossary .fo-summary td.fo-def-name {
    width: 180px; font-weight: 500; color: #2A1F0F;
  }
  .fo-section-glossary .fo-summary td.fo-def-desc {
    color: #6F6558; font-size: 11px;
  }
</style>
"""

    toolbar_html = (
        '<div class="fo-toolbar">'
        '<button type="button" onclick="window.foExpandAll&&window.foExpandAll()">Expand all</button>'
        '<button type="button" onclick="window.foCollapseAll&&window.foCollapseAll()">Collapse all</button>'
        '</div>'
    )

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
        + '</head><body style="margin:0;padding:24px;background:#F2ECE3">'
        + body
        + '</body></html>'
    )
