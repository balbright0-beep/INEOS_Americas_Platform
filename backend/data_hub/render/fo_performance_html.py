"""Render a parsed FO Performance report as a self-contained HTML fragment.

Uses the INEOS Design System (IDS) tokens so the report blends in with the
rest of the Americas Hub:

  - Surfaces: page #FAFAF9, card #FFFFFF, sunken #E3E1DC
  - Borders:  subtle #E3E1DC, default #D9D7D0
  - Text:     primary #1D1D1D, secondary #606060, tertiary #727272
  - Accent:   flare red #FF4639 (and hover #E63E32)
  - Status:   success #2E7D32, warning #BF360C, error #C4281D
  - Fonts:    Plus Jakarta Sans (headings) + Inter (body)

Two output modes:
  - fragment  (default): self-contained HTML + CSS fragment that embeds
                         inside the React LogisticsPage via
                         dangerouslySetInnerHTML. React wires the
                         collapsible-month click handler itself.
  - standalone (export): full <!doctype html> document with inline CSS +
                         a bootstrap <script> so the downloaded file works
                         offline without any assets.
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

# BD columns that get a subtle warning tint when the average exceeds 3 BD.
BD_KEYS_WITH_WARNING = {'avg_fo_to_disp_bd', 'avg_disp_to_pu_bd', 'avg_e2e_bd'}

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
    if value is None or value == '':
        return '', ''
    try:
        f = float(value)
    except (ValueError, TypeError):
        return html.escape(str(value)), ''
    pct = f * 100 if 0 <= f <= 1 else f
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
    if key in BD_KEYS_WITH_WARNING:
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
        is_pacing = section_id == 'fo_pacing'

        section_class = 'fo-section'
        if is_glossary:
            section_class += ' fo-section-glossary'
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
                tr_class = ''
                value_class = 'fo-summary-value'
                if 'MONTHLY WHOLESALE OBJECTIVE' in label_upper:
                    tr_class = 'fo-summary-objective'
                    value_class += ' fo-accent'
                elif 'NEW FOS NEEDED PER DAY' in label_upper:
                    tr_class = 'fo-summary-result'
                    value_class += ' fo-accent'
                elif 'ANTICIPATED REMAINING WHOLESALES' in label_upper:
                    tr_class = 'fo-summary-highlight'
                    value_class += ' fo-strong'
                elif label_upper.startswith('MTD WHOLESALES'):
                    tr_class = 'fo-summary-highlight'
                    value_class += ' fo-good-value'
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
        '<div class="fo-table-wrap"><table class="fo-table"><thead><tr>'
        + header_cells
        + '</tr></thead>'
        + ''.join(tbody_parts)
        + grand_tbody
        + '</table></div>'
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

    # All colors below come straight from backend/static/ids.css so the
    # report blends into the rest of the hub without importing the stylesheet.
    css = """
<style>
  .fo-report {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI',
                 Roboto, sans-serif;
    color: #1D1D1D;
    font-feature-settings: "ss01", "tnum";
    font-size: 13px;
    line-height: 1.5;
  }
  .fo-report h2.fo-title {
    font-family: 'Plus Jakarta Sans', -apple-system, system-ui, sans-serif;
    font-size: 16px; font-weight: 600; letter-spacing: -0.01em;
    margin: 0 0 4px; color: #1D1D1D;
  }
  .fo-report .fo-generated {
    font-size: 11px; color: #727272; margin: 0 0 16px;
    font-family: 'Inter', sans-serif;
  }

  .fo-report .fo-table-wrap {
    /* The wrap is the scroll container for BOTH axes. This:
       1. Contains the 15-column table horizontally so it never pushes
          past the card / page width.
       2. Creates a proper sticky-containing block so the thead actually
          pins while scrolling — the outer .ids-main has overflow-y:
          auto but no height constraint, so sticky fails there. */
    max-height: calc(100vh - 260px);
    overflow: auto;
    -webkit-overflow-scrolling: touch;
    border: 1px solid #E3E1DC; border-radius: 6px;
    background: #FFFFFF;
    overscroll-behavior: contain;
  }
  .fo-table {
    width: 100%; border-collapse: separate; border-spacing: 0;
    font-family: 'Inter', sans-serif; font-size: 12px;
    background: #FFFFFF;
  }
  .fo-table thead tr { background: #FFFFFF; }
  .fo-table th.fo-th {
    /* Sticky column headers — pin to the top of .fo-table-wrap as the
       user scrolls the daily rows. Opaque background prevents rows
       from bleeding through. */
    position: sticky; top: 0; z-index: 10;
    text-align: left; padding: 11px 12px; font-size: 10px;
    font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em;
    color: #727272;
    white-space: nowrap; background: #FFFFFF;
    box-shadow: inset 0 -2px 0 #D9D7D0;
  }
  .fo-table td {
    padding: 8px 12px; border-bottom: 1px solid #E3E1DC;
    vertical-align: middle; color: #606060; font-weight: 400;
  }
  .fo-table td.fo-date { color: #1D1D1D; white-space: nowrap; font-weight: 500; }
  .fo-table td.fo-num {
    text-align: right; font-variant-numeric: tabular-nums;
    color: #1D1D1D;
  }

  /* Month header — subtle sunken bg, clickable */
  .fo-table tr.fo-month-header { cursor: pointer; user-select: none; }
  .fo-table tr.fo-month-header td {
    background: #FAFAF9; font-weight: 600; color: #606060;
    padding: 9px 14px; border-top: 1px solid #E3E1DC;
    text-transform: uppercase; font-size: 10px; letter-spacing: 0.08em;
    transition: background 150ms cubic-bezier(0.25,0.46,0.45,0.94);
  }
  .fo-table tr.fo-month-header:hover td { background: #E9E8E5; color: #1D1D1D; }
  .fo-table tr.fo-month-header:focus { outline: none; }
  .fo-table tr.fo-month-header:focus td { box-shadow: inset 2px 0 0 #FF4639; }
  .fo-table .fo-caret {
    display: inline-block; width: 12px; margin-right: 8px;
    color: #FF4639; font-size: 10px;
    transition: transform 160ms ease;
  }
  .fo-table tbody.fo-month.fo-collapsed .fo-caret { transform: rotate(-90deg); }
  .fo-table tbody.fo-month.fo-collapsed tr:not(.fo-month-header) { display: none; }

  /* Data row hover state matches ids-table */
  .fo-table tbody.fo-month tr:not(.fo-month-header):not(.fo-month-total):hover td {
    background: #F0EFEC;
  }

  /* Month total */
  .fo-table tr.fo-month-total td {
    background: #F0EFEC; font-weight: 600; color: #1D1D1D;
    border-top: 1px solid #D9D7D0; border-bottom: 1px solid #D9D7D0;
  }

  /* Grand total — INEOS flare red */
  .fo-table tr.fo-grand-total td {
    background: #1D1D1D; color: #FAFAF9; font-weight: 600;
    padding: 12px; border-top: 2px solid #FF4639; border-bottom: none;
    letter-spacing: 0.02em;
  }
  .fo-table tr.fo-grand-total td.fo-date { color: #FAFAF9; }
  .fo-table tr.fo-grand-total td.fo-num { color: #FAFAF9; }

  /* Compliance bands — IDS status colors */
  .fo-table td.fo-good {
    color: #2E7D32; font-weight: 600;
  }
  .fo-table td.fo-warn {
    color: #BF360C; font-weight: 600;
  }
  .fo-table td.fo-bad {
    color: #C4281D; font-weight: 600;
    background: #FFEBE9;
  }
  .fo-table td.fo-bd-over { color: #BF360C; font-weight: 600; }

  /* Toolbar (standalone only) */
  .fo-toolbar {
    display: flex; gap: 8px; margin: 0 0 12px;
  }
  .fo-toolbar button {
    background: #FFFFFF; border: 1px solid #E3E1DC; color: #606060;
    font-family: 'Inter', sans-serif; font-size: 11px; font-weight: 500;
    padding: 7px 12px; border-radius: 4px; cursor: pointer;
    transition: all 150ms cubic-bezier(0.25,0.46,0.45,0.94);
  }
  .fo-toolbar button:hover {
    background: #FAFAF9; color: #1D1D1D; border-color: #D9D7D0;
  }

  /* Summary sections — styled like .ids-card */
  .fo-sections {
    margin-top: 24px; display: grid;
    grid-template-columns: 1fr 1fr; gap: 16px;
  }
  .fo-sections .fo-section-glossary { grid-column: 1 / -1; }
  .fo-section {
    background: #FFFFFF; border: 1px solid #E3E1DC;
    border-radius: 6px; padding: 20px 24px;
    transition: all 200ms cubic-bezier(0.25,0.46,0.45,0.94);
  }
  .fo-section:hover { border-color: #D9D7D0; box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.06); }
  .fo-section h3 {
    font-family: 'Inter', sans-serif; font-size: 11px; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.08em; color: #727272;
    margin: 0 0 14px;
  }
  .fo-summary { width: 100%; border-collapse: collapse; font-size: 13px; }
  .fo-summary td {
    padding: 9px 4px; border-bottom: 1px solid #E3E1DC;
    font-weight: 400; color: #606060;
  }
  .fo-summary tr:last-child td { border-bottom: none; }
  .fo-summary td.fo-summary-label { color: #1D1D1D; }
  .fo-summary td.fo-summary-value {
    text-align: right; font-weight: 600; font-variant-numeric: tabular-nums;
    width: 130px; color: #1D1D1D;
    font-family: 'Plus Jakarta Sans', 'Inter', sans-serif;
    font-size: 15px;
  }
  .fo-summary td.fo-summary-value.fo-accent { color: #FF4639; }
  .fo-summary td.fo-summary-value.fo-strong { color: #1D1D1D; }
  .fo-summary td.fo-summary-value.fo-good-value { color: #2E7D32; }
  .fo-summary td.fo-summary-note {
    color: #727272; font-size: 11px; font-style: italic; width: 220px;
    text-align: right;
  }

  /* The two highlighted rows — MONTHLY WHOLESALE OBJECTIVE (editable)
     and NEW FOs NEEDED PER DAY (computed result). Subtle flare-red
     accent bar on the left and slight background tint. */
  .fo-summary tr.fo-summary-objective td {
    background: rgba(255,70,57,0.04);
  }
  .fo-summary tr.fo-summary-objective td.fo-summary-label {
    border-left: 3px solid #FF4639; padding-left: 10px; font-weight: 600;
  }
  .fo-summary tr.fo-summary-result td {
    background: rgba(255,70,57,0.04);
    border-top: 1px solid #FF4639;
  }
  .fo-summary tr.fo-summary-result td.fo-summary-label {
    border-left: 3px solid #FF4639; padding-left: 10px; font-weight: 600;
  }
  .fo-summary tr.fo-summary-result td.fo-summary-value {
    font-size: 18px;
  }
  .fo-summary tr.fo-summary-highlight td.fo-strong { color: #1D1D1D; font-size: 16px; }

  /* Glossary */
  .fo-section-glossary .fo-summary td { padding: 7px 4px; }
  .fo-section-glossary .fo-summary td.fo-def-key {
    width: 60px; font-weight: 600; color: #727272;
    text-transform: uppercase; letter-spacing: 0.06em; font-size: 10px;
  }
  .fo-section-glossary .fo-summary td.fo-def-name {
    width: 200px; font-weight: 600; color: #1D1D1D;
  }
  .fo-section-glossary .fo-summary td.fo-def-desc {
    color: #606060; font-size: 12px;
  }

  @media (max-width: 900px) {
    .fo-sections { grid-template-columns: 1fr; }
    /* On phones the 100vh-260px budget gets tight once the URL bar,
       chrome, and the page header eat into the viewport, so fall back
       to a fixed pixel cap that always shows ~10 rows. */
    .fo-report .fo-table-wrap { max-height: 520px; }
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
        '<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@500;600;700&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">'
        + css
        + toggle_js
        + '</head><body style="margin:0;padding:32px;background:#FAFAF9;font-family:\'Inter\',sans-serif">'
        + body
        + '</body></html>'
    )


# ---------------------------------------------------------------------------
# Email-safe renderer
# ---------------------------------------------------------------------------
#
# Outlook / Gmail / Apple Mail have wildly different CSS support, and anything
# in a <style> block is a coin flip. The email renderer drops all <style>
# rules in favour of inline styles on every element, switches to plain table
# layouts (no flexbox, no grid), and sticks to web-safe fonts so the email
# renders identically in the major clients.
#
# Scope: current + prior month of daily rows, the Anticipated Wholesales and
# FO Pacing summary blocks, and the column-definitions glossary. The full
# multi-month history is intentionally trimmed — use the standalone export
# for that.
# ---------------------------------------------------------------------------


# Reduced column set that fits comfortably in a 640px email. Full-width
# exports keep all 15; email keeps the 7 the logistics team reads first.
EMAIL_COLUMNS = [
    ('date', 'Date'),
    ('fos_created', 'FOs Created'),
    ('dispatched', 'Dispatched'),
    ('picked_up', 'Picked Up'),
    ('delivered', 'Delivered'),
    ('fo_disp_compliance', 'FO>Disp'),
    ('disp_pu_compliance', 'Disp>PU'),
]


_EMAIL_FONT = "-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,Helvetica,sans-serif"
_EMAIL_MONO_FONT = "'SF Mono',Menlo,Consolas,monospace"
_EMAIL_BG = "#FAFAF9"
_EMAIL_CARD_BG = "#FFFFFF"
_EMAIL_BORDER = "#E3E1DC"
_EMAIL_RULE = "#D9D7D0"
_EMAIL_TEXT_PRIMARY = "#1D1D1D"
_EMAIL_TEXT_SECONDARY = "#606060"
_EMAIL_TEXT_MUTED = "#727272"
_EMAIL_ACCENT = "#FF4639"
_EMAIL_SUCCESS = "#2E7D32"
_EMAIL_WARNING = "#BF360C"
_EMAIL_ERROR = "#C4281D"
_EMAIL_SUNKEN = "#FAFAF9"


def _email_compliance(value: Any) -> tuple[str, str]:
    if value is None or value == '':
        return '—', _EMAIL_TEXT_MUTED
    try:
        f = float(value)
    except (ValueError, TypeError):
        return html.escape(str(value)), _EMAIL_TEXT_PRIMARY
    pct = f * 100 if 0 <= f <= 1 else f
    text = f"{pct:.0f}%"
    if pct >= COMPLIANCE_GOOD * 100:
        color = _EMAIL_SUCCESS
    elif pct >= COMPLIANCE_WARN * 100:
        color = _EMAIL_WARNING
    else:
        color = _EMAIL_ERROR
    return text, color


def _email_num(value: Any) -> str:
    return _fmt_number(value) or '—'


def _email_fos_per_day(base: dict[str, Any], objective: int) -> tuple[str, str]:
    """Return (formatted value, color) for the hero pacing stat."""
    if base.get('cutoff_passed'):
        return 'Cutoff passed', _EMAIL_TEXT_MUTED
    bd = base.get('remaining_bd') or 0
    if bd <= 0:
        return 'N/A', _EMAIL_TEXT_MUTED
    val = round((objective - base['mtd_pickups'] - base['existing_pipeline']) / bd, 1)
    return _fmt_number(val), _EMAIL_ACCENT


def _email_month_block(month: dict[str, Any], label_prefix: str) -> str:
    label = month.get('label', '')
    days = month.get('days') or []
    total = month.get('total') or {}

    thead = ''.join(
        f'<th align="{("left" if key == "date" else "right")}" '
        f'style="padding:8px 10px;background:{_EMAIL_CARD_BG};'
        f'font-family:{_EMAIL_FONT};font-size:10px;font-weight:600;'
        f'text-transform:uppercase;letter-spacing:0.06em;'
        f'color:{_EMAIL_TEXT_MUTED};border-bottom:2px solid {_EMAIL_RULE};'
        f'white-space:nowrap;">{html.escape(title)}</th>'
        for key, title in EMAIL_COLUMNS
    )

    row_style_base = (
        f'font-family:{_EMAIL_FONT};font-size:12px;'
        f'color:{_EMAIL_TEXT_PRIMARY};'
    )
    rows_html: list[str] = []
    for i, day in enumerate(days):
        bg = _EMAIL_CARD_BG if i % 2 == 0 else _EMAIL_SUNKEN
        cells: list[str] = []
        for key, _ in EMAIL_COLUMNS:
            v = day.get(key)
            if key == 'date':
                cells.append(
                    f'<td align="left" bgcolor="{bg}" '
                    f'style="padding:7px 10px;{row_style_base}font-weight:500;'
                    f'border-bottom:1px solid {_EMAIL_BORDER};white-space:nowrap;">'
                    f'{html.escape(str(v or ""))}</td>'
                )
            elif key in COMPLIANCE_KEYS:
                text, color = _email_compliance(v)
                cells.append(
                    f'<td align="right" bgcolor="{bg}" '
                    f'style="padding:7px 10px;{row_style_base}color:{color};'
                    f'font-weight:600;font-variant-numeric:tabular-nums;'
                    f'border-bottom:1px solid {_EMAIL_BORDER};">{text}</td>'
                )
            else:
                cells.append(
                    f'<td align="right" bgcolor="{bg}" '
                    f'style="padding:7px 10px;{row_style_base}'
                    f'font-variant-numeric:tabular-nums;'
                    f'border-bottom:1px solid {_EMAIL_BORDER};">'
                    f'{_email_num(v)}</td>'
                )
        rows_html.append('<tr>' + ''.join(cells) + '</tr>')

    # Total row — bolded bar
    if total:
        cells: list[str] = []
        for key, _ in EMAIL_COLUMNS:
            v = total.get(key)
            if key == 'date':
                cells.append(
                    f'<td align="left" bgcolor="#F0EFEC" '
                    f'style="padding:9px 10px;{row_style_base}font-weight:700;'
                    f'border-top:1px solid {_EMAIL_RULE};'
                    f'border-bottom:1px solid {_EMAIL_RULE};white-space:nowrap;">'
                    f'{html.escape(str(v or (label + " Total")))}</td>'
                )
            elif key in COMPLIANCE_KEYS:
                cells.append(
                    f'<td align="right" bgcolor="#F0EFEC" '
                    f'style="padding:9px 10px;{row_style_base}font-weight:700;'
                    f'border-top:1px solid {_EMAIL_RULE};'
                    f'border-bottom:1px solid {_EMAIL_RULE};">—</td>'
                )
            else:
                cells.append(
                    f'<td align="right" bgcolor="#F0EFEC" '
                    f'style="padding:9px 10px;{row_style_base}font-weight:700;'
                    f'font-variant-numeric:tabular-nums;'
                    f'border-top:1px solid {_EMAIL_RULE};'
                    f'border-bottom:1px solid {_EMAIL_RULE};">'
                    f'{_email_num(v)}</td>'
                )
        rows_html.append('<tr>' + ''.join(cells) + '</tr>')

    caption = (
        f'<tr><td colspan="{len(EMAIL_COLUMNS)}" '
        f'style="padding:12px 0 6px;'
        f'font-family:{_EMAIL_FONT};font-size:10px;font-weight:600;'
        f'color:{_EMAIL_TEXT_MUTED};text-transform:uppercase;'
        f'letter-spacing:0.08em;">{html.escape(label_prefix)} — {html.escape(label)}</td></tr>'
    )

    return (
        f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        f'border="0" style="width:100%;border-collapse:collapse;'
        f'margin:0 0 18px;">'
        f'{caption}'
        f'<tr>{thead}</tr>'
        + ''.join(rows_html)
        + '</table>'
    )


def _email_kpi_cell(label: str, value: str, color: str, sub: str = '') -> str:
    sub_html = (
        f'<div style="font-family:{_EMAIL_FONT};font-size:10px;'
        f'color:{_EMAIL_TEXT_MUTED};margin-top:2px;">{html.escape(sub)}</div>'
        if sub else ''
    )
    return (
        f'<td align="left" valign="top" bgcolor="{_EMAIL_CARD_BG}" '
        f'style="padding:14px 14px;border:1px solid {_EMAIL_BORDER};'
        f'border-radius:6px;border-top:2px solid {color};width:25%;">'
        f'<div style="font-family:{_EMAIL_FONT};font-size:9px;font-weight:600;'
        f'text-transform:uppercase;letter-spacing:0.08em;'
        f'color:{_EMAIL_TEXT_MUTED};">{html.escape(label)}</div>'
        f'<div style="font-family:{_EMAIL_FONT};font-size:22px;font-weight:700;'
        f'color:{color};margin-top:6px;line-height:1.1;'
        f'font-variant-numeric:tabular-nums;">{value}</div>'
        f'{sub_html}</td>'
    )


def _email_summary_table(rows: list[list[Any]]) -> str:
    row_style_base = (
        f'font-family:{_EMAIL_FONT};font-size:12px;'
        f'color:{_EMAIL_TEXT_PRIMARY};'
    )
    out: list[str] = []
    last = len(rows) - 1
    for i, row in enumerate(rows):
        cells = list(row) + [None] * (15 - len(row))
        label = str(cells[0] or '')
        value = cells[11]
        note = cells[13] or ''
        label_upper = label.upper()
        is_hero = (
            'MONTHLY WHOLESALE OBJECTIVE' in label_upper
            or 'NEW FOS NEEDED PER DAY' in label_upper
        )
        is_anticipated = 'ANTICIPATED REMAINING WHOLESALES' in label_upper
        is_mtd = label_upper.startswith('MTD WHOLESALES')

        if is_hero:
            value_color = _EMAIL_ACCENT
            label_weight = '700'
            bg = '#FFF5F4'
        elif is_anticipated:
            value_color = _EMAIL_TEXT_PRIMARY
            label_weight = '600'
            bg = _EMAIL_CARD_BG
        elif is_mtd:
            value_color = _EMAIL_SUCCESS
            label_weight = '600'
            bg = _EMAIL_CARD_BG
        else:
            value_color = _EMAIL_TEXT_PRIMARY
            label_weight = '400'
            bg = _EMAIL_CARD_BG

        border = (
            f'border-bottom:1px solid {_EMAIL_BORDER};'
            if i < last else ''
        )
        hero_border = (
            f'border-left:3px solid {_EMAIL_ACCENT};'
            if is_hero else ''
        )

        formatted = _fmt_number(value) if value is not None else '—'
        out.append(
            f'<tr>'
            f'<td bgcolor="{bg}" align="left" '
            f'style="padding:9px 10px;{row_style_base}'
            f'font-weight:{label_weight};{hero_border}{border}">'
            f'{html.escape(label)}</td>'
            f'<td bgcolor="{bg}" align="right" width="110" '
            f'style="padding:9px 10px;{row_style_base}font-weight:700;'
            f'color:{value_color};font-variant-numeric:tabular-nums;{border}">'
            f'{formatted}</td>'
            f'<td bgcolor="{bg}" align="right" width="180" '
            f'style="padding:9px 10px;font-family:{_EMAIL_FONT};font-size:10px;'
            f'color:{_EMAIL_TEXT_MUTED};font-style:italic;{border}">'
            f'{html.escape(str(note))}</td>'
            f'</tr>'
        )
    return (
        f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        f'border="0" style="width:100%;border-collapse:collapse;'
        f'border:1px solid {_EMAIL_BORDER};border-radius:6px;'
        f'background:{_EMAIL_CARD_BG};">'
        + ''.join(out)
        + '</table>'
    )


def _email_glossary(rows: list[list[Any]]) -> str:
    row_style_base = (
        f'font-family:{_EMAIL_FONT};font-size:11px;'
        f'color:{_EMAIL_TEXT_SECONDARY};'
    )
    out: list[str] = []
    last = len(rows) - 1
    for i, row in enumerate(rows):
        key = str(row[0] or '')
        name = str(row[1] or '')
        desc = str(row[2] or '')
        border = (
            f'border-bottom:1px solid {_EMAIL_BORDER};'
            if i < last else ''
        )
        out.append(
            f'<tr>'
            f'<td valign="top" width="55" '
            f'style="padding:8px 8px;{row_style_base}'
            f'font-weight:600;text-transform:uppercase;letter-spacing:0.06em;'
            f'font-size:9px;color:{_EMAIL_TEXT_MUTED};{border}">'
            f'{html.escape(key)}</td>'
            f'<td valign="top" width="150" '
            f'style="padding:8px 8px;{row_style_base}font-weight:600;'
            f'color:{_EMAIL_TEXT_PRIMARY};{border}">'
            f'{html.escape(name)}</td>'
            f'<td valign="top" '
            f'style="padding:8px 8px;{row_style_base};{border}">'
            f'{html.escape(desc)}</td>'
            f'</tr>'
        )
    return (
        f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        f'border="0" style="width:100%;border-collapse:collapse;'
        f'border:1px solid {_EMAIL_BORDER};border-radius:6px;'
        f'background:{_EMAIL_CARD_BG};">'
        + ''.join(out)
        + '</table>'
    )


def _email_section_header(label: str) -> str:
    return (
        f'<div style="font-family:{_EMAIL_FONT};font-size:11px;font-weight:700;'
        f'text-transform:uppercase;letter-spacing:0.1em;'
        f'color:{_EMAIL_TEXT_MUTED};margin:24px 0 10px;">'
        f'{html.escape(label)}</div>'
    )


def render_fo_performance_email(data: dict[str, Any]) -> str:
    """Return a full <!doctype html> document styled for email clients.

    The output is deliberately narrow (640px max), inline-styled, table-based,
    and scoped to the current + prior month of daily rows plus the Anticipated
    Wholesales / FO Pacing / Column Definitions sections. Suitable for
    pasting into an email draft or forwarding as an attachment.
    """
    months = data.get('months') or []
    sections = data.get('sections') or []
    base = data.get('base_metrics') or {}
    monthly_objective = int(data.get('monthly_objective') or 0)
    generated = data.get('generated_at', '')

    # Pick current + prior month by label
    current_label = base.get('month_label', '')
    prior_label = ''
    if current_label:
        try:
            from datetime import datetime as _dt, timedelta as _td
            cur_dt = _dt.strptime(current_label, '%b %Y')
            prior_dt = (cur_dt.replace(day=1) - _td(days=1)).replace(day=1)
            prior_label = prior_dt.strftime('%b %Y')
        except Exception:
            prior_label = ''

    by_label = {m.get('label'): m for m in months}
    current_month = by_label.get(current_label)
    prior_month = by_label.get(prior_label)

    # Fallback: use the last two months present if labels don't match
    if current_month is None and months:
        current_month = months[-1]
        current_label = current_month.get('label', '')
    if prior_month is None and len(months) >= 2:
        prior_month = months[-2]
        prior_label = prior_month.get('label', '')

    # Build the four KPI cells
    fos_per_day, fos_per_day_color = _email_fos_per_day(base, monthly_objective)
    kpi_cells = (
        _email_kpi_cell(
            'MTD Wholesales',
            _fmt_number(base.get('mtd_pickups', 0)),
            _EMAIL_SUCCESS,
            'Pickups so far',
        )
        + '<td width="12" style="width:12px;font-size:0;line-height:0;">&nbsp;</td>'
        + _email_kpi_cell(
            'Anticipated Remaining',
            _fmt_number(base.get('anticipated_remaining', 0)),
            _EMAIL_TEXT_PRIMARY,
            f'{base.get("current_month_created", 0)} current / {base.get("prior_month_awaiting", 0)} prior',
        )
        + '<td width="12" style="width:12px;font-size:0;line-height:0;">&nbsp;</td>'
        + _email_kpi_cell(
            'Monthly Objective',
            _fmt_number(monthly_objective),
            _EMAIL_TEXT_PRIMARY,
            'Wholesale target',
        )
        + '<td width="12" style="width:12px;font-size:0;line-height:0;">&nbsp;</td>'
        + _email_kpi_cell(
            'New FOs / Day',
            fos_per_day,
            fos_per_day_color,
            f'Through {base.get("cutoff_day_label", "cutoff")}',
        )
    )

    kpi_table = (
        f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        f'border="0" style="width:100%;border-collapse:separate;'
        f'border-spacing:0;margin:6px 0 20px;">'
        f'<tr>{kpi_cells}</tr>'
        f'</table>'
    )

    # Current + prior month tables
    month_blocks: list[str] = []
    if current_month:
        month_blocks.append(_email_month_block(current_month, 'Current Month'))
    if prior_month:
        month_blocks.append(_email_month_block(prior_month, 'Prior Month'))

    # Summary sections
    anticipated = next(
        (s for s in sections if s.get('id') == 'anticipated_wholesales'), None
    )
    pacing = next(
        (s for s in sections if s.get('id') == 'fo_pacing'), None
    )
    glossary = next(
        (s for s in sections if s.get('id') == 'column_definitions'), None
    )

    anticipated_html = (
        _email_section_header(anticipated.get('title', 'Anticipated Wholesales'))
        + _email_summary_table(anticipated.get('rows') or [])
    ) if anticipated else ''
    pacing_html = (
        _email_section_header(pacing.get('title', 'FO Pacing to Objective'))
        + _email_summary_table(pacing.get('rows') or [])
    ) if pacing else ''
    glossary_html = (
        _email_section_header('Column Definitions')
        + _email_glossary(glossary.get('rows') or [])
    ) if glossary else ''

    header_html = (
        f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        f'border="0" style="width:100%;border-collapse:collapse;">'
        f'<tr><td bgcolor="{_EMAIL_TEXT_PRIMARY}" '
        f'style="padding:18px 20px;border-top:3px solid {_EMAIL_ACCENT};">'
        f'<div style="font-family:{_EMAIL_FONT};font-size:10px;font-weight:600;'
        f'text-transform:uppercase;letter-spacing:0.12em;color:{_EMAIL_ACCENT};">'
        f'INEOS Americas · Logistics</div>'
        f'<div style="font-family:{_EMAIL_FONT};font-size:18px;font-weight:700;'
        f'color:#FFFFFF;margin-top:4px;line-height:1.2;">'
        f'Freight Order Performance — {html.escape(current_label or "Current Month")}</div>'
        f'<div style="font-family:{_EMAIL_FONT};font-size:11px;color:#9F9F9F;'
        f'margin-top:3px;">Daily flow-through, SLA compliance, and pacing to monthly objective.</div>'
        f'</td></tr></table>'
    )

    footer_html = (
        f'<div style="font-family:{_EMAIL_FONT};font-size:10px;'
        f'color:{_EMAIL_TEXT_MUTED};margin-top:24px;padding-top:14px;'
        f'border-top:1px solid {_EMAIL_BORDER};text-align:center;">'
        f'Generated {html.escape(generated)} · '
        f'Source: Vehicle Distribution workbook · INEOS Grenadier Americas'
        f'</div>'
    )

    inner = (
        header_html
        + '<div style="padding:20px 20px;">'
        + kpi_table
        + ''.join(month_blocks)
        + anticipated_html
        + pacing_html
        + glossary_html
        + footer_html
        + '</div>'
    )

    # Outer 640px wrapper — the classic email pattern. Centered on wide
    # screens, full-bleed on mobile.
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<meta name="x-apple-disable-message-reformatting">'
        f'<title>FO Performance — {html.escape(current_label or "")}</title>'
        '</head>'
        f'<body style="margin:0;padding:0;background:{_EMAIL_BG};'
        f'font-family:{_EMAIL_FONT};color:{_EMAIL_TEXT_PRIMARY};">'
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        f'border="0" bgcolor="{_EMAIL_BG}" style="background:{_EMAIL_BG};">'
        '<tr><td align="center" style="padding:24px 12px;">'
        '<table role="presentation" width="640" cellspacing="0" cellpadding="0" '
        f'border="0" bgcolor="{_EMAIL_CARD_BG}" '
        f'style="width:640px;max-width:640px;background:{_EMAIL_CARD_BG};'
        f'border:1px solid {_EMAIL_BORDER};border-radius:8px;overflow:hidden;">'
        f'<tr><td style="padding:0;">{inner}</td></tr>'
        '</table></td></tr></table>'
        '</body></html>'
    )
