"""Vehicle Distribution ingest — computes the Logistics Freight Order Performance
report from the weekly Vehicle Distribution xlsx sent by the logistics team.

The Vehicle Distribution workbook has a `Data File` sheet with one row per VIN
and columns for the full freight-order lifecycle:

  col 14 FO                — freight order number
  col 15 FO Create Date    — date the FO was created in SAP
  col 18 Dispatched Date   — carrier dispatch date
  col 20 PickUp Actual Date
  col 22 Delivered Date

From those four dates we reconstruct the same Daily Freight Order Activity
report that logistics circulates in Excel — daily rows grouped by month with
subtotals, a grand total, and three summary sections:

  - CURRENT MONTH ANTICIPATED WHOLESALES (AT PICKUP)
  - FO PACING TO OBJECTIVE (through month end)
  - COLUMN DEFINITIONS

All numbers (FOs Created, Dispatched, Picked Up, Delivered, average business
days per stage, SLA compliance %, Prior Month FO Pickups, Cumulative Awaiting
PU, MTD Pickups) are derived here in Python — no formulas from the source
workbook are needed.

Validated against the published FO_Performance_34.xlsx report: the grand total
and each daily row for Apr 2026 match to the last integer.
"""

from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Iterable

import numpy as np
import openpyxl


# Default SLA thresholds in business days — these match the logistics team's
# contracted carrier SLAs. If these ever change they can become config later.
SLA_FO_TO_DISPATCH_BD = 3
SLA_DISPATCH_TO_PICKUP_BD = 3

# Default monthly wholesale objective used by the FO PACING section. The
# logistics team enters this manually in Excel; we expose it as a function
# parameter so it can be wired up to admin config later.
DEFAULT_MONTHLY_OBJECTIVE = 477


# Column keys — these drive the HTML renderer and must stay stable.
COLUMN_KEYS = [
    'date',
    'fos_created',
    'dispatched',
    'picked_up',
    'delivered',
    'avg_fo_to_disp_bd',
    'sla_fo_to_disp',
    'fo_disp_compliance',
    'avg_disp_to_pu_bd',
    'sla_disp_to_pu',
    'disp_pu_compliance',
    'avg_e2e_bd',
    'prior_month_fo_pickups',
    'cumulative_awaiting_pu',
    'mtd_pickups',
]

COLUMN_LABELS = {
    'date': 'DATE',
    'fos_created': 'FOS CREATED',
    'dispatched': 'DISPATCHED',
    'picked_up': 'PICKED UP',
    'delivered': 'DELIVERED',
    'avg_fo_to_disp_bd': 'AVG FO TO DISP (BD)',
    'sla_fo_to_disp': 'SLA',
    'fo_disp_compliance': 'FO>DISP COMPLIANCE',
    'avg_disp_to_pu_bd': 'AVG DISP TO PU (BD)',
    'sla_disp_to_pu': 'SLA',
    'disp_pu_compliance': 'DISP>PU COMPLIANCE',
    'avg_e2e_bd': 'AVG E2E (BD)',
    'prior_month_fo_pickups': 'PRIOR MONTH FO PICKUPS',
    'cumulative_awaiting_pu': 'CUMULATIVE AWAITING PU',
    'mtd_pickups': 'MTD PICKUPS',
}


def _to_date(value: Any) -> date | None:
    """Normalize a cell value into a naive date or None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    # Some exports leave empty strings or 'N/A' in date columns.
    if isinstance(value, str):
        s = value.strip()
        if not s or s.lower() in ('n/a', 'na', '-', 'none', 'nan'):
            return None
        for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S'):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
    return None


def _busdays(start: date | None, end: date | None) -> int | None:
    """Business days between start and end (Mon-Fri, no holidays)."""
    if not start or not end:
        return None
    try:
        return int(np.busday_count(start, end))
    except (ValueError, TypeError):
        return None


def _round1(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 1)


def _mean(values: Iterable[float | int | None]) -> float | None:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def _month_end(d: date) -> date:
    last_day = calendar.monthrange(d.year, d.month)[1]
    return date(d.year, d.month, last_day)


def _month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def _busdays_inclusive(start: date, end: date) -> int:
    """Inclusive business days between two dates, counting both endpoints."""
    if end < start:
        return 0
    return int(np.busday_count(start, end + timedelta(days=1)))


def _load_fo_records(xlsx_path: str) -> list[dict[str, Any]]:
    """Read the Data File sheet and return a flat list of FO lifecycle records."""
    wb = openpyxl.load_workbook(xlsx_path, data_only=True)

    # Find the sheet — prefer 'Data File', fall back to the first sheet
    # whose first row contains 'FO Create Date'.
    sheet_name = None
    if 'Data File' in wb.sheetnames:
        sheet_name = 'Data File'
    else:
        for name in wb.sheetnames:
            first_row = [c.value for c in wb[name][1]]
            if any(isinstance(v, str) and 'fo create date' in v.lower() for v in first_row):
                sheet_name = name
                break
    if sheet_name is None:
        raise ValueError(
            "Could not find a 'Data File' sheet with an 'FO Create Date' column. "
            f"Available sheets: {wb.sheetnames}"
        )
    ws = wb[sheet_name]

    # Map header → column index so we tolerate header re-ordering.
    header_row = [str(c.value).strip() if c.value is not None else '' for c in ws[1]]
    def col(*candidates: str) -> int | None:
        lower = [h.lower() for h in header_row]
        for cand in candidates:
            cand_l = cand.lower()
            for i, h in enumerate(lower):
                if h == cand_l:
                    return i
        # Fallback: substring match
        for cand in candidates:
            cand_l = cand.lower()
            for i, h in enumerate(lower):
                if cand_l in h:
                    return i
        return None

    col_fo = col('FO')
    col_create = col('FO Create Date')
    col_dispatch = col('Dispatched Date', 'Dispatch Date')
    col_pickup = col('PickUp Actual Date', 'Pickup Actual Date', 'Pickup Date')
    col_delivered = col('Delivered Date')

    if col_fo is None or col_create is None:
        raise ValueError(
            f"Data File sheet is missing required columns. "
            f"Found headers: {header_row[:20]}"
        )

    records: list[dict[str, Any]] = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row:
            continue
        fo = row[col_fo] if col_fo < len(row) else None
        create = _to_date(row[col_create]) if col_create < len(row) else None
        if not fo or not create:
            continue
        dispatch = _to_date(row[col_dispatch]) if (col_dispatch is not None and col_dispatch < len(row)) else None
        pickup = _to_date(row[col_pickup]) if (col_pickup is not None and col_pickup < len(row)) else None
        delivered = _to_date(row[col_delivered]) if (col_delivered is not None and col_delivered < len(row)) else None
        records.append({
            'fo': str(fo).strip(),
            'create': create,
            'dispatch': dispatch,
            'pickup': pickup,
            'delivered': delivered,
        })
    return records


def _compute_daily_row(day: date, cohort: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute the daily flow-through row for FOs created on `day`."""
    fos = len(cohort)
    dispatched = sum(1 for r in cohort if r['dispatch'])
    picked_up = sum(1 for r in cohort if r['pickup'])
    delivered = sum(1 for r in cohort if r['delivered'])

    fo_to_disp = [_busdays(r['create'], r['dispatch']) for r in cohort if r['dispatch']]
    disp_to_pu = [_busdays(r['dispatch'], r['pickup']) for r in cohort if r['dispatch'] and r['pickup']]
    e2e = [_busdays(r['create'], r['delivered']) for r in cohort if r['delivered']]

    fo_to_disp = [b for b in fo_to_disp if b is not None]
    disp_to_pu = [b for b in disp_to_pu if b is not None]
    e2e = [b for b in e2e if b is not None]

    fo_disp_compl = None
    if fo_to_disp:
        fo_disp_compl = sum(1 for b in fo_to_disp if b <= SLA_FO_TO_DISPATCH_BD) / len(fo_to_disp)
    disp_pu_compl = None
    if disp_to_pu:
        disp_pu_compl = sum(1 for b in disp_to_pu if b <= SLA_DISPATCH_TO_PICKUP_BD) / len(disp_to_pu)

    return {
        'date': day.strftime('%a %m/%d'),
        '_iso_date': day.isoformat(),
        'fos_created': fos,
        'dispatched': dispatched,
        'picked_up': picked_up,
        'delivered': delivered,
        'avg_fo_to_disp_bd': _round1(_mean(fo_to_disp)),
        'sla_fo_to_disp': SLA_FO_TO_DISPATCH_BD,
        'fo_disp_compliance': round(fo_disp_compl, 3) if fo_disp_compl is not None else None,
        'avg_disp_to_pu_bd': _round1(_mean(disp_to_pu)),
        'sla_disp_to_pu': SLA_DISPATCH_TO_PICKUP_BD,
        'disp_pu_compliance': round(disp_pu_compl, 3) if disp_pu_compl is not None else None,
        'avg_e2e_bd': _round1(_mean(e2e)),
    }


def _compute_cumulative_columns(
    records: list[dict[str, Any]],
    day: date,
) -> tuple[int, int, int]:
    """Return (prior_month_fo_pickups, cumulative_awaiting_pu, mtd_pickups) for `day`.

    - prior_month_fo_pickups: FOs picked up on `day` whose create date was before
      the first of `day`'s month.
    - cumulative_awaiting_pu: FOs created on or before `day` that have either no
      pickup recorded or a pickup strictly after `day`.
    - mtd_pickups: FOs picked up between the first of `day`'s month and `day`
      inclusive.
    """
    month_start = _month_start(day)
    prior = sum(1 for r in records if r['pickup'] == day and r['create'] < month_start)
    awaiting = sum(
        1
        for r in records
        if r['create'] <= day and (r['pickup'] is None or r['pickup'] > day)
    )
    mtd_pu = sum(
        1 for r in records if r['pickup'] and month_start <= r['pickup'] <= day
    )
    return prior, awaiting, mtd_pu


def _compute_monthly_total(
    label: str,
    days: list[dict[str, Any]],
    cohort: list[dict[str, Any]],
    records: list[dict[str, Any]],
    month_end: date,
) -> dict[str, Any]:
    fos = sum(d['fos_created'] for d in days)
    dispatched = sum(d['dispatched'] for d in days)
    picked_up = sum(d['picked_up'] for d in days)
    delivered = sum(d['delivered'] for d in days)

    # Prior month FO pickups total = pickups this month for FOs created before
    # this month (count over the whole month, not just shown daily rows).
    month_start = _month_start(month_end)
    prior_month_total = sum(
        1
        for r in records
        if r['pickup'] and month_start <= r['pickup'] <= month_end and r['create'] < month_start
    )
    mtd_total = sum(
        1 for r in records if r['pickup'] and month_start <= r['pickup'] <= month_end
    )

    return {
        'date': f'{label} Total',
        '_iso_date': month_end.isoformat(),
        'fos_created': fos,
        'dispatched': dispatched,
        'picked_up': picked_up,
        'delivered': delivered,
        'avg_fo_to_disp_bd': None,
        'sla_fo_to_disp': None,
        'fo_disp_compliance': None,
        'avg_disp_to_pu_bd': None,
        'sla_disp_to_pu': None,
        'disp_pu_compliance': None,
        'avg_e2e_bd': None,
        'prior_month_fo_pickups': prior_month_total,
        'cumulative_awaiting_pu': None,
        'mtd_pickups': mtd_total,
    }


def _column_definitions() -> list[list[Any]]:
    """Glossary rendered at the bottom of the report."""
    entries = [
        ('Col A', 'Date',
         'Calendar date on which the Freight Order was created. Rows grouped by month with subtotals.'),
        ('Col B', 'FOs Created',
         'Number of new Freight Orders entered on this date. Each FO = one VIN assigned for transport from VPC to retailer.'),
        ('Col C', 'Dispatched',
         'FOs from this date that have been dispatched to a carrier (flow-through from creation cohort).'),
        ('Col D', 'Picked Up',
         'FOs from this date that have been picked up by the carrier.'),
        ('Col E', 'Delivered',
         'FOs from this date that have been delivered to the retailer.'),
        ('Col F', 'Avg FO to Disp (BD)',
         'Average business days between FO creation and carrier dispatch for this date\'s cohort.'),
        ('Col G', 'SLA',
         f'Contract SLA target for FO-to-dispatch: {SLA_FO_TO_DISPATCH_BD} business days.'),
        ('Col H', 'FO>Disp Compliance',
         'Percentage of dispatched FOs from this cohort that met the FO-to-dispatch SLA.'),
        ('Col I', 'Avg Disp to PU (BD)',
         'Average business days between dispatch and pickup for this date\'s cohort.'),
        ('Col J', 'SLA',
         f'Contract SLA target for dispatch-to-pickup: {SLA_DISPATCH_TO_PICKUP_BD} business days.'),
        ('Col K', 'Disp>PU Compliance',
         'Percentage of picked-up FOs from this cohort that met the dispatch-to-pickup SLA.'),
        ('Col L', 'Avg E2E (BD)',
         'Average end-to-end business days from FO creation to delivery for this cohort.'),
        ('Col M', 'Prior Month FO Pickups',
         'Carrier pickups occurring on this date for FOs that were created in a previous month.'),
        ('Col N', 'Cumulative Awaiting PU',
         'Running total of FOs created on or before this date that have not yet been picked up.'),
        ('Col O', 'MTD Pickups',
         'Month-to-date cumulative carrier pickups (all FOs regardless of creation month).'),
    ]
    return [[key, name, desc] + [None] * 12 for key, name, desc in entries]


def _build_summary_sections(
    records: list[dict[str, Any]],
    anchor_day: date,
    monthly_objective: int,
) -> list[dict[str, Any]]:
    """Build the two dynamic summary sections plus the static glossary."""
    month_start = _month_start(anchor_day)
    month_end = _month_end(anchor_day)

    # MTD Wholesales (pickups that have already happened this month)
    mtd_pickups = sum(
        1 for r in records if r['pickup'] and month_start <= r['pickup'] <= anchor_day
    )

    # Anticipated Remaining Wholesales = the full "awaiting pickup" backlog as
    # of the anchor day. Mirrors Col N (Cumulative Awaiting PU) at the anchor.
    anticipated_remaining = sum(
        1
        for r in records
        if r['create'] <= anchor_day and (not r['pickup'] or r['pickup'] > anchor_day)
    )

    # Match the logistics team's Excel convention for the "of which" rows:
    #   - "Current Month FOs Created" is literally the MTD Col B sum (total
    #     Apr FOs created so far, whether or not they've been picked up).
    #   - "Prior Month FOs Awaiting Pickup" is derived by subtraction so the
    #     two rows always add back to the Anticipated Remaining number.
    current_month_created = sum(
        1 for r in records if month_start <= r['create'] <= anchor_day
    )
    prior_month_awaiting = max(0, anticipated_remaining - current_month_created)

    # Pacing to objective
    existing_pipeline = anticipated_remaining
    remaining_bd = _busdays_inclusive(anchor_day, month_end)
    remaining_target = monthly_objective - mtd_pickups - existing_pipeline
    new_fos_needed = (
        round(remaining_target / remaining_bd, 1) if remaining_bd > 0 else None
    )

    month_label = anchor_day.strftime('%b %Y')
    prior_month_label = (month_start - timedelta(days=1)).strftime('%b')

    anticipated_rows: list[list[Any]] = []

    def row(label: str, value: Any = None, note: str = '') -> list[Any]:
        cells: list[Any] = [label] + [None] * 15
        if value is not None:
            cells[11] = value
        if note:
            cells[13] = note
        return cells

    anticipated_rows.append(row('MTD Wholesales (Pickups)', mtd_pickups, '(Col O)'))
    anticipated_rows.append(row('Anticipated Remaining Wholesales', anticipated_remaining, '(Col N)'))
    anticipated_rows.append(row(
        'of which: Current Month FOs Created (subset of above)',
        current_month_created,
        '(Col B month total)',
    ))
    anticipated_rows.append(row(
        f'of which: {prior_month_label} FOs Awaiting Pickup (subset of above)',
        prior_month_awaiting,
        f'(of {anticipated_remaining} total)',
    ))

    pacing_rows: list[list[Any]] = []
    pacing_rows.append(row('MONTHLY WHOLESALE OBJECTIVE', monthly_objective, 'Enter target'))
    pacing_rows.append(row('Less: MTD Wholesales (pickups already completed)', mtd_pickups))
    pacing_rows.append(row(
        f'Less: Existing Pipeline FOs Awaiting Pickup (created through {anchor_day.strftime("%b %d")})',
        existing_pipeline,
        f'({current_month_created} current mo + {prior_month_awaiting} prior)',
    ))
    pacing_rows.append(row(
        f'Remaining Business Days ({anchor_day.strftime("%b %d")} through {month_end.strftime("%b %d")}, inclusive)',
        remaining_bd,
    ))
    pacing_rows.append(row(
        f'NEW FOs NEEDED PER DAY (through {month_end.strftime("%b %d")})',
        new_fos_needed,
        'per business day',
    ))

    return [
        {
            'title': f'CURRENT MONTH ANTICIPATED WHOLESALES (AT PICKUP) — {month_label}',
            'rows': anticipated_rows,
        },
        {
            'title': f'FO PACING TO OBJECTIVE (through {month_end.strftime("%b %d")})',
            'rows': pacing_rows,
        },
        {
            'title': 'COLUMN DEFINITIONS',
            'rows': _column_definitions(),
        },
    ]


def ingest_vehicle_distribution(
    xlsx_path: str,
    *,
    monthly_objective: int = DEFAULT_MONTHLY_OBJECTIVE,
    anchor_day: date | None = None,
) -> dict[str, Any]:
    """Parse a Vehicle Distribution workbook and compute the Logistics FO
    Performance report structure.

    Parameters
    ----------
    xlsx_path : str
        Path to the Vehicle Distribution xlsx file.
    monthly_objective : int
        Monthly wholesale objective used by the FO PACING section.
    anchor_day : date | None
        'As of' date for the pacing / cumulative / MTD calculations. Defaults
        to the maximum creation date in the dataset (i.e. treat the file as
        current through its latest entry).
    """
    records = _load_fo_records(xlsx_path)
    if not records:
        raise ValueError("No FO records with a Create Date were found in the workbook.")

    # Group records by create date
    by_day: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for r in records:
        by_day[r['create']].append(r)

    sorted_days = sorted(by_day.keys())

    if anchor_day is None:
        anchor_day = max(
            [r['pickup'] for r in records if r['pickup']]
            + [r['dispatch'] for r in records if r['dispatch']]
            + sorted_days,
            default=sorted_days[-1],
        )

    # Build month buckets in chronological order
    months: list[dict[str, Any]] = []
    current_label: str | None = None
    current_month: dict[str, Any] | None = None
    current_month_cohort: list[dict[str, Any]] = []

    for day in sorted_days:
        month_label = day.strftime('%b %Y')
        if month_label != current_label:
            # Close out previous month
            if current_month is not None and current_label is not None:
                last_day = date(
                    datetime.strptime(current_label, '%b %Y').year,
                    datetime.strptime(current_label, '%b %Y').month,
                    1,
                )
                month_end = _month_end(last_day)
                current_month['total'] = _compute_monthly_total(
                    current_label,
                    current_month['days'],
                    current_month_cohort,
                    records,
                    month_end,
                )
                months.append(current_month)
            current_label = month_label
            current_month = {'label': month_label, 'days': [], 'total': None}
            current_month_cohort = []

        cohort = by_day[day]
        current_month_cohort.extend(cohort)

        daily = _compute_daily_row(day, cohort)
        prior, awaiting, mtd = _compute_cumulative_columns(records, day)
        daily['prior_month_fo_pickups'] = prior
        daily['cumulative_awaiting_pu'] = awaiting
        daily['mtd_pickups'] = mtd
        current_month['days'].append(daily)

    # Flush the final month
    if current_month is not None and current_label is not None:
        last_day_of_month = _month_end(
            date(
                datetime.strptime(current_label, '%b %Y').year,
                datetime.strptime(current_label, '%b %Y').month,
                1,
            )
        )
        current_month['total'] = _compute_monthly_total(
            current_label,
            current_month['days'],
            current_month_cohort,
            records,
            last_day_of_month,
        )
        months.append(current_month)

    # Grand total across every record
    grand_dispatched = sum(1 for r in records if r['dispatch'])
    grand_picked = sum(1 for r in records if r['pickup'])
    grand_delivered = sum(1 for r in records if r['delivered'])

    fo_to_disp = [_busdays(r['create'], r['dispatch']) for r in records if r['dispatch']]
    disp_to_pu = [_busdays(r['dispatch'], r['pickup']) for r in records if r['dispatch'] and r['pickup']]
    e2e = [_busdays(r['create'], r['delivered']) for r in records if r['delivered']]
    fo_to_disp = [b for b in fo_to_disp if b is not None]
    disp_to_pu = [b for b in disp_to_pu if b is not None]
    e2e = [b for b in e2e if b is not None]

    grand_total = {
        'date': 'GRAND TOTAL',
        'fos_created': len(records),
        'dispatched': grand_dispatched,
        'picked_up': grand_picked,
        'delivered': grand_delivered,
        'avg_fo_to_disp_bd': _round1(_mean(fo_to_disp)),
        'sla_fo_to_disp': SLA_FO_TO_DISPATCH_BD,
        'fo_disp_compliance': (
            round(sum(1 for b in fo_to_disp if b <= SLA_FO_TO_DISPATCH_BD) / len(fo_to_disp), 3)
            if fo_to_disp else None
        ),
        'avg_disp_to_pu_bd': _round1(_mean(disp_to_pu)),
        'sla_disp_to_pu': SLA_DISPATCH_TO_PICKUP_BD,
        'disp_pu_compliance': (
            round(sum(1 for b in disp_to_pu if b <= SLA_DISPATCH_TO_PICKUP_BD) / len(disp_to_pu), 3)
            if disp_to_pu else None
        ),
        'avg_e2e_bd': _round1(_mean(e2e)),
        'prior_month_fo_pickups': None,
        'cumulative_awaiting_pu': None,
        'mtd_pickups': None,
    }

    sections = _build_summary_sections(records, anchor_day, monthly_objective)

    columns = [{'key': k, 'label': COLUMN_LABELS[k]} for k in COLUMN_KEYS]

    result = {
        'title': 'DAILY FREIGHT ORDER ACTIVITY WITH SLA TRACKING AND FLOW-THROUGH (BUSINESS DAYS)',
        'columns': columns,
        'months': months,
        'grand_total': grand_total,
        'sections': sections,
        'anchor_day': anchor_day.isoformat(),
        'monthly_objective': monthly_objective,
        'generated_at': datetime.utcnow().isoformat() + 'Z',
    }

    total_days = sum(len(m['days']) for m in months)
    print(
        f"  Vehicle Distribution: {len(records)} FO records, "
        f"{len(months)} months, {total_days} daily rows "
        f"(anchor={anchor_day.isoformat()}, objective={monthly_objective})"
    )
    return result


# Backwards-compatible alias so admin/orchestrator INGEST_MAP can reference
# either `ingest_vehicle_distribution` or the old `ingest_fo_performance`
# during the cutover.
ingest_fo_performance = ingest_vehicle_distribution
