"""Compute allocation data structures from Hub-cached parquet files.

This replicates the logic in INEOS_Allocation_App/allocation_app.py but reads
from the individual uploaded source files (sap_export, sap_handover, etc.)
rather than the encrypted Master File. The output is a JSON-friendly dict with
the same 7 data structures the allocation_template.html expects:

  V_DATA         — compact indexed vehicle records
  V_DICT         — lookup dictionaries for decoding V_DATA indices
  DEALERS        — per-dealer metrics (OG, cumulative sales, YTD handovers)
  PLANT_AFFINITY — plant → dealer shipping history
  PIPELINE_COMP  — per-dealer non-sold vehicle mix
  SELL_THROUGH   — per-dealer sell-through rates per body|trim
  DAYS_TO_SELL   — per-dealer avg days-to-sell per body|trim + network avg
"""

from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime
from typing import Any

import pandas as pd


def _s(row: Any, key: str) -> str:
    """Safely extract a string value from a pandas Series row."""
    val = row.get(key)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ''
    return str(val).strip()


def _clean(name: str) -> str:
    n = str(name).strip().upper()
    for suffix in [" INEOS GRENADIER", " INEOS", " GRENADIER"]:
        n = n.replace(suffix, "")
    return n.strip()


def _parse_my(material: str) -> str:
    if "27" in material:
        return "MY27"
    if "26" in material:
        return "MY26"
    if "25" in material:
        return "MY25"
    if "24" in material:
        return "MY24"
    return ""


def _parse_body(material: str) -> str:
    return "QM" if "quartermaster" in material.lower() else "SW"


_STATUS_MAP = {
    "8. sold": "Sold",
    "7. dealer stock": "Dealer Stock",
    "6. in-transit": "In-Transit to Dealer",
    "5. arrived": "At Americas Port",
    "4. departed": "On Water",
    "3. built": "Built at Plant",
    "2. in production": "In Production",
    "1. preplan": "Preplanning",
    "planned": "Planned for Transfer",
    "vehicle written": "Written Off",
}

US_MARKETS = {"Central", "Northeast", "Southeast", "Western"}
RETAIL_CHANNELS = {"STOCK", "PRIVATE - RETAILER"}
BREAKPOINTS = [30, 60, 90, 120, 150, 180, 270, 365]


def _classify_status(raw: str) -> str:
    low = raw.lower()
    for key, label in _STATUS_MAP.items():
        if key in low:
            return label
    return "Awaiting Status"


# Extra dealer → market mappings not in the RBM sheet
_MKT_EXTRAS = {
    "MOSSY SD": "Western", "MOSSY TX": "Central",
    "RTGT": "Western", "CROWN DUBLIN": "Northeast",
    "SEWELL SAN ANTONIO": "Central", "ORLANDO": "Southeast",
    "ROSEVILLE": "Western", "MOSSY SAN DIEGO": "Western",
}


def _build_mkt_map(sap: pd.DataFrame) -> dict[str, str]:
    """Build dealer → market mapping from the SAP data.

    The SAP export's market_area is usually "AMERICAS" (unhelpful), but
    the Hub's sheet_builders already handle this via template extraction.
    We replicate a simplified version here using the data we have.
    """
    mkt_map: dict[str, str] = {}

    # Try to pull from the parquet if a market column exists
    for col in ('market', 'market_area', 'region_group'):
        if col in sap.columns:
            for _, r in sap.iterrows():
                name = _clean(str(r.get('customer_name', '')))
                mkt = str(r.get(col, '')).strip()
                if name and mkt and mkt.upper() != 'AMERICAS' and name not in mkt_map:
                    mkt_map[name] = mkt

    mkt_map.update(_MKT_EXTRAS)

    # Also try to load from the template if available
    try:
        import json, re
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        for path in [
            os.path.join(base, 'templates', 'dashboard_template.html'),
            os.path.join(base, 'outputs', 'Americas_Daily_Dashboard.html'),
        ]:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    html = f.read()
                for pattern in [r'const\s+DPD\s*=\s*(\[.*?\]);', r'const\s+INV\s*=\s*(\[.*?\]);']:
                    m = re.search(pattern, html, re.DOTALL)
                    if m:
                        data = json.loads(m.group(1))
                        for row in data:
                            d = str(row.get('d', row.get('n', ''))).strip().upper()
                            mk = str(row.get('m', '')).strip()
                            if d and mk and mk != 'TOTAL':
                                mkt_map[d] = mk
                break
    except Exception:
        pass

    return mkt_map


def _lookup_mkt(mkt_map: dict[str, str], name: str) -> str:
    n = name.strip()
    if n in mkt_map:
        return mkt_map[n]
    u = n.upper()
    if u in mkt_map:
        return mkt_map[u]
    for k, v in mkt_map.items():
        if u in k.upper() or k.upper() in u:
            return v
    return ""


def compute_allocation_data(cache_dir: str) -> dict[str, Any]:
    """Compute all 7 allocation data structures from cached parquets.

    Parameters
    ----------
    cache_dir : str
        Path to the Hub's cache directory (contains data/*.parquet files).

    Returns
    -------
    dict with keys: V_DATA, V_DICT, DEALERS, PLANT_AFFINITY, PIPELINE_COMP,
    SELL_THROUGH, DAYS_TO_SELL, DATA_TS
    """
    data_dir = os.path.join(cache_dir, 'data')

    def load(name: str) -> pd.DataFrame | None:
        for suffix in ('.parquet',):
            path = os.path.join(data_dir, f'{name}{suffix}')
            if os.path.exists(path):
                return pd.read_parquet(path)
        return None

    sap = load('sap_export')
    if sap is None:
        raise RuntimeError("SAP Vehicle Export not uploaded yet.")

    handover = load('sap_handover')
    if handover is None:
        handover = load('handover')
    sales_order = load('sales_order')

    mkt_map = _build_mkt_map(sap)

    # Merge handover dates onto SAP by VIN
    if handover is not None and 'vin' in handover.columns:
        ho_map = {}
        for _, r in handover.iterrows():
            vin = str(r.get('vin', '')).strip().upper()
            if len(vin) > 3:
                ho_map[vin] = r
    else:
        ho_map = {}

    today = datetime.now()

    # Build a unified row list (like allocation_app's export_rows but as dicts)
    rows = []
    for _, r in sap.iterrows():
        vin = str(r.get('vin', '')).strip().upper()
        if not vin:
            continue
        country = str(r.get('country', '')).strip()
        if not any(x in country.upper() for x in ['UNITED STATES', 'CANADA', 'MEXICO']):
            continue

        material = str(r.get('material', '')).strip()
        customer = str(r.get('customer_name', '')).strip()
        channel = str(r.get('channel', '')).strip().upper()
        status = str(r.get('status', '')).strip()
        dealer = _clean(customer)
        market = _lookup_mkt(mkt_map, dealer)

        # Handover date
        ho = ho_map.get(vin)
        ho_date = None
        if ho is not None:
            hd = ho.get('handover_date', None) if isinstance(ho, dict) else getattr(ho, 'handover_date', None)
            if pd.notna(hd):
                try:
                    ho_date = pd.to_datetime(hd)
                except Exception:
                    pass

        # DIS (days in stock)
        dis = 0
        try:
            dis_val = r.get('days_in_stock', r.get('dis', 0))
            if pd.notna(dis_val):
                dis = int(float(dis_val))
        except Exception:
            pass

        rows.append({
            'vin': vin,
            'material': material,
            'customer': customer,
            'dealer': dealer,
            'country': country,
            'channel': channel,
            'status': status,
            'market': market,
            'body': _parse_body(material),
            'my': _parse_my(material),
            'status_label': _classify_status(status),
            'trim': _s(r, 'trim'),
            'pack': _s(r, 'rough_pack'),
            'color': _s(r, 'ext_color'),
            'seats': _s(r, 'seats'),
            'roof': _s(r, 'roof_color'),
            'safari': _s(r, 'safari_windows'),
            'wheels': _s(r, 'wheels'),
            'tyres': _s(r, 'tyres'),
            'frame': _s(r, 'frame_color'),
            'tow': _s(r, 'tow_ball'),
            'heated_seats': _s(r, 'seat_heating'),
            'diff_locks': _s(r, 'diff_locks_rf'),
            'ladder': _s(r, 'access_ladder'),
            'plant': _s(r, 'plant_code'),
            'msrp': int(float(r.get('msrp', 0) or 0)),
            'so_no': _s(r, 'order_no'),
            'ho_date': ho_date,
            'dis': dis,
            'eta': _s(r, 'eta'),
            'vessel': _s(r, 'vessel'),
            # Extra option fields — mapped from SAP parquet column names
            'sound': _s(r, 'speaker_system'),
            'privacy_glass': _s(r, 'privacy_glass'),
            'air_intake': _s(r, 'raised_air_intake'),
            'winch': _s(r, 'front_winch'),
            'aux_battery': _s(r, 'aux_battery'),
            'aux_switch': _s(r, 'aux_switchbar'),
            'carpet': _s(r, 'carpet_mats'),
            'compass': _s(r, 'compass'),
            'centre_diff': _s(r, 'diff_lock_central'),
            'emerg_safety': _s(r, 'safety_package'),
            'floor_trim': _s(r, 'floor_trim'),
            'utility_rails': _s(r, 'utility_rails'),
            'smokers': _s(r, 'smokers_pack'),
            'spare_wheel': _s(r, 'spare_wheel_container'),
            'front_tow': _s(r, 'tow_plate_front'),
            'bump_strips': _s(r, 'rubber_bump_strips'),
            'steering': _s(r, 'steering_wheel'),
            'wheel_locks': _s(r, 'wheel_locks'),
            'stock_cat': _s(r, 'stock_category'),
        })

    print(f"  [allocation] {len(rows)} vehicles from SAP parquet (North America)")

    # --- V_DATA / V_DICT (compact indexed format) ---
    SPEC_KEYS = [
        "trim", "pack", "color", "seats", "roof", "safari", "wheels", "tyres",
        "frame", "tow", "heated_seats", "diff_locks", "ladder", "aux_battery",
        "aux_switch", "carpet", "compass", "centre_diff", "emerg_safety",
        "floor_trim", "winch", "utility_rails", "privacy_glass", "air_intake",
        "smokers", "spare_wheel", "front_tow", "bump_strips", "steering",
        "wheel_locks", "sound",
    ]
    INDEXED_KEYS = ["my", "body", "status", "channel", "dealer", "stock_cat", "plant", "material"] + SPEC_KEYS

    unique: dict[str, set] = {k: set() for k in INDEXED_KEYS}
    for row in rows:
        unique["my"].add(row["my"])
        unique["body"].add(row["body"])
        unique["status"].add(row["status_label"])
        unique["channel"].add(row["channel"])
        unique["dealer"].add(row["dealer"])
        unique["stock_cat"].add(row["stock_cat"])
        unique["plant"].add(row["plant"])
        unique["material"].add(row["material"])
        for k in SPEC_KEYS:
            unique[k].add(row.get(k, ""))

    v_dict: dict[str, list] = {}
    idx_maps: dict[str, dict[str, int]] = {}
    for k in INDEXED_KEYS:
        vals = sorted(unique[k] - {""}) + [""]
        v_dict[k] = vals
        idx_maps[k] = {v: i for i, v in enumerate(vals)}

    v_data = []
    for row in rows:
        rec = [
            row["vin"],
            row["so_no"],
            row["msrp"],
            0,  # so_value placeholder
            row["dis"],
            row["eta"],
            row["vessel"],
            idx_maps["my"].get(row["my"], len(v_dict["my"]) - 1),
            idx_maps["body"].get(row["body"], len(v_dict["body"]) - 1),
            idx_maps["status"].get(row["status_label"], len(v_dict["status"]) - 1),
            idx_maps["channel"].get(row["channel"], len(v_dict["channel"]) - 1),
            idx_maps["dealer"].get(row["dealer"], len(v_dict["dealer"]) - 1),
            idx_maps["stock_cat"].get(row["stock_cat"], len(v_dict["stock_cat"]) - 1),
            idx_maps["plant"].get(row["plant"], len(v_dict["plant"]) - 1),
            idx_maps["material"].get(row["material"], len(v_dict["material"]) - 1),
        ]
        for k in SPEC_KEYS:
            rec.append(idx_maps[k].get(row.get(k, ""), len(v_dict[k]) - 1))
        v_data.append(rec)

    print(f"  [allocation] V_DATA: {len(v_data)} records, V_DICT: {len(v_dict)} keys")

    # --- DEALERS ---
    us_rows = [r for r in rows if "UNITED STATES" in r["country"].upper() and r["channel"] in RETAIL_CHANNELS]
    print(f"  [allocation] US retail rows: {len(us_rows)} (of {len(rows)} total)")

    dealer_og: dict[str, int] = defaultdict(int)
    dealer_cum: dict[str, dict[int, int]] = defaultdict(lambda: {bp: 0 for bp in BREAKPOINTS})
    dealer_ytd: dict[str, int] = defaultdict(int)

    for r in us_rows:
        dealer = r["dealer"]
        if "IN_US_STK" in dealer or "INEOS US STOCK" in dealer or "INEOS AUTOMOTIVE" in dealer:
            continue

        status = r["status"].lower()
        if "dealer stock" in status or "7." in status:
            dealer_og[dealer] += 1

        ho = r["ho_date"]
        if ho is not None:
            try:
                days_ago = (today - ho).days
                for bp in BREAKPOINTS:
                    if days_ago <= bp:
                        dealer_cum[dealer][bp] += 1
                if ho.year == today.year:
                    dealer_ytd[dealer] += 1
            except Exception:
                pass

    all_dealers = set(dealer_og.keys()) | set(dealer_cum.keys())
    dealers = []
    for d in sorted(all_dealers):
        mkt = _lookup_mkt(mkt_map, d)
        # Include all dealers with a US market OR unknown market (don't drop
        # dealers just because the market map is incomplete on Render)
        if mkt and mkt not in US_MARKETS:
            continue  # skip explicitly non-US (e.g. Canada, Mexico)
        dealers.append({
            "name": d,
            "market": mkt or "Other",
            "og": dealer_og.get(d, 0),
            "cum": dealer_cum.get(d, {bp: 0 for bp in BREAKPOINTS}),
            "ytd_ho": dealer_ytd.get(d, 0),
        })
    print(f"  [allocation] DEALERS: {len(dealers)}")

    # --- PLANT_AFFINITY ---
    plant_dealer: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in us_rows:
        plant = r["plant"]
        dealer = r["dealer"]
        if not plant or "IN_US_STK" in dealer or "INEOS US STOCK" in dealer:
            continue
        mkt = _lookup_mkt(mkt_map, dealer)
        if mkt and mkt not in US_MARKETS:
            continue
        plant_dealer[plant][dealer] += 1

    plant_affinity = {}
    for plant, dlrs in plant_dealer.items():
        total = sum(dlrs.values())
        plant_affinity[plant] = {d: round(c / total, 4) for d, c in dlrs.items()}
    print(f"  [allocation] PLANT_AFFINITY: {len(plant_affinity)} plants")

    # --- PIPELINE_COMP ---
    pipeline: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in us_rows:
        if "8. sold" in r["status"].lower():
            continue
        dealer = r["dealer"]
        if "IN_US_STK" in dealer or "INEOS US STOCK" in dealer:
            continue
        mkt = _lookup_mkt(mkt_map, dealer)
        if mkt and mkt not in US_MARKETS:
            continue
        bt = f'{r["body"]}|{r["trim"]}'
        btc = f'{bt}|{r["color"]}'
        pipeline[dealer][bt] += 1
        pipeline[dealer][btc] += 1
    pipeline_comp = {d: dict(c) for d, c in pipeline.items()}
    print(f"  [allocation] PIPELINE_COMP: {len(pipeline_comp)} dealers")

    # --- SELL_THROUGH ---
    delivered: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    sold: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in us_rows:
        dealer = r["dealer"]
        if "IN_US_STK" in dealer or "INEOS US STOCK" in dealer:
            continue
        mkt = _lookup_mkt(mkt_map, dealer)
        if mkt and mkt not in US_MARKETS:
            continue
        status = r["status"].lower()
        cfg = f'{r["body"]}|{r["trim"]}'
        if "7. dealer stock" in status or "8. sold" in status:
            delivered[dealer][cfg] += 1
        if "8. sold" in status:
            sold[dealer][cfg] += 1

    net_del: dict[str, int] = defaultdict(int)
    net_sold: dict[str, int] = defaultdict(int)
    for d in delivered:
        for cfg, cnt in delivered[d].items():
            net_del[cfg] += cnt
            net_sold[cfg] += sold[d].get(cfg, 0)

    sell_through: dict[str, dict] = {}
    for dealer in delivered:
        sell_through[dealer] = {}
        for cfg in delivered[dealer]:
            d_cnt = delivered[dealer][cfg]
            s_cnt = sold[dealer].get(cfg, 0)
            dr = s_cnt / d_cnt if d_cnt > 0 else 0
            nr = net_sold[cfg] / net_del[cfg] if net_del[cfg] > 0 else 0
            sell_through[dealer][cfg] = {"d": d_cnt, "s": s_cnt, "r": round(dr, 3), "nr": round(nr, 3)}
    print(f"  [allocation] SELL_THROUGH: {len(sell_through)} dealers")

    # --- DAYS_TO_SELL ---
    dts_dealer: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    dts_network: dict[str, list] = defaultdict(list)
    for r in us_rows:
        if "8. sold" not in r["status"].lower():
            continue
        dis = r["dis"]
        if dis <= 0 or dis > 999:
            continue
        dealer = r["dealer"]
        if "IN_US_STK" in dealer or "INEOS US STOCK" in dealer:
            continue
        mkt = _lookup_mkt(mkt_map, dealer)
        if mkt and mkt not in US_MARKETS:
            continue
        cfg = f'{r["body"]}|{r["trim"]}'
        dts_dealer[dealer][cfg].append(dis)
        dts_network[cfg].append(dis)

    days_to_sell: dict[str, dict] = {}
    for dealer, configs in dts_dealer.items():
        days_to_sell[dealer] = {}
        for cfg, vals in configs.items():
            days_to_sell[dealer][cfg] = {"a": round(sum(vals) / len(vals), 1), "c": len(vals)}
    days_to_sell["_network"] = {}
    for cfg, vals in dts_network.items():
        days_to_sell["_network"][cfg] = {"a": round(sum(vals) / len(vals), 1), "c": len(vals)}
    print(f"  [allocation] DAYS_TO_SELL: {len(days_to_sell) - 1} dealers + network")

    return {
        "V_DATA": v_data,
        "V_DICT": v_dict,
        "DEALERS": dealers,
        "PLANT_AFFINITY": plant_affinity,
        "PIPELINE_COMP": pipeline_comp,
        "SELL_THROUGH": sell_through,
        "DAYS_TO_SELL": days_to_sell,
        "DATA_TS": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
