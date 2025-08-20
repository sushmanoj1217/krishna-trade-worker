# scripts/sheets_admin.py
from __future__ import annotations

import os
import sys
import time
import argparse
from typing import List, Any, Optional, Dict, Tuple
from datetime import datetime, timedelta

# We reuse the project's Sheets wrapper so auth/rate-limits are consistent
try:
    from integrations import sheets as sh
except Exception as e:
    print(f"[ERROR] Cannot import integrations.sheets: {e}", file=sys.stderr)
    sys.exit(1)

# ----------------------------
# Constants / Structure
# ----------------------------

RAW_TABS = [
    "Params_Override",
    "OC_Live",
    "Signals",
    "Trades",
    "Performance",
    "Status",
    "Events",
    "Snapshots",
]

VIEW_TABS_AND_FORMULAS: Dict[str, str] = {
    # Latest on top
    "OC_Live_VIEW": 'QUERY(OC_Live!A:Q, "select * where A is not null order by A desc", 1)',
    "Signals_VIEW": 'QUERY(Signals!A:V, "select * where B is not null order by B desc", 1)',
    "Signals_ELIGIBLE_VIEW": 'QUERY(Signals!A:V, "select * where K = TRUE order by B desc", 1)',
    "Trades_VIEW": 'QUERY(Trades!A:O, "select * where I is not null order by I desc", 1)',  # I=buy_time
    "Trades_OPEN_VIEW": 'QUERY(Trades!A:O, "select * where J is null or L is null order by I desc", 1)',  # open only
    "Status_VIEW": 'QUERY(Status!A:C, "select * where A is not null order by A desc", 1)',
    "Events_VIEW": 'QUERY(Events!A:E, "select * where A is not null order by A desc", 1)',
    "Snapshots_VIEW": 'QUERY(Snapshots!A:C, "select * where A is not null order by A desc", 1)',
}

HEADERS: Dict[str, List[str]] = {
    "Params_Override": ["key", "value", "updated_by", "updated_at"],
    "OC_Live": [
        "timestamp", "spot", "s1", "s2", "r1", "r2", "expiry", "signal",
        "vix", "pcr", "pcr_bucket", "max_pain", "max_pain_dist", "bias_tag", "stale",
        "ce_oi_delta_near", "pe_oi_delta_near",
    ],
    "Signals": [
        "signal_id", "ts", "side", "trigger",
        "c1", "c2", "c3", "c4", "c5", "c6",
        "eligible", "reason",
        "mv_pcr_ok", "mv_mp_ok", "mv_basis",
        "oc_bull_normal", "oc_bull_shortcover", "oc_bear_normal", "oc_bear_crash",
        "oc_pattern_basis", "near/cross", "notes",
    ],
    "Trades": [
        "trade_id", "signal_id", "symbol", "side",
        "buy_ltp", "exit_ltp", "sl", "tp",
        "basis", "buy_time", "exit_time",
        "result", "pnl", "dedupe_hash", "notes",
    ],
    "Performance": ["Metric", "Value", "Notes"],
    "Status": ["ts", "event", "detail"],
    "Events": ["ts", "source", "title", "severity", "hold"],
    "Snapshots": ["ts", "key", "value_json"],
}

# Retention env mapping (days). If None -> no pruning.
RET_ENV_KEYS: Dict[str, Tuple[str, Optional[int]]] = {
    "OC_Live": ("RETAIN_DAYS_OC_LIVE", 2),
    "Signals": ("RETAIN_DAYS_SIGNALS", 14),
    "Status": ("RETAIN_DAYS_STATUS", 7),
    "Events": ("RETAIN_DAYS_EVENTS", 7),
    "Snapshots": ("RETAIN_DAYS_SNAPSHOTS", 3),
    # "Trades": ("RETAIN_DAYS_TRADES", 365),  # Usually keep long; enable only if you want pruning
}

# Timestamp column indexes per tab (0-based)
TS_COL_INDEX: Dict[str, int] = {
    "OC_Live": 0,        # timestamp
    "Signals": 1,        # ts
    "Status": 0,         # ts
    "Events": 0,         # ts
    "Snapshots": 0,      # ts
    # "Trades": 9,       # buy_time (I) -> enable if pruning trades
}

# ----------------------------
# Utilities
# ----------------------------

def _log(msg: str):
    print(msg, flush=True)

def _now_ist_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def _env_int(name: str, default: Optional[int]) -> Optional[int]:
    s = os.getenv(name, "")
    if not s.strip():
        return default
    try:
        return int(s.strip())
    except Exception:
        return default

def _parse_ts(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    # Common formats
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except Exception:
            continue
    # ISO-ish fallback
    try:
        return datetime.fromisoformat(s.replace("Z", ""))
    except Exception:
        return None

def _get_retention_days(tab: str) -> Optional[int]:
    if tab not in RET_ENV_KEYS:
        return None
    env_key, default = RET_ENV_KEYS[tab]
    return _env_int(env_key, default)

def _ensure_header(tab: str, header: List[str]):
    rows = sh.get_all_values(tab)
    if not rows:
        sh.set_rows(tab, [header])
        return
    # If first row empty or mismatched length=0, set header
    if len(rows[0]) == 0:
        sh.set_rows(tab, [header])
        return
    # If header exists, keep it (idempotent)

def _ensure_view_tab(name: str, formula: str):
    sh.ensure_ws(name)
    # Put formula in A1
    sh.set_rows(name, [[f"={formula}"]])

def _setup_performance_template():
    rows = sh.get_all_values("Performance")
    if rows and len(rows) >= 2:
        return  # already has something
    # Build template
    tpl = [
        ["Metric", "Value", "Notes"],
        ["Win Rate", '=IFERROR( COUNTIF(Trades!L2:L,"win") / COUNTIF(Trades!L2:L,"<>"), 0 )', "Closed trades only"],
        ["Avg P/L (closed)", '=IFERROR( AVERAGE(FILTER(Trades!M2:M, Trades!L2:L<>"")), 0 )', ""],
        ["Total P/L (closed)", '=IFERROR( SUM(FILTER(Trades!M2:M, Trades!L2:L<>"")), 0 )', ""],
        ["Version", "", "App version tag"],
        # (Max Drawdown approx skipped to avoid complex array formulas)
    ]
    sh.set_rows("Performance", tpl)

def _timestamp_col_index(tab: str, header: List[str]) -> int:
    # Prefer configured index; else try to detect
    if tab in TS_COL_INDEX:
        return TS_COL_INDEX[tab]
    # Detect by common label names
    keys = [h.strip().lower() for h in header]
    for i, k in enumerate(keys):
        if k in ("timestamp", "ts", "time", "date_time", "buy_time"):
            return i
    return 0  # fallback

def _split_rows_by_cutoff(rows: List[List[Any]], col_idx: int, cutoff: datetime) -> Tuple[List[List[Any]], List[List[Any]]]:
    """
    Returns (older_rows, newer_rows) excluding header.
    """
    older, newer = [], []
    for r in rows[1:]:
        ts = r[col_idx] if col_idx < len(r) else ""
        dt = _parse_ts(str(ts))
        if dt and dt < cutoff:
            older.append(r)
        else:
            newer.append(r)
    return older, newer

def _ensure_archive_ws(arch_id: str, tab: str, header: List[str]):
    gc = sh.get_client()
    if not gc:
        return None
    arch = gc.open_by_key(arch_id)
    try:
        ws = arch.worksheet(tab)
    except Exception:
        ws = arch.add_worksheet(title=tab, rows=200, cols=max(26, len(header)))
        # put header
        ws.update(f"A1:{_col_letters(len(header))}1", [header])
    # ensure header present
    vals = ws.get_all_values()
    if not vals:
        ws.update(f"A1:{_col_letters(len(header))}1", [header])
    return ws

def _col_letters(n: int) -> str:
    n = max(1, n)
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _append_rows_ws(ws, header_len: int, rows: List[List[Any]]):
    """
    Append many rows to gspread worksheet with throttling.
    """
    if not rows:
        return
    # find next row
    vals = ws.get_all_values()
    start_idx = (len(vals) + 1) if vals else 1
    rng = f"A{start_idx}:{_col_letters(max(header_len, max(len(r) for r in rows)))}{start_idx + len(rows) - 1}"
    ws.update(rng, rows)

# ----------------------------
# Commands
# ----------------------------

def cmd_setup(args):
    _log("== SHEETS SETUP ==")
    # Ensure base tabs (project helper will create only missing)
    sh.ensure_tabs()

    # Ensure headers for RAW tabs (idempotent)
    for tab in RAW_TABS:
        sh.ensure_ws(tab)
        hdr = HEADERS.get(tab)
        if hdr:
            _ensure_header(tab, hdr)
            _log(f"  ✓ {tab} header ensured")
        else:
            _log(f"  ✓ {tab} ensured")

    # VIEW tabs + formulas
    for vtab, formula in VIEW_TABS_AND_FORMULAS.items():
        _ensure_view_tab(vtab, formula)
        _log(f"  ✓ {vtab} formula set for latest-first")

    # Performance template
    _setup_performance_template()
    _log("  ✓ Performance template ready")

    _log("✅ Setup complete. Open your sheet and check VIEW tabs for latest-first display.")

def cmd_archive(args):
    _log("== SHEETS ARCHIVE ==")
    dry = args.dry_run
    archive_enabled = _env_bool("SHEETS_ARCHIVE_ENABLED", False)
    arch_id = os.getenv("ARCHIVE_SPREADSHEET_ID", "").strip()

    if archive_enabled and not arch_id:
        _log("[WARN] SHEETS_ARCHIVE_ENABLED=true but ARCHIVE_SPREADSHEET_ID not set → will skip archive and delete instead.")

    tabs_done = 0
    total_moved = 0
    total_deleted = 0

    for tab, (env_key, default_days) in RET_ENV_KEYS.items():
        days = _get_retention_days(tab)
        if days is None:
            continue
        rows = sh.get_all_values(tab)
        if not rows:
            continue
        header = rows[0]
        col_idx = _timestamp_col_index(tab, header)
        cutoff = datetime.utcnow() - timedelta(days=days)

        older, newer = _split_rows_by_cutoff(rows, col_idx, cutoff)
        if not older:
            _log(f"  {tab}: nothing to prune (keep ≥ {days}d).")
            continue

        moved = 0
        deleted = 0

        if dry:
            _log(f"  {tab}: would move/delete {len(older)} rows older than {cutoff.date()} (keep ≥ {days}d)")
        else:
            if archive_enabled and arch_id:
                try:
                    ws_arch = _ensure_archive_ws(arch_id, tab, header)
                    if ws_arch:
                        _append_rows_ws(ws_arch, len(header), older)
                        moved = len(older)
                except Exception as e:
                    _log(f"  [WARN] archive move failed for {tab}: {e}")

            # Write back only header + newer rows to primary
            try:
                sh.set_rows(tab, [header] + newer)
                deleted = len(older)
            except Exception as e:
                _log(f"  [WARN] prune failed for {tab}: {e}")

            _log(f"  {tab}: moved {moved}, deleted {deleted}")

        tabs_done += 1
        total_moved += moved
        total_deleted += deleted

    _log(f"✅ Archive done. Tabs: {tabs_done}, moved: {total_moved}, deleted: {total_deleted}")

# ----------------------------
# Main
# ----------------------------

def main():
    parser = argparse.ArgumentParser(description="All-in-one Sheets setup + archive tool")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("setup", help="Create tabs, headers, and VIEW formulas")
    p1.set_defaults(func=cmd_setup)

    p2 = sub.add_parser("archive", help="Prune (and optionally archive) old rows as per env retention")
    p2.add_argument("--dry-run", action="store_true", help="Show what would change, but do not write")
    p2.set_defaults(func=cmd_archive)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
