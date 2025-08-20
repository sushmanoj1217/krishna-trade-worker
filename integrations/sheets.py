# integrations/sheets.py
from __future__ import annotations

import os
import time
import json
from typing import List, Any, Optional

import gspread
from gspread.exceptions import APIError

# ---------- Config ----------
SPREADSHEET_ID = os.getenv("GSHEET_TRADES_SPREADSHEET_ID", "").strip()
DEFAULT_WS = os.getenv("GSHEET_TRADES_WORKSHEET", "Trades")

# Throttling / retries
MIN_INTERVAL_MS = int(os.getenv("SHEETS_MIN_INTERVAL_MS", "300"))  # per-call gap
MAX_RETRIES = int(os.getenv("SHEETS_MAX_RETRIES", "5"))
BACKOFF_BASE = float(os.getenv("SHEETS_BACKOFF_BASE", "0.6"))

# Expected tabs we manage
EXPECTED_TABS = [
    "OC_Live",
    "Signals",
    "Trades",
    "Performance",
    "Events",
    "Status",
    "Snapshots",
    "Params_Override",
]

# ---------- Globals ----------
_last_call_ts = 0.0
_gc: Optional[gspread.Client] = None
_sh: Optional[gspread.Spreadsheet] = None


# ---------- Small utils ----------
def _now_ms() -> int:
    return int(time.time() * 1000)


def _sleep_until_gap():
    """Simple rate limiter so we don't spam Sheets and hit 429."""
    global _last_call_ts
    gap = MIN_INTERVAL_MS / 1000.0
    now = time.time()
    if now - _last_call_ts < gap:
        time.sleep(gap - (now - _last_call_ts))
    _last_call_ts = time.time()


def _retryable(fn, *args, **kwargs):
    """429-safe wrapper with exponential backoff."""
    n = 0
    while True:
        _sleep_until_gap()
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            code = None
            try:
                code = int(getattr(e, "response", None).status_code)  # type: ignore
            except Exception:
                pass
            text = str(e)
            if code in (429, 500, 503) or "Quota exceeded" in text:
                if n >= MAX_RETRIES:
                    raise
                back = (BACKOFF_BASE ** n) + 0.5
                time.sleep(back)
                n += 1
                continue
            raise


# ---------- Auth / Handles ----------
def _sa_dict() -> dict:
    raw = os.getenv("GOOGLE_SA_JSON", "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        # Sometimes users paste with outer quotes; try to strip
        if raw[0] in "\"'" and raw[-1] == raw[0]:
            return json.loads(raw[1:-1])
        raise


def get_client() -> Optional[gspread.Client]:
    global _gc
    if _gc is not None:
        return _gc
    sa = _sa_dict()
    if not sa:
        return None
    _gc = gspread.service_account_from_dict(sa)
    return _gc


def get_sh() -> Optional[gspread.Spreadsheet]:
    global _sh
    if _sh is not None:
        return _sh
    gc = get_client()
    if not gc or not SPREADSHEET_ID:
        return None
    _sh = _retryable(gc.open_by_key, SPREADSHEET_ID)
    return _sh


# ---------- Public helpers ----------
def now_str(tz="Asia/Kolkata") -> str:
    try:
        import datetime as _dt
        from zoneinfo import ZoneInfo  # py3.9+
        return _dt.datetime.now(ZoneInfo(tz)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        # Fallback UTC
        import datetime as _dt
        return _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def ensure_tabs():
    """
    Create only missing tabs (idempotent). Silently no-op if Sheets not configured.
    """
    sh = get_sh()
    if not sh:
        # Let caller log a friendly warning; many services may not need Sheets.
        return

    existing = {ws.title for ws in _retryable(sh.worksheets)}
    missing = [t for t in EXPECTED_TABS if t not in existing]
    for name in missing:
        try:
            _retryable(sh.add_worksheet, title=name, rows=200, cols=26)
        except APIError as e:
            # If someone created it in parallel; ignore “already exists”.
            if "already exists" not in str(e):
                raise


def ensure_ws(name: str):
    sh = get_sh()
    if not sh:
        return None
    try:
        return _retryable(sh.worksheet, name)
    except Exception:
        _retryable(sh.add_worksheet, title=name, rows=200, cols=26)
        return _retryable(sh.worksheet, name)


def get_all_values(tab: str) -> List[List[Any]]:
    ws = ensure_ws(tab)
    if not ws:
        return []
    try:
        return _retryable(ws.get_all_values)
    except APIError:
        return []


def append_row(tab: str, row: List[Any]):
    ws = ensure_ws(tab)
    if not ws:
        return
    _retryable(ws.append_row, row)


def set_rows(tab: str, rows: List[List[Any]]):
    """
    Replace entire sheet values with provided rows (limited size); safe wrapper.
    """
    ws = ensure_ws(tab)
    if not ws:
        return
    rng = f"A1:{_col_letters(len(rows[0]) if rows else 1)}{max(1, len(rows))}"
    _retryable(ws.update, rng, rows)


def update_row(tab: str, idx1: int, values: List[Any]):
    """
    Update a 1-indexed row.
    """
    ws = ensure_ws(tab)
    if not ws:
        return
    rng = f"A{idx1}:{_col_letters(len(values))}{idx1}"
    _retryable(ws.update, rng, [values])


def _col_letters(n: int) -> str:
    n = max(1, n)
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# ---------- Convenience for our bot ----------
def write_oc_live_row(data: List[Any]):
    """
    Append one row to OC_Live (our standard layout).
    """
    append_row("OC_Live", data)


def write_signal_row(data: List[Any]):
    append_row("Signals", data)


def write_trade_row(data: List[Any]):
    append_row("Trades", data)


def write_status(event: str, detail: str = ""):
    append_row("Status", [now_str(), event, detail])


def get_last_event_rows(n: int = 5):
    rows = get_all_values("Events")
    return rows[-n:] if rows else []


# Backward-compatible aliases (some legacy callers expect these names)
def get_sheet_values(tab: str) -> List[List[Any]]:
    return get_all_values(tab)


def append_status(event: str, detail: str = ""):
    write_status(event, detail)
# -------------------- EXTRA HELPERS (compat with callers) --------------------

def _rows_as_dicts(tab: str):
    """Return rows of a tab as list of dicts using header row."""
    rows = get_all_values(tab)
    if not rows:
        return []
    header = [h.strip().lower() for h in rows[0]]
    out = []
    for r in rows[1:]:
        d = {}
        for i, v in enumerate(r):
            key = header[i] if i < len(header) else f"col{i+1}"
            d[key] = v
        out.append(d)
    return out

def _date_yyyy_mm_dd(s: str) -> str:
    s = (s or "").strip()
    # Accept "YYYY-MM-DD HH:MM:SS" or already "YYYY-MM-DD"
    return s[:10] if len(s) >= 10 else s

def get_open_trades():
    """
    Return a list[dict] of open trades from Trades tab.
    Open = exit_time empty (or result empty).
    Keys use lowercased header names: trade_id, signal_id, symbol, side, buy_ltp,
    exit_ltp, sl, tp, basis, buy_time, exit_time, result, pnl, dedupe_hash ...
    """
    rows = _rows_as_dicts("Trades")
    out = []
    for d in rows:
        exit_time = d.get("exit_time", "").strip()
        result = d.get("result", "").strip()
        if exit_time == "" or result == "":
            out.append(d)
    return out

def get_today_signal_dedupes(tz="Asia/Kolkata"):
    """
    Return a set of dedupe_hash values for today's Signals (fallback to Trades).
    """
    try:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo(tz)).strftime("%Y-%m-%d")
    except Exception:
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Prefer Signals tab if it has 'dedupe_hash'
    sigs = _rows_as_dicts("Signals")
    has_dedupe = any("dedupe_hash" in d for d in sigs)
    dest = set()
    if has_dedupe:
        for d in sigs:
            ts = _date_yyyy_mm_dd(d.get("ts", ""))
            if ts == today:
                dh = d.get("dedupe_hash", "").strip()
                if dh:
                    dest.add(dh)
        if dest:
            return dest

    # Fallback: use Trades tab (may also contain dedupe_hash)
    trades = _rows_as_dicts("Trades")
    for d in trades:
        ts = _date_yyyy_mm_dd(d.get("buy_time", "")) or _date_yyyy_mm_dd(d.get("ts", ""))
        if ts == today:
            dh = d.get("dedupe_hash", "").strip()
            if dh:
                dest.add(dh)
    return dest

# Back-compat names some callers might expect
def get_today_dedupe_hashes():
    return get_today_signal_dedupes()

