# integrations/sheets.py
from __future__ import annotations

import os
import time
import json
from typing import List, Any, Optional, Dict

import gspread
from gspread.exceptions import APIError

# =========================
# Config (via env)
# =========================
SPREADSHEET_ID = os.getenv("GSHEET_TRADES_SPREADSHEET_ID", "").strip()
DEFAULT_WS = os.getenv("GSHEET_TRADES_WORKSHEET", "Trades")

# Throttling / retries
MIN_INTERVAL_MS = int(os.getenv("SHEETS_MIN_INTERVAL_MS", "300"))   # per-call gap
MAX_RETRIES     = int(os.getenv("SHEETS_MAX_RETRIES", "5"))
BACKOFF_BASE    = float(os.getenv("SHEETS_BACKOFF_BASE", "0.6"))

# Our expected tabs
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

# =========================
# Globals
# =========================
_last_call_ts: float = 0.0
_gc: Optional[gspread.Client] = None
_sh: Optional[gspread.Spreadsheet] = None
_sheet_full: bool = False  # trip when 10M cells cap is hit

# =========================
# Internal helpers
# =========================
def _sleep_until_gap():
    """Simple rate limiter so we don't spam Sheets and hit 429."""
    global _last_call_ts
    gap = max(0.0, MIN_INTERVAL_MS / 1000.0)
    now = time.time()
    if now - _last_call_ts < gap:
        time.sleep(gap - (now - _last_call_ts))
    _last_call_ts = time.time()


def _retryable(fn, *args, **kwargs):
    """429/5xx-safe wrapper with exponential backoff; marks _sheet_full if 10M cap hit."""
    global _sheet_full
    n = 0
    while True:
        _sleep_until_gap()
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            text = str(e)
            code = None
            try:
                code = int(getattr(e, "response", None).status_code)  # type: ignore[attr-defined]
            except Exception:
                pass

            if "increase the number of cells" in text:
                _sheet_full = True
                raise

            if code in (429, 500, 503) or "Quota exceeded" in text:
                if n >= MAX_RETRIES:
                    raise
                back = (BACKOFF_BASE ** n) + 0.5
                time.sleep(back)
                n += 1
                continue
            raise


def _sa_dict() -> dict:
    """
    GOOGLE_SA_JSON should be one-line JSON. If pasted with outer quotes, strip them.
    """
    raw = os.getenv("GOOGLE_SA_JSON", "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        if raw and raw[0] in "\"'" and raw[-1] == raw[0]:
            return json.loads(raw[1:-1])
        raise


# =========================
# Public: client / spreadsheet
# =========================
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


def now_str(tz="Asia/Kolkata") -> str:
    try:
        import datetime as _dt
        from zoneinfo import ZoneInfo
        return _dt.datetime.now(ZoneInfo(tz)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        import datetime as _dt
        return _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


# =========================
# Tabs & worksheet handles
# =========================
def ensure_tabs():
    """
    Create only missing tabs (idempotent). Silently no-op if Sheets not configured.
    """
    sh = get_sh()
    if not sh:
        return
    try:
        existing = {ws.title for ws in _retryable(sh.worksheets)}
    except APIError:
        return

    for name in EXPECTED_TABS:
        if name in existing:
            continue
        try:
            _retryable(sh.add_worksheet, title=name, rows=200, cols=26)
        except APIError as e:
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


# =========================
# CRUD helpers (throttled)
# =========================
def get_all_values(tab: str) -> List[List[Any]]:
    ws = ensure_ws(tab)
    if not ws:
        return []
    try:
        return _retryable(ws.get_all_values)
    except APIError:
        return []


def append_row(tab: str, row: List[Any]):
    """
    Generic appender. **Smart Signals support**:
    - If tab == "Signals", we **tap memory first** so /oc_now can render latest
      even when the workbook hits 10M cells or we hit 429s.
    """
    global _sheet_full
    if str(tab).strip().lower() == "signals":
        try:
            tap_signal_row(row)
        except Exception:
            pass

    if _sheet_full:
        return
    ws = ensure_ws(tab)
    if not ws:
        return
    try:
        _retryable(ws.append_row, row)
    except APIError as e:
        if "increase the number of cells" in str(e):
            _sheet_full = True
        # swallow; trading loop shouldn't crash


def set_rows(tab: str, rows: List[List[Any]]):
    global _sheet_full
    if _sheet_full:
        return
    ws = ensure_ws(tab)
    if not ws or not rows:
        return
    rng = f"A1:{_col_letters(len(rows[0]))}{max(1, len(rows))}"
    try:
        _retryable(ws.update, rng, rows)
    except APIError as e:
        if "increase the number of cells" in str(e):
            _sheet_full = True


def update_row(tab: str, idx1: int, values: List[Any]):
    global _sheet_full
    if _sheet_full:
        return
    ws = ensure_ws(tab)
    if not ws:
        return
    rng = f"A{idx1}:{_col_letters(len(values))}{idx1}"
    try:
        _retryable(ws.update, rng, [values])
    except APIError as e:
        if "increase the number of cells" in str(e):
            _sheet_full = True


def _col_letters(n: int) -> str:
    n = max(1, n)
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# =========================
# Convenience writers
# =========================
def write_oc_live_row(data: List[Any]):
    append_row("OC_Live", data)


def write_trade_row(data: List[Any]):
    append_row("Trades", data)


def write_status(event: str, detail: str = ""):
    append_row("Status", [now_str(), event, detail])


# Backward-compatible names (legacy callers)
def get_sheet_values(tab: str) -> List[List[Any]]:
    return get_all_values(tab)


def append_status(event: str, detail: str = ""):
    write_status(event, detail)


def get_last_event_rows(n: int = 5):
    rows = get_all_values("Events")
    return rows[-n:] if rows else []


# =========================
# Extra readers used by agents
# =========================
def _rows_as_dicts(tab: str):
    """Return rows of a tab as list of dicts using header row (lowercased keys)."""
    rows = get_all_values(tab)
    if not rows:
        return []
    header = [str(h).strip().lower() for h in rows[0]]
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
    return s[:10] if len(s) >= 10 else s


def _coerce_float(x):
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None


def _pick(d: dict, *candidates, default=""):
    for k in candidates:
        if k in d and str(d[k]).strip() != "":
            return d[k]
    return default


def get_open_trades():
    """
    Normalized open trades (exit not filled). Keys guaranteed:
    trade_id, signal_id, symbol, side, buy_ltp, exit_ltp, sl, tp,
    basis, buy_time, exit_time, result, pnl, dedupe_hash.
    Numeric fields coerced to float when possible.
    """
    raw = _rows_as_dicts("Trades")
    out = []
    for d in raw:
        buy_time  = _pick(d, "buy_time", "buy_ts", "entry_time", "ts")
        exit_time = _pick(d, "exit_time", "sell_time", "close_time")
        result    = _pick(d, "result")
        open_pos  = (str(exit_time).strip() == "") or (str(result).strip() == "")
        if not open_pos:
            continue

        side = str(_pick(d, "side")).strip().upper()
        if side in ("CALL", "C"): side = "CE"
        if side in ("PUT", "P"):  side = "PE"

        nd = {
            "trade_id":    _pick(d, "trade_id", "id"),
            "signal_id":   _pick(d, "signal_id"),
            "symbol":      _pick(d, "symbol", "ticker"),
            "side":        side,
            "buy_ltp":     _coerce_float(_pick(d, "buy_ltp", "buy_price", "entry_ltp", "entry_price", "buy")),
            "exit_ltp":    _coerce_float(_pick(d, "exit_ltp", "sell_ltp", "exit_price", "sell")),
            "sl":          _coerce_float(_pick(d, "sl", "stop", "stop_loss")),
            "tp":          _coerce_float(_pick(d, "tp", "target")),
            "basis":       _pick(d, "basis", "reason"),
            "buy_time":    str(buy_time),
            "exit_time":   str(exit_time),
            "result":      _pick(d, "result"),
            "pnl":         _coerce_float(_pick(d, "pnl", "p&l")),
            "dedupe_hash": _pick(d, "dedupe_hash", "hash"),
        }
        for k in ("buy_ltp", "exit_ltp", "sl", "tp", "pnl"):
            if nd[k] is None:
                nd[k] = 0.0
        out.append(nd)
    return out


def get_today_signal_dedupes(tz="Asia/Kolkata"):
    """Set of dedupe_hash for today's Signals; falls back to Trades if needed."""
    try:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo(tz)).strftime("%Y-%m-%d")
    except Exception:
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    dest = set()

    # Prefer Signals (if schema has dedupe_hash)
    sigs = _rows_as_dicts("Signals")
    if any("dedupe_hash" in d for d in sigs):
        for d in sigs:
            ts = _date_yyyy_mm_dd(_pick(d, "ts"))
            if ts == today:
                dh = _pick(d, "dedupe_hash").strip()
                if dh:
                    dest.add(dh)
        if dest:
            return dest

    # Fallback to Trades
    trades = _rows_as_dicts("Trades")
    for d in trades:
        ts = _date_yyyy_mm_dd(_pick(d, "buy_time", "ts", "entry_time"))
        if ts == today:
            dh = _pick(d, "dedupe_hash").strip()
            if dh:
                dest.add(dh)
    return dest


def get_today_dedupe_hashes():
    return get_today_signal_dedupes()


def count_today_trades(tz="Asia/Kolkata"):
    """
    Count of Trades with buy_time (or ts) == today (irrespective of exit).
    Used for daily trade cap.
    """
    try:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo(tz)).strftime("%Y-%m-%d")
    except Exception:
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    trades = _rows_as_dicts("Trades")
    cnt = 0
    for d in trades:
        ts = _date_yyyy_mm_dd(_pick(d, "buy_time", "ts", "entry_time"))
        if ts == today:
            cnt += 1
    return cnt


# =========================
# In-memory "last signal" capture (for /oc_now without Sheets)
# =========================
_LAST_SIGNAL_ROW: List[Any] | None = None
_EXPECTED_SIGNAL_COLS = [
    "signal_id", "ts", "side", "trigger",
    "c1", "c2", "c3", "c4", "c5", "c6",
    "eligible", "reason",
    "mv_pcr_ok", "mv_mp_ok", "mv_basis",
    "oc_bull_normal", "oc_bull_shortcover", "oc_bear_normal", "oc_bear_crash",
    "oc_pattern_basis", "near/cross", "notes",
]

def tap_signal_row(row: List[Any]):
    """Capture the last signal row in memory regardless of Sheets availability."""
    global _LAST_SIGNAL_ROW
    try:
        _LAST_SIGNAL_ROW = list(row)
    except Exception:
        _LAST_SIGNAL_ROW = row

def get_last_signal_dict() -> Dict[str, Any]:
    """Return normalized dict for the last signal captured in memory."""
    dest: Dict[str, Any] = {}
    row = _LAST_SIGNAL_ROW
    if not row:
        return dest
    for i, key in enumerate(_EXPECTED_SIGNAL_COLS):
        if i < len(row):
            dest[key] = row[i]
    return dest


# Writer that always taps memory first
def write_signal_row(data: List[Any]):
    try:
        tap_signal_row(data)
    except Exception:
        pass
    append_row("Signals", data)

# Legacy alias (accepts list or dict)
def log_signal_row(row_or_dict):
    if isinstance(row_or_dict, dict):
        row = [
            row_or_dict.get("signal_id") or row_or_dict.get("id", ""),
            row_or_dict.get("ts") or row_or_dict.get("time", ""),
            str(row_or_dict.get("side") or row_or_dict.get("type", "")).upper(),
            row_or_dict.get("trigger") or row_or_dict.get("level", ""),
            row_or_dict.get("c1", ""), row_or_dict.get("c2", ""), row_or_dict.get("c3", ""),
            row_or_dict.get("c4", ""), row_or_dict.get("c5", ""), row_or_dict.get("c6", ""),
            row_or_dict.get("eligible", ""), row_or_dict.get("reason", "") or row_or_dict.get("comment",""),
            row_or_dict.get("mv_pcr_ok",""), row_or_dict.get("mv_mp_ok",""), row_or_dict.get("mv_basis",""),
            row_or_dict.get("oc_bull_normal",""), row_or_dict.get("oc_bull_shortcover",""),
            row_or_dict.get("oc_bear_normal",""), row_or_dict.get("oc_bear_crash",""),
            row_or_dict.get("oc_pattern_basis",""),
            row_or_dict.get("near/cross") or row_or_dict.get("near_cross",""),
            row_or_dict.get("notes",""),
        ]
    else:
        row = list(row_or_dict)
    try:
        tap_signal_row(row)
    except Exception:
        pass
    append_row("Signals", row)


# =========================
# Trade closing helper (note-supported)
# =========================
def _find_trade_row_index(trade_id: str) -> Optional[int]:
    """
    Return 1-based row index in Trades for given trade_id (including header row as 1).
    """
    try:
        rows = get_all_values("Trades")
    except Exception:
        return None
    if not rows or len(rows) < 2:
        return None
    header = [str(h).strip().lower() for h in rows[0]]
    try:
        col_idx = header.index("trade_id")
    except ValueError:
        try:
            col_idx = header.index("id")
        except ValueError:
            return None
    for i, r in enumerate(rows[1:], start=2):
        if col_idx < len(r) and str(r[col_idx]).strip() == str(trade_id).strip():
            return i
    return None


def _header_col_map(header: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate([str(x).strip().lower() for x in header])}


def close_trade(trade_id: str, exit_ltp: float, result: str, pnl: float, note: Optional[str] = None) -> None:
    """
    Public helper used by EOD auto-flat / MV-reversal exit:
    Updates Trades row for given trade_id with exit_ltp, exit_time(IST), result, pnl.
    If `note` provided and a 'notes'/'note' column exists, fills it too.
    Safe on missing columns / 10M cap / 429 (no crash).
    """
    try:
        rows = get_all_values("Trades")
        if not rows or len(rows) < 2:
            return
        header = [str(h).strip().lower() for h in rows[0]]
        cmap = _header_col_map(header)
        idx = _find_trade_row_index(trade_id)
        if not idx:
            return

        row = rows[idx - 1]  # zero-based
        need = max(
            cmap.get("exit_ltp", 0),
            cmap.get("exit_time", 0),
            cmap.get("result", 0),
            cmap.get("pnl", 0),
            cmap.get("notes", 0),
            cmap.get("note", 0),
        ) + 1
        if len(row) < need:
            row = row + [""] * (need - len(row))

        def _set(col: str, val: Any):
            j = cmap.get(col)
            if j is not None:
                if col in ("exit_ltp", "pnl"):
                    try:
                        row[j] = f"{float(val):.2f}"
                    except Exception:
                        row[j] = str(val)
                else:
                    row[j] = str(val)

        _set("exit_ltp", exit_ltp)
        _set("exit_time", now_str())
        _set("result", result)
        _set("pnl", pnl)
        if note is not None:
            if "notes" in cmap:
                _set("notes", note)
            elif "note" in cmap:
                _set("note", note)

        update_row("Trades", idx, row)
    except Exception:
        # swallow; callers shouldn't crash
        return
