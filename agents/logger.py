# agents/logger.py
# Krishna Trade Worker — Sheet logger utilities
# Works with either:
#  1) Our Sheets wrapper (ensure_tab(name, headers), append_row(name, row))
#  2) Raw gspread Spreadsheet object (worksheet/add_worksheet/append_row/update)

from datetime import datetime
from typing import List, Dict, Optional

# ===== Tab names =====
OC_LIVE_TAB = "OC_Live"
SIGNALS_TAB = "Signals"
TRADES_TAB = "Trades"
PERF_TAB = "Performance"
EVENTS_TAB = "Events"
STATUS_TAB = "Status"
SNAPSHOTS_TAB = "Snapshots"
PARAMS_OVERRIDE_TAB = "Params_Override"

# ===== Headers =====
OC_LIVE_HEADERS = ["ts", "symbol", "spot", "s1", "s2", "r1", "r2", "expiry", "signal"]
SIGNALS_HEADERS  = ["ts", "symbol", "side", "price", "reason", "level", "sl", "tp", "rr", "signal_id"]
TRADES_HEADERS   = ["ts_open", "symbol", "side", "qty", "entry", "sl", "tp",
                    "ts_close", "exit_price", "pnl", "reason_open", "reason_close", "trade_id"]
PERF_HEADERS     = ["date", "symbol", "trades", "wins", "losses", "win_rate",
                    "avg_pnl", "gross_pnl", "net_pnl", "max_dd", "version", "notes"]
EVENTS_HEADERS   = ["date", "type", "window", "note", "active"]
STATUS_HEADERS   = ["ts", "worker_id", "shift_mode", "state", "message"]
SNAPSHOTS_HEADERS= ["ts", "key", "value", "blob"]
PARAMS_OVERRIDE_HEADERS = ["key", "value", "source", "status"]  # status: proposed/approved

# ===== Time helpers =====
def _now_iso() -> str:
    # TZ is handled by process env (TZ=Asia/Kolkata). Keep seconds precision.
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")

# ===== Low-level ensure/append helpers =====
def _ensure_tab(sheet, title: str, headers: List[str]) -> None:
    """
    Create tab if missing and ensure header row.
    Works with wrapper (ensure_tab) or gspread Spreadsheet.
    """
    # Preferred: wrapper
    try:
        if hasattr(sheet, "ensure_tab"):
            sheet.ensure_tab(title, headers=headers)
            return
    except Exception as e:
        print(f"[logger.ensure] wrapper ensure_tab failed for {title}: {e}", flush=True)

    # Fallback: raw gspread
    try:
        ws = None
        if hasattr(sheet, "worksheet"):
            try:
                ws = sheet.worksheet(title)
            except Exception:
                if hasattr(sheet, "add_worksheet"):
                    ws = sheet.add_worksheet(title=title, rows=10, cols=max(10, len(headers)))
                    try:
                        ws.append_row(headers)
                    except Exception:
                        pass

        if ws is not None:
            # If first row isn't headers, put headers
            try:
                first = ws.row_values(1)
            except Exception:
                first = []
            cleaned = [h.strip() for h in first]
            if cleaned != headers:
                try:
                    ws.clear()
                except Exception:
                    pass
                try:
                    ws.append_row(headers)
                except Exception:
                    # Some APIs prefer update ranges
                    try:
                        end_col = chr(64 + len(headers))  # naive A..Z (ok for our small header counts)
                        ws.update(f"A1:{end_col}1", [headers])
                    except Exception as e2:
                        print(f"[logger.ensure] header update fallback failed for {title}: {e2}", flush=True)
        else:
            print(f"[logger.ensure] could not obtain worksheet for {title}", flush=True)
    except Exception as e:
        print(f"[logger.ensure] gspread path failed for {title}: {e}", flush=True)

def _row_from_dict(headers: List[str], data: Dict) -> List[str]:
    row: List[str] = []
    for key in headers:
        val = data.get(key, "")
        # Default ts/date auto-fill if empty and header is ts/date
        if (key in ("ts", "ts_open", "ts_close", "date")) and (val == "" or val is None):
            val = _now_iso() if key != "date" else _now_iso().split(" ")[0]
        row.append("" if val is None else str(val))
    return row

def _append_row(sheet, title: str, headers: List[str], row: List[str]) -> None:
    # Preferred: wrapper
    try:
        if hasattr(sheet, "ensure_tab") and hasattr(sheet, "append_row"):
            sheet.ensure_tab(title, headers=headers)
            sheet.append_row(title, row)
            return
    except Exception as e:
        print(f"[logger.append] wrapper path failed for {title}: {e}", flush=True)

    # Fallback: raw gspread
    try:
        ws = None
        if hasattr(sheet, "worksheet"):
            try:
                ws = sheet.worksheet(title)
            except Exception:
                if hasattr(sheet, "add_worksheet"):
                    ws = sheet.add_worksheet(title=title, rows=10, cols=max(10, len(headers)))
                    try:
                        ws.append_row(headers)
                    except Exception:
                        pass
        if ws is None:
            print(f"[logger.append] could not obtain worksheet for {title}", flush=True)
            return
        ws.append_row(row)
    except Exception as e:
        print(f"[logger.append] gspread path failed for {title}: {e}", flush=True)

# ===== Public API =====
def ensure_all_headers(sheet, cfg: Optional[object] = None) -> None:
    """
    Ensure all operational tabs exist with correct headers.
    cfg is optional; presence not required.
    """
    tabs: Dict[str, List[str]] = {
        OC_LIVE_TAB: OC_LIVE_HEADERS,
        SIGNALS_TAB: SIGNALS_HEADERS,
        TRADES_TAB: TRADES_HEADERS,
        PERF_TAB: PERF_HEADERS,
        EVENTS_TAB: EVENTS_HEADERS,
        STATUS_TAB: STATUS_HEADERS,
        SNAPSHOTS_TAB: SNAPSHOTS_HEADERS,
        PARAMS_OVERRIDE_TAB: PARAMS_OVERRIDE_HEADERS,
    }
    for title, headers in tabs.items():
        _ensure_tab(sheet, title, headers)
    print("[logger.ensure_all_headers] ensured tabs: " + ", ".join(tabs.keys()), flush=True)

def log_status(sheet, data: Dict) -> None:
    """
    Heartbeat/status row → Status tab.
    Keys: ts?, worker_id, shift_mode, state, message
    """
    try:
        row = _row_from_dict(STATUS_HEADERS, data)
        _append_row(sheet, STATUS_TAB, STATUS_HEADERS, row)
    except Exception as e:
        print(f"[logger.log_status] failed: {e}", flush=True)

def log_oc_live(sheet, data: Dict) -> None:
    """
    Option Chain live snapshot → OC_Live tab.
    Keys: ts?, symbol, spot, s1, s2, r1, r2, expiry, signal
    """
    try:
        row = _row_from_dict(OC_LIVE_HEADERS, data)
        _append_row(sheet, OC_LIVE_TAB, OC_LIVE_HEADERS, row)
    except Exception as e:
        print(f"[logger.log_oc_live] failed: {e}", flush=True)

def log_signal(sheet, data: Dict) -> None:
    """
    Signal row → Signals tab.
    Keys: ts?, symbol, side, price, reason, level, sl, tp, rr, signal_id
    """
    try:
        row = _row_from_dict(SIGNALS_HEADERS, data)
        _append_row(sheet, SIGNALS_TAB, SIGNALS_HEADERS, row)
    except Exception as e:
        print(f"[logger.log_signal] failed: {e}", flush=True)

def log_trade(sheet, data: Dict) -> None:
    """
    Trade row → Trades tab (single-row model covering open & optional close).
    Keys:
      ts_open, symbol, side, qty, entry, sl, tp,
      ts_close?, exit_price?, pnl?, reason_open?, reason_close?, trade_id
    """
    try:
        row = _row_from_dict(TRADES_HEADERS, data)
        _append_row(sheet, TRADES_TAB, TRADES_HEADERS, row)
    except Exception as e:
        print(f"[logger.log_trade] failed: {e}", flush=True)

def log_perf(sheet, data: Dict) -> None:
    """
    EOD performance → Performance tab.
    Keys: date?, symbol, trades, wins, losses, win_rate, avg_pnl, gross_pnl,
          net_pnl, max_dd, version, notes
    """
    try:
        row = _row_from_dict(PERF_HEADERS, data)
        _append_row(sheet, PERF_TAB, PERF_HEADERS, row)
    except Exception as e:
        print(f"[logger.log_perf] failed: {e}", flush=True)

def log_event(sheet, data: Dict) -> None:
    """
    Events maintenance → Events tab.
    Keys: date?, type, window, note, active
    """
    try:
        row = _row_from_dict(EVENTS_HEADERS, data)
        _append_row(sheet, EVENTS_TAB, EVENTS_HEADERS, row)
    except Exception as e:
        print(f"[logger.log_event] failed: {e}", flush=True)

def log_snapshot(sheet, data: Dict) -> None:
    """
    Snapshot (key/value/blob) → Snapshots tab.
    Keys: ts?, key, value, blob
    """
    try:
        row = _row_from_dict(SNAPSHOTS_HEADERS, data)
        _append_row(sheet, SNAPSHOTS_TAB, SNAPSHOTS_HEADERS, row)
    except Exception as e:
        print(f"[logger.log_snapshot] failed: {e}", flush=True)

def log_params_override(sheet, rows: List[Dict]) -> None:
    """
    Bulk write proposed/approved params to Params_Override tab (append-only).
    Each item keys: key, value, source, status
    """
    try:
        for item in rows:
            row = _row_from_dict(PARAMS_OVERRIDE_HEADERS, item)
            _append_row(sheet, PARAMS_OVERRIDE_TAB, PARAMS_OVERRIDE_HEADERS, row)
    except Exception as e:
        print(f"[logger.log_params_override] failed: {e}", flush=True)
