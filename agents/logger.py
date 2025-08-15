# --- add this in agents/logger.py ---
from datetime import datetime
import time

STATUS_TAB = "Status"
STATUS_HEADERS = ["ts", "worker_id", "shift_mode", "state", "message"]

def _now_iso():
    # IST tz system TZ=Asia/Kolkata pe set hai; seconds precision enough
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")

def log_status(sheet, data: dict):
    """
    Write heartbeat/status to Google Sheet 'Status' tab.
    `sheet` can be either:
      1) our Sheets wrapper with ensure_tab(name, headers) + append_row(name, row), OR
      2) a gspread Spreadsheet object.
    """
    row = [
        _now_iso(),
        str(data.get("worker_id", "")),
        str(data.get("shift_mode", "")),
        str(data.get("state", "")),
        str(data.get("message", "")),
    ]

    # Preferred: our wrapper
    try:
        if hasattr(sheet, "ensure_tab") and hasattr(sheet, "append_row"):
            sheet.ensure_tab(STATUS_TAB, headers=STATUS_HEADERS)
            sheet.append_row(STATUS_TAB, row)
            return
    except Exception as e:
        print(f"[logger.log_status] wrapper path failed: {e}", flush=True)

    # Fallback: raw gspread
    try:
        ws = None
        if hasattr(sheet, "worksheet"):
            try:
                ws = sheet.worksheet(STATUS_TAB)
            except Exception:
                if hasattr(sheet, "add_worksheet"):
                    ws = sheet.add_worksheet(title=STATUS_TAB, rows=2, cols=len(STATUS_HEADERS))
                    try:
                        ws.append_row(STATUS_HEADERS)
                    except Exception as _:
                        pass
        if ws is not None:
            ws.append_row(row)
            return
    except Exception as e:
        print(f"[logger.log_status] gspread path failed: {e}", flush=True)

    # Last resort: no-op but avoid crashing heartbeat
    print(f"[logger.log_status] could not write status row={row}", flush=True)
# ===== Sheet headers & ensure helpers =====
from typing import List, Dict

OC_LIVE_TAB = "OC_Live"
SIGNALS_TAB = "Signals"
TRADES_TAB = "Trades"
PERF_TAB = "Performance"
EVENTS_TAB = "Events"
STATUS_TAB = "Status"
SNAPSHOTS_TAB = "Snapshots"
PARAMS_OVERRIDE_TAB = "Params_Override"

OC_LIVE_HEADERS = ["ts","symbol","spot","s1","s2","r1","r2","expiry","signal"]
SIGNALS_HEADERS  = ["ts","symbol","side","price","reason","level","sl","tp","rr","signal_id"]
TRADES_HEADERS   = ["ts_open","symbol","side","qty","entry","sl","tp","ts_close","exit_price","pnl","reason_open","reason_close","trade_id"]
PERF_HEADERS     = ["date","symbol","trades","wins","losses","win_rate","avg_pnl","gross_pnl","net_pnl","max_dd","version","notes"]
EVENTS_HEADERS   = ["date","type","window","note","active"]
STATUS_HEADERS   = ["ts","worker_id","shift_mode","state","message"]  # used by log_status
SNAPSHOTS_HEADERS= ["ts","key","value","blob"]
PARAMS_OVERRIDE_HEADERS = ["key","value","source","status"]  # status: proposed/approved

def _ensure_tab(sheet, title: str, headers: List[str]):
    """
    Create tab if missing and ensure header row.
    Works with either our wrapper (ensure_tab/append_row) or raw gspread Spreadsheet.
    """
    # Preferred: wrapper path
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
            # Try to read first row; if blank, write headers
            try:
                first = ws.row_values(1)
            except Exception:
                first = []
            if [h.strip() for h in first] != headers:
                # Put headers into row 1
                try:
                    ws.clear()
                except Exception:
                    pass
                try:
                    ws.append_row(headers)
                except Exception:
                    # Some APIs need update on A1
                    try:
                        ws.update(f"A1:{chr(64+len(headers))}1", [headers])
                    except Exception as e2:
                        print(f"[logger.ensure] header update fallback failed for {title}: {e2}", flush=True)
        else:
            print(f"[logger.ensure] could not obtain worksheet for {title}", flush=True)
    except Exception as e:
        print(f"[logger.ensure] gspread path failed for {title}: {e}", flush=True)

def ensure_all_headers(sheet, cfg=None):
    """
    Ensure all operational tabs & headers exist.
    cfg is optional; if provided and has .symbol, we ensure baseline for that symbol.
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

