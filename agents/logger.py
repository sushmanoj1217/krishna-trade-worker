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
