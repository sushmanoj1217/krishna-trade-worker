# agents/trade_loop.py
# -----------------------------------------------------------------------------
# Minimal trade loop that uses EXEC_GATES from signal_generator:
#   - tick(): evaluates signal; if eligible -> PAPER entry to "Trades" sheet
#   - auto_flat_1515(): utility to close all open paper trades by 15:15 IST (note only)
#
# This is a lightweight implementation; your real execution engine can replace
# paper parts with broker calls. Dedupe/daily cap already enforced in generator.
# -----------------------------------------------------------------------------

from __future__ import annotations

import os, json, time, logging
from typing import Any, Dict, Optional

try:
    import gspread  # type: ignore
except Exception:
    gspread = None  # type: ignore

from agents.signal_generator import generate_once

log = logging.getLogger(__name__)

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v.strip() if v and str(v).strip() else default

def _open_ws(name: str):
    if gspread is None:
        raise RuntimeError("gspread not installed")
    raw = _env("GOOGLE_SA_JSON"); sid = _env("GSHEET_TRADES_SPREADSHEET_ID")
    if not raw or not sid:
        raise RuntimeError("Sheets env missing")
    sa = json.loads(raw)
    gc = gspread.service_account_from_dict(sa)
    sh = gc.open_by_key(sid)
    try:
        return sh.worksheet(name)
    except Exception:
        return sh.add_worksheet(title=name, rows=1000, cols=26)

def _now_ist_str():
    return time.strftime("%Y-%m-%d %H:%M:%S IST", time.gmtime(time.time()+5.5*3600))

def _append_trade_row(row: Dict[str, Any]) -> None:
    try:
        ws = _open_ws("Trades")
        headers = [
            "entry_time","symbol","side","trigger","trigger_price","spot_at_entry",
            "mode","paper","qty","note","dedupe_key"
        ]
        values = [row.get(h,"") for h in headers]
        ws.append_row(values, value_input_option="RAW")
    except Exception as e:
        log.warning("Trades append failed: %s", e)

async def tick() -> Dict[str, Any]:
    """
    One iteration: evaluate EXEC_GATES; if eligible -> create PAPER trade row.
    Returns decision dict from signal_generator with 'paper_entry' flag possibly attached.
    """
    decision = await generate_once()
    if decision.get("eligible"):
        sym = (decision["snapshot"].get("symbol") or _env("OC_SYMBOL","NIFTY")).upper()
        side = decision.get("side")
        trig = decision.get("trigger_name")
        tpx  = decision.get("trigger_price")
        spot = decision["snapshot"].get("spot")
        key  = decision.get("dedupe_key") or ""

        # Paper entry stub; qty from env or 1
        qty = int(_env("PAPER_QTY","1") or "1")
        _append_trade_row({
            "entry_time": _now_ist_str(),
            "symbol": sym,
            "side": side,
            "trigger": trig,
            "trigger_price": tpx,
            "spot_at_entry": spot,
            "mode": "PAPER",
            "paper": "1",
            "qty": qty,
            "note": "EXEC_GATES pass",
            "dedupe_key": key,
        })
        decision["paper_entry"] = True
    else:
        decision["paper_entry"] = False

    return decision

def _now_ist_tuple():
    t = time.time() + 5.5*3600
    return (int(time.strftime("%H", time.gmtime(t))), int(time.strftime("%M", time.gmtime(t))))

def auto_flat_1515() -> Optional[str]:
    """
    Minimal hook: at/after 15:15 IST, append a note to Trades (no actual positions maintained here).
    In your real engine, close positions and write exit rows; here we just add a Status row idea.
    """
    hh, mm = _now_ist_tuple()
    if (hh,mm) >= (15,15):
        try:
            ws = _open_ws("Status")
            ws.append_row([_now_ist_str(), "AUTO_FLAT_1515", "All open paper trades considered closed"], value_input_option="RAW")
            return "AUTO_FLAT_1515 noted"
        except Exception:
            return None
    return None
