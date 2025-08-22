# agents/trade_loop.py
# -----------------------------------------------------------------------------
# Trade loop with EXITS:
#   - tick(): generate signal (EXEC_GATES) -> PAPER entry if eligible
#             then run exits on all open trades (tp_sl_watcher.process_open_trades)
#   - auto_flat_1515(): hard close at/after 15:15 IST (writes exit rows)
# -----------------------------------------------------------------------------

from __future__ import annotations

import os, json, time, logging
from typing import Any, Dict, Optional

try:
    import gspread  # type: ignore
except Exception:
    gspread = None  # type: ignore

from agents.signal_generator import generate_once
from agents.tp_sl_watcher import process_open_trades, force_flat_all

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
    """
    Appends a PAPER trade row to Trades; ensures minimum headers for entries exist.
    """
    try:
        ws = _open_ws("Trades")
        headers = [
            "entry_time","symbol","side","trigger","trigger_price","spot_at_entry",
            "mode","paper","qty","note","dedupe_key",
            # exits columns (may be blank on entry; tp_sl_watcher ensures presence)
            "exit_time","exit_spot","pnl_points","exit_reason"
        ]
        # ensure header row includes these
        cur_hdr = ws.row_values(1)
        if not cur_hdr:
            ws.update("A1", [headers])
            cur_hdr = headers
        else:
            need = [h for h in headers if h not in cur_hdr]
            if need:
                ws.update("A1", [cur_hdr + need])
                cur_hdr = cur_hdr + need

        values = [row.get(h,"") for h in cur_hdr]
        ws.append_row(values, value_input_option="RAW")
    except Exception as e:
        log.warning("Trades append failed: %s", e)

async def tick() -> Dict[str, Any]:
    """
    One iteration:
      1) Evaluate EXEC_GATES; if eligible -> create PAPER trade row
      2) Run exits on open trades (process_open_trades)
    Returns the decision dict from generate_once with 'paper_entry' flag.
    """
    decision = await generate_once()

    # Step 1: Paper entry if eligible
    if decision.get("eligible"):
        sym = (decision["snapshot"].get("symbol") or _env("OC_SYMBOL","NIFTY")).upper()
        side = decision.get("side")
        trig = decision.get("trigger_name")
        tpx  = decision.get("trigger_price")
        spot = decision["snapshot"].get("spot")
        key  = decision.get("dedupe_key") or ""

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

    # Step 2: Process exits for all open trades
    try:
        closed = await process_open_trades()
        if closed:
            log.info("trade_loop.tick: closed %d trades via exits", closed)
    except Exception as e:
        log.warning("trade_loop.tick: process_open_trades error: %s", e)

    return decision

def _now_ist_tuple():
    t = time.time() + 5.5*3600
    return (int(time.strftime("%H", time.gmtime(t))), int(time.strftime("%M", time.gmtime(t))))

def auto_flat_1515() -> Optional[str]:
    """
    Hard flat: at/after 15:15 IST, force-close all open paper trades (writes exit rows).
    """
    hh, mm = _now_ist_tuple()
    if (hh,mm) >= (15,15):
        try:
            n = force_flat_all(reason="TIME")
            ws = _open_ws("Status")
            ws.append_row([_now_ist_str(), "AUTO_FLAT_1515", f"Closed {n} open paper trades"], value_input_option="RAW")
            return f"AUTO_FLAT_1515 closed {n}"
        except Exception as e:
            log.warning("auto_flat_1515 error: %s", e)
            return None
    return None
