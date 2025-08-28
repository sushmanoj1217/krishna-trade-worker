# scripts/paper_exit_watcher.py
"""
Paper Exit Watcher (robust logging + CLI):

- हर N सेकंड में latest OC snapshot (spot) लेता है (DHAN provider via oc_refresh_shim)
- Trades sheet में OPEN (paper) rows पढ़ता है
- analytics.paper_exit.evaluate_exit() से TP/SL/Trail/AUTO_FLAT लागू करता है
- DRY/RUN मोड: EXIT_DRY_RUN=1 पर केवल Status log; =0 पर Trades row close

ENV:
  GSHEET_TRADES_SPREADSHEET_ID, GOOGLE_SA_JSON
  OC_SYMBOL (NIFTY/BANKNIFTY/FINNIFTY)
  LOOP_SECS   (default 18)
  EXIT_DRY_RUN (default 1)
  PERFORMANCE_SHEET_NAME (optional)
  TP_POINTS, SL_POINTS, TRAIL_TRIGGER_POINTS, TRAIL_OFFSET_POINTS (optional overrides)

CLI:
  python -m scripts.paper_exit_watcher --once      # एक टिक, फिर exit (debug के लिए)
  python -m scripts.paper_exit_watcher --loop      # सतत लूप (default)
"""

from __future__ import annotations
import os, json, time, argparse
from typing import List, Dict, Any, Optional
from datetime import datetime

import logging
import sys

import gspread
from gspread.utils import rowcol_to_a1

from analytics.paper_exit import ExitParams, TradeRow, evaluate_exit, _ceilnum, _norm_side

# ---------- Robust logger (stdout handler forced) ----------
LOGGER_NAME = "paper_exit_watcher"
logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(logging.INFO)
if not logger.handlers:
    _h = logging.StreamHandler(stream=sys.stdout)
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(_h)
logger.propagate = False

def _gc():
    sa = json.loads(os.environ["GOOGLE_SA_JSON"])
    return gspread.service_account_from_dict(sa)

def _open_sheet():
    sid = os.environ["GSHEET_TRADES_SPREADSHEET_ID"]
    gc = _gc()
    return gc.open_by_key(sid)

def _pick_ws(sh, name: str):
    try:
        return sh.worksheet(name)
    except Exception:
        return None

HDR = {
    "id": {"id", "trade_id", "uid", "key"},
    "status": {"status", "state"},
    "symbol": {"symbol", "sym"},
    "side": {"side", "pos", "position", "type"},
    "entry_level": {"entry_level", "level", "trigger_level"},
    "entry_spot": {"entry_spot", "spot_entry", "entry_underlying"},
    "qty": {"qty", "quantity", "lots"},
    "exit_time": {"exit_time", "closed_at"},
    "exit_spot": {"exit_spot", "spot_exit"},
    "pnl": {"pnl", "net pnl", "net_pnl", "profit"},
    "trail_max": {"trail_max", "trail_high", "trail_min"},
}

def _index_headers(ws) -> Dict[str, int]:
    rows = ws.get_values("A1:Z1")
    hdr_row = rows[0] if rows else []
    if not hdr_row:
        for r in range(1, 4):
            vals = ws.row_values(r)
            if any((c or "").strip() for c in vals):
                hdr_row = vals
                break
    idx: Dict[str, int] = {}
    for k, aliases in HDR.items():
        got = None
        for i, name in enumerate(hdr_row, start=1):
            n = (name or "").strip().lower()
            if n in aliases:
                got = i
                break
        if got:
            idx[k] = got
    return idx

def _read_open_trades(ws) -> List[TradeRow]:
    idx = _index_headers(ws)
    vals = ws.get_all_values()
    if not vals:
        return []
    rows = vals[1:]
    res: List[TradeRow] = []
    want_symbol = (os.environ.get("OC_SYMBOL", "") or "").strip().upper()
    for r in rows:
        if not any((x or "").strip() for x in r):
            continue
        def get(col):
            ci = idx.get(col)
            if not ci or ci-1 >= len(r): return None
            return r[ci-1]

        status = (get("status") or "").strip().upper()
        if status not in ("", "OPEN"):
            continue

        side = _norm_side(get("side") or "")
        symbol = (get("symbol") or "NIFTY").strip().upper()
        if want_symbol and symbol != want_symbol:
            continue

        entry_level = _ceilnum(get("entry_level"))
        entry_spot  = _ceilnum(get("entry_spot"))
        qty = _ceilnum(get("qty")) or 1.0

        tr = TradeRow(
            id=(get("id") or None),
            symbol=symbol,
            side=side,
            status="OPEN",
            entry_level=entry_level or (entry_spot or 0.0),
            entry_spot=entry_spot or entry_level or 0.0,
            qty=qty,
            trail_max=_ceilnum(get("trail_max")),
            raw={"row": r, "idx": idx},
        )
        res.append(tr)
    return res

def _append_status(sh, text: str):
    ws = _pick_ws(sh, "Status") or sh.add_worksheet("Status", rows=200, cols=10)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws.append_row([now, "paper_exit", text], value_input_option="USER_ENTERED")

def _close_trade_row(ws, trade: TradeRow, exit_spot: float, pnl_points: Optional[float], reason: str):
    idx = trade.raw["idx"]
    vals = ws.get_all_values()
    target_rownum = None
    for i, r in enumerate(vals[1:], start=2):
        ok = True
        s_col = idx.get("status")
        side_col = idx.get("side")
        lvl_col = idx.get("entry_level")
        id_col  = idx.get("id")
        if s_col and (r[s_col-1] or "").strip().upper() not in ("", "OPEN"):
            ok = False
        if ok and id_col and trade.id:
            ok = (r[id_col-1].strip() == trade.id)
        if ok and side_col:
            ok = ok and (_norm_side(r[side_col-1]) == trade.side)
        if ok and lvl_col:
            ok = ok and (_ceilnum(r[lvl_col-1]) == trade.entry_level)
        if ok:
            target_rownum = i
            break

    if not target_rownum:
        raise RuntimeError("Could not locate OPEN trade row to close")

    updates: Dict[int, Any] = {}
    def set_col(key, val):
        c = idx.get(key)
        if c:
            updates[c] = val

    set_col("status", "CLOSED")
    set_col("exit_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    set_col("exit_spot", exit_spot)
    if pnl_points is not None:
        set_col("pnl", pnl_points)

    for c, val in updates.items():
        a1 = rowcol_to_a1(target_rownum, c)
        ws.update(a1, [[val]], value_input_option="USER_ENTERED")

    tmax = trade.raw.get("trail_max_update", None)
    if tmax is not None and idx.get("trail_max"):
        a1 = rowcol_to_a1(target_rownum, idx["trail_max"])
        ws.update(a1, [[tmax]], value_input_option="USER_ENTERED")

def _get_snapshot() -> Dict[str, Any]:
    import asyncio
    from analytics.oc_refresh_shim import get_refresh
    return asyncio.run(get_refresh()({}))  # type: ignore

def _one_tick(sh, trades_ws, p: ExitParams, dry: bool) -> None:
    try:
        snap = _get_snapshot() or {}
        spot = float(snap.get("spot") or 0.0)
        if not spot:
            logger.warning("No spot from provider; skipping tick")
            return

        open_trades = _read_open_trades(trades_ws)
        if not open_trades:
            logger.info("No OPEN trades")
            return

        for tr in open_trades:
            out = evaluate_exit(spot, tr, p)
            if "trail_max" in out and out["trail_max"] is not None:
                tr.raw["trail_max_update"] = out["trail_max"]

            if out["action"] == "EXIT":
                msg = f"[{tr.symbol} {tr.side}] EXIT @{spot} reason={out['reason']} pnl={out.get('pnl_points')}"
                logger.info(msg)
                if dry:
                    _append_status(sh, msg + " (dry)")
                else:
                    try:
                        _close_trade_row(trades_ws, tr, out["exit_spot"], out.get("pnl_points"), out["reason"])
                        _append_status(sh, msg)
                    except Exception as e:
                        logger.exception("close failed: %s", e)
                        _append_status(sh, f"ERROR close: {e}")
            # HOLD → no spam
    except Exception as e:
        logger.exception("tick error: %s", e)

def main():
    ap = argparse.ArgumentParser(description="Paper Exit Watcher")
    ap.add_argument("--once", action="store_true", help="Run single tick and exit")
    ap.add_argument("--loop", action="store_true", help="Run forever loop (default)")
    args = ap.parse_args()

    # Visible banner (in case logging is muted elsewhere)
    print("[paper_exit_watcher] starting...", flush=True)

    try:
        loop_secs = int(os.environ.get("LOOP_SECS", "18"))
    except Exception:
        loop_secs = 18
    dry = os.environ.get("EXIT_DRY_RUN", "1") != "0"
    sym = os.environ.get("OC_SYMBOL", "NIFTY")

    logger.info("config: loop=%ss, dry_run=%s, symbol=%s", loop_secs, dry, sym)

    try:
        sh = _open_sheet()
    except KeyError as e:
        logger.error("Missing env for Sheets: %s", e)
        return
    except Exception as e:
        logger.exception("Cannot open spreadsheet: %s", e)
        return

    trades_ws = _pick_ws(sh, "Trades")
    if not trades_ws:
        logger.error("Trades worksheet not found")
        return
    else:
        logger.info("Sheets OK: Trades worksheet found")

    p = ExitParams()

    if args.once and not args.loop:
        _one_tick(sh, trades_ws, p, dry)
        logger.info("once done")
        return

    # default: loop
    logger.info("loop started")
    while True:
        _one_tick(sh, trades_ws, p, dry)
        time.sleep(loop_secs)

if __name__ == "__main__":
    main()
