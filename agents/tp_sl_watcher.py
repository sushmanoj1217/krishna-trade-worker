# agents/tp_sl_watcher.py
from __future__ import annotations

import os
from typing import Dict, Any, List, Optional

from utils.logger import log

# Snapshot of OC / MV
try:
    from utils.cache import get_snapshot
except Exception:
    def get_snapshot():
        return None

# Sheets IO (read open trades + targeted update by row)
try:
    from integrations import sheets as sh
except Exception:
    class _S:
        def get_all_values(self, *a, **k): return []
        def update_row(self, *a, **k): pass
        def get_open_trades(self): return []
    sh = _S()  # type: ignore

# ====== Config ======
IST = "Asia/Kolkata"
QTY_PER_TRADE = int(os.getenv("QTY_PER_TRADE", "1"))
AUTO_FLAT_HHMM = os.getenv("AUTO_FLAT_HHMM", "15:15")  # "HH:MM" IST

# Trailing disabled until live option LTP feed is wired
ENABLE_TRAIL = False


# ====== Time utils ======
def now_ist_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        import datetime as dt
        return dt.datetime.now(ZoneInfo(IST)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        import datetime as dt
        return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def is_after_hhmm_ist(hhmm: str) -> bool:
    try:
        from zoneinfo import ZoneInfo
        import datetime as dt
        h, m = [int(x) for x in hhmm.split(":")]
        now = dt.datetime.now(ZoneInfo(IST))
        return (now.hour, now.minute) >= (h, m)
    except Exception:
        return False


# ====== Sheets helpers ======
def _rows_as_dicts(rows: List[List[Any]]) -> List[Dict[str, Any]]:
    if not rows: return []
    header = [str(h).strip().lower() for h in rows[0]]
    out: List[Dict[str, Any]] = []
    for r in rows[1:]:
        d: Dict[str, Any] = {}
        for i, v in enumerate(r):
            key = header[i] if i < len(header) else f"col{i+1}"
            d[key] = v
        out.append(d)
    return out


def _find_trade_row_index(trade_id: str) -> Optional[int]:
    """
    Return 1-based row index in Trades for given trade_id (including header row as 1).
    """
    try:
        rows = sh.get_all_values("Trades")
    except Exception as e:
        log.warning(f"get_all_values(Trades) failed: {e}")
        return None
    if not rows or len(rows) < 2:
        return None
    header = [str(h).strip().lower() for h in rows[0]]
    try:
        col_idx = header.index("trade_id")
    except ValueError:
        # fallback: try "id"
        try:
            col_idx = header.index("id")
        except ValueError:
            return None
    for i, r in enumerate(rows[1:], start=2):
        if col_idx < len(r) and str(r[col_idx]).strip() == str(trade_id).strip():
            return i
    return None


def _col_map(header: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate([str(x).strip().lower() for x in header])}


def _update_trade_exit(trade_id: str, exit_ltp: float, result: str, pnl: float):
    """
    Update a single trade row in place: exit_ltp, exit_time, result, pnl.
    """
    try:
        rows = sh.get_all_values("Trades")
        if not rows or len(rows) < 2:
            return
        header = [str(h).strip().lower() for h in rows[0]]
        cmap = _col_map(header)
        idx = _find_trade_row_index(trade_id)
        if not idx:
            return

        # Build row with updated fields (keep other cells as-is)
        row = rows[idx - 1]  # zero-based in our local list
        # ensure row large enough
        need = max(
            cmap.get("exit_ltp", 0),
            cmap.get("exit_time", 0),
            cmap.get("result", 0),
            cmap.get("pnl", 0),
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
        _set("exit_time", now_ist_str())
        _set("result", result)
        _set("pnl", pnl)

        sh.update_row("Trades", idx, row)
    except Exception as e:
        log.warning(f"update_trade_exit failed for {trade_id}: {e}")


# ====== MV direction helpers ======
def _mv_dir_from_bias_tag(bias_tag: Optional[str]) -> Optional[str]:
    """
    Return 'BULL' or 'BEAR' from bias_tag like 'mvbullmp', 'mvbearmp', else None.
    """
    if not bias_tag:
        return None
    s = str(bias_tag).lower()
    if "mvbull" in s:
        return "BULL"
    if "mvbear" in s:
        return "BEAR"
    return None


def _trade_dir(side: str) -> Optional[str]:
    """
    For our paper system: CE/PE are both long options.
    CE → market bullish thesis; PE → market bearish thesis.
    """
    s = str(side).strip().upper()
    if s == "CE": return "BULL"
    if s == "PE": return "BEAR"
    return None


# ====== Core: one pass ======
def check_trades_once():
    """
    One pass of TP/SL/MV/Time checks over open trades.
    - Trailing 1:2: (DISABLED until live option LTP available)
    - MV reversal exit: exit trade if MV flips against thesis
    - Time exit: hard flat at AUTO_FLAT_HHMM (IST)
    """
    # Load context
    snap = get_snapshot()
    bias_dir = _mv_dir_from_bias_tag(getattr(snap, "bias_tag", None))
    trades = []
    try:
        trades = sh.get_open_trades()
    except Exception as e:
        log.error(f"tp/sl watcher: get_open_trades failed: {e}")
        return

    for t in trades:
        try:
            trade_id = t.get("trade_id") or ""
            side = str(t.get("side", "")).upper()
            # Some sheets rows might miss fields — be defensive:
            buy_ltp = 0.0
            try:
                buy_ltp = float(t.get("buy_ltp", 0.0) or 0.0)
            except Exception:
                buy_ltp = 0.0
            sl = 0.0
            try:
                sl = float(t.get("sl", 0.0) or 0.0)
            except Exception:
                sl = 0.0
            tp = 0.0
            try:
                tp = float(t.get("tp", 0.0) or 0.0)
            except Exception:
                tp = 0.0

            # 1) Time exit
            if is_after_hhmm_ist(AUTO_FLAT_HHMM):
                # paper-safe exit at entry price (pnl ~ 0) if live LTP not present
                _update_trade_exit(trade_id, buy_ltp, "time_exit", 0.0)
                log.info(f"tp/sl watcher: time_exit closed {trade_id}")
                continue

            # 2) MV reversal exit
            dir_trade = _trade_dir(side)
            if bias_dir and dir_trade and bias_dir != dir_trade:
                _update_trade_exit(trade_id, buy_ltp, "mv_reversal_exit", 0.0)
                log.info(f"tp/sl watcher: mv_reversal_exit closed {trade_id} ({dir_trade} → {bias_dir})")
                # TODO: Optional flip: open reverse direction entry here (paper)
                continue

            # 3) Trailing 1:2 (disabled) — placeholder
            if ENABLE_TRAIL and buy_ltp > 0 and sl > 0 and tp > 0:
                # require live option LTP feed to compute in-flight trail
                pass

        except Exception as e:
            log.error(f"tp/sl watcher err for trade {t.get('trade_id')}: {e}")


# ====== Backward-compat API (krishna_main.py expects trail_tick) ======
def trail_tick():
    """
    Backward-compatible wrapper so old imports keep working.
    """
    check_trades_once()


# ====== Public loop ======
def loop_forever(poll_secs: int = 10):
    """
    Synchronous loop; call from day loop thread/task.
    """
    import time
    log.info("tp/sl watcher: started")
    while True:
        try:
            check_trades_once()
        except Exception as e:
            log.error(f"tp/sl watcher main err: {e}")
        time.sleep(max(1, int(poll_secs)))
