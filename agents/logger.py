# agents/logger.py
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

# We call module-level helpers from integrations/sheets.py:
#   ensure_tab(tab_name, headers)
#   append_row(tab_name, row)
#   tail_rows(tab_name, n)   (optional; we fall back if missing)
from integrations import sheets

# ---------- Tab Headers ----------
OC_LIVE_HEADERS = [
    "ts", "symbol", "spot",
    "S1", "S2", "R1", "R2",
    "S1*", "S2*", "R1*", "R2*",
    "MV", "PCR", "VIX", "notes"
]

SIGNALS_HEADERS = [
    "ts", "symbol", "side", "level_tag",
    "trigger_level", "mv", "reason",
    "target_rr", "trail_after_rr"
]

TRADES_HEADERS = [
    "ts","id","symbol","side","state","level_tag","entry_trigger",
    "fill_spot","fill_opt_price","qty","risk_points","trail_started",
    "trail_stop","last_ltp","pnl_points","pnl_value","exit_reason"
]

PERF_HEADERS = [
    "ts", "date", "num_signals", "num_trades", "gross_pnl", "net_pnl",
    "win_count", "loss_count", "max_dd", "notes"
]

EVENTS_HEADERS = ["ts", "event", "start_ts", "end_ts", "meta"]
STATUS_HEADERS = ["ts","key","value"]
SNAPSHOTS_HEADERS = ["ts","key","json"]
PARAMS_OVERRIDE_HEADERS = ["ts","key","value","who","reason"]

# ---------- Sheet helpers ----------
def _ensure_tab(name: str, headers: List[str]) -> None:
    sheets.ensure_tab(name, headers)

def _append(name: str, row: List) -> None:
    sheets.append_row(name, row)

def _tail(name: str, n: int) -> List[List]:
    try:
        return sheets.tail_rows(name, n) or []
    except Exception:
        return []

# ---------- Ensures ----------
def ensure_all_headers() -> None:
    _ensure_tab("OC_Live", OC_LIVE_HEADERS)
    _ensure_tab("Signals", SIGNALS_HEADERS)
    _ensure_tab("Trades", TRADES_HEADERS)
    _ensure_tab("Performance", PERF_HEADERS)
    _ensure_tab("Events", EVENTS_HEADERS)
    _ensure_tab("Status", STATUS_HEADERS)
    _ensure_tab("Snapshots", SNAPSHOTS_HEADERS)
    _ensure_tab("Params_Override", PARAMS_OVERRIDE_HEADERS)

# ---------- Helpers ----------
def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")

# ---------- OC Live ----------
def log_oc_live(snapshot: Dict, notes: str = "") -> None:
    """
    snapshot keys expected:
    'symbol','spot','S1','S2','R1','R2','S1*','S2*','R1*','R2*','MV'
    PCR/VIX will be pulled from Status latest if not in snapshot.
    """
    ensure_all_headers()
    symbol = snapshot.get("symbol", "NIFTY")
    spot = snapshot.get("spot")
    latest = get_latest_status_map()
    row = [
        snapshot.get("ts") or _now(),
        symbol,
        spot,
        snapshot.get("S1"), snapshot.get("S2"),
        snapshot.get("R1"), snapshot.get("R2"),
        snapshot.get("S1*"), snapshot.get("S2*"),
        snapshot.get("R1*"), snapshot.get("R2*"),
        snapshot.get("MV"),
        snapshot.get("PCR") or latest.get("PCR"),
        snapshot.get("VIX") or latest.get("VIX"),
        notes,
    ]
    _append("OC_Live", row)

# ---------- Signals ----------
def log_signal(signal: Dict) -> None:
    """
    signal keys expected:
    'symbol','side','level_tag','trigger_level','mv','reason','target_rr','trail_after_rr'
    """
    ensure_all_headers()
    row = [
        signal.get("ts") or _now(),
        signal.get("symbol", "NIFTY"),
        signal.get("side"),
        signal.get("level_tag"),
        signal.get("trigger_level"),
        signal.get("mv"),
        signal.get("reason"),
        signal.get("target_rr"),
        signal.get("trail_after_rr"),
    ]
    _append("Signals", row)

# ---------- Trades (Paper) ----------
def _trade_row(tr: Dict) -> List:
    return [
        tr.get("updated_at") or tr.get("created_at") or _now(),
        tr.get("id"),
        tr.get("symbol"),
        tr.get("side"),
        tr.get("state"),
        tr.get("level_tag"),
        tr.get("entry_trigger"),
        tr.get("fill_spot"),
        tr.get("fill_opt_price"),
        tr.get("qty"),
        tr.get("risk_points"),
        tr.get("trail_started"),
        tr.get("trail_stop"),
        tr.get("last_ltp"),
        tr.get("pnl_points"),
        tr.get("pnl_value"),
        tr.get("exit_reason"),
    ]

def log_trade_open(tr: Dict) -> None:
    ensure_all_headers()
    _append("Trades", _trade_row(tr))

def log_trade_update(tr: Dict) -> None:
    _append("Trades", _trade_row(tr))

def log_trade_close(tr: Dict) -> None:
    _append("Trades", _trade_row(tr))

# ---------- Performance ----------
def log_perf_eod(stats: Dict) -> None:
    """
    stats keys expected:
    'date','num_signals','num_trades','gross_pnl','net_pnl','win_count','loss_count','max_dd','notes'
    """
    ensure_all_headers()
    row = [
        _now(),
        stats.get("date"),
        stats.get("num_signals"),
        stats.get("num_trades"),
        stats.get("gross_pnl"),
        stats.get("net_pnl"),
        stats.get("win_count"),
        stats.get("loss_count"),
        stats.get("max_dd"),
        stats.get("notes"),
    ]
    _append("Performance", row)

# ---------- Events ----------
def log_event(name: str, start_ts: str, end_ts: str, meta: str = "") -> None:
    ensure_all_headers()
    _append("Events", [_now(), name, start_ts, end_ts, meta])

# ---------- Status (PCR/VIX & misc) ----------
def log_market_context(pcr: float | None = None, vix: float | None = None) -> None:
    ensure_all_headers()
    ts = _now()
    if pcr is not None:
        _append("Status", [ts, "PCR", round(float(pcr), 4)])
    if vix is not None:
        _append("Status", [ts, "VIX", round(float(vix), 2)])

def set_status_kv(key: str, value) -> None:
    ensure_all_headers()
    _append("Status", [_now(), key, value])

def get_latest_status_map() -> Dict[str, str]:
    rows = _tail("Status", 300) or []
    out: Dict[str, str] = {}
    for r in rows[::-1]:
        if len(r) >= 3:
            k, v = r[1], r[2]
            if k not in out:
                out[k] = v
            if len(out) >= 8:
                break
    return out
