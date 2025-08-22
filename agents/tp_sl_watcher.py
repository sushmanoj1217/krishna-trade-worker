# agents/tp_sl_watcher.py
# -----------------------------------------------------------------------------
# Exit Manager (TP/SL/Trailing, MV-Reversal, STALE, Time-Flat)
#
# What it does:
#   - Reads open "paper" trades from the "Trades" sheet (no exit_time)
#   - Uses latest OC snapshot (analytics.oc_refresh.get_snapshot()) to evaluate exits:
#       E1  Time flat (>= 15:15 IST)                -> exit_reason = "TIME"
#       E2  Stop-Loss (adverse move ≥ SL pts)       -> "SL"
#       E3  Take-Profit (favorable move ≥ TP pts)   -> "TP"
#       E4  Trailing SL (after trigger)             -> "TRAIL"
#       E5  MV Reversal (N consecutive snaps)       -> "MV_REVERSAL"
#       E6  STALE data (K consecutive stale snaps)  -> "STALE"
#       E7  Manual/HOLD after entry (optional OFF)  -> (we do NOT auto-exit; renderer blocks new entries)
#
# State:
#   - Trailing/mv/stale counters persist in /tmp:
#       /tmp/exit_state.json:
#         {
#           "<dedupe_key>": {
#               "trail_line": float|null,
#               "mv_bad_streak": int,
#               "stale_streak": int,
#               "high_water": float|null,   # for CE (favorable high)
#               "low_water":  float|null    # for PE (favorable low)
#           }, ...
#         }
#
# Env knobs (symbol-wise defaults applied via *_NIFTY/BANKNIFTY/FINNIFTY or fallback):
#   TP_POINTS_*           (defaults: NIFTY=40, BANKNIFTY=100, FINNIFTY=60; fallback TP_POINTS=40)
#   SL_POINTS_*           (defaults: NIFTY=20, BANKNIFTY=60,  FINNIFTY=35; fallback SL_POINTS=20)
#   TRAIL_TRIGGER_POINTS_* (defaults: NIFTY=25, BANKNIFTY=70, FINNIFTY=45; fallback TRAIL_TRIGGER_POINTS=25)
#   TRAIL_OFFSET_POINTS_*  (defaults: NIFTY=15, BANKNIFTY=40, FINNIFTY=25; fallback TRAIL_OFFSET_POINTS=15)
#   MV_REV_CONFIRM        (default 2)   # consecutive opposite-MV snapshots needed
#   STALE_EXIT_CONFIRM    (default 2)   # consecutive stale/old snapshots needed
#   OC_FRESH_MAX_AGE_SEC  (default 90)  # staleness cutoff
#
# Sheet columns ensured if missing:
#   ["entry_time","symbol","side","trigger","trigger_price","spot_at_entry","mode","paper","qty","note","dedupe_key",
#    "exit_time","exit_spot","pnl_points","exit_reason"]
#
# Public:
#   async def process_open_trades() -> int    # returns #trades closed this run
#   def force_flat_all(reason: str = "TIME") -> int  # closes all open paper trades immediately
# -----------------------------------------------------------------------------

from __future__ import annotations

import os, json, time, logging
from typing import Any, Dict, Optional, List, Tuple

try:
    import gspread  # type: ignore
except Exception:
    gspread = None  # type: ignore

log = logging.getLogger(__name__)

# ---------------- Env helpers ----------------
def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v.strip() if v and str(v).strip() else default

def _sym_env(sym: str, base: str, default: float) -> float:
    s = (sym or "").upper()
    val = _env(f"{base}_{s}")
    if val is None:
        val = _env(base)
    try:
        return float(val) if val is not None else float(default)
    except Exception:
        return float(default)

# ---------------- Time helpers (IST) ----------------
def _now_ist_str():
    return time.strftime("%Y-%m-%d %H:%M:%S IST", time.gmtime(time.time()+5.5*3600))

def _now_ist_tuple():
    t = time.time() + 5.5*3600
    return (int(time.strftime("%H", time.gmtime(t))), int(time.strftime("%M", time.gmtime(t))))

def _after_1515() -> bool:
    hh, mm = _now_ist_tuple()
    return (hh,mm) >= (15,15)

# ---------------- Sheets IO ----------------
REQUIRED_HEADERS = [
    "entry_time","symbol","side","trigger","trigger_price","spot_at_entry",
    "mode","paper","qty","note","dedupe_key",
    "exit_time","exit_spot","pnl_points","exit_reason"
]

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
        return sh.add_worksheet(title=name, rows=1000, cols=32)

def _ensure_headers(ws) -> List[str]:
    try:
        hdr = ws.row_values(1)
    except Exception:
        hdr = []
    if not hdr:
        ws.update("A1", [REQUIRED_HEADERS])
        return REQUIRED_HEADERS
    # append any missing headers at end (keep original order)
    need = [h for h in REQUIRED_HEADERS if h not in hdr]
    if need:
        new_hdr = hdr + need
        ws.update("A1", [new_hdr])
        return new_hdr
    return hdr

def _get_all_records_with_index(ws) -> List[Tuple[int, Dict[str, Any]]]:
    # returns list of (row_idx, dict) for data rows
    vals = ws.get_all_values()
    if not vals: return []
    hdr = vals[0]
    rows = []
    for i in range(1, len(vals)):
        row = vals[i]
        rec = {}
        for c, key in enumerate(hdr):
            rec[key] = row[c] if c < len(row) else ""
        rows.append((i+1, rec))  # 1-based row index
    return rows

def _update_row(ws, row_idx: int, headers: List[str], kv: Dict[str, Any]) -> None:
    try:
        cur = ws.row_values(row_idx)
    except Exception:
        cur = []
    row = [cur[i] if i < len(cur) else "" for i in range(len(headers))]
    map_idx = {h:i for i,h in enumerate(headers)}
    for k,v in kv.items():
        if k not in map_idx:
            # header missing: extend
            headers.append(k)
            row.append("")
            map_idx[k] = len(headers)-1
        row[map_idx[k]] = v if v is not None else ""
    ws.update(f"A{row_idx}", [row])

# ---------------- State (trail/mv/stale) ----------------
STATE_PATH = "/tmp/exit_state.json"

def _load_state() -> Dict[str, Any]:
    try:
        with open(STATE_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_state(st: Dict[str, Any]) -> None:
    try:
        with open(STATE_PATH, "w") as f:
            json.dump(st, f)
    except Exception:
        pass

# ---------------- Config per symbol ----------------
def _cfg_for_symbol(sym: str) -> Dict[str, float]:
    s = (sym or "").upper()
    cfg = {
        "TP":  _sym_env(s, "TP_POINTS", 40.0 if s=="NIFTY" else (100.0 if s=="BANKNIFTY" else 60.0)),
        "SL":  _sym_env(s, "SL_POINTS", 20.0 if s=="NIFTY" else (60.0  if s=="BANKNIFTY" else 35.0)),
        "TRL_TRIG": _sym_env(s, "TRAIL_TRIGGER_POINTS", 25.0 if s=="NIFTY" else (70.0 if s=="BANKNIFTY" else 45.0)),
        "TRL_OFF":  _sym_env(s, "TRAIL_OFFSET_POINTS",  15.0 if s=="NIFTY" else (40.0 if s=="BANKNIFTY" else 25.0)),
        "MV_REV_N": float(_env("MV_REV_CONFIRM","2") or "2"),
        "STALE_N":  float(_env("STALE_EXIT_CONFIRM","2") or "2"),
        "FRESH_MAX": float(_env("OC_FRESH_MAX_AGE_SEC","90") or "90"),
    }
    return cfg

# ---------------- Snapshot helpers ----------------
def _get_snapshot() -> Dict[str, Any]:
    from analytics import oc_refresh
    snap = oc_refresh.get_snapshot() or {}
    # do not force refresh here; day loop/renderer refresh periodically.
    return snap

def _spot(snap: Dict[str, Any]) -> Optional[float]:
    try:
        return float(str(snap.get("spot")))
    except Exception:
        return None

def _mv(snap: Dict[str, Any]) -> str:
    return (snap.get("mv") or "").strip().lower()

def _stale_bad(snap: Dict[str, Any], fresh_max: float) -> bool:
    age = snap.get("age_sec")
    stale = bool(snap.get("stale"))
    try:
        agev = int(age) if age is not None else 999999
    except Exception:
        agev = 999999
    return stale or (agev > fresh_max)

# ---------------- PnL calc on underlying points ----------------
def _pnl_points(side: str, entry_spot: float, exit_spot: float) -> float:
    if side == "CE":
        return float(exit_spot) - float(entry_spot)
    else:  # "PE"
        return float(entry_spot) - float(exit_spot)

# ---------------- Trailing logic ----------------
def _update_trailing(state: Dict[str, Any], key: str, side: str, entry_spot: float, spot: float, cfg) -> Tuple[Optional[float], Optional[str]]:
    node = state.setdefault(key, {"trail_line": None, "mv_bad_streak": 0, "stale_streak": 0, "high_water": None, "low_water": None})
    trail = node.get("trail_line")
    trig  = float(cfg["TRL_TRIG"]); off = float(cfg["TRL_OFF"])

    # favorable watermarks
    if side == "CE":
        hw = node.get("high_water")
        node["high_water"] = max(hw, spot) if hw is not None else spot
        fav = (spot - entry_spot)
        if fav >= trig:
            # start/update trail
            new_trail = spot - off
            trail = max(trail, new_trail) if trail is not None else new_trail
    else:  # PE
        lw = node.get("low_water")
        node["low_water"] = min(lw, spot) if lw is not None else spot
        fav = (entry_spot - spot)
        if fav >= trig:
            new_trail = spot + off
            trail = min(trail, new_trail) if trail is not None else new_trail

    node["trail_line"] = trail
    return trail, None

def _trail_hit(side: str, spot: float, trail: Optional[float]) -> bool:
    if trail is None: return False
    if side == "CE":
        return spot <= trail
    else:
        return spot >= trail

# ---------------- MV reversal & stale counters ----------------
def _update_mv_stale(state: Dict[str, Any], key: str, side: str, mv: str, is_stale: bool) -> Tuple[int,int]:
    node = state.setdefault(key, {"trail_line": None, "mv_bad_streak": 0, "stale_streak": 0, "high_water": None, "low_water": None})
    # mv_bad_streak increments when MV is opposite to entry side permission
    is_bad = ((side == "CE" and mv in {"bearish","strong_bearish"}) or
              (side == "PE" and mv in {"bullish","big_move"}))
    node["mv_bad_streak"] = (node.get("mv_bad_streak",0) + 1) if is_bad else 0
    node["stale_streak"]  = (node.get("stale_streak",0) + 1) if is_stale else 0
    return node["mv_bad_streak"], node["stale_streak"]

# ---------------- Core: process open trades ----------------
async def process_open_trades() -> int:
    """
    Reads Trades; evaluates exits for open paper trades; updates row when exit.
    Returns number of trades closed.
    """
    ws = _open_ws("Trades")
    headers = _ensure_headers(ws)
    rows = _get_all_records_with_index(ws)

    from analytics import oc_refresh
    # Use current snapshot (day loop should refresh). If not present, try once.
    snap = oc_refresh.get_snapshot()
    if not snap:
        try:
            await oc_refresh.refresh_once()
        except Exception as e:
            log.warning("tp_sl_watcher: refresh_once error: %s", e)
        snap = oc_refresh.get_snapshot() or {}

    spot = _spot(snap)
    mv   = _mv(snap)
    closed = 0
    st = _load_state()

    for row_idx, rec in rows:
        try:
            if str(rec.get("paper","")) != "1": 
                continue
            if str(rec.get("exit_time","")).strip():
                continue  # already closed

            sym = (rec.get("symbol") or _env("OC_SYMBOL","NIFTY")).upper()
            side = (rec.get("side") or "").strip().upper()
            entry_spot = float(rec.get("spot_at_entry") or rec.get("entry_spot") or 0.0)
            trig = rec.get("trigger") or ""
            dedupe_key = rec.get("dedupe_key") or f"{side}|{trig}|{int(entry_spot)}"

            cfg = _cfg_for_symbol(sym)
            fresh_max = float(cfg["FRESH_MAX"])

            # Time-flat (always)
            if _after_1515():
                _update_row(ws, row_idx, headers, {
                    "exit_time": _now_ist_str(),
                    "exit_spot": spot if spot is not None else "",
                    "pnl_points": _pnl_points(side, entry_spot, spot) if spot is not None else "",
                    "exit_reason": "TIME",
                })
                closed += 1
                continue

            if spot is None:
                # no spot -> can't decide; continue to next
                continue

            # SL / TP
            TP = float(cfg["TP"]); SL = float(cfg["SL"])
            pnl_now = _pnl_points(side, entry_spot, spot)

            # Stop-Loss first (protective)
            if (side == "CE" and (entry_spot - spot) >= SL) or (side == "PE" and (spot - entry_spot) >= SL):
                _update_row(ws, row_idx, headers, {
                    "exit_time": _now_ist_str(),
                    "exit_spot": spot,
                    "pnl_points": pnl_now,
                    "exit_reason": "SL",
                })
                closed += 1
                continue

            # Take-Profit
            if (side == "CE" and (spot - entry_spot) >= TP) or (side == "PE" and (entry_spot - spot) >= TP):
                _update_row(ws, row_idx, headers, {
                    "exit_time": _now_ist_str(),
                    "exit_spot": spot,
                    "pnl_points": pnl_now,
                    "exit_reason": "TP",
                })
                closed += 1
                continue

            # Trailing (after trigger)
            trail, _ = _update_trailing(st, dedupe_key, side, entry_spot, spot, cfg)
            if _trail_hit(side, spot, trail):
                _update_row(ws, row_idx, headers, {
                    "exit_time": _now_ist_str(),
                    "exit_spot": spot,
                    "pnl_points": pnl_now,
                    "exit_reason": "TRAIL",
                })
                closed += 1
                continue

            # MV reversal & STALE
            is_stale = _stale_bad(snap, fresh_max)
            mv_bad_streak, stale_streak = _update_mv_stale(st, dedupe_key, side, mv, is_stale)

            if int(mv_bad_streak) >= int(cfg["MV_REV_N"]):
                _update_row(ws, row_idx, headers, {
                    "exit_time": _now_ist_str(),
                    "exit_spot": spot,
                    "pnl_points": pnl_now,
                    "exit_reason": "MV_REVERSAL",
                })
                closed += 1
                continue

            if int(stale_streak) >= int(cfg["STALE_N"]):
                _update_row(ws, row_idx, headers, {
                    "exit_time": _now_ist_str(),
                    "exit_spot": spot,
                    "pnl_points": pnl_now,
                    "exit_reason": "STALE",
                })
                closed += 1
                continue

        except Exception as e:
            log.warning("tp_sl_watcher: row %s error: %s", row_idx, e)

    _save_state(st)
    return closed

# ---------------- Force flat all ----------------
def force_flat_all(reason: str = "TIME") -> int:
    """
    Immediately exits all open paper trades (writes exit_time/reason now).
    """
    try:
        ws = _open_ws("Trades")
        headers = _ensure_headers(ws)
        rows = _get_all_records_with_index(ws)
        spot_val = ""  # unknown right now; keep blank
        closed = 0
        for row_idx, rec in rows:
            if str(rec.get("paper","")) != "1": 
                continue
            if str(rec.get("exit_time","")).strip():
                continue
            entry_spot = rec.get("spot_at_entry") or rec.get("entry_spot") or ""
            side = (rec.get("side") or "").strip().upper()
            try:
                pnl = _pnl_points(side, float(entry_spot), float(spot_val)) if spot_val != "" else ""
            except Exception:
                pnl = ""
            _update_row(ws, row_idx, headers, {
                "exit_time": _now_ist_str(),
                "exit_spot": spot_val,
                "pnl_points": pnl,
                "exit_reason": reason,
            })
            closed += 1
        return closed
    except Exception as e:
        log.warning("force_flat_all error: %s", e)
        return 0
