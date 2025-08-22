# scripts/backtest_runner.py
# -----------------------------------------------------------------------------
# Backtest Runner (date-range) for intraday OC-based strategy
#
# क्या करता है:
#   - "Snapshots" शीट से दिए गए date-range (IST) का historical snapshot stream पढ़ता है
#   - आपके final C1..C6 entry rules (CE @ S1*/S2*, PE @ R1*/R2*) apply करता है
#   - Exits: TP / SL / Trailing SL / MV-Reversal / Time (15:15 IST)
#   - हर closed trade को "Performance" शीट में append करता है
#
# Inputs (ENV):
#   BACKTEST_START=YYYY-MM-DD    # inclusive (IST). e.g., 2025-07-01
#   BACKTEST_END=YYYY-MM-DD      # inclusive (IST). e.g., 2025-07-31
#   OC_SYMBOL=NIFTY|BANKNIFTY|FINNIFTY  (default NIFTY)
#
#   LEVEL_BUFFER_*     NIFTY=12, BANKNIFTY=30, FINNIFTY=15 (fallback LEVEL_BUFFER=12)
#   ENTRY_BAND_*       NIFTY=3,  BANKNIFTY=8,  FINNIFTY=4  (fallback ENTRY_BAND=3)
#   TARGET_MIN_POINTS_* (RR check) NIFTY=30, BANKNIFTY=80, FINNIFTY=50 (fallback 30)
#   OI_FLAT_EPS=0      # |Δ|<=eps => flat
#
#   # Exit config (same semantics as tp_sl_watcher)
#   TP_POINTS_*            NIFTY=40, BANKNIFTY=100, FINNIFTY=60 (fallback TP_POINTS=40)
#   SL_POINTS_*            NIFTY=20, BANKNIFTY=60,  FINNIFTY=35 (fallback SL_POINTS=20)
#   TRAIL_TRIGGER_POINTS_* NIFTY=25, BANKNIFTY=70,  FINNIFTY=45 (fallback 25)
#   TRAIL_OFFSET_POINTS_*  NIFTY=15, BANKNIFTY=40,  FINNIFTY=25 (fallback 15)
#   MV_REV_CONFIRM=2
#
# Notes:
#   - यह runner सिर्फ backtest के लिए है: कोई live refresh/Telegram नहीं।
#   - Snapshots शीट की columns tolerant हैं: ['ts' या 'asof'], spot/s1/s2/r1/r2/pcr/max_pain/ce_oi_delta/pe_oi_delta/mv, symbol, expiry, source.
#   - Time windows enforce: 09:15–09:30 और 14:45–15:15 no-entry; exit at 15:15.
#   - Per-level per-day = max 1 attempt (dedupe)। Daily cap apply नहीं — backtest aggregate के लिए।
#   - Performance शीट पर headers auto-ensure होंगे।
# -----------------------------------------------------------------------------

from __future__ import annotations

import os, json, time, math, logging
from typing import Any, Dict, List, Tuple, Optional

try:
    import gspread  # type: ignore
except Exception:
    gspread = None  # type: ignore

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------- ENV helpers ----------------
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
def _parse_ts_ist(s: str) -> Tuple[int,int,int,int,int,int]:
    """
    Parse common forms: 'YYYY-MM-DD HH:MM:SS IST' or 'YYYY-MM-DD HH:MM:SS'
    Returns (Y,M,D,h,m,s) in IST.
    """
    if not s:  # fallback: now
        t = time.time() + 5.5*3600
        return (int(time.strftime("%Y", time.gmtime(t))),
                int(time.strftime("%m", time.gmtime(t))),
                int(time.strftime("%d", time.gmtime(t))),
                int(time.strftime("%H", time.gmtime(t))),
                int(time.strftime("%M", time.gmtime(t))),
                int(time.strftime("%S", time.gmtime(t))))
    s = s.replace("IST","").strip()
    try:
        date, clock = s.split()
        y,m,d = [int(x) for x in date.split("-")]
        hh,mm,ss = [int(x) for x in clock.split(":")]
        return (y,m,d,hh,mm,ss)
    except Exception:
        # last resort: today @ 12:00
        t = time.time() + 5.5*3600
        return (int(time.strftime("%Y", time.gmtime(t))),
                int(time.strftime("%m", time.gmtime(t))),
                int(time.strftime("%d", time.gmtime(t))), 12,0,0)

def _date_key(y,m,d) -> str:
    return f"{y:04d}-{m:02d}-{d:02d}"

def _ist_minutes(hh,mm) -> int:
    return hh*60 + mm

def _in_no_trade_window_ist(hh:int, mm:int) -> bool:
    mins = _ist_minutes(hh,mm)
    if 9*60+15 <= mins < 9*60+30: return True
    if 14*60+45 <= mins < 15*60+15: return True
    return False

def _after_1515(hh:int, mm:int) -> bool:
    return (hh,mm) >= (15,15)

# ---------------- Sheets IO ----------------
REQ_HEADERS_PERF = [
    "date","symbol","side","trigger","trigger_price",
    "entry_time","entry_spot","exit_time","exit_spot",
    "pnl_points","exit_reason","mv_at_entry","notes"
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
        return sh.add_worksheet(title=name, rows=2000, cols=26)

def _ensure_perf_headers(ws) -> List[str]:
    try:
        hdr = ws.row_values(1)
    except Exception:
        hdr = []
    if not hdr:
        ws.update("A1", [REQ_HEADERS_PERF])
        return REQ_HEADERS_PERF
    need = [h for h in REQ_HEADERS_PERF if h not in hdr]
    if need:
        ws.update("A1", [hdr + need])
        return hdr + need
    return hdr

def _append_perf_row(row: Dict[str, Any]) -> None:
    ws = _open_ws("Performance")
    hdr = _ensure_perf_headers(ws)
    vals = [row.get(h,"") for h in hdr]
    ws.append_row(vals, value_input_option="RAW")

def _get_snapshots() -> List[Dict[str,Any]]:
    ws = _open_ws("Snapshots")
    recs = ws.get_all_records() or []
    return recs

# ---------------- numeric helpers ----------------
def _num(x) -> Optional[float]:
    try:
        if x in (None,"","—"): return None
        return float(str(x).replace(",","").strip())
    except Exception:
        return None

def _fmt(x, d=2):
    if x is None: return "—"
    try: return f"{float(x):.{d}f}"
    except Exception: return str(x)

# ---------------- Strategy helpers ----------------
_ALLOWED_CE_MV = {"bullish","big_move"}
_ALLOWED_PE_MV = {"bearish","strong_bearish"}

def _shift_levels(s1,s2,r1,r2, buf) -> Dict[str, Optional[float]]:
    def sh(v, up: bool):
        if v is None or buf is None: return None
        return float(v) + float(buf) if up else float(v) - float(buf)
    return {"S1*": sh(s1,False), "S2*": sh(s2,False), "R1*": sh(r1,True), "R2*": sh(r2,True)}

def _pick_side(mv: str) -> Optional[str]:
    m = (mv or "").strip().lower()
    if m in _allowed_ce(): return "CE"
    if m in _allowed_pe(): return "PE"
    return None

def _allowed_ce(): return _ALLOWED_CE_MV
def _allowed_pe(): return _ALLOWED_PE_MV

def _nearest_trigger(spot: Optional[float], side: Optional[str], shifted: Dict[str, Optional[float]]) -> Tuple[Optional[str], Optional[float]]:
    if spot is None or side is None: return None, None
    pool = ["S1*","S2*"] if side=="CE" else ["R1*","R2*"]
    best = None; bestp=None; bestd=None
    for name in pool:
        tp = shifted.get(name)
        if tp is None: continue
        d = abs(float(spot) - float(tp))
        if bestd is None or d<bestd:
            best, bestp, bestd = name, tp, d
    return best, bestp

def _space_points(side: str, trig_name: str, trig_price: float, s1,s2,r1,r2) -> Optional[float]:
    try:
        if side=="CE":
            if trig_name=="S1*": return (r1 - trig_price) if (r1 is not None) else None
            if trig_name=="S2*": return (s1 - trig_price) if (s1 is not None) else None
        else:
            if trig_name=="R1*": return (trig_price - s1) if (s1 is not None) else None
            if trig_name=="R2*": return (trig_price - r1) if (r1 is not None) else None
    except Exception:
        return None
    return None

def _oi_class(x: Optional[float], eps: float) -> str:
    if x is None: return "na"
    if x > eps: return "up"
    if x < -eps: return "down"
    return "flat"

# ---------------- Exits logic (inline simulation) ----------------
def _cfg_exits(sym: str) -> Dict[str,float]:
    s = (sym or "").upper()
    return {
        "TP":  _sym_env(s, "TP_POINTS", 40.0 if s=="NIFTY" else (100.0 if s=="BANKNIFTY" else 60.0)),
        "SL":  _sym_env(s, "SL_POINTS", 20.0 if s=="NIFTY" else (60.0  if s=="BANKNIFTY" else 35.0)),
        "TRL_TRIG": _sym_env(s, "TRAIL_TRIGGER_POINTS", 25.0 if s=="NIFTY" else (70.0 if s=="BANKNIFTY" else 45.0)),
        "TRL_OFF":  _sym_env(s, "TRAIL_OFFSET_POINTS",  15.0 if s=="NIFTY" else (40.0 if s=="BANKNIFTY" else 25.0)),
        "MV_REV_N": float(_env("MV_REV_CONFIRM","2") or "2"),
    }

def _pnl_points(side: str, entry_spot: float, exit_spot: float) -> float:
    return (exit_spot - entry_spot) if side=="CE" else (entry_spot - exit_spot)

# ---------------- Core backtest ----------------
def run_backtest() -> None:
    sym = (_env("OC_SYMBOL","NIFTY") or "NIFTY").upper()
    buf = _sym_env(sym, "LEVEL_BUFFER", 12.0)
    band = _sym_env(sym, "ENTRY_BAND", 3.0)
    tgt_req = _sym_env(sym, "TARGET_MIN_POINTS", 30.0)
    oi_eps = float(_env("OI_FLAT_EPS","0"))

    y0, m0, d0 = [int(x) for x in (_env("BACKTEST_START","") or _date_key(*_parse_ts_ist(""))).split("-")]
    y1, m1, d1 = [int(x) for x in (_env("BACKTEST_END","")   or _date_key(*_parse_ts_ist(""))).split("-")]
    start_key = _date_key(y0,m0,d0)
    end_key   = _date_key(y1,m1,d1)

    snaps = _get_snapshots()
    if not snaps:
        raise RuntimeError("Snapshots sheet empty or not accessible")

    # normalize and filter by date range & symbol
    items: List[Tuple[str,Dict[str,Any]]] = []
    for r in snaps:
        ts = r.get("ts") or r.get("asof") or ""
        y,m,d,hh,mm,ss = _parse_ts_ist(str(ts))
        datek = _date_key(y,m,d)
        if datek < start_key or datek > end_key:
            continue
        rsym = str(r.get("symbol") or r.get("Symbol") or "").upper()
        if rsym and rsym != sym:
            continue
        row = {
            "date": datek, "hh": hh, "mm": mm, "ss": ss,
            "spot": _num(r.get("spot") or r.get("Spot")),
            "s1": _num(r.get("s1") or r.get("S1")), "s2": _num(r.get("s2") or r.get("S2")),
            "r1": _num(r.get("r1") or r.get("R1")), "r2": _num(r.get("r2") or r.get("R2")),
            "pcr": _num(r.get("pcr") or r.get("PCR")),
            "mp": _num(r.get("max_pain") or r.get("MP") or r.get("maxPain")),
            "ce": _num(r.get("ce_oi_delta") or r.get("CEΔ") or r.get("ceDelta")),
            "pe": _num(r.get("pe_oi_delta") or r.get("PEΔ") or r.get("peDelta")),
            "mv":  str(r.get("mv") or r.get("MV") or "").strip().lower(),
            "asof": ts,
        }
        items.append( (f"{datek} {hh:02d}:{mm:02d}:{ss:02d}", row) )

    # sort ascending by ts
    items.sort(key=lambda x: x[0])

    if not items:
        raise RuntimeError("No snapshots found in given date range / symbol")

    # state per day
    open_trade: Optional[Dict[str,Any]] = None
    last_date = None
    dedupe_today: set = set()
    closed_count = 0

    cfg_exit = _cfg_exits(sym)

    def flush_day(datek: str):
        nonlocal open_trade, dedupe_today
        # force close any open trade at day boundary (TIME)
        if open_trade:
            ot = open_trade
            pnl = _pnl_points(ot["side"], ot["entry_spot"], ot["last_spot"])
            _append_perf_row({
                "date": ot["date"], "symbol": sym, "side": ot["side"],
                "trigger": ot["trigger"], "trigger_price": ot["trigger_price"],
                "entry_time": ot["entry_ts"], "entry_spot": ot["entry_spot"],
                "exit_time": f"{ot['date']} 15:15:00 IST",
                "exit_spot": ot["last_spot"], "pnl_points": f"{pnl:.2f}",
                "exit_reason": "TIME", "mv_at_entry": ot["mv"], "notes": "day-end"
            })
        open_trade = None
        dedupe_today = set()

    for key, row in items:
        datek = row["date"]
        hh,mm,ss = row["hh"], row["mm"], row["ss"]
        spot = row["spot"]; s1=row["s1"]; s2=row["s2"]; r1=row["r1"]; r2=row["r2"]
        ce=row["ce"]; pe=row["pe"]; mv=row["mv"]; mp=row["mp"]; pcr=row["pcr"]
        shifted = _shift_levels(s1,s2,r1,r2, buf)

        if last_date is None:
            last_date = datek
        if datek != last_date:
            flush_day(last_date)
            last_date = datek

        # update any open trade last_spot & evaluate exits first
        if open_trade:
            ot = open_trade
            open_trade["last_spot"] = spot if spot is not None else ot.get("last_spot")

            # time flat at/after 15:15
            if _after_1515(hh,mm):
                pnl = _pnl_points(ot["side"], ot["entry_spot"], spot)
                _append_perf_row({
                    "date": ot["date"], "symbol": sym, "side": ot["side"],
                    "trigger": ot["trigger"], "trigger_price": ot["trigger_price"],
                    "entry_time": ot["entry_ts"], "entry_spot": ot["entry_spot"],
                    "exit_time": f"{datek} {hh:02d}:{mm:02d}:{ss:02d} IST",
                    "exit_spot": spot, "pnl_points": f"{pnl:.2f}",
                    "exit_reason": "TIME", "mv_at_entry": ot["mv"], "notes": ""
                })
                open_trade = None
                closed_count += 1
                # day end; continue to next tick
                continue

            # SL / TP
            TP = float(cfg_exit["TP"]); SL = float(cfg_exit["SL"])
            if spot is not None:
                fav = (spot - ot["entry_spot"]) if ot["side"]=="CE" else (ot["entry_spot"] - spot)
                adv = -fav
                if adv >= SL:
                    pnl = _pnl_points(ot["side"], ot["entry_spot"], spot)
                    _append_perf_row({
                        "date": ot["date"], "symbol": sym, "side": ot["side"],
                        "trigger": ot["trigger"], "trigger_price": ot["trigger_price"],
                        "entry_time": ot["entry_ts"], "entry_spot": ot["entry_spot"],
                        "exit_time": f"{datek} {hh:02d}:{mm:02d}:{ss:02d} IST",
                        "exit_spot": spot, "pnl_points": f"{pnl:.2f}",
                        "exit_reason": "SL", "mv_at_entry": ot["mv"], "notes": ""
                    })
                    open_trade = None
                    closed_count += 1
                    continue
                if fav >= TP:
                    pnl = _pnl_points(ot["side"], ot["entry_spot"], spot)
                    _append_perf_row({
                        "date": ot["date"], "symbol": sym, "side": ot["side"],
                        "trigger": ot["trigger"], "trigger_price": ot["trigger_price"],
                        "entry_time": ot["entry_ts"], "entry_spot": ot["entry_spot"],
                        "exit_time": f"{datek} {hh:02d}:{mm:02d}:{ss:02d} IST",
                        "exit_spot": spot, "pnl_points": f"{pnl:.2f}",
                        "exit_reason": "TP", "mv_at_entry": ot["mv"], "notes": ""
                    })
                    open_trade = None
                    closed_count += 1
                    continue

                # Trailing
                tr_trig = float(cfg_exit["TRL_TRIG"]); tr_off = float(cfg_exit["TRL_OFF"])
                trail = ot.get("trail")
                if ot["side"]=="CE":
                    # set/update trail when favorable >= trigger
                    if fav >= tr_trig:
                        new_tr = spot - tr_off
                        trail = max(trail, new_tr) if trail is not None else new_tr
                        ot["trail"] = trail
                    if trail is not None and spot <= trail:
                        pnl = _pnl_points(ot["side"], ot["entry_spot"], spot)
                        _append_perf_row({
                            "date": ot["date"], "symbol": sym, "side": ot["side"],
                            "trigger": ot["trigger"], "trigger_price": ot["trigger_price"],
                            "entry_time": ot["entry_ts"], "entry_spot": ot["entry_spot"],
                            "exit_time": f"{datek} {hh:02d}:{mm:02d}:{ss:02d} IST",
                            "exit_spot": spot, "pnl_points": f"{pnl:.2f}",
                            "exit_reason": "TRAIL", "mv_at_entry": ot["mv"], "notes": ""
                        })
                        open_trade = None
                        closed_count += 1
                        continue
                else:
                    if fav >= tr_trig:
                        new_tr = spot + tr_off
                        trail = min(trail, new_tr) if trail is not None else new_tr
                        ot["trail"] = trail
                    if trail is not None and spot >= trail:
                        pnl = _pnl_points(ot["side"], ot["entry_spot"], spot)
                        _append_perf_row({
                            "date": ot["date"], "symbol": sym, "side": ot["side"],
                            "trigger": ot["trigger"], "trigger_price": ot["trigger_price"],
                            "entry_time": ot["entry_ts"], "entry_spot": ot["entry_spot"],
                            "exit_time": f"{datek} {hh:02d}:{mm:02d}:{ss:02d} IST",
                            "exit_spot": spot, "pnl_points": f"{pnl:.2f}",
                            "exit_reason": "TRAIL", "mv_at_entry": ot["mv"], "notes": ""
                        })
                        open_trade = None
                        closed_count += 1
                        continue

                # MV reversal (consecutive opposite snapshots)
                mv_bad = ((ot["side"]=="CE" and mv in {"bearish","strong_bearish"}) or
                          (ot["side"]=="PE" and mv in {"bullish","big_move"}))
                if mv_bad:
                    ot["mv_bad_streak"] = int(ot.get("mv_bad_streak",0)) + 1
                    if ot["mv_bad_streak"] >= int(cfg_exit["MV_REV_N"]):
                        pnl = _pnl_points(ot["side"], ot["entry_spot"], spot)
                        _append_perf_row({
                            "date": ot["date"], "symbol": sym, "side": ot["side"],
                            "trigger": ot["trigger"], "trigger_price": ot["trigger_price"],
                            "entry_time": ot["entry_ts"], "entry_spot": ot["entry_spot"],
                            "exit_time": f"{datek} {hh:02d}:{mm:02d}:{ss:02d} IST",
                            "exit_spot": spot, "pnl_points": f"{pnl:.2f}",
                            "exit_reason": "MV_REVERSAL", "mv_at_entry": ot["mv"], "notes": ""
                        })
                        open_trade = None
                        closed_count += 1
                        continue
                else:
                    ot["mv_bad_streak"] = 0

        # If trade already open, skip entry search for this tick
        if open_trade:
            continue

        # ENTRY evaluation (C1..C6)
        side = _pick_side(mv)
        if side is None:
            continue  # C2 fail

        trig_name, trig_price = _nearest_trigger(spot, side, shifted)
        if not (trig_name and trig_price is not None and spot is not None):
            continue

        # C1: level band
        within = abs(float(spot) - float(trig_price)) <= float(band)
        if not within:
            continue

        # C4: time window
        if _in_no_trade_window_ist(hh,mm):
            continue

        # C3: OI pattern
        ce_sig = _oi_class(ce, float(_env("OI_FLAT_EPS","0")))
        pe_sig = _oi_class(pe, float(_env("OI_FLAT_EPS","0")))
        if side=="CE":
            c3_ok = ((ce_sig=="down" and pe_sig=="up") or
                     (ce_sig=="down" and pe_sig=="down") or
                     ((ce_sig in {"flat","down"}) and pe_sig=="up"))
        else:
            c3_ok = ((ce_sig=="up" and pe_sig=="down") or
                     (ce_sig=="down" and pe_sig=="down") or
                     (ce_sig=="up" and pe_sig in {"flat","down"}))
        if not c3_ok:
            continue

        # C6: space
        space = _space_points(side, trig_name, float(trig_price), s1,s2,r1,r2)
        if space is None or float(space) < float(tgt_req):
            continue

        # C5: dedupe per-level-per-day
        dkey = f"{datek}|{side}|{trig_name}"
        if dkey in dedupe_today:
            continue

        # ENTRY confirmed
        dedupe_today.add(dkey)
        open_trade = {
            "date": datek, "side": side, "trigger": trig_name, "trigger_price": float(trig_price),
            "entry_ts": f"{datek} {hh:02d}:{mm:02d}:{ss:02d} IST",
            "entry_spot": float(spot), "last_spot": float(spot),
            "trail": None, "mv": mv, "mv_bad_streak": 0
        }
        # (Performance row only when we exit — as per your live flow)

    # end loop
    if last_date is not None:
        flush_day(last_date)
    log.info("Backtest finished. Closed trades appended to Performance.")
    return

# ---------------- CLI ----------------
if __name__ == "__main__":
    run_backtest()
