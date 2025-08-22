# scripts/backtest_runner.py
# -----------------------------------------------------------------------------
# Backtest Runner (date-range) for intraday OC-based strategy
#
# क्या करता है:
#   - Primary source: "Snapshots" शीट (IST date-range filter)
#   - Fallback source: "OC_Live" शीट (जब Snapshots खाली/न मिले)
#   - Final C1..C6 entry rules लागू
#   - Exits: TP / SL / Trailing SL / MV-Reversal / Time (15:15 IST)
#   - Closed trades को "Performance" शीट में append करता है
#
# ENV:
#   BACKTEST_START=YYYY-MM-DD    # inclusive (IST)
#   BACKTEST_END=YYYY-MM-DD      # inclusive (IST)
#   OC_SYMBOL=NIFTY|BANKNIFTY|FINNIFTY  (default NIFTY)
#
# Tunables (symbol-wise fallbacks defined):
#   LEVEL_BUFFER_*, ENTRY_BAND_*, TARGET_MIN_POINTS_*, OI_FLAT_EPS
#   TP_POINTS_*, SL_POINTS_*, TRAIL_TRIGGER_POINTS_*, TRAIL_OFFSET_POINTS_*, MV_REV_CONFIRM
# -----------------------------------------------------------------------------

from __future__ import annotations

import os, json, time, logging
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
    """Parse 'YYYY-MM-DD HH:MM:SS [IST]' → (Y,M,D,h,m,s) in IST."""
    if not s:
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
        t = time.time() + 5.5*3600
        return (int(time.strftime("%Y", time.gmtime(t))),
                int(time.strftime("%m", time.gmtime(t))),
                int(time.strftime("%d", time.gmtime(t))), 12,0,0)

def _date_key(y,m,d) -> str: return f"{y:04d}-{m:02d}-{d:02d}"
def _ist_minutes(hh,mm) -> int: return hh*60 + mm
def _in_no_trade_window_ist(hh:int, mm:int) -> bool:
    mins = _ist_minutes(hh,mm)
    if 9*60+15 <= mins < 9*60+30: return True
    if 14*60+45 <= mins < 15*60+15: return True
    return False
def _after_1515(hh:int, mm:int) -> bool: return (hh,mm) >= (15,15)

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

# -------- tolerant numeric/string helpers --------
def _num(x) -> Optional[float]:
    try:
        if x in (None,"","—"): return None
        return float(str(x).replace(",","").strip())
    except Exception:
        return None

def _str(x) -> str:
    return "" if x is None else str(x)

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
    if m in _ALLOWED_CE_MV: return "CE"
    if m in _ALLOWED_PE_MV: return "PE"
    return None

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

# ---------------- Exits config ----------------
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

# ---------------- Source loaders ----------------
COLCAND = {
    "ts": ["ts","asof","As-of","AsOf","timestamp","time"],
    "symbol": ["symbol","Symbol"],
    "expiry": ["expiry","exp","Expiry","Exp"],
    "spot": ["spot","Spot"],
    "s1": ["s1","S1"], "s2": ["s2","S2"],
    "r1": ["r1","R1"], "r2": ["r2","R2"],
    "pcr": ["pcr","PCR"],
    "mp": ["max_pain","MP","maxPain","MaxPain"],
    "ce": ["ce_oi_delta","CEΔ","ceDelta","ce_oi","CE_OIΔ"],
    "pe": ["pe_oi_delta","PEΔ","peDelta","pe_oi","PE_OIΔ"],
    "mv": ["mv","MV","view","View"],
}

def _row_pick(d: Dict[str,Any], names: List[str], default=None):
    for n in names:
        if n in d and d[n] not in (None, ""):
            return d[n]
    return default

def _normalize_rows(recs: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    out: List[Dict[str,Any]] = []
    for r in recs:
        row = {
            "ts": _row_pick(r, COLCAND["ts"], ""),
            "symbol": _str(_row_pick(r, COLCAND["symbol"], "")),
            "expiry": _str(_row_pick(r, COLCAND["expiry"], "")),
            "spot": _num(_row_pick(r, COLCAND["spot"], None)),
            "s1": _num(_row_pick(r, COLCAND["s1"], None)),
            "s2": _num(_row_pick(r, COLCAND["s2"], None)),
            "r1": _num(_row_pick(r, COLCAND["r1"], None)),
            "r2": _num(_row_pick(r, COLCAND["r2"], None)),
            "pcr": _num(_row_pick(r, COLCAND["pcr"], None)),
            "mp": _num(_row_pick(r, COLCAND["mp"], None)),
            "ce": _num(_row_pick(r, COLCAND["ce"], None)),
            "pe": _num(_row_pick(r, COLCAND["pe"], None)),
            "mv": (_row_pick(r, COLCAND["mv"], "") or "").strip().lower(),
        }
        out.append(row)
    return out

def _load_sheet_rows(name: str) -> List[Dict[str,Any]]:
    ws = _open_ws(name)
    recs = ws.get_all_records() or []
    return _normalize_rows(recs)

def _get_snapshots_any() -> Tuple[str, List[Dict[str,Any]]]:
    """Returns (source_name, rows). Tries Snapshots, else OC_Live."""
    try:
        snaps = _load_sheet_rows("Snapshots")
    except Exception as e:
        log.warning("Snapshots read failed: %s", e)
        snaps = []
    if snaps:
        return "Snapshots", snaps

    log.info("Snapshots empty → falling back to OC_Live")
    try:
        live = _load_sheet_rows("OC_Live")
    except Exception as e:
        log.warning("OC_Live read failed: %s", e)
        live = []
    return ("OC_Live" if live else "none"), live

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

    source_name, snaps = _get_snapshots_any()
    if not snaps:
        raise RuntimeError("Snapshots/OC_Live both empty or not accessible")

    # normalize + filter
    items: List[Tuple[str,Dict[str,Any]]] = []
    for r in snaps:
        ts = _str(r.get("ts") or "")
        y,m,d,hh,mm,ss = _parse_ts_ist(ts or "")
        datek = _date_key(y,m,d)
        if datek < start_key or datek > end_key:
            continue
        rsym = (r.get("symbol") or "").upper()
        if rsym and rsym != sym:
            continue
        row = {
            "date": datek, "hh": hh, "mm": mm, "ss": ss,
            "spot": _num(r.get("spot")), "s1": _num(r.get("s1")), "s2": _num(r.get("s2")),
            "r1": _num(r.get("r1")), "r2": _num(r.get("r2")),
            "pcr": _num(r.get("pcr")), "mp": _num(r.get("mp")),
            "ce": _num(r.get("ce")), "pe": _num(r.get("pe")),
            "mv":  (r.get("mv") or "").strip().lower(),
            "asof": ts,
        }
        items.append( (f"{datek} {hh:02d}:{mm:02d}:{ss:02d}", row) )

    items.sort(key=lambda x: x[0])
    if not items:
        raise RuntimeError(f"No rows in {source_name} for given date range/symbol {sym}")

    log.info("Backtest source: %s (rows=%d)", source_name, len(items))

    # state per day
    open_trade: Optional[Dict[str,Any]] = None
    last_date = None
    dedupe_today: set = set()
    closed_count = 0

    cfg_exit = _cfg_exits(sym)

    def flush_day(datek: str):
        nonlocal open_trade, dedupe_today
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
        datek = row["date"]; hh,mm,ss = row["hh"], row["mm"], row["ss"]
        spot = row["spot"]; s1=row["s1"]; s2=row["s2"]; r1=row["r1"]; r2=row["r2"]
        ce=row["ce"]; pe=row["pe"]; mv=row["mv"]; mp=row["mp"]; pcr=row["pcr"]
        shifted = _shift_levels(s1,s2,r1,r2, buf)

        if last_date is None:
            last_date = datek
        if datek != last_date:
            flush_day(last_date)
            last_date = datek

        # update open trade & evaluate exits first
        if open_trade:
            ot = open_trade
            if spot is not None:
                ot["last_spot"] = spot

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
                open_trade = None; closed_count += 1
                continue

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
                    open_trade = None; closed_count += 1
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
                    open_trade = None; closed_count += 1
                    continue

                # trailing
                tr_trig = float(cfg_exit["TRL_TRIG"]); tr_off = float(cfg_exit["TRL_OFF"])
                trail = ot.get("trail")
                if ot["side"]=="CE":
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
                        open_trade = None; closed_count += 1
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
                        open_trade = None; closed_count += 1
                        continue

                # MV reversal
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
                        open_trade = None; closed_count += 1
                        continue
                else:
                    ot["mv_bad_streak"] = 0

        # search entry only if no open trade
        if open_trade:
            continue

        # ENTRY eval (C1..C6 simplified to needed checks for backtest)
        side = _pick_side(mv)
        if side is None:
            continue
        trig_name, trig_price = _nearest_trigger(spot, side, shifted)
        if not (trig_name and trig_price is not None and spot is not None):
            continue

        within = abs(float(spot) - float(trig_price)) <= float(band)  # C1
        if not within: continue
        if _in_no_trade_window_ist(hh,mm):  # C4 time
            continue

        # C3: OI delta patterns
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
        if not c3_ok: continue

        space = _space_points(side, trig_name, float(trig_price), s1,s2,r1,r2)  # C6
        tgt_req = _sym_env(sym, "TARGET_MIN_POINTS", 30.0)
        if space is None or float(space) < float(tgt_req):
            continue

        # per-level-per-day dedupe (C5 subset)
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

    if last_date is not None:
        flush_day(last_date)

    log.info("Backtest finished. Closed trades appended to Performance.")
    return

# ---------------- CLI ----------------
if __name__ == "__main__":
    run_backtest()
