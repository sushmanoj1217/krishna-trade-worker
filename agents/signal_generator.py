# agents/signal_generator.py
# -----------------------------------------------------------------------------
# Signal generator with EXEC_GATES + real QUOTES_SPREAD gate:
#   - C1..C6 enforcement
#   - Dedupe per-level-per-day, Daily cap, Freshness windows
#   - Spread/Liquidity gate via integrations.quotes_spread (bid/ask from snapshot chain)
#   - (Optional) Velocity, Exposure caps (keep MAX_* = 0 to disable)
# -----------------------------------------------------------------------------

from __future__ import annotations

import os, json, time, logging
from typing import Any, Dict, Optional, Tuple, List

try:
    import gspread  # type: ignore
except Exception:
    gspread = None  # type: ignore

log = logging.getLogger(__name__)

# ---------------- Env & helpers ----------------
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

def _to_float(x):
    try:
        if x in (None, "", "—"): return None
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None

def _fmt(x, digits=2):
    if x is None: return "—"
    try: return f"{float(x):.{digits}f}"
    except Exception: return str(x)

def _now_ist_tuple() -> Tuple[int,int,int,int,int,int]:
    t = time.time() + 5.5*3600
    return (int(time.strftime("%Y", time.gmtime(t))),
            int(time.strftime("%m", time.gmtime(t))),
            int(time.strftime("%d", time.gmtime(t))),
            int(time.strftime("%H", time.gmtime(t))),
            int(time.strftime("%M", time.gmtime(t))),
            int(time.strftime("%S", time.gmtime(t))))

def _in_no_trade_window_ist() -> bool:
    y,m,d,hh,mm,ss = _now_ist_tuple()
    mins = hh*60 + mm
    if 9*60+15 <= mins < 9*60+30: return True
    if 14*60+45 <= mins < 15*60+15: return True
    return False

# ---------------- Shift & side picking ----------------
_ALLOWED_CE_MV = {"bullish", "big_move"}
_ALLOWED_PE_MV = {"bearish", "strong_bearish"}

def _shift_levels(s1, s2, r1, r2, buf) -> Dict[str, Optional[float]]:
    def sh(v, up: bool):
        if v is None or buf is None: return None
        return float(v) + float(buf) if up else float(v) - float(buf)
    return {
        "S1*": sh(s1, up=False),
        "S2*": sh(s2, up=False),
        "R1*": sh(r1, up=True),
        "R2*": sh(r2, up=True),
    }

def _pick_side_and_triggers(mv: str) -> Tuple[Optional[str], List[str]]:
    m = (mv or "").strip().lower()
    if m in _ALLOWED_CE_MV: return "CE", ["S1*", "S2*"]
    if m in _ALLOWED_PE_MV: return "PE", ["R1*", "R2*"]
    return None, []

def _nearest_trigger(spot: Optional[float], triggers_ordered: List[str], sh: Dict[str, Optional[float]]) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    if spot is None: return None, None, None
    best = None; bestp = None; bestd = None
    for name in triggers_ordered:
        tp = sh.get(name)
        if tp is None: continue
        d = abs(float(spot) - float(tp))
        if bestd is None or d < bestd:
            best, bestp, bestd = name, tp, d
    return best, bestp, bestd

def _space_points(side: str, trig_name: str, trig_price: float, s1, s2, r1, r2) -> Optional[float]:
    try:
        if side == "CE":
            if trig_name == "S1*":
                if r1 is None: return None
                return float(r1) - float(trig_price)
            if trig_name == "S2*":
                if s1 is None: return None
                return float(s1) - float(trig_price)
        if side == "PE":
            if trig_name == "R1*":
                if s1 is None: return None
                return float(trig_price) - float(s1)
            if trig_name == "R2*":
                if r1 is None: return None
                return float(trig_price) - float(r1)
    except Exception:
        return None
    return None

# ---------------- Velocity tracker ----------------
_VELO_STATE = {"last_ts": None, "last_spot": None}

def _velocity_ok(spot: Optional[float], max_pps: float) -> Tuple[bool, str]:
    if spot is None: return True, "n/a"
    now = time.time()
    last_ts = _VELO_STATE["last_ts"]
    last_spot = _VELO_STATE["last_spot"]
    _VELO_STATE["last_ts"] = now
    _VELO_STATE["last_spot"] = spot
    if last_ts is None or last_spot is None:
        return True, "warmup"
    dt = max(1e-6, now - last_ts)
    pps = abs(float(spot) - float(last_spot)) / dt
    ok = pps <= max_pps
    return ok, f"speed {pps:.2f}≤{max_pps:.2f}"

# ---------------- Sheets IO ----------------
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

def _append_signal_row(row: Dict[str, Any]) -> None:
    try:
        ws = _open_ws("Signals")
        headers = [
            "ts","symbol","side","mv","trigger","trigger_price","spot",
            "s1","s2","r1","r2","pcr","max_pain","ce_oi_delta","pe_oi_delta",
            "c1","c2","c3","c4","c5","c6","reasons","source","asof","age_sec","dedupe_key"
        ]
        values = [row.get(h,"") for h in headers]
        ws.append_row(values, value_input_option="RAW")
    except Exception as e:
        log.warning("Signals append failed: %s", e)

def _today_ist_str() -> str:
    y,m,d,hh,mm,ss = _now_ist_tuple()
    return f"{y:04d}-{m:02d}-{d:02d}"

def _count_trades_today() -> int:
    try:
        ws = _open_ws("Trades")
        recs = ws.get_all_records() or []
        if not recs: return 0
        today = _today_ist_str()
        n=0
        for r in recs:
            s = str(r.get("entry_time") or r.get("ts") or r.get("date") or "")
            if today in s:
                n+=1
        return n
    except Exception as e:
        log.warning("count_trades_today failed: %s", e)
        return 0

# ---------------- Local dedupe cache ----------------
def _dedupe_path() -> str:
    return f"/tmp/sg_dedupe.{_today_ist_str()}.json"

def _load_cache() -> set:
    p = _dedupe_path()
    try:
        with open(p,"r") as f:
            return set(json.load(f))
    except Exception:
        return set()

def _save_cache(s: set) -> None:
    p = _dedupe_path()
    try:
        with open(p,"w") as f:
            json.dump(sorted(list(s)), f)
    except Exception:
        pass

# ---------------- Exposure helpers (keep OFF with MAX_* = 0) ----------------
def _lot_size(sym: str) -> int:
    try:
        v = int(_sym_env(sym, "LOT_SIZE", 1))
        return max(1, v)
    except Exception:
        return 1

def _premium_estimate_pts(sym: str) -> float:
    s = (sym or "").upper()
    default = 50.0 if s=="NIFTY" else (150.0 if s=="BANKNIFTY" else 70.0)
    return _sym_env(s, "ENTRY_PREMIUM_POINTS", default)

def _current_portfolio_exposure(sym: str) -> float:
    try:
        ws = _open_ws("Trades")
        recs = ws.get_all_records() or []
    except Exception:
        return 0.0
    est = _premium_estimate_pts(sym)
    lot = _lot_size(sym)
    exp = 0.0
    for r in recs:
        if str(r.get("paper","")) != "1": 
            continue
        if str(r.get("exit_time","")).strip():
            continue
        try:
            q = int(r.get("qty") or 0)
        except Exception:
            q = 0
        exp += q * est * lot
    return float(exp)

# ---------------- Core evaluation ----------------
async def generate_once() -> Dict[str, Any]:
    from analytics import oc_refresh  # late import
    # quotes integration (late import to avoid hard failure)
    try:
        from integrations import quotes_spread as qsp  # type: ignore
    except Exception:
        qsp = None  # type: ignore

    result: Dict[str, Any] = {
        "eligible": False, "side": None, "trigger_name": None, "trigger_price": None,
        "c": {}, "reasons": {}, "dedupe_key": None, "snapshot": {}
    }

    # Ensure fresh snapshot
    try:
        await oc_refresh.refresh_once()
    except Exception as e:
        log.warning("generate_once: refresh error: %s", e)

    snap = oc_refresh.get_snapshot() or {}
    result["snapshot"] = snap

    sym  = (snap.get("symbol") or _env("OC_SYMBOL","NIFTY")).upper()
    mv   = (snap.get("mv") or "").strip().lower()
    spot = _to_float(snap.get("spot"))
    s1   = _to_float(snap.get("s1")); s2 = _to_float(snap.get("s2"))
    r1   = _to_float(snap.get("r1")); r2 = _to_float(snap.get("r2"))
    pcr  = _to_float(snap.get("pcr")); mp = _to_float(snap.get("max_pain"))
    ce_d = _to_float(snap.get("ce_oi_delta")); pe_d = _to_float(snap.get("pe_oi_delta"))
    src  = snap.get("source") or "—"
    asof = snap.get("asof") or ""
    age  = int(snap.get("age_sec") or 0)
    stale= bool(snap.get("stale"))

    buf  = _sym_env(sym, "LEVEL_BUFFER", 12.0)
    band = _sym_env(sym, "ENTRY_BAND", 3.0)
    tgt_req = _sym_env(sym, "TARGET_MIN_POINTS", 30.0)
    fresh_max = int(float(_env("OC_FRESH_MAX_AGE_SEC","90")))
    oi_eps = float(_env("OI_FLAT_EPS","0"))

    # Shifted levels
    shifted = _shift_levels(s1, s2, r1, r2, buf)

    # ---- C2: MV allow-list ----
    side, trigger_list = _pick_side_and_triggers(mv)
    c2_ok = side is not None
    result["c"]["C2"] = c2_ok
    result["reasons"]["C2"] = f"MV={mv or '—'}"

    # ---- C1: Level trigger band ----
    trig_name, trig_price, dist = _nearest_trigger(spot, trigger_list, shifted)
    c1_ok = None; c1_state = "—"
    if side and trig_name and trig_price is not None and spot is not None:
        within = abs(float(spot) - float(trig_price)) <= float(band)
        if within:
            c1_ok = True; c1_state = "CROSS"
        else:
            near = abs(float(spot) - float(trig_price)) <= float(band)*2
            c1_ok = False; c1_state = "NEAR" if near else "FAR"
    else:
        c1_ok = False; c1_state = "no trigger"
    result["c"]["C1"] = c1_ok
    result["reasons"]["C1"] = f"{c1_state} {trig_name or ''}@{_fmt(trig_price)} band±{_fmt(band,0)}"

    if not (c1_ok is True):
        return _finalize_and_log(result, sym, side, trig_name, trig_price, snap, shifted, buf, band, tgt_req, pcr, mp, ce_d, pe_d, src, asof, age)

    # ---- C3: OI Δ pattern ----
    def cls(val: Optional[float]) -> str:
        if val is None: return "na"
        if val >  oi_eps: return "up"
        if val < -oi_eps: return "down"
        return "flat"
    ce_sig, pe_sig = cls(ce_d), cls(pe_d)
    if side == "CE":
        c3_ok = ((ce_sig == "down" and pe_sig == "up") or
                 (ce_sig == "down" and pe_sig == "down") or
                 ((ce_sig in {"flat","down"}) and pe_sig == "up"))
    else:
        c3_ok = ((ce_sig == "up" and pe_sig == "down") or
                 (ce_sig == "down" and pe_sig == "down") or
                 (ce_sig == "up" and pe_sig in {"flat","down"}))
    result["c"]["C3"] = c3_ok
    result["reasons"]["C3"] = f"CEΔ={ce_sig} / PEΔ={pe_sig}"

    # ---- C4: Session/Timing ----
    in_block = _in_no_trade_window_ist()
    fresh_ok = (age <= fresh_max) and (not stale)
    c4_ok = (not in_block) and fresh_ok
    c4_reason = []
    c4_reason.append("time OK" if not in_block else "blocked time")
    c4_reason.append(f"fresh {age}s≤{fresh_max}s" if fresh_ok else "stale/old")
    result["c"]["C4"] = c4_ok
    result["reasons"]["C4"] = ", ".join(c4_reason)

    # ---- C5: Risk & Hygiene ----
    # (a) Daily cap
    max_per_day = _env("MAX_TRADES_PER_DAY")
    cap_ok = True
    cap_reason = "OK"
    if max_per_day:
        try:
            cap = int(max_per_day)
            curr = _count_trades_today()
            if curr >= cap:
                cap_ok = False; cap_reason = f"DailyCap {curr}/{cap}"
        except Exception:
            pass

    # (b) Dedupe per-level-per-day
    date = _today_ist_str()
    price_band = f"{round(float(trig_price)) if trig_price is not None else 'NA'}"
    dedupe_key = f"{date}|{side}|{trig_name}|{price_band}"
    cache = _load_cache()
    dedupe_ok = (dedupe_key not in cache)

    # (c) Velocity (optional)
    velo_ok, velo_reason = True, "off"
    if _env("ENABLE_VELO_CHECK","0") in {"1","true","on","yes"}:
        vmax = float(_env("VELOCITY_MAX_PPS","50"))
        velo_ok, details = _velocity_ok(spot, vmax)
        velo_reason = details

    # (d) Spread/Liquidity (REAL quotes via snapshot chain)
    spread_ok, spread_reason = True, "off"
    if _env("ENABLE_SPREAD_CHECK","0") in {"1","true","on","yes"}:
        limit_bp = float(_env("SPREAD_MAX_BP","150"))
        if qsp is not None:
            ok, reason = qsp.estimate_spread(sym, side or "", spot, snap.get("expiry"), limit_bp)
            if ok is None:
                spread_ok, spread_reason = True, "quotes n/a"
            else:
                spread_ok, spread_reason = bool(ok), reason
        else:
            spread_ok, spread_reason = True, "module n/a"

    # (e) Exposure caps (keep 0/off for your setup)
    qty = int(_env("PAPER_QTY","1") or "1")
    lot = _lot_size(sym)
    est = _premium_estimate_pts(sym)
    would = qty * est * lot
    curr_port = _current_portfolio_exposure(sym)
    max_trade_r = float(_env("MAX_EXPOSURE_PER_TRADE","0") or "0")
    max_port_r  = float(_env("MAX_PORTFOLIO_EXPOSURE","0") or "0")
    exposure_ok = True
    exposure_notes = []
    if max_trade_r > 0 and would > max_trade_r:
        exposure_ok = False; exposure_notes.append(f"PerTrade>{int(max_trade_r)}")
    if max_port_r > 0 and (curr_port + would) > max_port_r:
        exposure_ok = False; exposure_notes.append(f"Portfolio>{int(max_port_r)}")

    c5_ok = cap_ok and dedupe_ok and velo_ok and spread_ok and exposure_ok and (not bool(snap.get("hold"))) and (not bool(snap.get("daily_cap_hit")))
    c5_parts = []
    if not cap_ok: c5_parts.append(cap_reason)
    if not dedupe_ok: c5_parts.append("Dedupe")
    if not velo_ok: c5_parts.append(f"Velocity {velo_reason}")
    if _env("ENABLE_SPREAD_CHECK","0") in {"1","true","on","yes"}:
        c5_parts.append(f"Spread {spread_reason}")
    if not exposure_ok: c5_parts.append("Exposure " + "/".join(exposure_notes))
    if snap.get("hold"): c5_parts.append("HOLD")
    if snap.get("daily_cap_hit"): c5_parts.append("DailyCap")
    if not c5_parts: c5_parts.append("OK")

    result["c"]["C5"] = c5_ok
    result["reasons"]["C5"] = "; ".join(c5_parts)
    result["dedupe_key"] = dedupe_key

    # ---- C6: Proximity & Space (RR room) ----
    space = _space_points(side, trig_name, float(trig_price), s1, s2, r1, r2) if (side and trig_name and trig_price is not None) else None
    if space is None:
        c6_ok = False; c6_reason = "space n/a"
    else:
        c6_ok = (float(space) >= float(tgt_req))
        c6_reason = f"space { _fmt(space,0) } ≥ target { _fmt(tgt_req,0) }"
    result["c"]["C6"] = c6_ok
    result["reasons"]["C6"] = c6_reason

    # ---- Eligibility ----
    all_ok = (c1_ok is True) and c2_ok and (result["c"]["C3"] is True) and (result["c"]["C4"] is True) and (result["c"]["C5"] is True) and (result["c"]["C6"] is True)
    result["eligible"] = bool(all_ok)
    result["side"] = side
    result["trigger_name"] = trig_name
    result["trigger_price"] = float(trig_price) if trig_price is not None else None

    # mark dedupe if eligible OR even if near-cross attempt considered
    if dedupe_ok and (c1_ok is True):
        cache.add(dedupe_key)
        _save_cache(cache)

    # Append to Signals
    try:
        _append_signal_row({
            "ts": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()+5.5*3600)),
            "symbol": sym, "side": side or "", "mv": mv,
            "trigger": trig_name or "", "trigger_price": trig_price or "",
            "spot": spot or "",
            "s1": s1 or "", "s2": s2 or "", "r1": r1 or "", "r2": r2 or "",
            "pcr": pcr or "", "max_pain": mp or "",
            "ce_oi_delta": ce_d or "", "pe_oi_delta": pe_d or "",
            "c1": result["c"]["C1"], "c2": result["c"]["C2"], "c3": result["c"]["C3"],
            "c4": result["c"]["C4"], "c5": result["c"]["C5"], "c6": result["c"]["C6"],
            "reasons": json.dumps(result["reasons"], ensure_ascii=False),
            "source": src, "asof": asof, "age_sec": age,
            "dedupe_key": dedupe_key,
        })
    except Exception as e:
        log.warning("Signals append error: %s", e)

    return result

def _finalize_and_log(result: Dict[str,Any], sym: str, side: Optional[str], trig_name: Optional[str],
                      trig_price: Optional[float], snap: dict, shifted: dict, buf: float, band: float,
                      tgt_req: float, pcr, mp, ce_d, pe_d, src, asof, age) -> Dict[str,Any]:
    try:
        _append_signal_row({
            "ts": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()+5.5*3600)),
            "symbol": sym, "side": side or "", "mv": (snap.get("mv") or ""),
            "trigger": trig_name or "", "trigger_price": trig_price or "",
            "spot": snap.get("spot") or "",
            "s1": snap.get("s1") or "", "s2": snap.get("s2") or "", "r1": snap.get("r1") or "", "r2": snap.get("r2") or "",
            "pcr": pcr or "", "max_pain": mp or "",
            "ce_oi_delta": ce_d or "", "pe_oi_delta": pe_d or "",
            "c1": result["c"].get("C1"), "c2": result["c"].get("C2"), "c3": result["c"].get("C3"),
            "c4": result["c"].get("C4"), "c5": result["c"].get("C5"), "c6": result["c"].get("C6"),
            "reasons": json.dumps(result["reasons"], ensure_ascii=False),
            "source": src, "asof": asof, "age_sec": age,
            "dedupe_key": "",
        })
    except Exception as e:
        log.debug("NEAR append skipped: %s", e)
    return result
