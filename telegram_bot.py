# telegram_bot.py
# ------------------------------------------------------------------------------------
# PTB v20+ compatible bot wiring
# - init() -> Application (do NOT start polling here; main controls single-poller)
# - /oc_now renders snapshot & evaluates C1..C6 exactly per your spec (CE at S*,S2* ; PE at R*,R2*)
#
# Env (symbol-wise configurable; sensible defaults):
#   TELEGRAM_BOT_TOKEN=...
#   # Shift buffer (points)
#   LEVEL_BUFFER_NIFTY=12
#   LEVEL_BUFFER_BANKNIFTY=30
#   LEVEL_BUFFER_FINNIFTY=15
#   LEVEL_BUFFER=12                # fallback
#
#   # Entry band around shifted trigger (points)
#   ENTRY_BAND_NIFTY=3
#   ENTRY_BAND_BANKNIFTY=8
#   ENTRY_BAND_FINNIFTY=4
#   ENTRY_BAND=3                   # fallback
#
#   # Freshness (seconds) for C4
#   OC_FRESH_MAX_AGE_SEC=90
#
#   # No-trade windows (IST) for C4
#   # fixed per spec: 09:15–09:30 and 14:45–15:15
#
#   # Target minimum space for C6 (points)
#   TARGET_MIN_POINTS_NIFTY=30
#   TARGET_MIN_POINTS_BANKNIFTY=80
#   TARGET_MIN_POINTS_FINNIFTY=50
#   TARGET_MIN_POINTS=30
#
#   # OI flat epsilon (absolute). e.g., 0 -> strict, 1000 -> tiny deltas treated flat
#   OI_FLAT_EPS=0
#
# Notes:
# - C5 hygiene: dedupe/daily cap/spread checks are placeholders here (we report "OK"),
#   क्योंकि ये execution loop/Sheets पर enforce होते हैं. Renderer में केवल status दिखता है.
# ------------------------------------------------------------------------------------
from __future__ import annotations

import os
import time
import logging
from typing import Any, Dict, Optional, Tuple, List

from telegram import Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

log = logging.getLogger(__name__)

# ---------- env helpers ----------
def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v.strip() if v and str(v).strip() else default

def _sym_env(sym: str, base: str, default: float) -> float:
    s = (sym or "").upper()
    val = _env(f"{base}_{s}")  # e.g., ENTRY_BAND_NIFTY
    if val is None:
        val = _env(base)       # e.g., ENTRY_BAND
    try:
        return float(val) if val is not None else float(default)
    except Exception:
        return float(default)

# ---------- number/format helpers ----------
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

def _signfmt(x: Optional[float]) -> str:
    if x is None: return "—"
    try:
        xf = float(x); s = "+" if xf >= 0 else ""
        if abs(xf) >= 1_000_000: return f"{s}{int(xf):,}"
        return f"{s}{xf:.2f}"
    except Exception:
        return "—"

def _boolmark(ok: Optional[bool]) -> str:
    if ok is None: return "—"
    return "✅" if ok else "❌"

# ---------- time helpers (IST) ----------
def _now_ist_tuple() -> Tuple[int,int,int,int,int]:
    # returns (Y,M,D,h,m) IST
    t = time.time() + 5.5*3600
    return (int(time.strftime("%Y", time.gmtime(t))),
            int(time.strftime("%m", time.gmtime(t))),
            int(time.strftime("%d", time.gmtime(t))),
            int(time.strftime("%H", time.gmtime(t))),
            int(time.strftime("%M", time.gmtime(t))))

def _in_no_trade_window_ist() -> bool:
    y, m, d, hh, mm = _now_ist_tuple()
    mins = hh*60 + mm
    # 09:15–09:30
    if 9*60+15 <= mins < 9*60+30: return True
    # 14:45–15:15
    if 14*60+45 <= mins < 15*60+15: return True
    return False

# ---------- logic per spec ----------
_ALLOWED_CE_MV = {"bullish", "big_move"}
_ALLOWED_PE_MV = {"bearish", "strong_bearish"}

def _classify_oi(val: Optional[float], eps: float) -> str:
    if val is None: return "na"
    try:
        v = float(val)
        if v >  eps: return "up"
        if v < -eps: return "down"
        return "flat"
    except Exception:
        return "na"

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

def _space_points(sym: str, side: str, trig_name: str, trig_price: float, s1, s2, r1, r2, buf: float) -> Optional[float]:
    # Per spec:
    #  CE @ S1* => space to R1
    #  CE @ S2* => space to S1
    #  PE @ R1* => space down to S1 (distance = trig - S1)
    #  PE @ R2* => space down to R1 (distance = trig - R1)
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

def _target_min_points(sym: str) -> float:
    s = (sym or "").upper()
    if s == "BANKNIFTY": return _sym_env(s, "TARGET_MIN_POINTS", 80.0)
    if s == "FINNIFTY":  return _sym_env(s, "TARGET_MIN_POINTS", 50.0)
    return _sym_env(s, "TARGET_MIN_POINTS", 30.0)  # NIFTY default

def _buffer_points(sym: str) -> float:
    s = (sym or "").upper()
    if s == "BANKNIFTY": return _sym_env(s, "LEVEL_BUFFER", 30.0)
    if s == "FINNIFTY":  return _sym_env(s, "LEVEL_BUFFER", 15.0)
    return _sym_env(s, "LEVEL_BUFFER", 12.0)

def _entry_band(sym: str) -> float:
    s = (sym or "").upper()
    if s == "BANKNIFTY": return _sym_env(s, "ENTRY_BAND", 8.0)
    if s == "FINNIFTY":  return _sym_env(s, "ENTRY_BAND", 4.0)
    return _sym_env(s, "ENTRY_BAND", 3.0)

# ---------- /oc_now ----------
async def oc_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    from analytics import oc_refresh  # late import to avoid circulars
    try:
        await oc_refresh.refresh_once()
    except Exception as e:
        log.warning("oc_now: refresh_once error: %s", e)

    snap = oc_refresh.get_snapshot() or {}
    if not snap:
        await update.message.reply_text("OC snapshot unavailable (rate-limit/first snapshot). मैंने refresh kick किया है — ~15s बाद फिर `/oc_now` भेजें.")
        return

    # --- read snapshot ---
    sym  = (snap.get("symbol") or _env("OC_SYMBOL","NIFTY")).upper()
    exp  = snap.get("expiry") or "—"
    spot = _to_float(snap.get("spot"))
    s1   = _to_float(snap.get("s1")); s2 = _to_float(snap.get("s2"))
    r1   = _to_float(snap.get("r1")); r2 = _to_float(snap.get("r2"))
    pcr  = _to_float(snap.get("pcr")); mp = _to_float(snap.get("max_pain"))
    ce_d = _to_float(snap.get("ce_oi_delta")); pe_d = _to_float(snap.get("pe_oi_delta"))
    mv   = (snap.get("mv") or "").strip().lower()
    src  = snap.get("source") or "—"
    asof = snap.get("asof") or "—"
    age  = int(snap.get("age_sec") or 0)
    stale= bool(snap.get("stale"))

    # --- config ---
    buf  = _buffer_points(sym)
    band = _entry_band(sym)
    fresh_max = int(float(_env("OC_FRESH_MAX_AGE_SEC","90")))
    oi_eps = float(_env("OI_FLAT_EPS","0"))

    # --- shifted levels ---
    shifted = _shift_levels(s1, s2, r1, r2, buf)

    # --- decide side by MV allow-list (C2) ---
    side, trigger_names = _pick_side_and_triggers(mv)
    c2_ok = side is not None
    c2_reason = f"MV={mv or '—'} " + ("OK" if c2_ok else "block")

    # --- choose nearest trigger among allowed (for display & C1/C6) ---
    trig_name, trig_price, dist = _nearest_trigger(spot, trigger_names, shifted)

    # --- C1: Level Trigger (NEAR/CROSS within entry band) ---
    c1_ok = None
    c1_state = "—"
    if spot is not None and trig_price is not None:
        within = abs(float(spot) - float(trig_price)) <= float(band)
        if within:
            # We don't distinguish exchange fill here; treat as NEAR/CROSS same band
            c1_ok = True
            c1_state = "CROSS"  # touched band ⇒ eligible to fill (per spec)
        else:
            # NEAR only if close but outside band (say ≤ 2× band) — purely informational
            near = abs(float(spot) - float(trig_price)) <= float(band)*2
            c1_ok = False
            c1_state = "NEAR" if near else "FAR"

    # --- C3: OI Delta Pattern Confirmation ---
    # classify signs with epsilon
    ce_sig = _classify_oi(ce_d, oi_eps)
    pe_sig = _classify_oi(pe_d, oi_eps)
    c3_ok = None
    c3_reason = "—"
    if side == "CE":
        # Allowed patterns:
        # 1) CE down & PE up
        # 2) CE down & PE down (short-cover squeeze)
        # 3) (CE flat or down) & PE up
        c3_ok = ((ce_sig == "down" and pe_sig == "up") or
                 (ce_sig == "down" and pe_sig == "down") or
                 ((ce_sig in {"flat","down"}) and pe_sig == "up"))
        c3_reason = f"CEΔ={ce_sig} / PEΔ={pe_sig}"
    elif side == "PE":
        # Mirror:
        # 1) CE up & PE down
        # 2) CE down & PE down (short-cover squeeze)
        # 3) CE up & (PE flat or down)
        c3_ok = ((ce_sig == "up" and pe_sig == "down") or
                 (ce_sig == "down" and pe_sig == "down") or
                 (ce_sig == "up" and pe_sig in {"flat","down"}))
        c3_reason = f"CEΔ={ce_sig} / PEΔ={pe_sig}"
    else:
        c3_ok = False
        c3_reason = "MV unknown"

    # --- C4: Session/Timing Safety ---
    # no-trade windows + freshness
    in_block = _in_no_trade_window_ist()
    fresh_ok = (age <= fresh_max) and (not stale)
    c4_ok = (not in_block) and fresh_ok
    c4_parts = []
    c4_parts.append("time OK" if not in_block else "blocked time")
    c4_parts.append(f"fresh {age}s≤{fresh_max}s" if fresh_ok else "stale/old")
    c4_reason = ", ".join(c4_parts)

    # --- C5: Risk & Hygiene Gates ---
    # Execution-time hygiene (dedupe, daily cap, velocity, spread) live engine में enforce होंगे.
    # Renderer में placeholder OK दिखाएँ; HOLD/cap flags अगर snapshot में हैं तो respect करें.
    hold = bool(snap.get("hold"))
    cap  = bool(snap.get("daily_cap_hit"))
    if hold or cap:
        c5_ok = False
        c5_reason = "HOLD" if hold else "DailyCap"
    else:
        c5_ok = True
        c5_reason = "OK"

    # --- C6: Proximity & Space (RR room) ---
    c6_ok = None
    c6_reason = "—"
    tgt_req = _target_min_points(sym)
    if side and trig_name and trig_price is not None:
        space = _space_points(sym, side, trig_name, float(trig_price), s1, s2, r1, r2, buf)
        if space is not None:
            c6_ok = (float(space) >= float(tgt_req))
            c6_reason = f"space { _fmt(space,0) } ≥ target { _fmt(tgt_req,0) }"
        else:
            c6_ok = False
            c6_reason = "space n/a"

    # --- Compose output ---
    # Header
    lines: List[str] = []
    lines.append("OC Snapshot")
    lines.append(f"Symbol: {sym}  |  Exp: {exp}  |  Spot: { _fmt(spot) }")
    lines.append(f"Levels: S1 { _fmt(s1) }  S2 { _fmt(s2) }  R1 { _fmt(r1) }  R2 { _fmt(r2) }")
    lines.append(f"Shifted: S1 `{ _fmt(shifted['S1*']) }`  S2 { _fmt(shifted['S2*']) }  R1 `{ _fmt(shifted['R1*']) }`  R2 { _fmt(shifted['R2*']) }")
    lines.append(f"Buffer: { _fmt(buf,0) }  |  MV: {mv or '—'}  |  PCR: { _fmt(pcr) }  |  MP: { _fmt(mp) }")
    stale_tag = "  |  ⚠️ STALE" if stale else ""
    lines.append(f"Source: {src}  |  As-of: {asof}  |  Age: {int(age)}s{stale_tag}")
    lines.append("")

    # Checks
    lines.append("Checks")
    if trig_name:
        lines.append(f"- C1: { _boolmark(c1_ok) } {c1_state} @ {trig_name} ({ _fmt(trig_price) })")
    else:
        lines.append(f"- C1: { _boolmark(False) } no trigger (MV/levels)")
    lines.append(f"- C2: { _boolmark(c2_ok) } {c2_reason}")
    lines.append(f"- C3: { _boolmark(c3_ok) } {c3_reason}  (raw CEΔ={_signfmt(ce_d)}, PEΔ={_signfmt(pe_d)})")
    lines.append(f"- C4: { _boolmark(c4_ok) } {c4_reason}")
    lines.append(f"- C5: { _boolmark(c5_ok) } {c5_reason}")
    lines.append(f"- C6: { _boolmark(c6_ok) } {c6_reason}")
    lines.append("")

    # Summary
    failed = []
    if not (c1_ok is True): failed.append("C1")
    if not c2_ok: failed.append("C2")
    if not (c3_ok is True): failed.append("C3")
    if not (c4_ok is True): failed.append("C4")
    if not (c5_ok is True): failed.append("C5")
    if not (c6_ok is True): failed.append("C6")

    if not failed and side and trig_name and trig_price is not None:
        summary = f"✅ Eligible — {side} @ {trig_name} ({_fmt(trig_price)})"
    else:
        if c1_ok is False and c1_state == "NEAR" and side and trig_name:
            summary = f"⏳ NEAR — waiting at {trig_name} ({_fmt(trig_price)})"
        else:
            summary = f"❌ Not eligible — failed: {', '.join(failed) if failed else 'rules'}"

    lines.append(f"Summary: {summary}")

    text = "\n".join(lines)
    await update.message.reply_text(text)

# ---------- init ----------
def init() -> Application:
    token = _env("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN missing")

    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler("oc_now", oc_now))
    log.info("/oc_now handler registered")
    return app
