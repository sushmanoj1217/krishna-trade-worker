# agents/signal_generator.py
from __future__ import annotations

import os
import math
import random
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # py311+ normally available

# ---- internal deps (safe fallbacks) ----
from utils.logger import log

# params (env/overrides)
try:
    from utils import params as P
except Exception:
    P = None  # safe fallback below

# last-good OC snapshot
try:
    from utils.cache import get_snapshot
except Exception:
    def get_snapshot(): return None  # type: ignore

# Sheets IO & helpers (with our memory tap/write in integrations.sheets)
try:
    from integrations import sheets as sh
except Exception:
    class _S:
        def get_today_dedupe_hashes(self): return set()
        def count_today_trades(self): return 0
        def append_row(self, *a, **k): pass
        def write_signal_row(self, *a, **k): pass
        def tap_signal_row(self, *a, **k): pass
    sh = _S()  # type: ignore

# oc_auto + approvals toggles
try:
    from utils.state import is_oc_auto, approvals_required
except Exception:
    def is_oc_auto(): return True
    def approvals_required(): return False


# =========================
# Config & helpers
# =========================
IST = "Asia/Kolkata"

def now_ist_str() -> str:
    try:
        if ZoneInfo is not None:
            return datetime.now(ZoneInfo(IST)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def today_yyyymmdd_ist() -> str:
    try:
        if ZoneInfo is not None:
            return datetime.now(ZoneInfo(IST)).strftime("%Y%m%d")
    except Exception:
        pass
    return datetime.utcnow().strftime("%Y%m%d")

def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)).strip())
    except Exception:
        return default

def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)).strip())
    except Exception:
        return default

def _sym() -> str:
    return os.getenv("OC_SYMBOL", "NIFTY").upper()

def _buffers_for(sym: str) -> float:
    # Priority: params override → env symbol-specific → default map
    if P and hasattr(P, "entry_band_points"):
        try:
            v = float(P.entry_band_points(sym))  # type: ignore
            if v > 0:
                return v
        except Exception:
            pass
    env_key = f"ENTRY_BAND_POINTS_{sym}"
    if os.getenv(env_key):
        return _env_float(env_key, 12.0)
    # fallback defaults
    defaults = {"NIFTY": 12.0, "BANKNIFTY": 30.0, "FINNIFTY": 15.0}
    return defaults.get(sym, 12.0)

def _mp_support_dist_for(sym: str) -> float:
    if P and hasattr(P, "max_pain_support_dist"):
        try:
            v = float(P.max_pain_support_dist(sym))  # type: ignore
            if v > 0:
                return v
        except Exception:
            pass
    env_key = f"MP_SUPPORT_DIST_{sym}"
    # defaults from brief: e.g., NIFTY 25 / BANKNIFTY 60 / FINNIFTY 30
    defaults = {"NIFTY": 25.0, "BANKNIFTY": 60.0, "FINNIFTY": 30.0}
    return _env_float(env_key, defaults.get(sym, 25.0))

def _min_target_points_for(sym: str) -> float:
    if P and hasattr(P, "min_target_points"):
        try:
            v = float(P.min_target_points(sym))  # type: ignore
            if v > 0:
                return v
        except Exception:
            pass
    env_key = f"MIN_TARGET_POINTS_{sym[0]}" if sym in ("NIFTY","BANKNIFTY","FINNIFTY") else f"MIN_TARGET_POINTS_{sym}"
    # safe defaults
    defaults = {"NIFTY": 20.0, "BANKNIFTY": 40.0, "FINNIFTY": 25.0}
    return _env_float(env_key, defaults.get(sym, 20.0))

def _pcr_bands() -> Tuple[float, float]:
    bh = _env_float("PCR_BULL_HIGH", 1.10)
    bl = _env_float("PCR_BEAR_LOW", 0.90)
    return bh, bl

def _no_trade_window_ist() -> bool:
    # 09:15–09:30 & 14:45–15:15 IST
    try:
        if ZoneInfo is None:
            return False
        now = datetime.now(ZoneInfo(IST))
        hhmm = now.hour * 100 + now.minute
        if 915 <= hhmm < 930:  # open noise window
            return True
        if 1445 <= hhmm <= 1515:  # close management
            return True
    except Exception:
        return False
    return False

def _daily_trade_cap_ok() -> bool:
    cap = _env_int("MAX_TRADES_PER_DAY", 9999)
    try:
        used = int(getattr(sh, "count_today_trades", lambda : 0)())
    except Exception:
        used = 0
    return used < cap

def _one_of(*vals) -> Any:
    for v in vals:
        if v is not None:
            return v
    return None

def _bool_to_cell(x: Optional[bool]) -> str:
    if x is True: return "TRUE"
    if x is False: return "FALSE"
    return ""


# =========================
# Snapshot wrapper
# =========================
@dataclass
class Snapshot:
    spot: Optional[float] = None
    vix: Optional[float] = None
    pcr: Optional[float] = None
    pcr_bucket: Optional[str] = None
    max_pain: Optional[float] = None
    max_pain_dist: Optional[float] = None
    bias_tag: Optional[str] = None
    expiry: Optional[str] = None
    s1: Optional[float] = None
    s2: Optional[float] = None
    r1: Optional[float] = None
    r2: Optional[float] = None
    buffer_points: Optional[float] = None
    stale: bool = False
    # optional OI deltas near trigger (if available)
    ce_oi_delta_near: Optional[float] = None
    pe_oi_delta_near: Optional[float] = None

def _snap_from_obj(obj: Any) -> Snapshot:
    if not obj:
        return Snapshot()
    def g(k, d=None): return getattr(obj, k, d)
    return Snapshot(
        spot=g("spot"), vix=g("vix"), pcr=g("pcr"), pcr_bucket=g("pcr_bucket"),
        max_pain=g("max_pain"), max_pain_dist=_one_of(g("max_pain_dist"), g("mp_dist")),
        bias_tag=g("bias_tag"), expiry=g("expiry"),
        s1=g("s1"), s2=g("s2"), r1=g("r1"), r2=g("r2"),
        buffer_points=_one_of(g("buffer_points"), g("buffer")),
        stale=g("stale", False),
        ce_oi_delta_near=_one_of(getattr(obj, "ce_oi_delta_near", None), getattr(obj, "oi_ce_near", None)),
        pe_oi_delta_near=_one_of(getattr(obj, "pe_oi_delta_near", None), getattr(obj, "oi_pe_near", None)),
    )


# =========================
# Trigger utils
# =========================
@dataclass
class TriggerInfo:
    side: str              # "CE" or "PE"
    trigger: str           # "S1*" | "S2*" | "R1*" | "R2*"
    level_price: float     # shifted trigger price (S-b or R+b)
    status: str            # "NEAR" | "CROSS" | ""

def _shifted(val: Optional[float], buf: float, sign: int) -> Optional[float]:
    if val is None:
        return None
    try:
        if sign < 0:
            return float(val) - float(buf)
        return float(val) + float(buf)
    except Exception:
        return None

def _detect_trigger(s: Snapshot, buf: float) -> Optional[TriggerInfo]:
    """
    Decide which trigger is currently active based on SPOT proximity to shifted levels.
    - CE @ S1*, S2*   where S* = S - b
    - PE @ R1*, R2*   where R* = R + b
    NEAR if within 0.5*buf (min 2pts). CROSS if within 0.1*buf (min 0.5pt).
    """
    if s.spot is None or buf <= 0:
        return None
    tol_near = max(2.0, 0.5 * buf)
    tol_cross = max(0.5, 0.1 * buf)

    candidates: List[TriggerInfo] = []
    s1s = _shifted(s.s1, buf, -1)
    s2s = _shifted(s.s2, buf, -1)
    r1s = _shifted(s.r1, buf, +1)
    r2s = _shifted(s.r2, buf, +1)

    def add(side, name, price):
        if price is None:
            return
        d = abs(s.spot - price)
        status = "CROSS" if d <= tol_cross else ("NEAR" if d <= tol_near else "")
        if status:
            candidates.append(TriggerInfo(side, name, price, status))

    add("CE", "S1*", s1s)
    add("CE", "S2*", s2s)
    add("PE", "R1*", r1s)
    add("PE", "R2*", r2s)

    # pick closest
    if not candidates:
        return None
    candidates.sort(key=lambda t: abs(s.spot - t.level_price))
    return candidates[0]


# =========================
# MV gate (1-of-2)
# =========================
@dataclass
class MVGate:
    pcr_ok: Optional[bool]
    mp_ok: Optional[bool]
    basis: str

def _mv_gate(side: str, s: Snapshot, sym: str) -> MVGate:
    bh, bl = _pcr_bands()
    pcr = s.pcr
    mpd = s.max_pain_dist
    mp_dist_req = _mp_support_dist_for(sym)

    pcr_ok = None
    mp_ok = None
    basis_parts = []

    if pcr is not None:
        if side == "CE":
            pcr_ok = (pcr >= bh)
        elif side == "PE":
            pcr_ok = (pcr <= bl)
        basis_parts.append(f"pcr {pcr:.2f} vs bands {bh:.2f}/{bl:.2f}")

    if mpd is None and (s.spot is not None and s.max_pain is not None):
        mpd = abs(float(s.spot) - float(s.max_pain))

    if mpd is not None:
        if side == "CE":
            mp_ok = (float(s.spot or 0) >= float(s.max_pain or 0) + mp_dist_req) if (s.spot is not None and s.max_pain is not None) else (mpd >= mp_dist_req)
        else:
            mp_ok = (float(s.spot or 0) <= float(s.max_pain or 0) - mp_dist_req) if (s.spot is not None and s.max_pain is not None) else (mpd >= mp_dist_req)
        basis_parts.append(f"mpΔ {mpd:.2f} vs req {mp_dist_req:.2f}")

    basis = " | ".join(basis_parts)
    return MVGate(pcr_ok, mp_ok, basis)


# =========================
# OC-Pattern gate (1-of-2)
# =========================
@dataclass
class OCPattern:
    bull_normal: Optional[bool]
    bull_shortcover: Optional[bool]
    bear_normal: Optional[bool]
    bear_crash: Optional[bool]
    basis: str

def _oc_pattern(s: Snapshot) -> OCPattern:
    ce = s.ce_oi_delta_near
    pe = s.pe_oi_delta_near
    # Heuristic: signs only if deltas available
    bull_normal = bull_short = bear_normal = bear_crash = None
    basis = ""
    if ce is not None and pe is not None:
        # CE OI ↓ & PE OI ↑ (bull_normal)
        bull_normal = (ce < 0 and pe > 0)
        # CE OI ↓ & PE OI ↓ (bull_shortcover, stronger)
        bull_short = (ce < 0 and pe < 0)
        # CE OI ↑ & PE OI ↑ (bear_normal)
        bear_normal = (ce > 0 and pe > 0)
        # CE OI ↑ & PE OI ↓ (bear_crash, stronger)
        bear_crash = (ce > 0 and pe < 0)
        basis = f"ΔOI near: CE {ce:+.0f}, PE {pe:+.0f}"
    return OCPattern(bull_normal, bull_short, bear_normal, bear_crash, basis)


# =========================
# C1..C6 checks
# =========================
@dataclass
class Checks:
    c1: Optional[bool]
    c2: Optional[bool]
    c3: Optional[bool]
    c4: Optional[bool]
    c5: Optional[bool]
    c6: Optional[bool]
    reason: str

def _checks(side: str, trig: TriggerInfo, s: Snapshot, sym: str, buf: float,
            entry: float, sl: float, tp: float, ocp: OCPattern, mv: MVGate) -> Checks:
    reasons: List[str] = []

    # C1 TriggerCross (NEAR/CROSS acceptable)
    c1 = trig.status in ("NEAR", "CROSS")
    if not c1: reasons.append("C1")

    # C2 FlowBias@Trigger — coarse: use bias_tag
    bias = (s.bias_tag or "").lower()
    if side == "CE":
        c2 = ("mvbull" in bias) or ("bull" in (s.pcr_bucket or "").lower())
    else:
        c2 = ("mvbear" in bias) or ("bear" in (s.pcr_bucket or "").lower())
    if not c2: reasons.append("C2")

    # C3 WallSupport(ΣΔOI) near trigger → use OC pattern in the direction
    if side == "CE":
        c3 = True if (ocp.bull_shortcover or ocp.bull_normal) else (False if (ocp.bull_shortcover is not None or ocp.bull_normal is not None) else None)
    else:
        c3 = True if (ocp.bear_crash or ocp.bear_normal) else (False if (ocp.bear_crash is not None or ocp.bear_normal is not None) else None)
    if c3 is False: reasons.append("C3")

    # C4 Momentum(3–5m) — not available yet → None (tag only)
    c4 = None

    # C5 RR feasible (≥ 2×SL and ≥ MIN_TARGET_POINTS)
    min_target = _min_target_points_for(sym)
    rr_ok = (abs(tp - entry) >= 2 * abs(entry - sl)) and (abs(tp - entry) >= min_target)
    c5 = rr_ok
    if not c5: reasons.append(f"C5(min {min_target:.0f})")

    # C6 SystemGates — no-trade windows + caps (HOLD gate optional)
    gates_ok = (not _no_trade_window_ist()) and _daily_trade_cap_ok()
    c6 = gates_ok
    if not c6: reasons.append("C6")

    reason = "" if (all(x is not False for x in (c1,c2,c3,c4,c5,c6))) else ("Blocked: " + ",".join(reasons))
    return Checks(c1, c2, c3, c4, c5, c6, reason)


# =========================
# Entry/SL/TP calculators
# =========================
@dataclass
class EntryPack:
    entry: float
    sl: float
    tp: float

def _entry_sl_tp(side: str, trig: TriggerInfo, buf: float) -> EntryPack:
    """
    Rule from brief: use buffer as SL distance; TP = 2×buffer from entry.
    CE entries at S*; PE entries at R*.
    """
    e = float(trig.level_price)
    b = float(buf)
    if side == "CE":
        sl = e - b
        tp = e + (2 * b)
    else:  # PE
        sl = e + b
        tp = e - (2 * b)
    return EntryPack(e, sl, tp)


# =========================
# Dedupe guard
# =========================
_DEDUPE_MEM: set[str] = set()

def _today_dedupes() -> set[str]:
    try:
        return set(getattr(sh, "get_today_dedupe_hashes", lambda : set())())
    except Exception:
        return set()

def _dedupe_hash(side: str, trig: TriggerInfo) -> str:
    dt = today_yyyymmdd_ist()
    lvl = int(round(trig.level_price))
    return f"{dt}|{side}|{trig.trigger}|{lvl}"


# =========================
# Signal id
# =========================
def _rand4() -> str:
    import string
    return "".join(random.choice(string.ascii_uppercase) for _ in range(4))

def _new_signal_id() -> str:
    return f"SIG-{today_yyyymmdd_ist()}-{datetime.utcnow().strftime('%H%M%S')}-{_rand4()}"


# =========================
# Public API
# =========================
def run_once() -> Optional[Dict[str, Any]]:
    """
    One pass: if a trigger is NEAR/CROSS, build & emit a signal.
    Returns a dict with summary or None if nothing to do.
    """
    if not is_oc_auto():
        return None

    raw = get_snapshot()
    s = _snap_from_obj(raw)
    if not s or s.spot is None:
        return None

    sym = _sym()
    buf = float(s.buffer_points or _buffers_for(sym))
    trig = _detect_trigger(s, buf)
    if not trig:
        return None

    # Dedupe (one-attempt-per-level)
    dh = _dedupe_hash(trig.side, trig)
    already = (dh in _DEDUPE_MEM) or (dh in _today_dedupes())
    if already:
        log.info(f"Level attempt guard: already tried {dh}")
        return None
    _DEDUPE_MEM.add(dh)

    # Entry/SL/TP
    pack = _entry_sl_tp(trig.side, trig, buf)

    # Gates: MV + OC pattern
    mv = _mv_gate(trig.side, s, sym)
    ocp = _oc_pattern(s)

    # 6-Checks
    chk = _checks(trig.side, trig, s, sym, buf, pack.entry, pack.sl, pack.tp, ocp, mv)

    # Eligibility: 6/6 TRUE + (MV 1-of-2) + (OC-Pattern 1-of-2)
    mv_one_of_two = (mv.pcr_ok is True) or (mv.mp_ok is True)
    if trig.side == "CE":
        oc_one_of_two = (ocp.bull_shortcover is True) or (ocp.bull_normal is True)
    else:
        oc_one_of_two = (ocp.bear_crash is True) or (ocp.bear_normal is True)

    all_checks_true = all(x is True for x in (chk.c1, chk.c2, chk.c3, chk.c4, chk.c5, chk.c6)) \
                      if chk.c4 is not None else all(x is True or x is None for x in (chk.c1, chk.c2, chk.c3, chk.c4, chk.c5, chk.c6))
    eligible = bool(all_checks_true and mv_one_of_two and oc_one_of_two)

    # Build row (fixed order per brief)
    row = [
        _new_signal_id(),              # signal_id
        now_ist_str(),                 # ts
        trig.side,                     # side (CE/PE)
        trig.trigger,                  # trigger (S1*/S2*/R1*/R2*)
        _bool_to_cell(chk.c1),         # c1
        _bool_to_cell(chk.c2),         # c2
        _bool_to_cell(chk.c3),         # c3
        _bool_to_cell(chk.c4),         # c4
        _bool_to_cell(chk.c5),         # c5
        _bool_to_cell(chk.c6),         # c6
        _bool_to_cell(eligible),       # eligible
        chk.reason,                    # reason
        _bool_to_cell(mv.pcr_ok),      # mv_pcr_ok
        _bool_to_cell(mv.mp_ok),       # mv_mp_ok
        mv.basis,                      # mv_basis
        _bool_to_cell(ocp.bull_normal),       # oc_bull_normal
        _bool_to_cell(ocp.bull_shortcover),   # oc_bull_shortcover
        _bool_to_cell(ocp.bear_normal),       # oc_bear_normal
        _bool_to_cell(ocp.bear_crash),        # oc_bear_crash
        ocp.basis,                     # oc_pattern_basis
        trig.status,                   # near/cross
        f"{sym} buf={buf:.0f} | entry={pack.entry:.2f} sl={pack.sl:.2f} tp={pack.tp:.2f}",  # notes
    ]

    # Memory tap first (so /oc_now never blanks)
    try:
        if hasattr(sh, "tap_signal_row"):
            sh.tap_signal_row(row)  # type: ignore[attr-defined]
    except Exception as e:
        log.warning(f"signal tap failed: {e}")

    # Sheet write (best-effort; safe if capped or 429)
    try:
        if hasattr(sh, "write_signal_row"):
            sh.write_signal_row(row)  # type: ignore[attr-defined]
        else:
            sh.append_row("Signals", row)
    except Exception as e:
        log.warning(f"Signals append failed: {e}")

    # Console log summary (seen in your logs earlier)
    log.info(
        f"Signal {row[0]} {trig.side} {trig.trigger} eligible={eligible} "
        f"entry={pack.entry:.1f} sl={pack.sl:.1f} tp={pack.tp:.1f}"
    )

    return {
        "signal_id": row[0],
        "eligible": eligible,
        "side": trig.side,
        "trigger": trig.trigger,
        "near_cross": trig.status,
        "entry": pack.entry,
        "sl": pack.sl,
        "tp": pack.tp,
        "mv_pcr_ok": mv.pcr_ok,
        "mv_mp_ok": mv.mp_ok,
        "oc_bull_normal": ocp.bull_normal,
        "oc_bull_shortcover": ocp.bull_shortcover,
        "oc_bear_normal": ocp.bear_normal,
        "oc_bear_crash": ocp.bear_crash,
        "mv_basis": mv.basis,
        "oc_pattern_basis": ocp.basis,
        "checks": {
            "c1": chk.c1, "c2": chk.c2, "c3": chk.c3, "c4": chk.c4, "c5": chk.c5, "c6": chk.c6,
            "reason": chk.reason,
        },
        "dedupe_hash": dh,
    }


# Backward compatible aliases (your day loop may call any of these)
def tick() -> Optional[Dict[str, Any]]:
    return run_once()

def generate_once() -> Optional[Dict[str, Any]]:
    return run_once()


# ============ Minimal approvals API (for /approve /deny) ============
_PENDING: Dict[str, Dict[str, Any]] = {}

def list_pending_ids() -> List[str]:
    # If approvals mode ON, you can add to _PENDING in your trade loop.
    return list(_PENDING.keys())

def approve(signal_id: str) -> bool:
    if signal_id in _PENDING:
        _PENDING.pop(signal_id, None)
        return True
    return False

def deny(signal_id: str) -> bool:
    if signal_id in _PENDING:
        _PENDING.pop(signal_id, None)
        return True
    return False

