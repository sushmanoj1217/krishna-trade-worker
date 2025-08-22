# analytics/oc_refresh.py
# ------------------------------------------------------------
# Unified OC refresh core with stable API:
#   - refresh_once(*args, **kwargs) -> dict snapshot
#   - get_snapshot() -> dict|None
#   - set_snapshot(snap: dict) -> None
#
# Provider-first; else Google Sheets fallback with DERIVATIONS:
#   - mv (bullish/bearish/"" ) from PCR + MaxPain vs Spot (+ tie-break via dPCR)
#   - ce_oi_delta / pe_oi_delta:
#       a) many alias delta cols (oi_delta/oi change/oi chg/Δ etc.)
#       b) else from absolute OI vs previous row (many aliases)
#       c) else sign-only proxy via dPCR (prev→curr) or MV tag
#       d) FINAL fallback: sign-only proxy via **current PCR**
#   - NEW: read HOLD / DAILY_CAP_HIT from Params_Override sheet or env overrides
# ------------------------------------------------------------
from __future__ import annotations
import importlib, inspect, logging, re
from typing import Any, Callable, Optional, Dict, Tuple
import json, os, time

try:
    import gspread  # type: ignore
except Exception:
    gspread = None  # type: ignore

_log = logging.getLogger(__name__)

_SNAPSHOT: Optional[dict] = None

# -------- Public API --------
def set_snapshot(snap: dict) -> None:
    global _SNAPSHOT
    if isinstance(snap, dict):
        _SNAPSHOT = snap

def get_snapshot() -> Optional[dict]:
    return _SNAPSHOT

# -------- Provider discovery --------
_MODULE_CANDIDATES = [
    "analytics.oc_sources", "analytics.oc_core", "analytics.oc_backend",
    "integrations.dhan_oc", "integrations.oc_feed",
    "providers.dhan_oc", "providers.oc",
    "dhan.oc", "oc.providers",
]
_FN_CANDIDATE_NAMES = [
    "refresh_once","refresh_now","run_once","refresh","do_refresh",
    "refresh_tick","refresh_snapshot","oc_refresh","fetch_levels",
    "get_oc_snapshot","compute_levels","compute_snapshot","build_snapshot","get_levels",
]
def _score_name(name: str) -> int:
    n = name.lower()
    order = {nm: i for i, nm in enumerate(_FN_CANDIDATE_NAMES)}
    if n in order: return order[n]
    if "refresh" in n: return 50
    if any(k in n for k in ("snapshot","levels","oc")): return 60
    return 999
def _discover_provider() -> Tuple[Optional[Callable[..., Any]], str, bool]:
    for mod_name in _MODULE_CANDIDATES:
        try:
            m = importlib.import_module(mod_name)
        except Exception:
            continue
        cands: list[tuple[int,int,str,Callable[...,Any],bool]] = []
        for nm in dir(m):
            obj = getattr(m, nm, None)
            if callable(obj):
                sc = _score_name(nm)
                if sc < 999:
                    try:
                        sig = inspect.signature(obj)
                        req = sum(
                            1 for p in sig.parameters.values()
                            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
                            and p.default is inspect._empty
                        )
                    except Exception:
                        req = 0
                    cands.append((sc, req, nm, obj, inspect.iscoroutinefunction(obj)))
        if cands:
            cands.sort(key=lambda t: (t[0], t[1]))
            _, req, nm, fn, is_coro = cands[0]
            _log.info("oc_refresh: provider %s.%s (async=%s, req=%s)", mod_name, nm, is_coro, req)
            return fn, f"{mod_name}.{nm}", is_coro
    return None, "", False
_PROVIDER_FN, _PROVIDER_NAME, _PROVIDER_IS_ASYNC = _discover_provider()

# -------- Helpers to extract snapshots --------
def _looks_like_snapshot(d: Any) -> bool:
    if not isinstance(d, dict): return False
    k = set(x.lower() for x in d.keys())
    if "spot" in k and ({"s1","s2","r1","r2"} & k): return True
    if {"symbol","expiry","spot"} <= k: return True
    if "levels" in k and "spot" in k: return True
    return False
def _extract_snapshot(ret: Any) -> Optional[dict]:
    if _looks_like_snapshot(ret): return ret
    if isinstance(ret, (tuple, list)):
        for x in ret:
            if _looks_like_snapshot(x): return x
    for attr in ("snapshot","data","result"):
        if hasattr(ret, attr):
            try:
                val = getattr(ret, attr)
                if _looks_like_snapshot(val): return val
            except Exception:
                pass
    return None
def _call_variants(fn: Callable, is_async: bool):
    async def _runner():
        variants = [((),{}), ((None,),{}), (({},),{})]
        for a,k in variants:
            try:
                res = fn(*a, **k)
                if inspect.isawaitable(res):
                    res = await res
                return res
            except TypeError:
                continue
        res = fn()
        if inspect.isawaitable(res):
            res = await res
        return res
    return _runner()

# -------- Sheets helpers + aggressive alias handling --------
def _env(name: str) -> Optional[str]:
    v = os.environ.get(name)
    return v.strip() if v and v.strip() else None
def _open_by_key():
    if gspread is None:
        raise RuntimeError("gspread not installed")
    raw = _env("GOOGLE_SA_JSON")
    sid = _env("GSHEET_TRADES_SPREADSHEET_ID")
    if not raw or not sid:
        raise RuntimeError("Sheets env missing")
    sa = json.loads(raw)
    gc = gspread.service_account_from_dict(sa)
    return gc.open_by_key(sid)
def _open_ws(name: str):
    sh = _open_by_key()
    try:
        return sh.worksheet(name)
    except Exception:
        raise
def _open_oc_ws():
    sh = _open_by_key()
    try:
        return sh.worksheet("OC_Live")
    except Exception:
        return sh.worksheet("Snapshots")

# normalize keys (remove spaces/punct, map greek Δ -> delta)
def _norm_key(k: str) -> str:
    s = str(k).lower()
    s = s.replace("Δ", "delta").replace("∆", "delta")
    s = re.sub(r"[\s\-\.\(\)\[\]/]+", "_", s)  # spaces/punct -> underscore
    s = re.sub(r"__+", "_", s).strip("_")
    return s
def _to_float(x):
    try:
        if x in (None, "", "—"): return None
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None
def _norm_row_anynums(d: Dict[str, Any]) -> Dict[str, Any]:
    return {_norm_key(k): v for k, v in d.items()}

_CE_TOKS = ("ce",)
_PE_TOKS = ("pe",)
_CALL_TOKS = ("call",)
_PUT_TOKS = ("put",)
_OI_TOKS = ("oi", "openinterest", "open_interest")
_DELTA_TOKS = ("delta", "chg", "change", "d")  # 'd' last resort (with oi)
def _is_delta_key(norm: str, side: str) -> bool:
    side_ok = (side in norm) or (side == "ce" and any(t in norm for t in _CALL_TOKS)) or (side == "pe" and any(t in norm for t in _PUT_TOKS))
    if not side_ok: return False
    if not any(t in norm for t in _OI_TOKS): return False
    if not any(t in norm for t in _DELTA_TOKS): return False
    return True
def _is_abs_oi_key(norm: str, side: str) -> bool:
    side_ok = (side in norm) or (side == "ce" and any(t in norm for t in _CALL_TOKS)) or (side == "pe" and any(t in norm for t in _PUT_TOKS))
    if not side_ok: return False
    if not any(t in norm for t in _OI_TOKS): return False
    if any(t in norm for t in _DELTA_TOKS): return False
    return True
def _pick_oi_delta_any(row: Dict[str, Any], prev: Optional[Dict[str, Any]], side: str) -> Optional[float]:
    # 1) explicit delta-like columns
    for k, v in row.items():
        nk = _norm_key(k)
        if _is_delta_key(nk, side):
            val = _to_float(v)
            if val is not None:
                return float(val)
    # 2) compute from abs OI vs prev (many aliases)
    best_curr_val = best_prev_val = None
    for k, v in row.items():
        nk = _norm_key(k)
        if _is_abs_oi_key(nk, side):
            val = _to_float(v)
            if val is not None:
                best_curr_val = val
                break
    if prev is not None:
        for k, v in prev.items():
            nk = _norm_key(k)
            if _is_abs_oi_key(nk, side):
                val = _to_float(v)
                if val is not None:
                    best_prev_val = val
                    break
    if best_curr_val is not None and best_prev_val is not None:
        try:
            return float(best_curr_val) - float(best_prev_val)
        except Exception:
            pass
    return None

def _derive_mv(pcr: Optional[float], max_pain: Optional[float], spot: Optional[float], dpcr: Optional[float]) -> str:
    score = 0
    if isinstance(pcr, (int,float)):
        if pcr >= 1.0: score += 1
        elif pcr <= 1.0: score -= 1
    if isinstance(max_pain, (int,float)) and isinstance(spot, (int,float)):
        if max_pain > spot: score += 1
        elif max_pain < spot: score -= 1
    if score > 0: return "bullish"
    if score < 0: return "bearish"
    if isinstance(dpcr, (int,float)):
        if dpcr < 0: return "bullish"
        if dpcr > 0: return "bearish"
    return ""

def _truthy(x: Any) -> Optional[bool]:
    if x is None: return None
    s = str(x).strip().lower()
    if s in {"1","true","yes","y","on","t"}: return True
    if s in {"0","false","no","n","off","f"}: return False
    return None

def _read_override_flags() -> Dict[str, bool]:
    """Env overrides for HOLD / DAILY_CAP_HIT."""
    hold = None
    for k in ("HOLD_OVERRIDE","SYSTEM_HOLD","HOLD"):
        v = _env(k)
        tv = _truthy(v) if v is not None else None
        if tv is not None:
            hold = tv
            break
    # daily cap via env (optional)
    cap = None
    for k in ("DAILY_CAP_HIT","DAILY_CAP","CAP_HIT"):
        v = _env(k)
        tv = _truthy(v) if v is not None else None
        if tv is not None:
            cap = tv
            break
    return {"hold": bool(hold) if hold is not None else False,
            "daily_cap_hit": bool(cap) if cap is not None else False,
            "hold_set": hold is not None, "cap_set": cap is not None}

def _read_params_override() -> Dict[str, bool]:
    """Read Params_Override sheet (last non-empty row) for flags."""
    out = {"hold": False, "daily_cap_hit": False}
    try:
        ws = _open_ws("Params_Override")
    except Exception:
        return out
    try:
        rows = ws.get_all_records()  # list[dict]
    except Exception:
        return out
    if not rows:
        return out
    last = rows[-1]
    # normalize keys
    last_norm = { _norm_key(k): v for k, v in last.items() }

    hold_keys = ("hold","system_hold","manual_hold")
    cap_keys = ("daily_cap_hit","daily_cap","cap_hit")
    # parse truthy/falsy
    for k in hold_keys:
        if k in last_norm:
            tv = _truthy(last_norm[k])
            if tv is not None:
                out["hold"] = tv
                break
    for k in cap_keys:
        if k in last_norm:
            tv = _truthy(last_norm[k])
            if tv is not None:
                out["daily_cap_hit"] = tv
                break
    return out

def _open_ws_and_rows() -> Optional[list[dict]]:
    try:
        ws = _open_oc_ws()
        return ws.get_all_records()
    except Exception as e:
        _log.warning("oc_refresh: sheets read failed: %s", e)
        return None

def _build_from_sheet() -> Optional[dict]:
    rows = _open_ws_and_rows()
    if not rows:
        return None

    last_raw = rows[-1]
    prev_raw = rows[-2] if len(rows) >= 2 else None
    last = _norm_row_anynums(last_raw)
    prev = _norm_row_anynums(prev_raw) if prev_raw else None

    sym = (last.get("symbol") or last.get("sym") or _env("OC_SYMBOL") or "")
    sym = str(sym).upper()
    exp = last.get("expiry") or last.get("exp") or ""

    spot = _to_float(last.get("spot"))
    s1 = _to_float(last.get("s1")); s2 = _to_float(last.get("s2"))
    r1 = _to_float(last.get("r1")); r2 = _to_float(last.get("r2"))
    pcr = _to_float(last.get("pcr")); mp = _to_float(last.get("max_pain"))

    # dPCR for proxy/tie-break
    dpcr = None
    if prev is not None:
        p_prev = _to_float(prev.get("pcr"))
        if p_prev is not None and pcr is not None:
            dpcr = pcr - p_prev

    # Try explicit mv tag, else derive
    mv_tag = (last.get("mv") or last.get("move") or last.get("trend") or "")
    mv_tag = str(mv_tag).strip().lower()
    if not mv_tag:
        mv_tag = _derive_mv(pcr, mp, spot, dpcr)

    # OI Δ detection (aggressive)
    ce_d = _pick_oi_delta_any(last, prev, "ce") or _pick_oi_delta_any(last, prev, "call")
    pe_d = _pick_oi_delta_any(last, prev, "pe") or _pick_oi_delta_any(last, prev, "put")

    # If still missing, proxy from dPCR (sign-only)
    if ce_d is None and pe_d is None and isinstance(dpcr, (int,float)) and dpcr != 0:
        mag = max(1.0, abs(dpcr) * 1000.0)
        if dpcr > 0:
            pe_d, ce_d = mag, -mag   # PCR up → PE up / CE down
        else:
            pe_d, ce_d = -mag, mag   # PCR down → PE down / CE up

    # If STILL missing, proxy from MV
    if ce_d is None and pe_d is None and mv_tag:
        sign = 1.0 if mv_tag == "bullish" else (-1.0 if mv_tag == "bearish" else 0.0)
        if sign != 0.0:
            ce_d = -1.0 * sign
            pe_d =  1.0 * sign

    # FINAL fallback: proxy directly from current PCR
    if ce_d is None and pe_d is None and isinstance(pcr, (int,float)) and pcr != 1.0:
        if pcr > 1.0:
            pe_d =  1.0; ce_d = -1.0
        else:
            pe_d = -1.0; ce_d =  1.0

    # Flags: Params_Override + env overrides
    flags_sheet = _read_params_override()
    flags_env = _read_override_flags()
    hold = flags_env["hold"] if flags_env.get("hold_set") else flags_sheet.get("hold", False)
    daily_cap_hit = flags_env["daily_cap_hit"] if flags_env.get("cap_set") else flags_sheet.get("daily_cap_hit", False)

    snap = {
        "symbol": sym,
        "expiry": exp,
        "spot": spot,
        "s1": s1, "s2": s2, "r1": r1, "r2": r2,
        "pcr": pcr, "max_pain": mp,
        "ce_oi_delta": ce_d, "pe_oi_delta": pe_d,
        "mv": mv_tag,
        # NEW flags consumed by eligibility_api C5 gate:
        "hold": bool(hold),
        "daily_cap_hit": bool(daily_cap_hit),
        "source": "sheets",
        "ts": int(time.time()),
    }
    return snap

# -------- Main entry --------
async def refresh_once(*args, **kwargs) -> dict:
    status = "ok"; reason = ""; snap: Optional[dict] = None

    if _PROVIDER_FN is not None:
        try:
            ret = await _call_variants(_PROVIDER_FN, _PROVIDER_IS_ASYNC)
            snap = _extract_snapshot(ret)
            if snap is None and isinstance(ret, dict) and isinstance(ret.get("snapshot"), dict):
                snap = ret["snapshot"]
        except Exception as e:
            status, reason = "provider_error", str(e)

    if snap is None:
        s2 = _build_from_sheet()
        if s2 is not None:
            snap = s2
            if status == "ok" and reason == "":
                status, reason = "fallback", "sheets"

    if isinstance(snap, dict):
        set_snapshot(snap)

    return {"status": status, "reason": reason, "snapshot": snap, "provider": _PROVIDER_NAME}
