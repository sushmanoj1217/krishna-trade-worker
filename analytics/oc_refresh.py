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
#       a) explicit delta cols if present
#       b) else from absolute CE_OI/PE_OI vs previous row
#       c) else sign-only proxy from dPCR (prev vs curr)
# Env: GOOGLE_SA_JSON, GSHEET_TRADES_SPREADSHEET_ID, OC_SYMBOL
# ------------------------------------------------------------
from __future__ import annotations
import importlib, inspect, logging
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

# -------- Sheets fallback + derivations --------
def _env(name: str) -> Optional[str]:
    v = os.environ.get(name)
    return v.strip() if v and v.strip() else None

def _open_ws():
    if gspread is None:
        raise RuntimeError("gspread not installed")
    raw = _env("GOOGLE_SA_JSON")
    sid = _env("GSHEET_TRADES_SPREADSHEET_ID")
    if not raw or not sid:
        raise RuntimeError("Sheets env missing")
    sa = json.loads(raw)
    gc = gspread.service_account_from_dict(sa)
    sh = gc.open_by_key(sid)
    try:
        return sh.worksheet("OC_Live")
    except Exception:
        return sh.worksheet("Snapshots")

_NUMERIC = {
    "spot","s1","s2","r1","r2","pcr","max_pain",
    "ce_oi_delta","pe_oi_delta","ce_oi_change","pe_oi_change",
    "ce_oi","pe_oi","ceoi","peoi",
}
def _to_float(x):
    try:
        if x in (None,"","—"): return None
        return float(str(x).replace(",","").strip())
    except Exception:
        return None
def _norm_row(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        key = str(k).strip().lower().replace(" ", "_")
        out[key] = _to_float(v) if key in _NUMERIC else v
    return out

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
    # tie-break: recent PCR drift
    if isinstance(dpcr, (int,float)):
        if dpcr < 0: return "bullish"
        if dpcr > 0: return "bearish"
    return ""

def _pick_delta(curr: Dict[str, Any], prev: Optional[Dict[str, Any]], kind: str) -> Optional[float]:
    kind = kind.lower()  # "ce" or "pe"
    # explicit deltas / aliases
    for key in (f"{kind}_oi_delta", f"{kind}_oi_change", f"{kind}_oiΔ", f"{kind}oi_delta", f"{kind}oi_change"):
        if key in curr and curr[key] is not None:
            return float(curr[key])
    # compute from absolute OI vs prev
    for abs_key in (f"{kind}_oi", f"{kind}oi"):
        c = curr.get(abs_key)
        p = prev.get(abs_key) if prev else None
        if c is not None and p is not None:
            try:
                return float(c) - float(p)
            except Exception:
                pass
    return None

def _build_from_sheet() -> Optional[dict]:
    try:
        ws = _open_ws()
        rows = ws.get_all_records()  # list[dict]
        if not rows:
            return None
        last = _norm_row(rows[-1])
        prev = _norm_row(rows[-2]) if len(rows) >= 2 else None

        sym = (last.get("symbol") or last.get("sym") or _env("OC_SYMBOL") or "").upper()
        exp = last.get("expiry") or last.get("exp") or ""

        # dPCR (for proxies/tie-breaks)
        dpcr = None
        if prev and isinstance(last.get("pcr"), (int,float)) and isinstance(prev.get("pcr"), (int,float)):
            try:
                dpcr = float(last["pcr"]) - float(prev["pcr"])
            except Exception:
                dpcr = None

        # OI deltas: explicit → abs diff → dPCR proxy
        ce_d = _pick_delta(last, prev, "ce")
        pe_d = _pick_delta(last, prev, "pe")
        if ce_d is None and pe_d is None and isinstance(dpcr, (int,float)) and dpcr != 0:
            # sign-only proxy from dPCR
            mag = max(1.0, abs(dpcr) * 1000.0)  # just for display; sign matters
            if dpcr > 0:
                pe_d, ce_d = mag, -mag   # PCR up → PE up / CE down (proxy)
            else:
                pe_d, ce_d = -mag, mag   # PCR down → PE down / CE up (proxy)

        # MV tag: prefer explicit; else derive
        mv_tag = (last.get("mv") or last.get("move") or last.get("trend") or "")
        mv_tag = str(mv_tag).strip().lower()
        if not mv_tag:
            mv_tag = _derive_mv(last.get("pcr"), last.get("max_pain"), last.get("spot"), dpcr)

        snap = {
            "symbol": sym,
            "expiry": exp,
            "spot": last.get("spot"),
            "s1": last.get("s1"),
            "s2": last.get("s2"),
            "r1": last.get("r1"),
            "r2": last.get("r2"),
            "pcr": last.get("pcr"),
            "max_pain": last.get("max_pain"),
            "ce_oi_delta": ce_d,
            "pe_oi_delta": pe_d,
            "mv": mv_tag,
            "source": "sheets",
            "ts": int(time.time()),
        }
        return snap
    except Exception as e:
        _log.warning("oc_refresh: sheets fallback failed: %s", e)
        return None

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
