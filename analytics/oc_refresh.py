# analytics/oc_refresh.py
# ------------------------------------------------------------
# Unified OC refresh core with stable API:
#   - refresh_once(*args, **kwargs) -> dict snapshot
#   - get_snapshot() -> dict|None
#   - set_snapshot(snap: dict) -> None
#
# Works in two stages:
# 1) Tries to call a provider in known modules (Dhan/OC sources)
#    with flexible args and extracts a snapshot.
# 2) If provider missing/fails, falls back to Google Sheets:
#    builds snapshot from the last row of "OC_Live" (else "Snapshots"),
#    and DERIVES missing fields:
#       - mv (bullish/bearish/empty) from PCR + MaxPain vs Spot
#       - ce_oi_delta / pe_oi_delta from delta columns OR by diffing
#         current vs previous row CE_OI/PE_OI (if present)
#
# Env used:
#   GOOGLE_SA_JSON, GSHEET_TRADES_SPREADSHEET_ID, OC_SYMBOL
# ------------------------------------------------------------
from __future__ import annotations
import importlib
import inspect
import logging
from typing import Any, Callable, Optional, Dict, Tuple

import json, os, time

# Sheets (optional)
try:
    import gspread  # type: ignore
except Exception:
    gspread = None  # type: ignore

_log = logging.getLogger(__name__)

# Module-global latest snapshot
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
    "analytics.oc_sources",
    "analytics.oc_core",
    "analytics.oc_backend",
    "integrations.dhan_oc",
    "integrations.oc_feed",
    "providers.dhan_oc",
    "providers.oc",
    "dhan.oc",
    "oc.providers",
]
_FN_CANDIDATE_NAMES = [
    "refresh_once",
    "refresh_now",
    "run_once",
    "refresh",
    "do_refresh",
    "refresh_tick",
    "refresh_snapshot",
    "oc_refresh",
    "fetch_levels",
    "get_oc_snapshot",
    "compute_levels",
    "compute_snapshot",
    "build_snapshot",
    "get_levels",
]

def _score_name(name: str) -> int:
    n = name.lower()
    order = {nm: i for i, nm in enumerate(_FN_CANDIDATE_NAMES)}
    if n in order: return order[n]
    if "refresh" in n: return 50
    if any(k in n for k in ("snapshot", "levels", "oc")): return 60
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
                score = _score_name(nm)
                if score < 999:
                    # required positional params (for arg-flex)
                    try:
                        sig = inspect.signature(obj)
                        req = sum(
                            1 for p in sig.parameters.values()
                            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
                            and p.default is inspect._empty
                        )
                    except Exception:
                        req = 0
                    cands.append((score, req, nm, obj, inspect.iscoroutinefunction(obj)))
        if cands:
            cands.sort(key=lambda t: (t[0], t[1]))  # best score, fewer required args
            _, req, nm, fn, is_coro = cands[0]
            _log.info("oc_refresh: provider %s.%s (async=%s, req=%s)", mod_name, nm, is_coro, req)
            return fn, f"{mod_name}.{nm}", is_coro
    return None, "", False

_PROVIDER_FN, _PROVIDER_NAME, _PROVIDER_IS_ASYNC = _discover_provider()

def _extract_snapshot(ret: Any) -> Optional[dict]:
    """Heuristic: find a dict with expected OC keys."""
    def looks(d: Any) -> bool:
        if not isinstance(d, dict): return False
        k = set(x.lower() for x in d.keys())
        if "spot" in k and ({"s1","s2","r1","r2"} & k):
            return True
        if {"symbol","expiry","spot"} <= k:
            return True
        if "levels" in k and "spot" in k:
            return True
        return False

    if looks(ret): return ret
    if isinstance(ret, (tuple, list)):
        for x in ret:
            if looks(x): return x
    for attr in ("snapshot","data","result"):
        if hasattr(ret, attr):
            try:
                val = getattr(ret, attr)
                if looks(val): return val
            except Exception:
                pass
    return None

def _call_variants(fn: Callable, is_async: bool):
    """Return a coroutine that tries (), (None,), ({},) in order."""
    async def _runner():
        variants = [
            ((), {}),
            ((None,), {}),
            (({},), {}),
        ]
        for a,k in variants:
            try:
                if is_async:
                    res = fn(*a, **k)  # may or may not be awaitable
                    if inspect.isawaitable(res):
                        res = await res
                else:
                    res = fn(*a, **k)
                return res
            except TypeError:
                continue
        # last resort
        res = fn()
        if inspect.isawaitable(res):
            res = await res
        return res
    return _runner()

# -------- Sheets fallback --------
def _env(name: str) -> Optional[str]:
    v = os.environ.get(name)
    return v if v and v.strip() else None

def _open_status_ws():
    """Open spreadsheet and return 'OC_Live' worksheet if present (else 'Snapshots')."""
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
        ws = sh.worksheet("OC_Live")
    except Exception:
        ws = sh.worksheet("Snapshots")
    return ws

_NUMERIC_COLS = {
    "spot","s1","s2","r1","r2","pcr","max_pain",
    "ce_oi_delta","pe_oi_delta","ce_oi_change","pe_oi_change",
    "ce_oi","pe_oi",
}

def _to_float(x):
    try:
        if x in (None,"","—"): return None
        return float(str(x).replace(",","").strip())
    except Exception:
        return None

def _row_norm(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        key = str(k).strip().lower()
        out[key] = _to_float(v) if key in _NUMERIC_COLS else v
    return out

def _derive_mv(pcr: Optional[float], max_pain: Optional[float], spot: Optional[float]) -> str:
    """
    Simple, robust MV derivation when explicit tag missing:
    +1 if PCR>=1.0, -1 if PCR<=1.0
    +1 if MP>spot, -1 if MP<spot
    score>0 -> bullish, score<0 -> bearish, else "".
    """
    score = 0
    if isinstance(pcr, (int,float)):
        if pcr >= 1.0: score += 1
        elif pcr <= 1.0: score -= 1
    if isinstance(max_pain, (int,float)) and isinstance(spot, (int,float)):
        if max_pain > spot: score += 1
        elif max_pain < spot: score -= 1
    if score > 0: return "bullish"
    if score < 0: return "bearish"
    return ""

def _pick_oi_delta(curr: Dict[str, Any], prev: Optional[Dict[str, Any]], ce_or_pe: str) -> Optional[float]:
    """
    Try multiple ways to get OI delta for CE/PE:
      1) explicit delta columns: ce_oi_delta / ce_oi_change (and pe_…)
      2) compute from current ce_oi minus prev ce_oi (if both present)
    """
    ce_or_pe = ce_or_pe.lower()
    # explicit delta
    for key in (f"{ce_or_pe}_oi_delta", f"{ce_or_pe}_oi_change", f"{ce_or_pe}_oiΔ"):
        if key in curr and curr[key] is not None:
            return float(curr[key])
    # compute from absolute OI vs previous row
    curr_abs = curr.get(f"{ce_or_pe}_oi")
    prev_abs = prev.get(f"{ce_or_pe}_oi") if prev else None
    if curr_abs is not None and prev_abs is not None:
        try:
            return float(curr_abs) - float(prev_abs)
        except Exception:
            pass
    return None

def _build_from_sheet() -> Optional[dict]:
    try:
        ws = _open_status_ws()
        rows = ws.get_all_records()  # list of dicts with header mapping
        if not rows:
            return None
        last = _row_norm(rows[-1])
        prev = _row_norm(rows[-2]) if len(rows) >= 2 else None

        sym = (last.get("symbol") or last.get("sym") or _env("OC_SYMBOL") or "").upper()
        exp = last.get("expiry") or last.get("exp") or ""

        # derive deltas if missing
        ce_delta = _pick_oi_delta(last, prev, "ce")
        pe_delta = _pick_oi_delta(last, prev, "pe")

        # mv: prefer tag from sheet; else derive from PCR/MP/spot
        mv_tag = (last.get("mv") or last.get("move") or last.get("trend") or "").strip().lower()
        if not mv_tag:
            mv_tag = _derive_mv(last.get("pcr"), last.get("max_pain"), last.get("spot"))

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
            "ce_oi_delta": ce_delta,
            "pe_oi_delta": pe_delta,
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
    """
    Try provider; else read last OC snapshot from Sheets.
    Always returns a dict: {"status": "...", "snapshot": dict|None, ...}
    and publishes _SNAPSHOT if extracted.
    """
    status = "ok"
    reason = ""
    snap: Optional[dict] = None

    # 1) Provider
    if _PROVIDER_FN is not None:
        try:
            ret = await _call_variants(_PROVIDER_FN, _PROVIDER_IS_ASYNC)
            snap = _extract_snapshot(ret)
            if snap is None and isinstance(ret, dict) and "snapshot" in ret and isinstance(ret["snapshot"], dict):
                snap = ret["snapshot"]
        except Exception as e:
            status, reason = "provider_error", str(e)

    # 2) Sheets fallback
    if snap is None:
        s2 = _build_from_sheet()
        if s2 is not None:
            snap = s2
            if status == "ok" and reason == "":
                status = "fallback"
                reason = "sheets"

    # 3) Publish (if any)
    if isinstance(snap, dict):
        set_snapshot(snap)

    return {
        "status": status,
        "reason": reason,
        "snapshot": snap,
        "provider": _PROVIDER_NAME,
    }
