# providers/dhan_oc.py
# -----------------------------------------------------------------------------
# Thin provider wrapper for Option-Chain refresh with robust backoff.
#
# Responsibilities:
#  - Resolve real fetch function (default: integrations.option_chain_dhan.fetch_levels)
#  - Call it with **exponential backoff** on 429/5xx/provider-errors
#  - Bubble up a normalized dict snapshot (status/source/asof preserved from provider)
#
# ENV (optional):
#   DHAN_PROVIDER_FUNC              -> dotted path to async function(p) -> dict
#   DHAN_MAX_RETRIES                -> default 3
#   DHAN_429_COOLDOWN_SECS          -> base cooldown for 429 (default 4)
#   OC_BACKOFF_BASE_SECS            -> base backoff (default 2)
#   OC_BACKOFF_JITTER_FRAC          -> jitter fraction (default 0.25)
# -----------------------------------------------------------------------------

from __future__ import annotations
import asyncio, importlib, logging, os, random, time
from typing import Any, Awaitable, Callable, Dict, Optional

log = logging.getLogger(__name__)

def _env(name: str, default: Optional[str]=None) -> Optional[str]:
    v = os.environ.get(name)
    return v.strip() if v and str(v).strip() else default

def _is_rate_limit(err: BaseException, payload: Optional[Dict[str, Any]]) -> bool:
    txt = f"{err!s}".lower()
    if "429" in txt or "rate" in txt and "limit" in txt:
        return True
    if payload:
        # Common Dhan error shapes we saw earlier
        if str(payload.get("status","")).lower() in {"rate_limit","rate-limit"}:
            return True
        data = payload.get("Data") or payload.get("data") or {}
        if isinstance(data, dict):
            # not guaranteed — best-effort
            if any("rate" in str(v).lower() and "limit" in str(v).lower() for v in data.values()):
                return True
    return False

def _is_retryable_http(err: BaseException, payload: Optional[Dict[str, Any]]) -> bool:
    txt = f"{err!s}".lower()
    # Retry on 5xx, timeouts, connection resets
    return any(s in txt for s in ["500","502","503","504","timeout","timed out","connection reset","temporarily unavailable"])

def _jittered(base: float, frac: float) -> float:
    j = base * float(frac)
    return max(0.0, base + random.uniform(-j, j))

def _now_ist_str() -> str:
    t = time.time() + 5.5 * 3600
    return time.strftime("%Y-%m-%d %H:%M:%S IST", time.gmtime(t))

def _resolve_func() -> Callable[..., Awaitable[Dict[str, Any]]]:
    # Priority: env override → default integrations module
    path = _env("DHAN_PROVIDER_FUNC") or "integrations.option_chain_dhan.fetch_levels"
    mod_path, _, fn_name = path.rpartition(".")
    if not mod_path:
        raise ImportError(f"DHAN_PROVIDER_FUNC invalid: {path!r}")
    mod = importlib.import_module(mod_path)
    fn = getattr(mod, fn_name, None)
    if fn is None:
        raise ImportError(f"{path}: function not found")
    if not asyncio.iscoroutinefunction(fn):
        raise TypeError(f"{path}: must be async def")
    return fn  # async (p) -> dict

async def _call_with_backoff(p, fn: Callable[..., Awaitable[Dict[str, Any]]]) -> Dict[str, Any]:
    max_retries = int(float(_env("DHAN_MAX_RETRIES", "3")))
    base429 = float(_env("DHAN_429_COOLDOWN_SECS", "4"))
    base = float(_env("OC_BACKOFF_BASE_SECS", "2"))
    jitter_frac = float(_env("OC_BACKOFF_JITTER_FRAC", "0.25"))

    last_exc: Optional[BaseException] = None
    last_payload: Optional[Dict[str, Any]] = None

    for attempt in range(0, max_retries + 1):
        try:
            snap = await fn(p)  # expected to return normalized dict with 'status' etc.
            # If provider marks a recoverable error in payload, decide here:
            st = str(snap.get("status","")).lower()
            if st in {"ok", "success"}:
                return snap
            # Provider error object (keep for decisioning)
            last_payload = snap
            # if it's a rate limit-like content, backoff inside loop:
            if _is_rate_limit(RuntimeError(st), snap):
                if attempt < max_retries:
                    # prefer Retry-After if present
                    retry_after = snap.get("retry_after") or snap.get("Retry-After") or None
                    if retry_after:
                        try:
                            wait = float(retry_after)
                        except Exception:
                            wait = _jittered(base429, jitter_frac)
                    else:
                        wait = _jittered(base429 * (2 ** attempt), jitter_frac)
                    log.warning("oc_refresh: rate-limit detected; retrying in %.1fs (attempt %d/%d)",
                                wait, attempt + 1, max_retries)
                    await asyncio.sleep(wait)
                    continue
                # out of retries: return last payload as-is
                return snap
            # Non rate-limit provider failure: retry on 5xx-like hints inside text if any
            if attempt < max_retries:
                wait = _jittered(base * (2 ** attempt), jitter_frac)
                log.warning("oc_refresh: provider error '%s'; retrying in %.1fs (attempt %d/%d)",
                            st, wait, attempt + 1, max_retries)
                await asyncio.sleep(wait)
                continue
            return snap
        except BaseException as e:
            last_exc = e
            txt = f"{e!s}"
            # Optional structured payload on exception (if thrown by inner layer)
            payload = getattr(e, "payload", None)
            if _is_rate_limit(e, payload):
                if attempt < max_retries:
                    wait = _jittered(base429 * (2 ** attempt), jitter_frac)
                    log.warning("oc_refresh: rate-limit detected; retrying in %.1fs (attempt %d/%d)",
                                wait, attempt + 1, max_retries)
                    await asyncio.sleep(wait)
                    continue
            if _is_retryable_http(e, payload) and attempt < max_retries:
                wait = _jittered(base * (2 ** attempt), jitter_frac)
                log.warning("oc_refresh: retryable error '%s'; backoff %.1fs (attempt %d/%d)",
                            txt, wait, attempt + 1, max_retries)
                await asyncio.sleep(wait)
                continue
            # Non-retryable or out-of-retries: surface provider_error frame
            asof = _now_ist_str()
            return {
                "status": "provider_error",
                "source": "provider",
                "asof": asof,
                "error": str(e),
                "meta": {"retries": attempt, "when": asof}
            }

    # Defensive guard; shouldn't reach
    asof = _now_ist_str()
    return {
        "status": "provider_error",
        "source": "provider",
        "asof": asof,
        "error": str(last_exc or "unknown"),
        "meta": {"payload": last_payload, "when": asof}
    }

# Public API expected by shim
async def refresh_once(p) -> Dict[str, Any]:
    """
    Universal entrypoint used by oc_refresh_shim.
    - Resolves configured provider func (async)
    - Applies robust backoff
    - Returns provider's snapshot dict
    """
    fn = _resolve_func()
    return await _call_with_backoff(p, fn)
