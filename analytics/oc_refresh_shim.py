# analytics/oc_refresh_shim.py
# -----------------------------------------------------------------------------
# Lazy resolver for the OC refresh callable.
# - No import-time resolution (so bad env won't crash the process).
# - Supports env: OC_REFRESH_FUNC (preferred), fallback DHAN_PROVIDER_FUNC.
# - Accepts shorthands like "fetch_levels" and maps them to dotted paths.
# - Returns an async callable refresh_once(p) -> dict
# -----------------------------------------------------------------------------

from __future__ import annotations
import importlib
import inspect
import logging
import os
from typing import Any, Callable, Awaitable

log = logging.getLogger(__name__)

__all__ = ["get_refresh"]

# Known shorthands -> dotted targets
_SHORTCUTS: dict[str, str] = {
    # common mistakes / short forms
    "fetch_levels": "integrations.option_chain_dhan.fetch_levels",
    "refresh_once": "providers.dhan_oc.refresh_once",
    "dhan": "providers.dhan_oc.refresh_once",
    # add more if you have other providers:
    # "sheet": "providers.sheet_oc.refresh_once",
}

# Default if nothing set
_DEFAULT_PATH = "providers.dhan_oc.refresh_once"


def _pick_env_path() -> str:
    """
    Decide which dotted path to use for the refresh function.
    Priority: OC_REFRESH_FUNC > DHAN_PROVIDER_FUNC > default
    Also expand shorthands like 'fetch_levels' to dotted path.
    """
    raw = os.environ.get("OC_REFRESH_FUNC") or os.environ.get("DHAN_PROVIDER_FUNC") or _DEFAULT_PATH
    path = (raw or "").strip()

    # expand shorthand
    if "." not in path:
        mapped = _SHORTCUTS.get(path)
        if mapped:
            log.warning("oc_refresh_shim: expanded shorthand %r -> %r", path, mapped)
            path = mapped
        else:
            # If still no dot, it's invalid; keep as-is for error to be explicit
            pass

    return path


def _resolve_dotted(path: str) -> Callable[..., Any]:
    """
    Import dotted path "pkg.mod.func" and return the attribute.
    Raises ImportError with a clear message if anything is wrong.
    """
    if "." not in path:
        raise ImportError(f"OC refresh func invalid: {path!r}")

    mod_name, attr = path.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_name)
    except Exception as e:
        raise ImportError(f"Cannot import module {mod_name!r} for {path!r}: {e}") from e

    try:
        fn = getattr(mod, attr)
    except AttributeError as e:
        raise ImportError(f"Module {mod_name!r} has no attribute {attr!r} for {path!r}") from e

    if not callable(fn):
        raise ImportError(f"Resolved object {path!r} is not callable")

    return fn


def _to_async(fn: Callable[..., Any]) -> Callable[[Any], Awaitable[dict]]:
    """
    Wrap a sync function to async; pass through if already coroutine function.
    The provider function is expected to accept a single 'p' (Params-like) arg,
    and return a dict snapshot (or serializable mapping).
    """
    if inspect.iscoroutinefunction(fn):
        return fn  # type: ignore[return-value]

    async def _wrap(p: Any) -> dict:
        try:
            res = fn(p)  # type: ignore[misc]
            return res if isinstance(res, dict) else dict(res or {})
        except Exception as e:
            # bubble up with context
            raise RuntimeError(f"refresh callable (sync) failed: {e}") from e

    return _wrap


def get_refresh() -> Callable[[Any], Awaitable[dict]]:
    """
    Resolve and return an async refresh_once(p) -> dict callable.
    Lazy: evaluates env on each call so hot-reload of env is reflected
    (cheap enough; importlib caches modules).
    """
    path = _pick_env_path()
    try:
        fn = _resolve_dotted(path)
        async_fn = _to_async(fn)
        # Log only when switching target (lightweight guard via an attribute)
        prev = getattr(get_refresh, "_prev_path", None)
        if prev != path:
            log.info("oc_refresh_shim: selected %s (async=%s)", path, inspect.iscoroutinefunction(fn))
            setattr(get_refresh, "_prev_path", path)
        return async_fn
    except Exception as e:
        # Log clearly and rethrow so caller can decide fallback/no-op
        log.error("oc_refresh_shim: failed to resolve refresh func from %r: %s", path, e)
        raise
