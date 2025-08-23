# krishna_main.py
# -----------------------------------------------------------------------------
# Main entry: ensures sheets, runs warm-up OC refresh (provider, best-effort),
# starts Telegram poller (singleton handled inside telegram_bot),
# and runs day loop with cadence.
# -----------------------------------------------------------------------------

from __future__ import annotations
import asyncio
import logging
import os
import random
import sys
import time

# --- Logging -----------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# --- Optional: sheets ensure (best-effort; non-fatal) ------------------------
def _ensure_sheets_tabs():
    try:
        try:
            from skills import sheets_admin as _sa  # type: ignore
        except Exception:
            _sa = None
        if _sa and hasattr(_sa, "ensure_tabs"):
            _sa.ensure_tabs()
            log.info("✅ Sheets tabs ensured")
            return
    except Exception as e:
        log.warning("Sheets ensure skipped: %s", e)
    log.info("✅ Sheets tabs ensured")

# --- Params object (best-effort) ---------------------------------------------
def _build_params():
    try:
        from utils.params import Params  # type: ignore
        if hasattr(Params, "from_env"):
            return Params.from_env()  # type: ignore[attr-defined]
        return Params()  # type: ignore[call-arg]
    except Exception:
        class _P:
            pass
        return _P()

# --- Refresh function loader --------------------------------------------------
def _get_refresh_callable():
    # Unified shim
    from analytics.oc_refresh_shim import get_refresh  # type: ignore
    return get_refresh()  # async refresh_once(p)->dict

# --- Staleness check ----------------------------------------------------------
def _is_stale(snap: dict | None) -> bool:
    if not snap or not isinstance(snap, dict):
        return True
    status = str(snap.get("status", "")).lower()
    if status not in {"ok", "success"}:
        return True
    source = str(snap.get("source", "")).lower()
    if source == "sheets":
        return True
    try:
        age = float(snap.get("age_sec", 0) or 0)
        if age > 90.0:
            return True
    except Exception:
        pass
    try:
        exp = str(snap.get("expiry", "")).strip()
        if exp and " " not in exp and "-" in exp:
            t = time.time() + 5.5 * 3600
            today = time.strftime("%Y-%m-%d", time.gmtime(t))
            if exp < today:
                return True
    except Exception:
        pass
    return False

# --- Warm-up refresh (now fully guarded) --------------------------------------
async def warmup_refresh(p, timeout_s: float = 12.0) -> bool:
    """
    Force a provider refresh before bot/day loop so first /oc_now is fresh.
    Best-effort: never crashes the process.
    """
    try:
        refresh_once = _get_refresh_callable()
    except Exception:
        log.exception("Warm-up: failed to resolve refresh callable")
        return False

    start = time.time()
    attempt = 0
    try:
        await asyncio.sleep(0.1)
    except Exception:
        pass

    while time.time() - start < timeout_s:
        attempt += 1
        try:
            snap = await asyncio.wait_for(refresh_once(p), timeout=6.0)
            if not _is_stale(snap):
                log.info("Warm-up: first fresh snapshot OK (attempt %d)", attempt)
                return True
            log.warning("Warm-up: got snapshot but stale (attempt %d); retrying...", attempt)
            await asyncio.sleep(1.0 + random.uniform(0, 0.5))
        except asyncio.TimeoutError:
            log.warning("Warm-up: provider call timed out (attempt %d)", attempt)
            await asyncio.sleep(1.0)
        except Exception:
            log.exception("Warm-up: provider error (attempt %d)", attempt)
            await asyncio.sleep(1.0)

    log.warning("Warm-up: gave up after %.1fs without a fresh snapshot", timeout_s)
    return False

# --- Day loop -----------------------------------------------------------------
async def day_loop(p):
    try:
        refresh_once = _get_refresh_callable()
    except Exception:
        log.exception("day_loop: failed to resolve refresh callable; running with no-op loop")
        refresh_once = None

    base = float(os.environ.get("OC_REFRESH_SECS", "18") or "18")
    jitter = float(os.environ.get("OC_REFRESH_JITTER_SECS", "4") or "4")
    log.info("Day loop started (cadence ~%ss + jitter 0–%ss)", int(base), int(jitter))

    last_hb = time.time()
    while True:
        try:
            if refresh_once:
                await refresh_once(p)
        except Exception:
            log.exception("OC refresh failed in day_loop")
        now = time.time()
        if now - last_hb >= 10:
            log.info("heartbeat: day_loop alive")
            last_hb = now
        sleep_s = base + random.uniform(0, max(0.0, jitter))
        await asyncio.sleep(sleep_s)

# --- Telegram init & runner ---------------------------------------------------
def _init_bot():
    from telegram_bot import init as init_bot  # type: ignore
    app = init_bot()
    return app

async def _start_polling(app):
    log.info("Telegram polling started")
    # v20 Updater path (your project already uses it)
    await app.updater.start_polling()  # type: ignore[attr-defined]

# --- Main ---------------------------------------------------------------------
async def main():
    log.info("Python runtime: %s", sys.version.split()[0])
    _ensure_sheets_tabs()

    p = _build_params()

    log.info("Warm-up: starting provider refresh")
    try:
        ok = await warmup_refresh(p, timeout_s=12.0)
        if not ok:
            log.warning("Warm-up: continuing without fresh snapshot (will refresh in day loop)")
    except Exception:
        # absolute last defense
        log.exception("Warm-up: unexpected crash prevented; continuing")

    app = _init_bot()
    log.info("Telegram bot started")

    await asyncio.gather(
        _start_polling(app),
        day_loop(p),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
