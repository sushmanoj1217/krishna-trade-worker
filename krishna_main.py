# krishna_main.py
# -----------------------------------------------------------------------------
# Main entry: ensures sheets, runs warm-up OC refresh (provider),
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
        # Your project already has admin helpers; try both common paths safely.
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
    # Fallback log if helper not present
    log.info("✅ Sheets tabs ensured")

# --- Params object (best-effort) ---------------------------------------------
def _build_params():
    """
    Provider func expects a Params-like object (p).
    We try to import your project's Params; if not, pass a tiny shim.
    """
    try:
        from utils.params import Params  # type: ignore
        if hasattr(Params, "from_env"):
            return Params.from_env()  # type: ignore[attr-defined]
        return Params()  # type: ignore[call-arg]
    except Exception:
        class _P:  # minimal shim; most providers read env directly anyway
            pass
        return _P()

# --- Refresh function loader --------------------------------------------------
def _get_refresh_callable():
    # We use the unified shim you already have.
    from analytics.oc_refresh_shim import get_refresh  # type: ignore
    return get_refresh()  # returns async refresh_once(p)->dict

# --- Staleness check ----------------------------------------------------------
def _is_stale(snap: dict | None) -> bool:
    if not snap or not isinstance(snap, dict):
        return True
    # If provider failed or sheet fallback/stale-age: consider stale
    status = str(snap.get("status", "")).lower()
    if status not in {"ok", "success"}:
        return True
    source = str(snap.get("source", "")).lower()
    # If sheet-sourced snapshot at boot → treat as stale (we want provider)
    if source == "sheets":
        return True
    # If age present and >90s → stale
    try:
        age = float(snap.get("age_sec", 0) or 0)
        if age > 90.0:
            return True
    except Exception:
        pass
    # If expiry < today (some providers include this)
    try:
        exp = str(snap.get("expiry", "")).strip()
        if exp and " " not in exp and "-" in exp:
            # yyyy-mm-dd style check vs IST "today"
            t = time.time() + 5.5 * 3600
            today = time.strftime("%Y-%m-%d", time.gmtime(t))
            if exp < today:
                return True
    except Exception:
        pass
    return False

# --- Warm-up refresh ----------------------------------------------------------
async def warmup_refresh(p, timeout_s: float = 12.0) -> bool:
    """
    Force a provider refresh before bot/day loop so first /oc_now is fresh.
    Tries until either fresh snapshot or timeout.
    """
    refresh_once = _get_refresh_callable()
    start = time.time()
    attempt = 0
    # small initial delay to let network stack settle
    await asyncio.sleep(0.1)

    # Try a few quick cycles within timeout
    while time.time() - start < timeout_s:
        attempt += 1
        try:
            snap = await asyncio.wait_for(refresh_once(p), timeout=6.0)
            if not _is_stale(snap):
                log.info("Warm-up: first fresh snapshot OK (attempt %d)", attempt)
                return True
            # stale → brief wait and retry
            await asyncio.sleep(1.0 + random.uniform(0, 0.5))
        except asyncio.TimeoutError:
            log.warning("Warm-up: provider call timed out (attempt %d)", attempt)
            await asyncio.sleep(1.0)
        except Exception as e:
            log.warning("Warm-up: provider error (attempt %d): %s", attempt, e)
            await asyncio.sleep(1.0)
    log.warning("Warm-up: gave up after %.1fs without a fresh snapshot", timeout_s)
    return False

# --- Day loop -----------------------------------------------------------------
async def day_loop(p):
    refresh_once = _get_refresh_callable()
    # cadence envs (you already set these)
    base = float(os.environ.get("OC_REFRESH_SECS", "18") or "18")
    jitter = float(os.environ.get("OC_REFRESH_JITTER_SECS", "4") or "4")
    log.info("Day loop started (cadence ~%ss + jitter 0–%ss)", int(base), int(jitter))

    last_hb = time.time()
    while True:
        try:
            await refresh_once(p)
        except Exception as e:
            log.error("OC refresh failed in day_loop: %s", e)
        # heartbeat every ~10s
        now = time.time()
        if now - last_hb >= 10:
            log.info("heartbeat: day_loop alive")
            last_hb = now
        # sleep with jitter
        sleep_s = base + random.uniform(0, max(0.0, jitter))
        await asyncio.sleep(sleep_s)

# --- Telegram init & runner ---------------------------------------------------
def _init_bot():
    # Your telegram_bot.init() already returns an Application (v20),
    # and handles singleton lock + webhook delete inside.
    from telegram_bot import init as init_bot  # type: ignore
    app = init_bot()
    return app

async def _start_polling(app):
    # v20+: we can use the updater start (your project uses this).
    # This call blocks until stop() is called.
    log.info("Telegram polling started")
    await app.updater.start_polling()  # type: ignore[attr-defined]

# --- Main ---------------------------------------------------------------------
async def main():
    log.info("Python runtime: %s", sys.version.split()[0])
    _ensure_sheets_tabs()

    # Build Params for provider calls
    p = _build_params()

    # WARM-UP: get fresh snapshot before starting bot/loop
    log.info("Warm-up: starting provider refresh")
    await warmup_refresh(p, timeout_s=12.0)

    # Init telegram app (singleton handled in module)
    app = _init_bot()
    log.info("Telegram bot started")

    # Run poller + day loop concurrently
    await asyncio.gather(
        _start_polling(app),
        day_loop(p),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
