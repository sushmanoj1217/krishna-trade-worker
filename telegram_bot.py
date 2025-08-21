# telegram_bot.py
from __future__ import annotations

import os
import time
import traceback
from typing import Any, Dict, Optional, Tuple

# ---- logging ---------------------------------------------------------------
try:
    from utils.logger import log
except Exception:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("telegram_bot")

# ---- project deps ----------------------------------------------------------
# OC snapshot refresh (last-good cache inside oc_refresh/in cache utils)
try:
    from analytics.oc_refresh import refresh_once
except Exception as e:  # pragma: no cover
    refresh_once = None
    log.error(f"Import failed: analytics.oc_refresh.refresh_once: {e}")

# Params (buffers, bands…)
try:
    from utils.params import Params
except Exception as e:  # pragma: no cover
    Params = None  # type: ignore
    log.error(f"Import failed: utils.params.Params: {e}")

# Optional Sheets taps (not required for /oc_now)
try:
    from integrations import sheets as sh
except Exception:
    sh = None  # type: ignore

# Optional ops command bridge (to not break your /run oc_auto etc)
try:
    from ops import commands as ops_cmds  # expect: handle_run_command(update, context)
    _HAS_OPS = hasattr(ops_cmds, "handle_run_command")
except Exception:
    ops_cmds = None  # type: ignore
    _HAS_OPS = False

# Optional signal generator
try:
    import agents.signal_generator as sg
    _HAS_SG = True
except Exception:
    sg = None  # type: ignore
    _HAS_SG = False

# ---- telegram imports (PTB v20+) ------------------------------------------
from telegram import Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

# ----------------------------------------------------------------------------
# Module state
_LAST_OC_SNAPSHOT: Optional[Dict[str, Any]] = None
_LAST_OC_TS: float = 0.0
_MIN_OC_REUSE_SEC = 8  # allow quick /oc_now right after /run oc_now without spamming OC feed

def _ensure_params() -> Any:
    """Get merged Params (defaults+overrides) with safe fallback."""
    if Params is None:
        class _P:  # minimal fallback
            symbol = os.getenv("OC_SYMBOL", "NIFTY")
            buffer_points = None
        return _P()
    try:
        return Params()
    except Exception as e:
        log.error(f"Params init failed: {e}")
        class _P:  # minimal fallback
            symbol = os.getenv("OC_SYMBOL", "NIFTY")
            buffer_points = None
        return _P()

# ----------------------------------------------------------------------------
# Helpers: compute + format
def _try_compute_checks(snapshot: Dict[str, Any], params: Any) -> Dict[str, Any]:
    """
    Try multiple well-known entrypoints in agents.signal_generator, return a normalized dict.
    If none found or error → return structure with 'unknown' marks (so UI shows gracefully).
    """
    result: Dict[str, Any] = {
        "eligible": False,
        # checks
        "c1": None, "c2": None, "c3": None, "c4": None, "c5": None, "c6": None,
        "c1_reason": "", "c2_reason": "", "c3_reason": "", "c4_reason": "", "c5_reason": "", "c6_reason": "",
        # MV & OC-pattern
        "mv_pcr_ok": None, "mv_mp_ok": None, "mv_basis": "",
        "oc_pattern": "", "oc_pattern_basis": "",
        # triggers/near-cross
        "near": "", "cross": "",
        # decision reason
        "reason": "",
    }
    if not _HAS_SG:
        result["reason"] = "signal_engine_unavailable"
        return result

    candidates = [
        "evaluate_checks",     # (snapshot, params) -> dict
        "evaluate",            # (snapshot, params) -> dict OR (signal, dict)
        "generate_signal",     # (snapshot, params) -> dict
        "gen_signal",          # (snapshot, params) -> dict
    ]
    called = False
    for name in candidates:
        fn = getattr(sg, name, None)
        if not callable(fn):
            continue
        try:
            out = fn(snapshot, params)  # type: ignore
            called = True
            # normalize
            if isinstance(out, tuple) and len(out) == 2 and isinstance(out[1], dict):
                out = out[1]
            if isinstance(out, dict):
                result.update({k: out.get(k, result.get(k)) for k in result.keys()})
                if "checks" in out and isinstance(out["checks"], dict):
                    for ck, v in out["checks"].items():
                        if ck in result:
                            result[ck] = v
                for a, b in [("mv_pcr", "mv_pcr_ok"), ("mv_mp", "mv_mp_ok")]:
                    if a in out and result.get(b) is None:
                        result[b] = out[a]
            break
        except Exception as e:
            log.error(f"signal_generator.{name} failed: {e}\n{traceback.format_exc()}")
            result["reason"] = f"{name}_error"
            called = True
            break

    if not called:
        result["reason"] = "no_entrypoint_in_signal_generator"

    return result


def _fmt_bool(v: Optional[bool]) -> str:
    if v is True:
        return "✅"
    if v is False:
        return "❌"
    return "—"

def _fmt_none(v: Any, alt: str = "—") -> str:
    return alt if v is None else str(v)

def _derive_bias(checks: Dict[str, Any]) -> str:
    tags = []
    if checks.get("mv_pcr_ok") or checks.get("mv_mp_ok"):
        tags.append("mv")
    if checks.get("oc_pattern"):
        tags.append(checks["oc_pattern"])
    return " ".join(tags) if tags else "None"

def _format_oc_now(snapshot: Dict[str, Any], checks: Dict[str, Any]) -> str:
    spot = snapshot.get("spot")
    vix = snapshot.get("vix")
    pcr = snapshot.get("pcr")
    mp = snapshot.get("max_pain")
    mp_dist = None
    try:
        if spot is not None and mp is not None:
            mp_dist = round(float(spot) - float(mp), 2)
    except Exception:
        mp_dist = None

    s1 = snapshot.get("s1"); s2 = snapshot.get("s2")
    r1 = snapshot.get("r1"); r2 = snapshot.get("r2")
    exp = snapshot.get("expiry")

    t_s1 = snapshot.get("t_s1", s1)
    t_s2 = snapshot.get("t_s2", s2)
    t_r1 = snapshot.get("t_r1", r1)
    t_r2 = snapshot.get("t_r2", r2)

    eligible = checks.get("eligible", False)
    bias = _derive_bias(checks)

    mv_line = f"MV: PCR {pcr if pcr is not None else '—'} → {_fmt_bool(checks.get('mv_pcr_ok'))}  " \
              f"MPΔ {mp_dist if mp_dist is not None else '—'} → {_fmt_bool(checks.get('mv_mp_ok'))}"
    oc_line = f"OC-Pattern: {_fmt_none(checks.get('oc_pattern'))}"

    checks_line = (
        f"6-Checks "
        f"C1 {_fmt_bool(checks.get('c1'))} · "
        f"C2 {_fmt_bool(checks.get('c2'))} · "
        f"C3 {_fmt_bool(checks.get('c3'))} · "
        f"C4 {_fmt_bool(checks.get('c4'))} · "
        f"C5 {_fmt_bool(checks.get('c5'))} · "
        f"C6 {_fmt_bool(checks.get('c6'))}"
    )

    header = (
        f"Spot {spot if spot is not None else '—'} | VIX {_fmt_none(vix)} | PCR {_fmt_none(pcr)}\n"
        f"MaxPain {mp if mp is not None else '—'} (Δ {_fmt_none(mp_dist)}) | Bias {bias} | Exp {exp if exp else '—'}\n"
    )
    levels = (
        f"Levels\n"
        f"S1 {s1} / S2 {s2} / R1 {r1} / R2 {r2}\n"
        f"Triggers → S1 {t_s1:.2f} | S2 {t_s2:.2f} | R1 {t_r1:.2f} | R2 {t_r2:.2f}\n"
    )

    decision = f"Decision: {'Eligible' if eligible else 'Not Eligible'}"
    reason = checks.get("reason", "")
    if reason:
        decision += f" | {reason}"

    msg = header + levels + checks_line + "\n" + oc_line + "\n" + mv_line + "\n" + decision
    return msg

# ----------------------------------------------------------------------------
# Telegram command handlers (async for PTB v20+)
async def _refresh_and_compute(force_refresh: bool) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any], str]:
    """
    Returns: (snapshot_or_none, checks, note)
    note contains reason if snapshot missing (rate limit etc).
    """
    global _LAST_OC_SNAPSHOT, _LAST_OC_TS

    params = _ensure_params()
    snap: Optional[Dict[str, Any]] = None
    note = ""

    now = time.time()
    can_reuse = (now - _LAST_OC_TS) < _MIN_OC_REUSE_SEC

    if refresh_once is None:
        note = "oc_refresh_unavailable"
        snap = _LAST_OC_SNAPSHOT
    else:
        try:
            if force_refresh or not can_reuse or _LAST_OC_SNAPSHOT is None:
                snap = refresh_once(params)  # sync function in your codebase
                if snap:
                    _LAST_OC_SNAPSHOT = snap
                    _LAST_OC_TS = now
            else:
                snap = _LAST_OC_SNAPSHOT
        except Exception as e:
            note = f"OC refresh failed: {e.__class__.__name__}"
            log.error(f"OC refresh failed in /oc_now: {e}")
            snap = _LAST_OC_SNAPSHOT

    checks: Dict[str, Any] = {}
    if snap:
        try:
            checks = _try_compute_checks(snap, params)
        except Exception as e:
            log.error(f"try_compute_checks failed: {e}\n{traceback.format_exc()}")
            checks = {"eligible": False, "reason": "compute_error"}
    else:
        checks = {"eligible": False, "reason": "no_snapshot"}

    return snap, checks, note


async def cmd_oc_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    snap, checks, note = await _refresh_and_compute(force_refresh=False)
    if not snap:
        await update.message.reply_text(
            "OC snapshot unavailable (rate-limit/first snapshot). 20–30s बाद फिर से /oc_now भेजें."
        )
        return

    msg = _format_oc_now(snap, checks)
    if note:
        msg = f"{msg}\n\n_note: {note}_"
    await update.message.reply_text(msg)


async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    parts = text.split()
    if len(parts) >= 2 and parts[1].lower() == "oc_now":
        snap, checks, note = await _refresh_and_compute(force_refresh=True)
        if not snap:
            await update.message.reply_text("OC snapshot unavailable (rate-limit?). बाद में /run oc_now फिर से करें.")
            return
        msg = _format_oc_now(snap, checks)
        if note:
            msg = f"{msg}\n\n_note: {note}_"
        await update.message.reply_text(msg)
        return

    if _HAS_OPS:
        try:
            # ops bridge may still be sync; call directly
            ops_cmds.handle_run_command(update, context)  # type: ignore
            return
        except Exception as e:
            log.error(f"ops_cmds.handle_run_command failed: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("Run command failed in ops bridge.")
            return

    await update.message.reply_text("Use: /run oc_now  |  (other /run commands via ops are not wired here)")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Krishna bot online. Try /oc_now or /run oc_now")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Commands:\n"
        "/oc_now — latest OC snapshot + checks\n"
        "/run oc_now — force refresh + checks\n"
        "/run oc_auto on|off|status — (if ops bridge enabled)\n"
    )

# ----------------------------------------------------------------------------
# Boot (async for PTB v20+)
async def init(token: Optional[str] = None) -> Application:
    token = token or os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN missing")

    app: Application = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("oc_now", cmd_oc_now))
    app.add_handler(CommandHandler("run", cmd_run))

    # Initialize & start application, then start polling (non-blocking)
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    log.info("Telegram polling started")
    return app
