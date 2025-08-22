# telegram_bot.py
# v20-compatible Application factory with /oc_now
from __future__ import annotations

import os
import logging
from typing import Any, Dict, Optional

from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram import Update

# prefer oc_refresh_shim if present (it selects the right refresh impl)
try:
    from analytics.oc_refresh_shim import refresh_once, get_snapshot
except Exception:
    from analytics.oc_refresh import refresh_once, get_snapshot  # type: ignore

log = logging.getLogger(__name__)

def _bold(x: str) -> str: return f"*{x}*"
def _code(x: str) -> str: return f"`{x}`"

def _fmt_num(x: Optional[float]) -> str:
    try:
        if x is None: return "—"
        return f"{float(x):.2f}"
    except Exception:
        return "—"

async def _handle_oc_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Kick a refresh, then read snapshot
    try:
        await refresh_once()
    except Exception as e:
        log.warning("refresh_once failed: %s", e)

    s: Dict[str, Any] = get_snapshot() or {}
    if not s:
        await update.message.reply_text("OC snapshot unavailable (no data). Try again in 20–30s.")
        return

    # header fields
    sym = str(s.get("symbol") or "—")
    exp = str(s.get("expiry") or "—")
    spot = _fmt_num(s.get("spot"))
    s1, s2, r1, r2 = _fmt_num(s.get("s1")), _fmt_num(s.get("s2")), _fmt_num(s.get("r1")), _fmt_num(s.get("r2"))
    pcr, mp = _fmt_num(s.get("pcr")), _fmt_num(s.get("max_pain"))
    mv = str(s.get("mv") or "")

    # source / staleness
    source = str(s.get("source") or "?")
    asof = str(s.get("asof") or "")
    age = s.get("age_sec")
    age_txt = (f"{int(age)}s" if isinstance(age, (int, float)) else "—")
    stale = bool(s.get("stale"))
    stale_reason = s.get("stale_reason") or []

    # Compute shifted levels (12 buffer) same as before
    try:
        buf = float(os.environ.get("LEVEL_BUFFER", "12"))
    except Exception:
        buf = 12.0
    try:
        s1_shift = float(s.get("s1")) - buf if s.get("s1") is not None else None
        s2_shift = float(s.get("s2")) - buf if s.get("s2") is not None else None
        r1_shift = float(s.get("r1")) + buf if s.get("r1") is not None else None
        r2_shift = float(s.get("r2")) + buf if s.get("r2") is not None else None
    except Exception:
        s1_shift = s2_shift = r1_shift = r2_shift = None

    # Build message
    parts = []
    parts.append(_bold("OC Snapshot"))
    meta = f"Symbol: {sym}  |  Exp: {exp}  |  Spot: {spot}"
    parts.append(meta)

    lvls = f"Levels: S1 {s1}  S2 {s2}  R1 {r1}  R2 {r2}"
    parts.append(lvls)

    sh = f"Shifted: S1 {_code(_fmt_num(s1_shift))}  S2 {_fmt_num(s2_shift)}  R1 {_code(_fmt_num(r1_shift))}  R2 {_fmt_num(r2_shift)}"
    parts.append(sh)

    parts.append(f"Buffer: {buf:.2f}  |  MV: {mv or ' '}  |  PCR: {pcr}  |  MP: {mp}")
    src_line = f"Source: {source}  |  As-of: {asof or 'n/a'}  |  Age: {age_txt}"
    if stale:
        src_line += "  |  ⚠️ STALE " + ("; ".join(stale_reason) if stale_reason else "")
    parts.append(src_line)

    # Checks (delegate to existing eligibility if available)
    try:
        from agents import eligibility_api as elig  # type: ignore
        res = elig.check_now(s)
        checks = res.get("checks") or []
        parts.append("")
        parts.append("Checks")
        for c in checks:
            ok = "✅" if c.get("ok") else "❌"
            reason = c.get("reason") or ""
            parts.append(f"- {c.get('id')}: {ok} {reason}")
        # Summary (override if stale)
        summary = res.get("summary") or ""
        if stale:
            summary = "⚠️ STALE DATA — live mismatch; no trade. (Reasons: " + ", ".join(stale_reason) + ")"
        parts.append("")
        parts.append(f"Summary: {summary}")
    except Exception:
        # Fallback summary if eligibility module not present
        if stale:
            parts.append("")
            parts.append("Summary: ⚠️ STALE DATA — live mismatch; no trade.")
        else:
            parts.append("")
            parts.append("Summary: (eligibility module unavailable)")

    text = "\n".join(parts)
    await update.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)

def init():
    """Build and return a PTB Application with handlers registered."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN not set")
    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("oc_now", _handle_oc_now))
    log.info("/oc_now handler registered")
    return app
