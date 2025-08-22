# skills/examples/oc_now.py
# ------------------------------------------------------------
# Telegram command handler for /oc_now (python-telegram-bot v20+).
# Adds on-demand warm-up: tries refresh a few times and renders C1..C6.
# ------------------------------------------------------------
from __future__ import annotations
import asyncio
from typing import Any
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

try:
    from analytics.oc_refresh_shim import get_snapshot, refresh_once
except Exception:
    def get_snapshot(): return None
    async def refresh_once(*args, **kwargs): return {"status": "noop"}

from agents.eligibility_api import check_now

WARMUP_TRIES = 5          # up to ~15s total
WARMUP_DELAY_SECS = 3.0

def _fmt_val(x: Any) -> str:
    if x is None:
        return "—"
    try:
        if isinstance(x, float):
            return f"{x:.2f}"
        return str(x)
    except Exception:
        return str(x)

def _fmt_check(c) -> str:
    mark = "✅" if c.get("ok") else "❌"
    return f"{c.get('id')}: {mark} {c.get('reason', '—')}"

def _build_text(result: dict) -> str:
    h = result.get("header", {}) or {}
    shifted = h.get("shifted", {}) or {}
    lines = []
    lines.append(f"*OC Snapshot*")
    lines.append(f"Symbol: `{h.get('symbol','—')}`  |  Exp: `{h.get('expiry','—')}`  |  Spot: `{_fmt_val(h.get('spot'))}`")
    lines.append(f"Levels: S1 `{_fmt_val(h.get('S1'))}`  S2 `{_fmt_val(h.get('S2'))}`  R1 `{_fmt_val(h.get('R1'))}`  R2 `{_fmt_val(h.get('R2'))}`")
    lines.append(f"Shifted: S1* `{_fmt_val(shifted.get('S1*'))}`  S2* `{_fmt_val(shifted.get('S2*'))}`  R1* `{_fmt_val(shifted.get('R1*'))}`  R2* `{_fmt_val(shifted.get('R2*'))}`")
    buf = (h.get("buffers") or {}).get("entry_band")
    lines.append(f"Buffer: `{_fmt_val(buf)}`  |  MV: `{h.get('mv','—')}`  |  PCR: `{_fmt_val(h.get('pcr'))}`  |  MP: `{_fmt_val(h.get('max_pain'))}`")
    lines.append("")
    lines.append("*Checks*")
    for c in result.get("checks", []):
        lines.append("- " + _fmt_check(c))
    lines.append("")
    if result.get("eligible"):
        side = result.get("side") or "—"
        lvl = result.get("level") or "—"
        tp = _fmt_val(result.get("trigger_price"))
        lines.append(f"*Summary:* ✅ Eligible — `{side}` @ `{lvl}` (`{tp}`)")
    else:
        failed = [c.get("id") for c in result.get("checks", []) if not c.get("ok")]
        lines.append(f"*Summary:* ❌ Not eligible — failed: `{', '.join(failed) if failed else '—'}`")
    return "\n".join(lines)

async def _get_or_warmup_snapshot() -> Any:
    snap = get_snapshot()
    if snap is not None:
        return snap
    # Warm-up: kick a few refreshes; shim will publish snapshot if present
    for _ in range(WARMUP_TRIES):
        try:
            await refresh_once()
        except Exception:
            pass
        await asyncio.sleep(WARMUP_DELAY_SECS)
        snap = get_snapshot()
        if snap is not None:
            return snap
    return None

async def cmd_oc_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    snap = await _get_or_warmup_snapshot()
    if snap is None:
        await update.message.reply_text(
            "OC snapshot unavailable (rate-limit/first snapshot). "
            "मैंने refresh kick किया है — ~15s बाद फिर `/oc_now` भेजें."
        )
        return
    result = check_now(snap)
    await update.message.reply_markdown(_build_text(result))

def register(application):
    application.add_handler(CommandHandler("oc_now", cmd_oc_now))
