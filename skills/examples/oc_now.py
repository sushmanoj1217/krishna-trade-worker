# skills/examples/oc_now.py
# ------------------------------------------------------------
# Telegram command handler for /oc_now (python-telegram-bot v20+).
# Adds on-demand warm-up: if snapshot is missing (first run / rate-limit),
# it will try calling refresh_once() a few times with short delays and then
# render C1..C6 if data becomes available.
# ------------------------------------------------------------
from __future__ import annotations
import asyncio
from typing import Any
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

# Snapshot + safe refresh shim (awaitable)
try:
    from analytics.oc_refresh_shim import get_snapshot, refresh_once
except Exception:
    # ultra-safe fallbacks
    def get_snapshot():
        return None
    async def refresh_once(*args, **kwargs):
        return {"status": "noop"}

from agents.eligibility_api import check_now

# ---- Tunables for warm-up attempts ----
WARMUP_TRIES = 3           # how many attempts to kick a refresh when snapshot is None
WARMUP_DELAY_SECS = 2.0    # delay between attempts (keep small to avoid 429 spam)

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
    lines.append(
        f"Symbol: `{h.get('symbol','—')}`  |  Exp: `{h.get('expiry','—')}`  |  Spot: `{_fmt_val(h.get('spot'))}`"
    )
    lines.append(
        f"Levels: S1 `{_fmt_val(h.get('S1'))}`  S2 `{_fmt_val(h.get('S2'))}`  "
        f"R1 `{_fmt_val(h.get('R1'))}`  R2 `{_fmt_val(h.get('R2'))}`"
    )
    lines.append(
        f"Shifted: S1* `{_fmt_val(shifted.get('S1*'))}`  S2* `{_fmt_val(shifted.get('S2*'))}`  "
        f"R1* `{_fmt_val(shifted.get('R1*'))}`  R2* `{_fmt_val(shifted.get('R2*'))}`"
    )
    buf = (h.get("buffers") or {}).get("entry_band")
    mv = h.get("mv")
    pcr = h.get("pcr")
    mp = h.get("max_pain")
    lines.append(
        f"Buffer: `{_fmt_val(buf)}`  |  MV: `{mv or '—'}`  |  PCR: `{_fmt_val(pcr)}`  |  MP: `{_fmt_val(mp)}`"
    )
    lines.append("")  # spacer

    lines.append("*Checks*")
    for c in result.get("checks", []):
        lines.append("- " + _fmt_check(c))

    lines.append("")  # spacer
    if result.get("eligible"):
        side = result.get("side") or "—"
        lvl = result.get("level") or "—"
        tp = result.get("trigger_price")
        tp_s = _fmt_val(tp)
        lines.append(f"*Summary:* ✅ Eligible — `{side}` @ `{lvl}` (`{tp_s}`)")
    else:
        failed = [c.get("id") for c in result.get("checks", []) if not c.get("ok")]
        failed_s = ", ".join(failed) if failed else "—"
        lines.append(f"*Summary:* ❌ Not eligible — failed: `{failed_s}`")
    return "\n".join(lines)

async def _get_or_warmup_snapshot() -> Any:
    snap = get_snapshot()
    if snap is not None:
        return snap
    # Best-effort warm-up: try a few quick refreshes
    for _ in range(WARMUP_TRIES):
        try:
            # refresh_once is awaitable via shim and arg-safe
            await refresh_once()
        except Exception:
            # ignore: we only need snapshot to appear
            pass
        await asyncio.sleep(WARMUP_DELAY_SECS)
        snap = get_snapshot()
        if snap is not None:
            return snap
    return None

async def cmd_oc_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    snap = await _get_or_warmup_snapshot()
    if snap is None:
        # still unavailable after warm-up → clear + short guidance
        # keep it brief to avoid chat spam
        await update.message.reply_text(
            "OC snapshot unavailable (rate-limit/first snapshot). "
            "मैंने बैकग्राउंड में refresh kick किया है — 20–30s बाद फिर `/oc_now` भेजें."
        )
        return
    result = check_now(snap)
    text = _build_text(result)
    await update.message.reply_markdown(text)

def register(application):
    application.add_handler(CommandHandler("oc_now", cmd_oc_now))
