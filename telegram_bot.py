import os
from datetime import datetime
from typing import Optional

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
)

from utils.logger import log
from analytics.oc_refresh import get_snapshot
from utils.params import Params
from utils.time_windows import NOW_IST
from agents.signal_generator import build_checks_for_snapshot

OWNER_ID = int(os.getenv("TELEGRAM_OWNER_ID", "0"))
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# runtime toggle (seen by krishna_main via env read once; here local too)
OC_AUTO_STATE = {"enabled": True}

def _is_owner(user_id: int) -> bool:
    return OWNER_ID and user_id == OWNER_ID

def fmt_checks_row(ok: Optional[bool]) -> str:
    if ok is None:
        return "—"
    return "✅" if ok else "❌"

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update.effective_user.id):
        await update.message.reply_text("Unauthorized.")
        return
    await update.message.reply_text("KTW bot online. Try /oc_now or /run oc_auto status")

async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update.effective_user.id):
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("Use on|off|status")
        return
    sub = args[0].lower()
    if sub == "oc_auto":
        if len(args) == 1 or args[1] == "status":
            await update.message.reply_text(f"oc_auto={OC_AUTO_STATE['enabled']}")
        elif args[1] == "on":
            OC_AUTO_STATE["enabled"] = True
            await update.message.reply_text("oc_auto=True")
        elif args[1] == "off":
            OC_AUTO_STATE["enabled"] = False
            await update.message.reply_text("oc_auto=False")
        else:
            await update.message.reply_text("Use on|off|status")
    elif sub == "oc_now":
        await cmd_oc_now(update, context)
    else:
        await update.message.reply_text("Use on|off|status")

async def cmd_oc_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update.effective_user.id):
        return
    snap = get_snapshot()
    if not snap or snap.stale:
        await update.message.reply_text(
            "OC snapshot unavailable (rate-limit/first snapshot). 20–30s बाद फिर से /oc_now भेजें."
        )
        return

    p = Params.from_env()

    # build six-checks & tags using same logic as signal generator
    checks = build_checks_for_snapshot(snap, p)  # returns dict
    cvals = [fmt_checks_row(checks.get(f"C{i}")) for i in range(1, 7)]

    mv_bits = checks.get("mv_bits", {})
    oc_bits = checks.get("oc_bits", {})
    mv_pcr_ok = mv_bits.get("pcr_ok")
    mv_mp_ok = mv_bits.get("mp_ok")
    mv_basis = mv_bits.get("basis", "")
    oc_basis = oc_bits.get("basis", "")

    def fmt_bool(b):
        if b is None:
            return "—"
        return "✅" if b else "❌"

    header = (
        f"Spot {snap.spot:.2f} | VIX {snap.vix or '—'} | PCR {snap.pcr or '—'} {'?' if snap.pcr is None else ''}\n"
        f"MaxPain {snap.max_pain:.1f} (Δ {abs(snap.spot - snap.max_pain):.2f}) | Bias {snap.bias or 'None'} | Exp {snap.expiry}\n"
    )

    levels = (
        f"S1 {snap.s1} / S2 {snap.s2} / R1 {snap.r1} / R2 {snap.r2}\n"
        f"Triggers → S1 {snap.s1:.2f} | S2 {snap.s2:.2f} | R1 {snap.r1:.2f} | R2 {snap.r2:.2f}\n"
    )

    checks_line = f"6-Checks C1 {cvals[0]} · C2 {cvals[1]} · C3 {cvals[2]} · C4 {cvals[3]} · C5 {cvals[4]} · C6 {cvals[5]}\n"

    oc_pattern = f"OC-Pattern: {oc_basis}\n"
    mv_line = f"MV: PCR {snap.pcr or '—'} → {fmt_bool(mv_pcr_ok)}  MPΔ {abs(snap.spot - snap.max_pain):.2f} → {fmt_bool(mv_mp_ok)}\n"

    eligible = checks.get("eligible")
    decision = f"Decision: {'Eligible' if eligible else 'Not Eligible'}"

    text = header + "\n" + "Levels\n" + levels + checks_line + oc_pattern + mv_line + decision
    await update.message.reply_text(text)

async def cmd_force_flat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update.effective_user.id):
        return
    # placeholder – sheet loop will auto-flat at 15:15
    await update.message.reply_text("Force flat requested (paper).")

async def cmd_trade_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update.effective_user.id):
        return
    await update.message.reply_text("Trades pending: (paper mode)")

async def cmd_unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if _is_owner(update.effective_user.id):
        await update.message.reply_text("Unknown. Try /oc_now or /run oc_auto status")

async def init():
    token = BOT_TOKEN
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN missing")

    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("oc_now", cmd_oc_now))
    app.add_handler(CommandHandler("force_flat", cmd_force_flat))
    app.add_handler(CommandHandler("trade_status", cmd_trade_status))
    app.add_handler(CommandHandler("run", cmd_run))
    app.add_handler(MessageHandler(filters.ALL, cmd_unknown))

    return app
