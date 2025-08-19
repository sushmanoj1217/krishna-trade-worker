import os
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from utils.logger import log
from analytics.oc_refresh import refresh_once
from agents.signal_generator import run_once as run_signal
from agents.trade_loop import place_trade

OWNER = str(os.getenv("TELEGRAM_OWNER_ID", "")).strip()

async def _owner_only(update: Update) -> bool:
    return OWNER and str(update.effective_user.id) == OWNER

async def oc_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _owner_only(update):
        return
    snap = refresh_once()
    if not snap:
        await update.message.reply_text("No OC data")
        return
    b = snap.extras.get("buffer")
    msg = (
        f"Spot {snap.spot}\n"
        f"Levels S1 {snap.s1} / S2 {snap.s2} / R1 {snap.r1} / R2 {snap.r2}\n"
        f"Triggers S1* {snap.extras.get('s1s')} / S2* {snap.extras.get('s2s')} / "
        f"R1* {snap.extras.get('r1s')} / R2* {snap.extras.get('r2s')} (b={b})\n"
        f"PCR {snap.pcr} | MaxPain {snap.max_pain} (Δ {snap.max_pain_dist}) | bias {snap.bias_tag}"
    )
    await update.message.reply_text(msg)

async def run_signal_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _owner_only(update):
        return
    s = run_signal()
    if not s:
        await update.message.reply_text("No eligible signal")
        return
    await update.message.reply_text(f"Signal {s.id} {s.side} {s.trigger} eligible={s.eligible}\n{s.reason}")

async def place(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _owner_only(update):
        return
    s = run_signal()
    if not s or not s.eligible:
        await update.message.reply_text("No eligible signal to place")
        return
    place_trade(s)
    await update.message.reply_text(f"Placed trade for {s.id}")

async def health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("ok")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _owner_only(update):
        return
    await update.message.reply_text("Welcome. /oc_now /run_signal /place /health")

async def init():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token:
        log.warning("TELEGRAM_BOT_TOKEN missing; bot disabled")
        return None
    try:
        app = Application.builder().token(token).build()
    except Exception as e:
        # Fail-safe: don't crash the worker if PTB/runtime mismatch
        log.error(f"Telegram Application init failed: {e}")
        return None
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("health", health))
    app.add_handler(CommandHandler("oc_now", oc_now))
    app.add_handler(CommandHandler("run_signal", run_signal_cmd))
    app.add_handler(CommandHandler("place", place))
    return app
