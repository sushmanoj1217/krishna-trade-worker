# telegram_bot.py  — handlers-only (no auto start)
from __future__ import annotations
import os
import logging
from telegram.ext import ApplicationBuilder, CommandHandler

# /oc_now handler register import
try:
    from skills.examples.oc_now import register as register_oc_now
except Exception:
    register_oc_now = None

BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
_log = logging.getLogger(__name__)

def build_application():
    token = os.environ.get(BOT_TOKEN_ENV)
    if not token:
        raise RuntimeError(f"{BOT_TOKEN_ENV} not set")
    app = ApplicationBuilder().token(token).build()
    register_handlers(app)
    return app

def register_handlers(app):
    async def ping(update, context):
        await update.message.reply_text("pong")
    app.add_handler(CommandHandler("ping", ping))

    # 👇 यहीं “वो एक लाइन” का असली प्रभाव है
    if register_oc_now:
        register_oc_now(app)
        _log.info("/oc_now handler registered")
    else:
        _log.warning("skills.examples.oc_now not available; /oc_now not registered")

# यहां से polling **start नहीं** करते — krishna_main.py से ही होगा
if __name__ == "__main__":
    print("handlers-only module; start polling from krishna_main.py")
