# telegram_bot.py — handlers-only module (no auto start)
from __future__ import annotations

import os
import logging
from typing import Optional

from telegram.ext import Application, ApplicationBuilder, CommandHandler

# Optional: /oc_now handler
try:
    from skills.examples.oc_now import register as register_oc_now  # (app) -> None
except Exception:
    register_oc_now = None  # type: ignore

BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
_log = logging.getLogger(__name__)


def _require_token() -> str:
    token = os.environ.get(BOT_TOKEN_ENV)
    if not token:
        raise RuntimeError(f"{BOT_TOKEN_ENV} not set in environment")
    return token


def register_handlers(app: Application) -> None:
    """Attach core and optional handlers to the Application."""
    # /ping
    async def ping(update, context):
        await update.message.reply_text("pong")

    app.add_handler(CommandHandler("ping", ping))

    # /oc_now (if module present)
    if register_oc_now:
        try:
            register_oc_now(app)
            _log.info("/oc_now handler registered")
        except Exception as e:
            _log.warning("Failed to register /oc_now: %s", e)
    else:
        _log.warning("skills.examples.oc_now not available; /oc_now not registered")


def build_application() -> Application:
    """Back-compat builder (same as init). Does NOT start polling."""
    token = _require_token()
    app = ApplicationBuilder().token(token).build()
    register_handlers(app)
    return app


def init() -> Application:
    """
    Entry expected by krishna_main.py:
    Builds and returns a configured Application.
    Does NOT call run_polling/start_polling — main file controls that.
    """
    return build_application()


# Safety: don’t auto-start from this module
if __name__ == "__main__":
    print("telegram_bot.py is handlers-only. Start polling from krishna_main.py")
