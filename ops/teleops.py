# path: ops/teleops.py
from integrations import telegram
from ops import commands

def tick(sheet, cfg, state):
    for upd in telegram.fetch_updates(timeout_sec=5) or []:
        text, chat_id, sender_id = telegram.extract_command_text(upd)
        if not text: continue
        # readonly commands only
        commands.handle(text, sender_id, cfg, state, sheet)
