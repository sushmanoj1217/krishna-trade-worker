# ops/tele_router.py
# Minimal Telegram long-polling router (no extra deps beyond requests)
# Commands:
#   /status  -> writes heartbeat row + replies current state
#   /oc_now  -> fetches Dhan OC snapshot & appends OC_Live row, replies summary

from __future__ import annotations
import os, time, threading, requests
from typing import Any, Dict, List, Optional
from agents import logger
from analytics import oc_refresh

API = "https://api.telegram.org"

def _bot_token() -> str:
    return os.getenv("TELEGRAM_BOT_TOKEN", "")

def _allowed_user_ids() -> List[str]:
    raw = os.getenv("TELEGRAM_USER_ID", "")
    return [x.strip() for x in raw.split(",") if x.strip()]

def _tg_get_updates(offset: Optional[int]) -> List[Dict[str, Any]]:
    url = f"{API}/bot{_bot_token()}/getUpdates"
    params = {"timeout": 25}
    if offset is not None:
        params["offset"] = offset
    try:
        r = requests.get(url, params=params, timeout=30)
        j = r.json()
        if not j.get("ok"):
            return []
        return j.get("result", [])
    except Exception:
        return []

def _tg_send(chat_id: int, text: str) -> None:
    url = f"{API}/bot{_bot_token()}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=15)
    except Exception:
        pass

def _fmt_float(x) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return str(x)

def _handle_status(sheet, cfg, chat_id: int):
    try:
        logger.log_status(sheet, {
            "worker_id": cfg.worker_id,
            "shift_mode": cfg.shift_mode,
            "state": "OK",
            "message": f"status ping {cfg.symbol}",
        })
    except Exception:
        pass
    _tg_send(chat_id, f"OK ✅\nshift={cfg.shift_mode} worker={cfg.worker_id}\nsymbol={cfg.symbol}")

def _handle_oc_now(sheet, cfg, chat_id: int):
    oc = None
    try:
        oc = oc_refresh.get_snapshot(cfg)
    except Exception as e:
        oc = None
    if not oc:
        _tg_send(chat_id, "OC snapshot failed ❌ (using Dhan); try again later.")
        return
    # Append to OC_Live
    try:
        logger.log_oc_live(sheet, {
            "ts": "",
            "symbol": oc.get("symbol") or cfg.symbol,
            "spot": oc.get("spot"),
            "s1": oc.get("s1"), "s2": oc.get("s2"),
            "r1": oc.get("r1"), "r2": oc.get("r2"),
            "expiry": oc.get("expiry") or "",
            "signal": "",
        })
    except Exception:
        pass
    msg = (
        f"OC updated ✅\n"
        f"spot={_fmt_float(oc.get('spot'))}  "
        f"S1={_fmt_float(oc.get('s1'))}  S2={_fmt_float(oc.get('s2'))}\n"
        f"R1={_fmt_float(oc.get('r1'))}  R2={_fmt_float(oc.get('r2'))}\n"
        f"expiry={oc.get('expiry')}"
    )
    _tg_send(chat_id, msg)

def _authorized(uid: int) -> bool:
    return (not _allowed_user_ids()) or (str(uid) in _allowed_user_ids())

def _router_loop(sheet, cfg):
    if not _bot_token():
        print("[tele_router] TELEGRAM_BOT_TOKEN missing; router off", flush=True)
        return
    offset = None
    print("[tele_router] started polling", flush=True)
    while True:
        updates = _tg_get_updates(offset)
        for up in updates:
            offset = up.get("update_id", offset)
            if offset is not None:
                offset += 1
            msg = up.get("message") or up.get("edited_message")
            if not msg:
                continue
            chat = msg.get("chat") or {}
            chat_id = chat.get("id")
            user = msg.get("from") or {}
            uid = user.get("id")
            text = (msg.get("text") or "").strip()
            if not chat_id or not uid or not text:
                continue
            if not _authorized(uid):
                _tg_send(chat_id, "unauthorized")
                continue
            t = text.lower()
            if t == "/status" or t == "/start":
                _handle_status(sheet, cfg, chat_id)
            elif t.startswith("/run oc_now") or t == "/oc_now" or t == "/run oc_now":
                _handle_oc_now(sheet, cfg, chat_id)
            else:
                _tg_send(chat_id, "commands: /status, /oc_now")
        time.sleep(1)

def start(sheet, cfg):
    th = threading.Thread(target=_router_loop, args=(sheet, cfg), daemon=True)
    th.start()
