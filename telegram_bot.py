# telegram_bot.py
from __future__ import annotations
import os
from datetime import datetime
from typing import Optional

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from utils.logger import log
from utils.cache import get_snapshot
from utils.params import Params
from utils.time_windows import IST, is_no_trade_now
from utils.state import (
    is_oc_auto, set_oc_auto, get_last_signal, is_last_signal_placed,
    set_approvals_required, approvals_required, list_pending, approve, deny
)
from integrations.news_feed import hold_active
from integrations import sheets as sh
from utils import telemetry

APP_VERSION = os.getenv("APP_VERSION", "dev")

# ---------- helpers ----------
def _owner_id() -> Optional[int]:
    v = os.getenv("TELEGRAM_OWNER_ID", "").strip()
    try: return int(v) if v else None
    except: return None

def _authorized(user_id: Optional[int]) -> bool:
    owner = _owner_id()
    return (owner is None) or (user_id == owner)

def _num(x, nd=2):
    if x is None: return "—"
    try: return f"{float(x):.{nd}f}"
    except: return str(x)

def _check(ok: bool) -> str: return "✅" if ok else "❌"

def _near_or_cross(tag: str, spot: float, lvl: Optional[float], buf: int):
    if lvl is None or spot is None: return "—", None
    d = round(spot - lvl, 2); half = max(1, int(buf * 0.5))
    if tag in ("S1*", "S2*"):
        if spot <= lvl: return "CROSS", d
        if (lvl - spot) <= half: return "NEAR", d
    else:
        if spot >= lvl: return "CROSS", d
        if (spot - lvl) <= half: return "NEAR", d
    return "—", d

def _build_oc_now_message() -> str:
    snap = get_snapshot()
    if not snap:
        return "<b>/oc_now</b>\nNo OC snapshot yet."
    p = Params()
    b = int(snap.extras.get("buffer", p.buffer_points()) if snap.extras else p.buffer_points())
    s1,s2,r1,r2 = snap.s1, snap.s2, snap.r1, snap.r2
    s1s = snap.extras.get("s1s") if snap.extras else None
    s2s = snap.extras.get("s2s") if snap.extras else None
    r1s = snap.extras.get("r1s") if snap.extras else None
    r2s = snap.extras.get("r2s") if snap.extras else None
    s1st, s1d = _near_or_cross("S1*", snap.spot, s1s, b)
    s2st, s2d = _near_or_cross("S2*", snap.spot, s2s, b)
    r1st, r1d = _near_or_cross("R1*", snap.spot, r1s, b)
    r2st, r2d = _near_or_cross("R2*", snap.spot, r2s, b)

    hold_on, hold_reason = hold_active()
    last_sig = get_last_signal()
    sig_line = "—"
    if last_sig:
        sig_line = f"{last_sig['id']} ({'placed' if is_last_signal_placed() else 'pending'}) {last_sig['side']}@{last_sig['trigger']}"
    opens = sh.get_open_trades_count()

    # stale banner via OC_Live last row
    oc_state = sh.latest_oc_state()
    stale = oc_state.get("stale") or False
    stale_txt = " | <b>STALE</b>" if stale else ""

    header = (
        f"<b>/oc_now</b>  <i>{datetime.now(tz=IST).strftime('%Y-%m-%d %H:%M:%S %Z')}</i>{stale_txt}\n"
        f"Spot <b>{_num(snap.spot,2)}</b> | PCR {_num(snap.pcr)} | "
        f"MaxPain <b>{_num(snap.max_pain,0)}</b> (Δ {_num(snap.max_pain_dist)}) "
        f"| HOLD={ 'ON' if hold_on else 'OFF'}{('('+hold_reason+')' if hold_on else '')}"
        f"{(' → ' + snap.bias_tag) if snap.bias_tag else ''}\n"
        f"<i>Signal:</i> {sig_line} | <i>Open trades:</i> {opens}"
    )
    levels = (
        "<b>Levels</b>\n"
        f"S1 {_num(s1,0)}  S2 {_num(s2,0)}  R1 {_num(r1,0)}  R2 {_num(r2,0)}\n"
        f"Triggers*  S1* <b>{_num(s1s,0)}</b>  S2* <b>{_num(s2s,0)}</b>  "
        f"R1* <b>{_num(r1s,0)}</b>  R2* <b>{_num(r2s,0)}</b>  (buffer={b})"
    )
    trig = (
        "<b>Trigger status</b>\n"
        f"• S1* {s1st} (Δ={_num(s1d)}) | S2* {s2st} (Δ={_num(s2d)})\n"
        f"• R1* {r1st} (Δ={_num(r1d)}) | R2* {r2st} (Δ={_num(r2d)})"
    )
    mv = snap.extras.get("mv", {}) if snap.extras else {}
    mv_block = (
        f"<b>MV</b> → PCR {_num(snap.pcr)} (hi≥{mv.get('pcr_hi','—')} / lo≤{mv.get('pcr_lo','—')}) | "
        f"MaxPain Δ {_num(snap.max_pain_dist)} (need±{mv.get('mp_need','—')})\n"
        f"• CE_OK={_check(bool(mv.get('ce_ok')))} [{mv.get('ce_basis','—')}]\n"
        f"• PE_OK={_check(bool(mv.get('pe_ok')))} [{mv.get('pe_basis','—')}]"
    )
    ocp = snap.extras.get("ocp", {}) if snap.extras else {}
    ocp_block = (
        "<b>OC-Pattern</b>\n"
        f"• CE_OK={_check(bool(ocp.get('ce_ok')))} ({ocp.get('ce_type','-')}) [{ocp.get('basis_ce','—')}]\n"
        f"• PE_OK={_check(bool(ocp.get('pe_ok')))} ({ocp.get('pe_type','-')}) [{ocp.get('basis_pe','—')}]"
    )
    return "\n\n".join([header, levels, trig, mv_block, ocp_block])

# ---------- auth guard ----------
async def _guard(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else None
    if not _authorized(uid):
        try: await update.effective_message.reply_text("Unauthorized.")
        except Exception: pass
        return False
    return True

# ---------- commands ----------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    await update.message.reply_text("Namaste 👋\nBot is up. Try /oc_now")

async def whoami_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    uid = update.effective_user.id if update.effective_user else None
    cid = update.effective_chat.id if update.effective_chat else None
    await update.message.reply_text(f"user_id={uid} chat_id={cid}")

async def health_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    t = telemetry.get()
    oc_ok_at = t["marks"].get("oc_ok_at")
    d429 = t["counters"].get("dhan_429", 0)
    oc_ok = t["counters"].get("oc_fetch_success", 0)
    oc_fail = t["counters"].get("oc_fetch_fail", 0)
    oc_state = sh.latest_oc_state()
    await update.message.reply_text(
        f"OK {datetime.now(tz=IST).strftime('%H:%M:%S %Z')} | "
        f"OC ok:{oc_ok} fail:{oc_fail} 429:{d429} | stale={oc_state.get('stale')} ts={oc_state.get('timestamp')} | "
        f"oc_auto={is_oc_auto()}")

async def version_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    await update.message.reply_text(f"Version: {APP_VERSION} | tz=IST | oc_auto={is_oc_auto()}")

async def oc_now_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    try:
        await update.message.reply_text(_build_oc_now_message(), parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        log.error(f"/oc_now failed: {e}")
        await update.message.reply_text("Error building OC snapshot.")

async def run_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): 
        return
    if not context.args:
        await update.message.reply_text(
            "Usage: /run oc_auto on|off|status | oc_now | approvals on|off|status"
        )
        return

    sub = context.args[0].lower()

    if sub == "oc_auto":
        # status (no arg or explicit)
        if len(context.args) == 1 or context.args[1].lower() == "status":
            await update.message.reply_text(f"oc_auto={is_oc_auto()}")
            return
        val = context.args[1].lower()
        if val == "on":
            set_oc_auto(True)
            await update.message.reply_text("oc_auto: ON")
        elif val == "off":
            set_oc_auto(False)
            await update.message.reply_text("oc_auto: OFF")
        else:
            await update.message.reply_text("Use on|off|status")

    elif sub == "oc_now":
        await oc_now_cmd(update, context)

    elif sub == "approvals":
        # approvals status/on/off
        if len(context.args) == 1 or context.args[1].lower() == "status":
            await update.message.reply_text(f"approvals_required={approvals_required()}")
            return
        on = context.args[1].lower()
        if on in ("on", "off"):
            set_approvals_required(on == "on")
            await update.message.reply_text(f"approvals_required={approvals_required()}")
        else:
            await update.message.reply_text("Use approvals on|off|status")

    else:
        await update.message.reply_text(
            "Unknown /run subcommand. Try: oc_auto | oc_now | approvals"
        )
async def set_levels_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    if not context.args:
        await update.message.reply_text(
            "Usage:\n"
            "/set_levels buffer <points>\n"
            "/set_levels mpdist <points>\n"
            "/set_levels pcr <bull_hi> <bear_lo>\n"
            "/set_levels target <points>\n")
        return
    symbol = os.getenv("OC_SYMBOL","NIFTY").upper()
    sub = context.args[0].lower()
    if sub == "buffer" and len(context.args) >= 2:
        try: val = int(float(context.args[1])); 
        except: await update.message.reply_text("buffer must be a number"); return
        m = sh.get_overrides_map(); key = "ENTRY_BAND_POINTS_MAP"; parts = {}
        raw = (m.get(key,"") or "").replace(";",",")
        for kv in raw.split(","):
            if "=" in kv:
                k,v = kv.split("=",1); parts[k.strip().upper()] = v.strip()
        parts[symbol] = str(val)
        new_val = ",".join(f"{k}={parts[k]}" for k in sorted(parts.keys()))
        sh.upsert_override(key, new_val)
        await update.message.reply_text(f"{key}={new_val}")
    elif sub == "mpdist" and len(context.args) >= 2:
        try: val = int(float(context.args[1]))
        except: await update.message.reply_text("mpdist must be a number"); return
        key = f"MP_SUPPORT_DIST_{symbol}"
        sh.upsert_override(key, str(val))
        await update.message.reply_text(f"{key}={val}")
    elif sub == "pcr" and len(context.args) >= 3:
        try:
            bull = float(context.args[1]); bear = float(context.args[2])
        except:
            await update.message.reply_text("pcr needs two numbers: bull_hi bear_lo"); return
        sh.upsert_override("PCR_BULL_HIGH", str(bull))
        sh.upsert_override("PCR_BEAR_LOW", str(bear))
        await update.message.reply_text(f"PCR_BULL_HIGH={bull} PCR_BEAR_LOW={bear}")
    elif sub == "target" and len(context.args) >= 2:
        try: val = int(float(context.args[1]))
        except: await update.message.reply_text("target must be a number"); return
        key_map = {"NIFTY":"MIN_TARGET_POINTS_N","BANKNIFTY":"MIN_TARGET_POINTS_B","FINNIFTY":"MIN_TARGET_POINTS_F"}
        key = key_map.get(symbol, "MIN_TARGET_POINTS_N")
        sh.upsert_override(key, str(val))
        await update.message.reply_text(f"{key}={val}")
    else:
        await update.message.reply_text("Unknown /set_levels subcommand")

async def hold_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    if not context.args or context.args[0].lower() not in ("on","off","status"):
        await update.message.reply_text("Usage: /hold on|off|status"); return
    sub = context.args[0].lower()
    if sub == "status":
        rows = sh.get_last_event_rows(5)
        txt = ["Last Events:"]
        for r in rows: txt.append(" | ".join(str(x) for x in r))
        on, reason = hold_active()
        txt.append(f"HOLD={on} {reason}")
        await update.message.reply_text("\n".join(txt)); return
    status = "HOLD" if sub == "on" else "CLEAR"
    sh.append_row("Events", [sh.now_str(), "manual", status])
    await update.message.reply_text(f"Events: {status}")

# approvals UX
async def approve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    if not context.args:
        # list pending
        items = list_pending()
        if not items:
            await update.message.reply_text("No pending signals."); return
        msg = "Pending signals:\n" + "\n".join(f"{s['id']} {s['side']}@{s['trigger']} entry={s['entry']}" for s in items)
        await update.message.reply_text(msg); return
    sid = context.args[0]
    ok = approve(sid)
    await update.message.reply_text("approved" if ok else "not found")

async def deny_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    if not context.args:
        await update.message.reply_text("Usage: /deny <signal_id>"); return
    sid = context.args[0]
    ok = deny(sid)
    await update.message.reply_text("denied" if ok else "not found")

# ---------- bootstrap ----------
async def init() -> Optional[Application]:
    from telegram.ext import Application
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        log.warning("TELEGRAM_BOT_TOKEN missing; bot disabled")
        return None
    try:
        app = Application.builder().token(token).build()
    except Exception as e:
        log.error(f"Telegram Application init failed: {e}")
        return None

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("whoami", whoami_cmd))
    app.add_handler(CommandHandler("health", health_cmd))
    app.add_handler(CommandHandler("version", version_cmd))
    app.add_handler(CommandHandler("oc_now", oc_now_cmd))
    app.add_handler(CommandHandler("run", run_cmd))
    app.add_handler(CommandHandler("set_levels", set_levels_cmd))
    app.add_handler(CommandHandler("hold", hold_cmd))
    app.add_handler(CommandHandler("approve", approve_cmd))
    app.add_handler(CommandHandler("deny", deny_cmd))
    return app
