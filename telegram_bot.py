# telegram_bot.py
from __future__ import annotations
import os, asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from utils.logger import log
from utils.cache import get_snapshot
from utils.params import Params
from utils.rr import rr_feasible
from utils.time_windows import is_no_trade_now, IST
from utils.state import is_oc_auto, set_oc_auto, get_last_signal, is_last_signal_placed
from integrations.news_feed import hold_active
from integrations import sheets as sh
from agents.tp_sl_watcher import trail_tick
from agents.trade_loop import force_flat_all

APP_VERSION = os.getenv("APP_VERSION", "dev")

# ------------------------------ helpers ------------------------------
def _owner_id() -> Optional[int]:
    v = os.getenv("TELEGRAM_OWNER_ID", "").strip()
    try:
        return int(v) if v else None
    except Exception:
        return None

def _authorized(user_id: Optional[int]) -> bool:
    owner = _owner_id()
    return (owner is None) or (user_id == owner)

def _num(x, nd=2):
    if x is None: return "—"
    try: return f"{float(x):.{nd}f}"
    except Exception: return str(x)

def _check(ok: bool) -> str: return "✅" if ok else "❌"

def _now_str() -> str: return datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S %Z")

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

def _mv_block(extras: dict, pcr, mp, mpd) -> str:
    mv = extras.get("mv", {}) if extras else {}
    return (
        f"<b>MV</b> → PCR {_num(pcr)} (hi≥{mv.get('pcr_hi','—')} / lo≤{mv.get('pcr_lo','—')}) | "
        f"MaxPain Δ {_num(mpd)} (need±{mv.get('mp_need','—')})\n"
        f"• CE_OK={_check(bool(mv.get('ce_ok')))} [{mv.get('ce_basis','—')}]\n"
        f"• PE_OK={_check(bool(mv.get('pe_ok')))} [{mv.get('pe_basis','—')}]"
    )

def _ocp_block(extras: dict) -> str:
    ocp = extras.get("ocp", {}) if extras else {}
    return (
        "<b>OC-Pattern</b>\n"
        f"• CE_OK={_check(bool(ocp.get('ce_ok')))} ({ocp.get('ce_type','-')}) [{ocp.get('basis_ce','—')}]\n"
        f"• PE_OK={_check(bool(ocp.get('pe_ok')))} ({ocp.get('pe_type','-')}) [{ocp.get('basis_pe','—')}]"
    )

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

    header = (
        f"<b>/oc_now</b>  <i>{_now_str()}</i>\n"
        f"Spot <b>{_num(snap.spot,2)}</b> | VIX {_num(snap.vix)} | PCR {_num(snap.pcr)} | "
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
    mv_block = _mv_block(snap.extras or {}, snap.pcr, snap.max_pain, snap.max_pain_dist)
    ocp_block = _ocp_block(snap.extras or {})

    return "\n\n".join([header, levels, trig, mv_block, ocp_block])

# ------------------------------ handlers ------------------------------
async def _guard(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else None
    if not _authorized(uid):
        try: await update.effective_message.reply_text("Unauthorized.")
        except Exception: pass
        return False
    return True

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    await update.message.reply_text("Namaste 👋\nBot is up. Try /oc_now")

async def health_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    await update.message.reply_text(f"OK {datetime.now(tz=IST).strftime('%H:%M:%S %Z')} | OC-AUTO={is_oc_auto()}")

async def version_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    await update.message.reply_text(f"Version: {APP_VERSION} | tz=IST | oc_auto={is_oc_auto()}")

async def oc_now_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    try:
        msg = _build_oc_now_message()
        await update.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        log.error(f"/oc_now failed: {e}")
        await update.message.reply_text("Error building OC snapshot.")

# ---- Ops Panel ----
async def run_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    if not context.args:
        await update.message.reply_text("Usage: /run oc_auto on|off|status | oc_now")
        return
    sub = context.args[0].lower()
    if sub == "oc_auto":
        if len(context.args) < 2:
            await update.message.reply_text(f"oc_auto={is_oc_auto()}")
            return
        val = context.args[1].lower()
        if val == "on":
            set_oc_auto(True); await update.message.reply_text("oc_auto: ON")
        elif val == "off":
            set_oc_auto(False); await update.message.reply_text("oc_auto: OFF")
        else:
            await update.message.reply_text("Use on|off|status")
    elif sub == "oc_now":
        await oc_now_cmd(update, context)
    else:
        await update.message.reply_text("Unknown /run subcommand.")

async def force_flat_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    await force_flat_all("force_flat_cmd")
    await update.message.reply_text("Forced flat all open trades.")

async def trade_status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    open_trades = sh.get_open_trades()
    msg = f"Open trades: {len(open_trades)}\n"
    for t in open_trades[:10]:
        msg += f"• {t['trade_id']} {t['side']} buy={t['buy_ltp']} sl={t['sl']} tp={t['tp']}\n"
    await update.message.reply_text(msg or "No open trades.")

async def eod_report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    rec = sh.get_recent_trades(50)
    wins = [t for t in rec if str(t.get("result")) in ("tp","mv_flip") and float(t.get("pnl",0))>0]
    losses = [t for t in rec if str(t.get("result")) in ("sl","flat") and float(t.get("pnl",0))<=0]
    wr = (len(wins)/max(1,len(wins)+len(losses)))*100.0
    await update.message.reply_text(f"EOD report (last {len(rec)}): WR={wr:.1f}% wins={len(wins)} losses={len(losses)}")

async def eod_tuner_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    from agents.eod_tuner import run as tuner_run
    tuner_run()
    await update.message.reply_text("EOD tuner executed.")

# /set_levels — for quick buffer override (Params_Override)
async def set_levels_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    if not context.args:
        await update.message.reply_text("Usage: /set_levels buffer <points>\nExample: /set_levels buffer 12")
        return
    sub = context.args[0].lower()
    if sub == "buffer" and len(context.args) >= 2:
        try:
            val = int(float(context.args[1]))
        except Exception:
            await update.message.reply_text("buffer must be a number")
            return
        symbol = os.getenv("OC_SYMBOL","NIFTY").upper()
        # update ENTRY_BAND_POINTS_MAP for current symbol
        m = sh.get_overrides_map()
        key = "ENTRY_BAND_POINTS_MAP"
        raw = m.get(key, "")
        parts = {}
        if raw:
            for kv in raw.replace(";",",").split(","):
                if "=" in kv:
                    k,v = kv.split("=",1)
                    k=k.strip().upper(); v=v.strip()
                    if k: parts[k]=v
        parts[symbol] = str(val)
        new_val = ",".join(f"{k}={parts[k]}" for k in sorted(parts.keys()))
        sh.upsert_override(key, new_val)
        await update.message.reply_text(f"Buffer override set: {key}={new_val}")
    else:
        await update.message.reply_text("Only 'buffer' supported currently.")

# /hold on|off — writes Events row checked by hold_active()
async def hold_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    if not context.args or context.args[0].lower() not in ("on","off","status"):
        await update.message.reply_text("Usage: /hold on|off|status")
        return
    sub = context.args[0].lower()
    if sub == "status":
        on, reason = hold_active()
        await update.message.reply_text(f"HOLD={on} {reason}")
        return
    status = "HOLD" if sub == "on" else "CLEAR"
    sh.append_row("Events", [sh.now_str(), "manual", status])
    await update.message.reply_text(f"Events: {status}")

# ------------------------------ bootstrap ------------------------------
async def init() -> Optional[Application]:
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
    app.add_handler(CommandHandler("health", health_cmd))
    app.add_handler(CommandHandler("version", version_cmd))
    app.add_handler(CommandHandler("oc_now", oc_now_cmd))
    app.add_handler(CommandHandler("run", run_cmd))
    app.add_handler(CommandHandler("force_flat", force_flat_cmd))
    app.add_handler(CommandHandler("trade_status", trade_status_cmd))
    app.add_handler(CommandHandler("eod_report", eod_report_cmd))
    app.add_handler(CommandHandler("eod_tuner", eod_tuner_cmd))
    app.add_handler(CommandHandler("set_levels", set_levels_cmd))
    app.add_handler(CommandHandler("hold", hold_cmd))
    return app
