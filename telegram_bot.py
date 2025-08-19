# telegram_bot.py
# Python-Telegram-Bot v20+
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
from utils.state import is_oc_auto, set_oc_auto
from integrations.news_feed import hold_active
from integrations import sheets as sh
from agents.tp_sl_watcher import trail_tick
from agents.trade_loop import force_flat_all
from ops import handlers as ops

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

def _six_checks_preview(side: str, tag: str, entry: float, buf: int, bias_tag: Optional[str], p: Params):
    # mirrors generator simplification
    c1 = True; r1 = "TriggerCross"
    bull = (bias_tag or "").startswith("mv_bull")
    bear = (bias_tag or "").startswith("mv_bear")
    if side == "CE":
        c2 = bull; r2 = f"FlowBias {'bull' if bull else 'flat/bear'}"
        sl = entry - buf
    else:
        c2 = bear; r2 = f"FlowBias {'bear' if bear else 'flat/bull'}"
        sl = entry + buf
    c3, r3 = True, "WallSupport placeholder"
    c4, r4 = True, "Momentum placeholder"
    rr_ok, risk, tp = rr_feasible(entry, sl, p.min_target_points())
    c5, r5 = rr_ok, f"RR risk={_num(risk,0)} tp={_num(tp,0)}"
    hold_on, h_reason = hold_active()
    caps_ok = (sh.count_today_trades() < int(os.getenv('MAX_TRADES_PER_DAY','10')))
    c6 = (not is_no_trade_now()) and (not hold_on) and caps_ok
    sysr = []
    if is_no_trade_now(): sysr.append("NoTradeWindow")
    if hold_on: sysr.append(h_reason)
    if not caps_ok: sysr.append("DayCap")
    r6 = "SystemGates " + (",".join(sysr) if sysr else "OK")
    return (c1,c2,c3,c4,c5,c6), (r1,r2,r3,r4,r5,r6), sl, tp

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

    # choose candidate
    cand = None
    for tag, st, lvl in (("S1*", s1st, s1s), ("S2*", s2st, s2s), ("R1*", r1st, r1s), ("R2*", r2st, r2s)):
        if st == "CROSS" and lvl:
            cand = (tag, lvl); break
    if cand is None:
        for tag, st, lvl in (("S1*", s1st, s1s), ("S2*", s2st, s2s), ("R1*", r1st, r1s), ("R2*", r2st, r2s)):
            if st == "NEAR" and lvl:
                cand = (tag, lvl); break

    dec = []
    table = ""
    if cand:
        tag, lvl = cand
        side = "CE" if tag in ("S1*","S2*") else "PE"
        (c1,c2,c3,c4,c5,c6), (r1,r2,r3,r4,r5,r6), sl, tp = _six_checks_preview(side, tag, float(lvl), b, snap.bias_tag, p)
        table = (
            "<b>6-Checks</b>\n"
            f"• C1 {_check(c1)} {r1}\n"
            f"• C2 {_check(c2)} {r2}\n"
            f"• C3 {_check(c3)} {r3}\n"
            f"• C4 {_check(c4)} {r4}\n"
            f"• C5 {_check(c5)} {r5}\n"
            f"• C6 {_check(c6)} {r6}"
        )
        dec.append(f"<b>Decision</b>: {'Eligible' if all([c1,c2,c3,c4,c5,c6]) else 'Not Eligible'} – {side} @ {tag}")
        dec.append(f"Entry={_num(lvl,0)} SL={_num(sl,0)} TP={_num(tp,0)}")
    else:
        dec.append("<b>Decision</b>: No trigger NEAR/CROSS")

    mv_block = _mv_block(snap.extras or {}, snap.pcr, snap.max_pain, snap.max_pain_dist)
    ocp_block = _ocp_block(snap.extras or {})

    header = (
        f"<b>/oc_now</b>  <i>{_now_str()}</i>\n"
        f"Spot <b>{_num(snap.spot,2)}</b> | VIX {_num(snap.vix)} | PCR {_num(snap.pcr)} | "
        f"MaxPain <b>{_num(snap.max_pain,0)}</b> (Δ {_num(snap.max_pain_dist)}) "
        f"| HOLD={ 'ON' if hold_on else 'OFF'}{('('+hold_reason+')' if hold_on else '')}"
        f"{(' → ' + snap.bias_tag) if snap.bias_tag else ''}"
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
    blocks = [header, levels, trig, mv_block, ocp_block]
    if table: blocks.append(table)
    blocks.append("\n".join(dec))
    return "\n\n".join(blocks)

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
        await update.message.reply_text("Usage: /run oc_auto on|off|status")
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

# ops_* passthrough
async def ops_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    if not context.args:
        await update.message.reply_text("Usage: /ops <mem_backup|git_file_update|render_restart|tick_speed N|diag_conflict|learn TEXT|queue|approve I|list>")
        return
    sub = context.args[0].lower()
    if sub == "mem_backup":
        await update.message.reply_text(ops.ops_mem_backup())
    elif sub == "git_file_update":
        await update.message.reply_text(ops.ops_git_file_update())
    elif sub == "render_restart":
        await update.message.reply_text(ops.ops_render_restart())
    elif sub == "tick_speed" and len(context.args) >= 2:
        await update.message.reply_text(ops.ops_tick_speed(context.args[1]))
    elif sub == "diag_conflict":
        await update.message.reply_text(ops.ops_diag_conflict())
    elif sub == "learn":
        payload = " ".join(context.args[1:]) if len(context.args)>1 else ""
        await update.message.reply_text(ops.ops_learn(payload))
    elif sub == "queue":
        await update.message.reply_text(ops.ops_queue())
    elif sub == "approve" and len(context.args)>=2:
        try: idx = int(context.args[1])
        except: idx = -1
        await update.message.reply_text(ops.ops_approve(idx))
    elif sub == "list":
        await update.message.reply_text(ops.ops_list())
    else:
        await update.message.reply_text("Unknown /ops subcommand")

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
    app.add_handler(CommandHandler("oc_now", oc_now_cmd))
    app.add_handler(CommandHandler("run", run_cmd))
    app.add_handler(CommandHandler("force_flat", force_flat_cmd))
    app.add_handler(CommandHandler("trade_status", trade_status_cmd))
    app.add_handler(CommandHandler("eod_report", eod_report_cmd))
    app.add_handler(CommandHandler("eod_tuner", eod_tuner_cmd))
    app.add_handler(CommandHandler("ops", ops_cmd))
    return app
