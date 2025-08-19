# telegram_bot.py
# Python-Telegram-Bot v20+ compatible
import os
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from utils.logger import log
from utils.cache import get_snapshot
from utils.params import Params
from utils.rr import rr_feasible
from utils.time_windows import is_no_trade_now

IST = ZoneInfo("Asia/Kolkata")

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
    if x is None:
        return "—"
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return str(x)

def _check_mark(ok: bool) -> str:
    return "✅" if ok else "❌"

def _now_str() -> str:
    return datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S %Z")

def _near_or_cross(side: str, trigger_tag: str, spot: float, lvl: Optional[float], buf: int) -> tuple[str, Optional[float]]:
    """
    Returns status "CROSS"/"NEAR"/"—" and distance (spot-lvl).
    For supports (S*): cross if spot <= lvl; NEAR if within 0.5*buffer.
    For resistances (R*): cross if spot >= lvl; NEAR if within 0.5*buffer.
    """
    if lvl is None or spot is None:
        return "—", None
    d = round(spot - lvl, 2)
    half = max(1, int(buf * 0.50))
    if trigger_tag in ("S1*", "S2*"):
        if spot <= lvl:
            return "CROSS", d
        if lvl - spot <= half:
            return "NEAR", d
    else:
        if spot >= lvl:
            return "CROSS", d
        if spot - lvl <= half:
            return "NEAR", d
    return "—", d

def _mv_block(extras: dict, pcr: Optional[float], mp: Optional[float], mpd: Optional[float]) -> str:
    mv = extras.get("mv", {}) if extras else {}
    ce_ok = mv.get("ce_ok", False)
    pe_ok = mv.get("pe_ok", False)
    ce_basis = mv.get("ce_basis", "—")
    pe_basis = mv.get("pe_basis", "—")
    pcr_hi = mv.get("pcr_hi", "—")
    pcr_lo = mv.get("pcr_lo", "—")
    need = mv.get("mp_dist_need", "—")
    lines = []
    lines.append(
        f"<b>MV</b> → PCR {_num(pcr)} (hi≥{pcr_hi} / lo≤{pcr_lo}) | MaxPain Δ {_num(mpd)} (need±{need})"
    )
    lines.append(
        f"• CE_OK={_check_mark(bool(ce_ok))} [{ce_basis}]"
    )
    lines.append(
        f"• PE_OK={_check_mark(bool(pe_ok))} [{pe_basis}]"
    )
    return "\n".join(lines)

def _six_checks_summary(side: str, trigger: str, entry_lvl: float, buf: int, bias_tag: Optional[str], p: Params):
    """
    A light implementation matching signal generator placeholders:
    C1 TriggerCross, C2 FlowBias@Trigger (approx via mv_* bias tags),
    C3 WallSupport placeholder True, C4 Momentum placeholder True,
    C5 RR feasible via rr_feasible(), C6 SystemGates (no-trade windows)
    """
    # C1: already ensured by caller when we choose this candidate
    c1 = True
    # C2: directional taste via bias_tag (mv_bull_mp/mv_bear_mp)
    if side == "CE":
        c2 = (bias_tag or "").startswith("mv_bull")
        sl = entry_lvl - buf
    else:
        c2 = (bias_tag or "").startswith("mv_bear")
        sl = entry_lvl + buf
    # C3, C4 placeholders (real OC-pattern/momentum gates come later)
    c3 = True
    c4 = True
    rr_ok, risk, tp = rr_feasible(entry_lvl, sl, p.min_target_points())
    c5 = rr_ok
    c6 = not is_no_trade_now()

    checks_line = (
        f"C1:{_check_mark(c1)} C2:{_check_mark(c2)} C3:{_check_mark(c3)} "
        f"C4:{_check_mark(c4)} C5:{_check_mark(c5)} C6:{_check_mark(c6)}"
    )
    reason = f"{checks_line} | RR risk={_num(risk,0)} tp={_num(tp,0)}"
    all_ok = all([c1, c2, c3, c4, c5, c6])
    return all_ok, reason, sl, tp

def _build_oc_now_message() -> str:
    snap = get_snapshot()
    if not snap:
        return "<b>/oc_now</b>\nNo OC snapshot yet."

    p = Params()
    b = int(snap.extras.get("buffer", p.buffer_points()) if snap.extras else p.buffer_points())

    # Levels & shifted triggers
    s1, s2, r1, r2 = snap.s1, snap.s2, snap.r1, snap.r2
    s1s = snap.extras.get("s1s") if snap.extras else None
    s2s = snap.extras.get("s2s") if snap.extras else None
    r1s = snap.extras.get("r1s") if snap.extras else None
    r2s = snap.extras.get("r2s") if snap.extras else None

    # Trigger status summary
    s1_state, s1d = _near_or_cross("CE", "S1*", snap.spot, s1s, b)
    s2_state, s2d = _near_or_cross("CE", "S2*", snap.spot, s2s, b)
    r1_state, r1d = _near_or_cross("PE", "R1*", snap.spot, r1s, b)
    r2_state, r2d = _near_or_cross("PE", "R2*", snap.spot, r2s, b)

    # Decide a candidate for 6-checks preview:
    # priority: first CROSS among S1*,S2*,R1*,R2*; else NEAR; else None
    cand = None
    for tag, state, lvl in (("S1*", s1_state, s1s), ("S2*", s2_state, s2s),
                            ("R1*", r1_state, r1s), ("R2*", r2_state, r2s)):
        if state == "CROSS" and lvl:
            cand = (tag, lvl)
            break
    if cand is None:
        for tag, state, lvl in (("S1*", s1_state, s1s), ("S2*", s2_state, s2s),
                                ("R1*", r1_state, r1s), ("R2*", r2_state, r2s)):
            if state == "NEAR" and lvl:
                cand = (tag, lvl)
                break

    # Compute 6-checks preview (for chosen candidate, if any)
    decision_lines = []
    if cand:
        tag, lvl = cand
        side = "CE" if tag in ("S1*", "S2*") else "PE"
        all_ok, reason, sl, tp = _six_checks_summary(side, tag, float(lvl), b, snap.bias_tag, p)
        decision = f"{'Eligible' if all_ok else 'Not Eligible'} – {side} @ {tag}"
        decision_lines.append(f"<b>Decision</b>: {decision}")
        decision_lines.append(reason)
        decision_lines.append(f"Entry={_num(lvl,0)} SL={_num(sl,0)} TP={_num(tp,0)}")
    else:
        decision_lines.append("<b>Decision</b>: No trigger NEAR/CROSS")

    # Assemble message
    header = (
        f"<b>/oc_now</b>  <i>{_now_str()}</i>\n"
        f"Spot <b>{_num(snap.spot,2)}</b>"
        f" | VIX {_num(snap.vix)}"
        f" | PCR {_num(snap.pcr)}"
        f" | MaxPain <b>{_num(snap.max_pain,0)}</b> (Δ {_num(snap.max_pain_dist)})"
        f"{(' → ' + snap.bias_tag) if snap.bias_tag else ''}"
    )

    levels = (
        "<b>Levels</b>\n"
        f"S1 {_num(s1,0)}  S2 {_num(s2,0)}  R1 {_num(r1,0)}  R2 {_num(r2,0)}\n"
        f"Triggers*  S1* <b>{_num(s1s,0)}</b>  S2* <b>{_num(s2s,0)}</b>  "
        f"R1* <b>{_num(r1s,0)}</b>  R2* <b>{_num(r2s,0)}</b>  "
        f"(buffer={b})"
    )

    trig = (
        "<b>Trigger status</b>\n"
        f"• S1* {s1_state} (Δ={_num(s1d)}) | S2* {s2_state} (Δ={_num(s2d)})\n"
        f"• R1* {r1_state} (Δ={_num(r1d)}) | R2* {r2_state} (Δ={_num(r2d)})"
    )

    mv_block = _mv_block(snap.extras or {}, snap.pcr, snap.max_pain, snap.max_pain_dist)

    # OC-Pattern placeholder (to be implemented in next task)
    oc_pat = "<b>OC-Pattern</b> – pending (ΔOI basis will show here)"

    blocks = [header, levels, trig, mv_block, oc_pat, "\n".join(decision_lines)]
    return "\n\n".join(blocks)

# ------------------------------ handlers ------------------------------

async def _guard(update: Update) -> bool:
    """Return True if allowed, else inform and return False."""
    uid = update.effective_user.id if update.effective_user else None
    if not _authorized(uid):
        try:
            await update.effective_message.reply_text("Unauthorized.")
        except Exception:
            pass
        return False
    return True

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update):
        return
    await update.message.reply_text("Namaste 👋\nBot is up. Try /oc_now")

async def health_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update):
        return
    await update.message.reply_text(f"OK {datetime.now(tz=IST).strftime('%H:%M:%S %Z')}")

async def oc_now_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update):
        return
    try:
        msg = _build_oc_now_message()
        await update.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        log.error(f"/oc_now failed: {e}")
        await update.message.reply_text("Error building OC snapshot.")

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

    # alias: /run oc_now
    async def run_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await _guard(update):
            return
        if context.args and len(context.args) >= 1 and context.args[0].lower() == "oc_now":
            await oc_now_cmd(update, context)
        else:
            await update.message.reply_text("Usage: /run oc_now")

    app.add_handler(CommandHandler("run", run_cmd))

    return app
