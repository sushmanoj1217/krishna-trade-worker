# telegram_bot.py
from __future__ import annotations

import os
import asyncio
from typing import Optional, Dict, Any, List

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ---- internal imports ----
from utils.logger import log

# state toggles (with safe fallbacks)
try:
    from utils.state import (
        is_oc_auto,
        set_oc_auto,
        approvals_required,
        set_approvals_required,
    )
except Exception:
    _OC_AUTO = True
    _APPROVALS = False
    def is_oc_auto(): return _OC_AUTO
    def set_oc_auto(v: bool):
        globals()["_OC_AUTO"] = bool(v)
    def approvals_required(): return _APPROVALS
    def set_approvals_required(v: bool):
        globals()["_APPROVALS"] = bool(v)

# OC snapshot cache & refresher (safe fallbacks)
try:
    from utils.cache import get_snapshot
except Exception:
    def get_snapshot(): return None

try:
    from analytics.oc_refresh import refresh_once
except Exception:
    async def refresh_once(): return None

# Sheets wrapper (safe no-op fallbacks)
try:
    from integrations import sheets as sh
except Exception:
    class _S:
        def get_all_values(self, *a, **k): return []
        def now_str(self): 
            import datetime as _dt
            return _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        def append_row(self, *a, **k): pass
        def write_status(self, *a, **k): pass
        def write_signal_row(self, *a, **k): pass
        def write_trade_row(self, *a, **k): pass
    sh = _S()  # type: ignore

# telemetry (optional)
try:
    from utils import telemetry
except Exception:
    class _T:
        def health_dict(self): return {}
        def version_string(self): return os.getenv("RENDER_GIT_COMMIT","")[:7] or "dev"
    telemetry = _T()  # type: ignore


# ========= config =========
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OWNER_ID = os.getenv("TELEGRAM_OWNER_ID", "").strip()
try:
    OWNER_ID = int(OWNER_ID) if OWNER_ID else None  # type: ignore[assignment]
except Exception:
    OWNER_ID = None  # type: ignore[assignment]
IST = "Asia/Kolkata"


# ========= tiny utils =========
def _now_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        import datetime as dt
        return dt.datetime.now(ZoneInfo(IST)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        import datetime as dt
        return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

async def _guard(update: Update) -> bool:
    if OWNER_ID is None:
        return True
    uid = update.effective_user.id if update.effective_user else None
    if uid != OWNER_ID:
        try:
            await update.effective_chat.send_message("Not authorized.")
        except Exception:
            pass
        return False
    return True

def _fmt_num(x) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return str(x)

def _to_bool(x) -> Optional[bool]:
    s = str(x).strip().lower()
    if s in ("1","true","yes","y","✅","ok"): return True
    if s in ("0","false","no","n","❌"): return False
    return None

def _rows_as_dicts(rows: List[List[Any]]) -> List[Dict[str, Any]]:
    if not rows: return []
    header = [str(h).strip().lower() for h in rows[0]]
    out: List[Dict[str,Any]] = []
    for r in rows[1:]:
        d: Dict[str,Any] = {}
        for i,v in enumerate(r):
            key = header[i] if i < len(header) else f"col{i+1}"
            d[key] = v
        out.append(d)
    return out


# ========= latest signal fetch (for full conditions) =========
def _latest_signal_dict() -> Dict[str, Any]:
    """
    Read last row from Sheets→Signals and normalize keys we need.
    Returns {} if sheet not available/empty.
    """
    try:
        rows = sh.get_all_values("Signals")
    except Exception as e:
        log.warning(f"signals get_all_values failed: {e}")
        rows = []
    if not rows or len(rows) < 2:
        return {}

    d = _rows_as_dicts(rows)[-1]  # last row
    # Normalize booleans & fields we use in rendering
    norm = {
        "signal_id": d.get("signal_id",""),
        "ts": d.get("ts",""),
        "side": str(d.get("side","")).upper(),
        "trigger": d.get("trigger",""),
        "near_cross": d.get("near/cross","") or d.get("near_cross",""),
        "eligible": _to_bool(d.get("eligible","")),
        "eligible_reason": d.get("reason","") or d.get("notes",""),
        "c1": _to_bool(d.get("c1","")), "c2": _to_bool(d.get("c2","")),
        "c3": _to_bool(d.get("c3","")), "c4": _to_bool(d.get("c4","")),
        "c5": _to_bool(d.get("c5","")), "c6": _to_bool(d.get("c6","")),
        "mv_pcr_ok": _to_bool(d.get("mv_pcr_ok","")),
        "mv_mp_ok": _to_bool(d.get("mv_mp_ok","")),
        "mv_basis": d.get("mv_basis",""),
        "oc_bull_normal": _to_bool(d.get("oc_bull_normal","")),
        "oc_bull_shortcover": _to_bool(d.get("oc_bull_shortcover","")),
        "oc_bear_normal": _to_bool(d.get("oc_bear_normal","")),
        "oc_bear_crash": _to_bool(d.get("oc_bear_crash","")),
        "oc_pattern_basis": d.get("oc_pattern_basis",""),
        "notes": d.get("notes",""),
    }
    return norm


# ========= /oc_now render =========
def _render_oc_now(snap, sig: Dict[str,Any]) -> str:
    # snapshot safe getters
    g = lambda k, d="?" : getattr(snap, k, d) if snap else d
    spot, vix, pcr = g("spot"), g("vix"), g("pcr")
    pcr_bucket = g("pcr_bucket","?")
    mp, mp_dist, bias, expiry = g("max_pain"), g("max_pain_dist"), g("bias_tag"), g("expiry")
    s1, s2, r1, r2 = g("s1"), g("s2"), g("r1"), g("r2")
    buf = g("buffer_points", g("buffer",""))
    stale = g("stale", False)

    try:
        btxt = f" (buf {int(buf)})" if buf not in ("","?",None) else ""
    except Exception:
        btxt = ""

    # Header
    line1 = f"*Spot* {spot} | *VIX* {_fmt_num(vix)} | *PCR* {_fmt_num(pcr)} [{pcr_bucket}]"
    line2 = f"*MaxPain* {mp} (Δ {_fmt_num(mp_dist)}) | *Bias* {bias} | *Exp* {expiry}"
    if stale:
        line2 += "  ⚠️*STALE*"

    # Levels & shifted triggers
    def _num(x):
        try: return float(x)
        except Exception: return None
    def shifter(val, sign):
        vb = _num(val); bb = _num(buf)
        if vb is None: return "?"
        if bb is None: return f"{vb:.2f}"
        return f"{(vb - bb if sign<0 else vb + bb):.2f}"

    levels = (
        f"*Levels*\n"
        f"S1 {s1} / S2 {s2} / R1 {r1} / R2 {r2}{btxt}\n"
        f"Triggers → S1* {shifter(s1,-1)} | S2* {shifter(s2,-1)} | R1* {shifter(r1,+1)} | R2* {shifter(r2,+1)}"
    )

    # Pull latest signal fields (if any)
    def flag(x: Optional[bool]) -> str:
        return "✅" if x is True else ("❌" if x is False else "—")

    # Six-checks + reasons
    c_line = f"*6-Checks* C1 {flag(sig.get('c1'))} · C2 {flag(sig.get('c2'))} · C3 {flag(sig.get('c3'))} · C4 {flag(sig.get('c4'))} · C5 {flag(sig.get('c5'))} · C6 {flag(sig.get('c6'))}"
    trig_line = ""
    if sig.get("trigger") or sig.get("near_cross"):
        trig_line = f"*Trigger:* {sig.get('trigger','?')} · *Status:* {sig.get('near_cross','')}"
    ocp = "*OC-Pattern:* " + (sig.get("oc_pattern_basis") or _best_pattern(sig))
    mvb = "*MV:* " + _mv_summary(sig, pcr, mp, mp_dist)

    # Decision
    dec = f"*Decision:* {'Eligible' if sig.get('eligible') else 'Not Eligible'}"
    if sig.get("eligible_reason"):
        dec += f" | {sig.get('eligible_reason')}"
    if sig.get("signal_id"):
        dec += f"\n*Signal:* `{sig.get('signal_id')}`"

    parts = [line1, line2, "", levels, trig_line, c_line, ocp, mvb, "", dec]
    return "\n".join([p for p in parts if p])

def _best_pattern(sig: Dict[str,Any]) -> str:
    # prefer explicit basis
    if sig.get("oc_pattern_basis"): return sig["oc_pattern_basis"]
    # otherwise infer strongest true flag
    if sig.get("oc_bull_shortcover"): return "bull_shortcover"
    if sig.get("oc_bull_normal"): return "bull_normal"
    if sig.get("oc_bear_crash"): return "bear_crash"
    if sig.get("oc_bear_normal"): return "bear_normal"
    return ""

def _mv_summary(sig: Dict[str,Any], pcr, mp, mp_dist) -> str:
    pcr_ok = sig.get("mv_pcr_ok")
    mp_ok  = sig.get("mv_mp_ok")
    basis  = sig.get("mv_basis","")
    parts = []
    parts.append(f"PCR {pcr} → {('✅' if pcr_ok else '❌' if pcr_ok is False else '—')}")
    parts.append(f"MPΔ {_fmt_num(mp_dist)} → {('✅' if mp_ok else '❌' if mp_ok is False else '—')}")
    if basis: parts.append(f"| {basis}")
    return " ".join(parts)


# ========= commands =========
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    await update.message.reply_text("Hi! Try /whoami, /oc_now, /run oc_auto status", disable_web_page_preview=True)

async def whoami_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    uid = update.effective_user.id if update.effective_user else "?"
    await update.message.reply_text(f"user_id={uid} | owner={OWNER_ID if OWNER_ID else 'None'}")

async def version_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    try: ver = telemetry.version_string()
    except Exception: ver = os.getenv("RENDER_GIT_COMMIT","")[:7] or "dev"
    await update.message.reply_text(f"version: {ver}")

async def health_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    try:
        h = telemetry.health_dict()
        line = f"OC ok:{h.get('oc_ok')} fail:{h.get('oc_fail')} 429:{h.get('dhan_429')} | stale={h.get('stale')} ts={h.get('last_ts')}"
    except Exception:
        line = "health: ok"
    await update.message.reply_text(line)

async def oc_now_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    try:
        # 1) current/last-good snapshot
        snap = get_snapshot()
        if not snap or getattr(snap, "stale", False):
            try:
                await asyncio.to_thread(refresh_once)
                snap2 = get_snapshot()
                if snap2: snap = snap2
            except Exception as e:
                log.warning(f"/oc_now refresh_once failed: {e}")

        # 2) latest signal row (for conditions)
        sig = _latest_signal_dict()

        # 3) if nothing at all, explain gracefully
        if not snap and not sig:
            await update.message.reply_text(
                "OC snapshot unavailable (rate-limit/first snapshot). 20–30s बाद फिर से /oc_now भेजें."
            )
            return

        text = _render_oc_now(snap, sig)
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    except Exception as e:
        log.error(f"/oc_now failed: {e}", exc_info=True)
        await update.message.reply_text("Temporary issue in /oc_now. Try again shortly.")

async def run_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    if not context.args:
        await update.message.reply_text(
            "Usage: /run oc_auto on|off|status | oc_now | approvals on|off|status"
        )
        return
    sub = context.args[0].lower()

    if sub == "oc_auto":
        if len(context.args) == 1 or context.args[1].lower() == "status":
            await update.message.reply_text(f"oc_auto={is_oc_auto()}")
            return
        val = context.args[1].lower()
        if val == "on":
            set_oc_auto(True);  await update.message.reply_text("oc_auto: ON")
        elif val == "off":
            set_oc_auto(False); await update.message.reply_text("oc_auto: OFF")
        else:
            await update.message.reply_text("Use on|off|status")

    elif sub == "oc_now":
        await oc_now_cmd(update, context)

    elif sub == "approvals":
        if len(context.args) == 1 or context.args[1].lower() == "status":
            await update.message.reply_text(f"approvals_required={approvals_required()}")
            return
        on = context.args[1].lower()
        if on in ("on","off"):
            set_approvals_required(on == "on")
            await update.message.reply_text(f"approvals_required={approvals_required()}")
        else:
            await update.message.reply_text("Use approvals on|off|status")

    else:
        await update.message.reply_text("Unknown /run subcommand. Try: oc_auto | oc_now | approvals")

async def approve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    try:
        from agents import signal_generator as sig
    except Exception:
        sig = None

    if not context.args:
        try:
            if sig and hasattr(sig, "list_pending_ids"):
                ids = sig.list_pending_ids()
                await update.message.reply_text("Pending: " + (", ".join(ids) if ids else "None"))
                return
        except Exception as e:
            log.warning(f"/approve list pending failed: {e}")
        await update.message.reply_text("Usage: /approve <signal_id>")
        return

    sid = context.args[0]
    ok = False
    try:
        if sig and hasattr(sig, "approve"):
            ok = bool(sig.approve(sid))
    except Exception as e:
        log.warning(f"/approve {sid} failed: {e}")
    await update.message.reply_text(f"approved={ok} for {sid}")

async def deny_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    try:
        from agents import signal_generator as sig
    except Exception:
        sig = None

    if not context.args:
        await update.message.reply_text("Usage: /deny <signal_id>")
        return

    sid = context.args[0]
    ok = False
    try:
        if sig and hasattr(sig, "deny"):
            ok = bool(sig.deny(sid))
    except Exception as e:
        log.warning(f"/deny {sid} failed: {e}")
    await update.message.reply_text(f"denied={ok} for {sid}")

async def set_levels_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    if not context.args:
        await update.message.reply_text(
            "Usage: /set_levels buffer <pts> | mpdist <pts> | pcr <bull_high> <bear_low> | target <pts>"
        )
        return

    sub = context.args[0].lower()
    args = context.args[1:]

    def _push_override(key: str, value: str):
        pushed = False
        try:
            from utils import params as P
            if hasattr(P, "set_override"):
                P.set_override(key, value)
                pushed = True
        except Exception:
            pushed = False
        if not pushed:
            try:
                sh.append_row("Params_Override", [_now_str(), key, value])
            except Exception:
                pass
        return pushed

    try:
        if sub == "buffer" and len(args) >= 1:
            val = args[0]; _push_override("ENTRY_BAND_POINTS", val)
            await update.message.reply_text(f"buffer override → {val}")

        elif sub == "mpdist" and len(args) >= 1:
            val = args[0]; _push_override("MP_SUPPORT_DIST", val)
            await update.message.reply_text(f"max-pain distance override → {val}")

        elif sub == "pcr" and len(args) >= 2:
            bh, bl = args[0], args[1]
            _push_override("PCR_BULL_HIGH", bh); _push_override("PCR_BEAR_LOW", bl)
            await update.message.reply_text(f"PCR bands override → bull_high={bh} bear_low={bl}")

        elif sub == "target" and len(args) >= 1:
            val = args[0]; _push_override("MIN_TARGET_POINTS", val)
            await update.message.reply_text(f"min target points override → {val}")

        else:
            await update.message.reply_text("Usage: /set_levels buffer <pts> | mpdist <pts> | pcr <bull_high> <bear_low> | target <pts>")
    except Exception as e:
        log.error(f"/set_levels failed: {e}", exc_info=True)
        await update.message.reply_text("set_levels failed")

# free-text approvals
async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    t = (update.message.text or "").strip().lower()
    if t in ("approve","approved"):
        await approve_cmd(update, context)
    elif t in ("deny","denied","reject","rejected"):
        await deny_cmd(update, context)
    # else: ignore

# ========= App init =========
async def init() -> Optional[Application]:
    token = BOT_TOKEN
    if not token:
        log.error("Telegram token missing.")
        return None

    app: Application = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("whoami", whoami_cmd))
    app.add_handler(CommandHandler("version", version_cmd))
    app.add_handler(CommandHandler("health", health_cmd))
    app.add_handler(CommandHandler("oc_now", oc_now_cmd))
    app.add_handler(CommandHandler("run", run_cmd))
    app.add_handler(CommandHandler("approve", approve_cmd))
    app.add_handler(CommandHandler("deny", deny_cmd))
    app.add_handler(CommandHandler("set_levels", set_levels_cmd))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), text_router))

    return app
