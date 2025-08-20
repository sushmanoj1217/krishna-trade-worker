# telegram_bot.py
from __future__ import annotations

import os
import asyncio
from typing import Optional, Tuple

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

# ---- internal imports (defensive) ----
from utils.logger import log

# optional/defensive imports (not all repos expose same APIs)
try:
    from utils.state import (
        is_oc_auto,
        set_oc_auto,
        approvals_required,
        set_approvals_required,
    )
except Exception:  # fallback shims
    _OC_AUTO = True
    _APPROVALS = False

    def is_oc_auto() -> bool:
        return _OC_AUTO

    def set_oc_auto(v: bool):
        nonlocal_vars = {"_OC_AUTO": v}  # no-op for static analyzers
        globals()["_OC_AUTO"] = v

    def approvals_required() -> bool:
        return _APPROVALS

    def set_approvals_required(v: bool):
        nonlocal_vars = {"_APPROVALS": v}
        globals()["_APPROVALS"] = v

try:
    from utils.cache import get_snapshot  # last-good OC snapshot
except Exception:
    def get_snapshot():
        return None

try:
    from analytics.oc_refresh import refresh_once
except Exception:
    async def refresh_once():
        return None

# Sheets helpers (safe if missing)
try:
    from integrations import sheets as sh
except Exception:
    class _DummySheets:
        def write_status(self, *a, **k): pass
        def append_status(self, *a, **k): pass
        def write_signal_row(self, *a, **k): pass
        def write_trade_row(self, *a, **k): pass
        def now_str(self): 
            import datetime as _dt
            return _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        def append_row(self, *a, **k): pass
        def ensure_tabs(self): pass
    sh = _DummySheets()  # type: ignore

# Telemetry (optional)
try:
    from utils import telemetry
except Exception:
    class _DummyTelem:
        def health_dict(self): return {}
        def version_string(self): 
            return os.getenv("RENDER_GIT_COMMIT", "")[:7] or "dev"
    telemetry = _DummyTelem()  # type: ignore


# ========= Config =========
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OWNER_ID = os.getenv("TELEGRAM_OWNER_ID", "").strip()
try:
    OWNER_ID = int(OWNER_ID) if OWNER_ID else None  # type: ignore[assignment]
except Exception:
    OWNER_ID = None  # type: ignore[assignment]

IST = "Asia/Kolkata"


# ========= Small utils =========
def _now_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        import datetime as dt
        return dt.datetime.now(ZoneInfo(IST)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        import datetime as dt
        return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

async def _guard(update: Update) -> bool:
    """Only allow the configured owner (if set)."""
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

def _fmt_bool(b: Optional[bool]) -> str:
    return "True" if b else "False"

def _fmt_num(x) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return str(x)


# ========= OC_NOW RENDERER =========
def _render_oc_now(snap) -> str:
    """Build compact /oc_now report. Works with partial fields; never crashes."""
    if not snap:
        return "*OC snapshot unavailable.*"

    # Safe getters
    g = lambda k, d="?" : getattr(snap, k, d)
    spot = g("spot")
    vix = g("vix")
    pcr = g("pcr")
    pcr_bucket = g("pcr_bucket")
    mp = g("max_pain")
    mp_dist = g("max_pain_dist")
    bias = g("bias_tag")
    expiry = g("expiry")
    stale = g("stale", False)

    s1, s2, r1, r2 = g("s1"), g("s2"), g("r1"), g("r2")
    b = g("buffer_points", g("buffer", ""))  # buffer might be in snapshot
    try:
        btxt = f" (buf {int(b)})" if b not in ("", None, "?") else ""
    except Exception:
        btxt = ""

    # Header
    line1 = f"*Spot* {spot} | *VIX* {_fmt_num(vix)} | *PCR* {_fmt_num(pcr)} [{pcr_bucket}]"
    line2 = f"*MaxPain* {mp} (Δ {_fmt_num(mp_dist)}) | *Bias* {bias} | *Exp* {expiry}"
    if stale:
        line2 += "  ⚠️*STALE*"

    # Levels
    trig = lambda lv, kind: f"{lv - b if kind=='S' and isinstance(b,(int,float)) else lv + b if kind=='R' and isinstance(b,(int,float)) else '?'}"
    levels = (
        f"*Levels*\n"
        f"S1 {s1} / S2 {s2} / R1 {r1} / R2 {r2}{btxt}\n"
        f"Triggers → S1* {_fmt_num(s1 if s1=='?' else (float(s1) - float(b) if str(b).isdigit() else s1))} | "
        f"S2* {_fmt_num(s2 if s2=='?' else (float(s2) - float(b) if str(b).isdigit() else s2))} | "
        f"R1* {_fmt_num(r1 if r1=='?' else (float(r1) + float(b) if str(b).isdigit() else r1))} | "
        f"R2* {_fmt_num(r2 if r2=='?' else (float(r2) + float(b) if str(b).isdigit() else r2))}"
    )

    # Checks/blocks (best-effort; may be missing)
    c = lambda k: "✅" if g(k, False) else ("❌" if g(k, None) is not None else "—")
    checks = (
        f"*6-Checks* C1 {c('c1')} C2 {c('c2')} C3 {c('c3')} C4 {c('c4')} C5 {c('c5')} C6 {c('c6')}"
    )
    mv = f"*MV:* PCR≥/≤ flags {g('mv_pcr_ok','?')}, MP-dist {g('mv_mp_ok','?')} | basis {g('mv_basis','')}"
    ocp = f"*OC-Pattern:* {g('oc_pattern_basis','')}"
    decision = f"*Decision:* {'Eligible' if g('eligible', False) else 'Not Eligible'} | {g('eligible_reason','')}".strip()

    parts = [line1, line2, "", levels, "", checks, ocp, mv, "", decision]
    return "\n".join([p for p in parts if p is not None])


# ========= Command handlers =========
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    await update.message.reply_text("Hi! Try /whoami, /oc_now, /run oc_auto status", disable_web_page_preview=True)

async def whoami_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    uid = update.effective_user.id if update.effective_user else "?"
    await update.message.reply_text(f"user_id={uid} | owner={OWNER_ID if OWNER_ID else 'None'}")

async def version_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    ver = ""
    try:
        ver = telemetry.version_string()
    except Exception:
        ver = os.getenv("RENDER_GIT_COMMIT", "")[:7] or "dev"
    await update.message.reply_text(f"version: {ver}")

async def health_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    line = ""
    try:
        h = telemetry.health_dict()
        # Expected keys (best-effort)
        ok = h.get("oc_ok")
        fail = h.get("oc_fail")
        r429 = h.get("dhan_429")
        stale = h.get("stale")
        ts = h.get("last_ts")
        line = f"OC ok:{ok} fail:{fail} 429:{r429} | stale={stale} ts={ts}"
    except Exception:
        line = "health: ok"
    await update.message.reply_text(line)

async def oc_now_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    try:
        snap = get_snapshot()
        if not snap or getattr(snap, "stale", False):
            try:
                await asyncio.to_thread(refresh_once)  # non-blocking to main loop
                snap2 = get_snapshot()
                if snap2: snap = snap2
            except Exception as e:
                log.warning(f"/oc_now refresh_once failed: {e}")

        if not snap:
            await update.message.reply_text(
                "OC अभी unavailable है (rate-limit/first snapshot pending). 20–30s बाद फिर से /oc_now भेजें."
            )
            return

        text = _render_oc_now(snap)
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

# ----- approvals -----
async def approve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    # Try to use agents.signal_generator API if available
    try:
        from agents import signal_generator as sig
    except Exception:
        sig = None

    if not context.args:
        # list pending
        try:
            if sig and hasattr(sig, "list_pending_ids"):
                ids = sig.list_pending_ids()
                if ids:
                    await update.message.reply_text("Pending: " + ", ".join(ids))
                else:
                    await update.message.reply_text("No pending approvals.")
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

# ----- overrides: /set_levels -----
async def set_levels_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    if not context.args:
        await update.message.reply_text(
            "Usage: /set_levels buffer <pts> | mpdist <pts> | pcr <bull_high> <bear_low> | target <pts>"
        )
        return

    sub = context.args[0].lower()
    args = context.args[1:]

    async def _ack(msg: str):
        try:
            await update.message.reply_text(msg)
        except Exception:
            pass

    # Try via utils.params setter; else write to sheet
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
            val = args[0]
            _push_override("ENTRY_BAND_POINTS", val)
            await _ack(f"buffer override → {val}")

        elif sub == "mpdist" and len(args) >= 1:
            val = args[0]
            _push_override("MP_SUPPORT_DIST", val)
            await _ack(f"max-pain distance override → {val}")

        elif sub == "pcr" and len(args) >= 2:
            bh, bl = args[0], args[1]
            _push_override("PCR_BULL_HIGH", bh)
            _push_override("PCR_BEAR_LOW", bl)
            await _ack(f"PCR bands override → bull_high={bh} bear_low={bl}")

        elif sub == "target" and len(args) >= 1:
            val = args[0]
            _push_override("MIN_TARGET_POINTS", val)
            await _ack(f"min target points override → {val}")

        else:
            await _ack("Usage: /set_levels buffer <pts> | mpdist <pts> | pcr <bull_high> <bear_low> | target <pts>")

    except Exception as e:
        log.error(f"/set_levels failed: {e}", exc_info=True)
        await _ack("set_levels failed")


# ----- natural approvals via free text -----
async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _guard(update): return
    txt = (update.message.text or "").strip().lower()
    if txt in ("approve", "approved"):
        await approve_cmd(update, context)
    elif txt in ("deny", "denied", "reject", "rejected"):
        await deny_cmd(update, context)
    # else ignore; keep bot quiet


# ========= App init =========
async def init() -> Optional[Application]:
    token = BOT_TOKEN
    if not token:
        log.error("Telegram token missing.")
        return None

    app: Application = ApplicationBuilder().token(token).build()

    # commands
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("whoami", whoami_cmd))
    app.add_handler(CommandHandler("version", version_cmd))
    app.add_handler(CommandHandler("health", health_cmd))
    app.add_handler(CommandHandler("oc_now", oc_now_cmd))
    app.add_handler(CommandHandler("run", run_cmd))
    app.add_handler(CommandHandler("approve", approve_cmd))
    app.add_handler(CommandHandler("deny", deny_cmd))
    app.add_handler(CommandHandler("set_levels", set_levels_cmd))

    # natural language approvals
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), text_router))

    return app
