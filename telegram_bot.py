# telegram_bot.py
# ------------------------------------------------------------------------------------
# PTB v20+ compatible bot wiring
# - init() -> Application (do NOT start polling here; main controls single-poller)
# - /oc_now renders snapshot with ALWAYS-FILLED Summary (labels aligned with Checks)
#
# Env:
#   TELEGRAM_BOT_TOKEN=...
#   LEVEL_BUFFER=12
#   OC_MAX_SNAPSHOT_AGE_SEC=300
# ------------------------------------------------------------------------------------
from __future__ import annotations

import os
import logging
from typing import Any, Dict, Optional

from telegram import Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

log = logging.getLogger(__name__)

# ---------- small utils ----------
def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v.strip() if v and str(v).strip() else default

def _to_float(x):
    try:
        if x in (None, "", "—"): return None
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None

def _fmt(x, digits=2):
    if x is None:
        return "—"
    try:
        return f"{float(x):.{digits}f}"
    except Exception:
        return str(x)

def _signfmt(x: Optional[float]) -> str:
    if x is None: return "—"
    try:
        xf = float(x)
        s = "+" if xf >= 0 else ""
        if abs(xf) >= 1_000_000:
            return f"{s}{int(xf):,}"
        return f"{s}{xf:.2f}"
    except Exception:
        return "—"

def _boolmark(ok: Optional[bool]) -> str:
    return "✅" if ok else "❌"

def _pick_summary(s: Dict[str, Any]) -> str:
    for k in ("summary", "summary_text", "summary_line", "final_summary", "summary_str"):
        txt = (s.get(k) or "").strip()
        if txt:
            return txt
    return ""  # will fallback

def _derive_mv(pcr: Optional[float], mp: Optional[float], spot: Optional[float],
               ce_d: Optional[float], pe_d: Optional[float]) -> str:
    score = 0
    try:
        if isinstance(pcr,(int,float)): score += 1 if float(pcr) >= 1.0 else -1
    except Exception: pass
    try:
        if isinstance(mp,(int,float)) and isinstance(spot,(int,float)):
            score += 1 if float(mp) > float(spot) else -1
    except Exception: pass
    if score > 0: return "bullish"
    if score < 0: return "bearish"
    if isinstance(ce_d,(int,float)) and isinstance(pe_d,(int,float)) and ce_d != pe_d:
        return "bullish" if pe_d > ce_d else "bearish"
    return ""

def _fallback_summary(s: Dict[str, Any]) -> str:
    # HARD guards
    if s.get("stale"):
        rs = s.get("stale_reason") or []
        reason = "; ".join(rs) if rs else "stale"
        return f"⚠️ STALE DATA — live mismatch; no trade. (Reasons: {reason})"
    if s.get("hold") or s.get("daily_cap_hit"):
        tags = []
        if s.get("hold"): tags.append("HOLD")
        if s.get("daily_cap_hit"): tags.append("DailyCap")
        return f"🚫 System {' & '.join(tags)} — no trade."

    mv = (s.get("mv") or "").strip().lower()
    pcr = _to_float(s.get("pcr")); mp = _to_float(s.get("max_pain")); spot = _to_float(s.get("spot"))
    ce_d= _to_float(s.get("ce_oi_delta")); pe_d = _to_float(s.get("pe_oi_delta"))

    if not mv:
        mv = _derive_mv(pcr, mp, spot, ce_d, pe_d)

    # OIΔ alignment (C3)
    c3_ok = None
    if isinstance(ce_d,(int,float)) and isinstance(pe_d,(int,float)):
        if mv == "bearish": c3_ok = (ce_d > 0 and (pe_d is None or pe_d <= 0))
        elif mv == "bullish": c3_ok = (pe_d > 0 and (ce_d is None or ce_d <= 0))

    # PCR/MP (C4)
    c4_ok = None
    if isinstance(pcr,(int,float)) and isinstance(mp,(int,float)) and isinstance(spot,(int,float)):
        if mv == "bearish": c4_ok = (pcr < 1.0 and mp <= spot)
        elif mv == "bullish": c4_ok = (pcr >= 1.0 and mp >= spot)

    side, level = (None, None)
    if mv == "bearish": side, level = "CE", "S1*"
    elif mv == "bullish": side, level = "PE", "R1*"

    fails = []
    if c4_ok is False: fails.append("C4")
    if c3_ok is False: fails.append("C3")

    if mv and (c4_ok is True) and (c3_ok is True):
        return f"✅ Eligible — {side} @ {level}"
    if mv:
        return f"❌ Not eligible — failed: {', '.join(sorted(fails)) or 'gates'}"
    return "❔ Insufficient data — waiting for live feed"

# ---------- /oc_now ----------
async def oc_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    from analytics import oc_refresh  # import here to avoid circulars
    try:
        await oc_refresh.refresh_once()
    except Exception as e:
        log.warning("oc_now: refresh_once error: %s", e)

    snap = oc_refresh.get_snapshot() or {}
    if not snap:
        await update.message.reply_text("OC snapshot unavailable (rate-limit/first snapshot). मैंने refresh kick किया है — ~15s बाद फिर `/oc_now` भेजें.")
        return

    sym  = (snap.get("symbol") or _env("OC_SYMBOL","NIFTY")).upper()
    exp  = snap.get("expiry") or "—"
    spot = _to_float(snap.get("spot"))
    s1   = _to_float(snap.get("s1")); s2 = _to_float(snap.get("s2"))
    r1   = _to_float(snap.get("r1")); r2 = _to_float(snap.get("r2"))
    pcr  = _to_float(snap.get("pcr")); mp = _to_float(snap.get("max_pain"))
    ce_d = _to_float(snap.get("ce_oi_delta")); pe_d = _to_float(snap.get("pe_oi_delta"))
    mv   = (snap.get("mv") or "").strip().lower()
    src  = snap.get("source") or "—"
    asof = snap.get("asof") or "—"
    age  = snap.get("age_sec")
    stale= bool(snap.get("stale"))
    buf  = _to_float(_env("LEVEL_BUFFER","12"))

    # shifted
    def _shift(v, b):
        return None if v is None or b is None else (float(v) - float(b) if v in (s1, s2) else float(v) + float(b))
    s1s = _shift(s1, buf)
    s2s = _shift(s2, buf)
    r1s = _shift(r1, buf)
    r2s = _shift(r2, buf)

    # Checks
    c1_ok = None
    if spot is not None and buf is not None and s1 is not None and r1 is not None:
        near_s1 = abs(spot - s1) <= buf
        near_r1 = abs(spot - r1) <= buf
        crossed = (spot <= s1s) or (spot >= r1s) if (s1s is not None and r1s is not None) else False
        c1_ok = (near_s1 or near_r1) and not crossed

    c2_ok = bool(mv)
    c2_reason = f"MV={mv}" if mv else "MV missing"

    c3_ok = None
    if ce_d is not None and pe_d is not None and mv:
        if mv == "bearish":
            c3_ok = (ce_d > 0 and pe_d <= 0)
        elif mv == "bullish":
            c3_ok = (pe_d > 0 and ce_d <= 0)

    mp_ok = None
    if mp is not None and spot is not None and mv:
        mp_ok = (mp <= spot) if mv == "bearish" else (mp >= spot)
    pcr_ok = None
    if pcr is not None and mv:
        pcr_ok = (pcr < 1.0) if mv == "bearish" else (pcr >= 1.0)

    hold = bool(snap.get("hold")); cap = bool(snap.get("daily_cap_hit"))
    c5_ok = not (hold or cap)
    c5_reason = "HOLD" if hold else ("DailyCap" if cap else "OK")
    c6_ok = True

    summary = _pick_summary(snap)
    if not summary:
        summary = _fallback_summary({
            **snap,
            "mv": mv,
            "pcr": pcr, "max_pain": mp, "spot": spot,
            "ce_oi_delta": ce_d, "pe_oi_delta": pe_d
        })

    lines = []
    lines.append("OC Snapshot")
    lines.append(f"Symbol: {sym}  |  Exp: {exp}  |  Spot: { _fmt(spot) }")
    lines.append(f"Levels: S1 { _fmt(s1) }  S2 { _fmt(s2) }  R1 { _fmt(r1) }  R2 { _fmt(r2) }")
    lines.append(f"Shifted: S1 `{ _fmt(s1s) }`  S2 { _fmt(s2s) }  R1 `{ _fmt(r1s) }`  R2 { _fmt(r2s) }")
    tail = f"Buffer: { _fmt(buf) }  |  MV: {mv or '—'}  |  PCR: { _fmt(pcr) }  |  MP: { _fmt(mp) }"
    lines.append(tail)
    stale_tag = "  |  ⚠️ STALE" if stale else ""
    lines.append(f"Source: {src}  |  As-of: {asof}  |  Age: {int(age or 0)}s{stale_tag}")
    lines.append("")
    lines.append("Checks")
    lines.append(f"- C1: { _boolmark(c1_ok) } NEAR, not crossed")
    lines.append(f"- C2: { _boolmark(c2_ok) } {c2_reason}")
    lines.append(f"- C3: { _boolmark(c3_ok) } CEΔ={_signfmt(ce_d)} / PEΔ={_signfmt(pe_d)}")
    pcr_tick = "✓" if pcr_ok else "×" if pcr_ok is not None else "—"
    mp_tick  = "✓" if mp_ok else "×" if mp_ok is not None else "—"
    lines.append(f"- C4: { _boolmark((pcr_ok is True) and (mp_ok is True)) } PCR={_fmt(pcr)} {pcr_tick} | MP={_fmt(mp)} vs spot {_fmt(spot)} {mp_tick}")
    lines.append(f"- C5: { _boolmark(c5_ok) } {c5_reason}")
    lines.append(f"- C6: { _boolmark(c6_ok) } new")
    lines.append("")
    lines.append(f"Summary: {summary}")

    text = "\n".join(lines)
    await update.message.reply_text(text)

# ---------- init ----------
def init() -> Application:
    token = _env("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN missing")

    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler("oc_now", oc_now))
    log.info("/oc_now handler registered")
    return app
