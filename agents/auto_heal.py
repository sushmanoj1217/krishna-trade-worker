# path: agents/auto_heal.py
import json, pathlib
from datetime import datetime
from tzlocal import get_localzone
from agents import logger

DATA_DIR = pathlib.Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

def _today() -> str:
    return datetime.now(get_localzone()).strftime("%Y-%m-%d")

def generate_suggestions(sheet, cfg):
    day = _today()
    rows = sheet.read_all("Trades") or []
    loses = []
    for r in rows:
        ts = str(r.get("ts_buy",""))
        if not ts.startswith(day): continue
        try:
            pnl = float(r.get("pnl") or 0)
            if pnl < 0:
                loses.append(r)
        except Exception:
            pass

    # very basic heuristic: if more than 2 SL, increase entry band
    sl_count = 0
    for r in loses:
        reason = (r.get("reason_exit","") or "").upper()
        if "SL" in reason: sl_count += 1

    suggestions = {}
    if sl_count >= 2:
        suggestions["entry_rules.entry_band_points"] = "+2"  # suggest widening by 2 pts
    # more rules can be added here

    out = {"date": day, "symbol": cfg.symbol, "losers": len(loses), "sl_count": sl_count, "suggestions": suggestions}
    (DATA_DIR / "auto_heal.json").write_text(json.dumps(out, indent=2))
    logger.log_status(sheet, {"state":"OK", "message": f"auto_heal suggestions saved ({len(suggestions)} items)"})

    # Also write a params override file for next day (non-destructive; human can edit)
    overrides = {}
    if "entry_rules.entry_band_points" in suggestions:
        overrides.setdefault("entry_rules", {})
        overrides["entry_rules"]["entry_band_points"] = None  # set None to indicate "+2"
        overrides["_diff"] = {"entry_rules.entry_band_points": "+2"}
    (DATA_DIR / "params_override.json").write_text(json.dumps(overrides, indent=2))
