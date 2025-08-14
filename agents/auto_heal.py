# path: agents/auto_heal.py
import json, pathlib
from datetime import datetime
from tzlocal import get_localzone
from agents import logger
from storage.sheet_persistence import write_params_override_to_sheet

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
        if ts.startswith(day):
            try:
                if float(r.get("pnl") or 0) < 0:
                    loses.append(r)
            except Exception:
                pass

    sl_count = 0
    for r in loses:
        reason = (r.get("reason_exit","") or "").upper()
        if "SL" in reason: sl_count += 1

    suggestions = {}
    if sl_count >= 2:
        suggestions["entry_rules.entry_band_points"] = "+2"

    out = {"date": day, "symbol": cfg.symbol, "losers": len(loses), "sl_count": sl_count, "suggestions": suggestions}
    (DATA_DIR / "auto_heal.json").write_text(json.dumps(out, indent=2))
    logger.log_status(sheet, {"state":"OK", "message": f"auto_heal suggestions saved ({len(suggestions)} items)"})

    overrides = {}
    if "entry_rules.entry_band_points" in suggestions:
        overrides.setdefault("entry_rules", {})
        overrides["entry_rules"]["entry_band_points"] = None  # human will apply +2 tomorrow
        overrides["_diff"] = {"entry_rules.entry_band_points": "+2"}

    # write file (picked up next boot) AND push to sheet for persistence
    (DATA_DIR / "params_override.json").write_text(json.dumps(overrides, indent=2))
    try:
        write_params_override_to_sheet(sheet, cfg.symbol, overrides)
    except Exception:
        pass
