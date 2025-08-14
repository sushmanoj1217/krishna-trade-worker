# path: housekeeping/schedulers.py
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from tzlocal import get_localzone

def start_schedulers(app, shift_mode: str):
    """
    app = {
      "oc_secs": int,
      "oc_refresh": fn,
      "paper_tick": fn,
      "pre_eod_flatten": fn,
      "eod": fn,
      "nightly": fn,
      "heartbeat": fn,
      # optional:
      "daily_summary": fn,
      "tele_ops": fn,
    }
    """
    tz = get_localzone()
    sch = BackgroundScheduler(timezone=tz)
    # OC refresh
    sch.add_job(app["oc_refresh"], "interval", seconds=int(app.get("oc_secs", 10)), id="oc_refresh")
    # paper tick (day only)
    if shift_mode == "DAY":
        sch.add_job(app["paper_tick"], "interval", seconds=3, id="paper_tick")
    # heartbeat
    sch.add_job(app["heartbeat"], "interval", seconds=60, id="heartbeat")
    # pre-EOD flatten & EOD (NSE regular close 15:30 IST)
    sch.add_job(app["pre_eod_flatten"], CronTrigger(hour=15, minute=28), id="pre_eod_flatten")
    sch.add_job(app["eod"], CronTrigger(hour=15, minute=31), id="eod")

    # Daily summary 15:35 IST (Day shift)
    if shift_mode == "DAY" and app.get("daily_summary"):
        sch.add_job(app["daily_summary"], CronTrigger(hour=15, minute=35), id="daily_summary")

    # Telegram ops polling (Day shift)
    if shift_mode == "DAY" and app.get("tele_ops"):
        sch.add_job(app["tele_ops"], "interval", seconds=20, id="tele_ops")

    # Nightly job (Night shift)
    if shift_mode == "NIGHT":
        sch.add_job(app["nightly"], CronTrigger(hour=20, minute=0), id="nightly")

    sch.start()
