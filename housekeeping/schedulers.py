# path: housekeeping/schedulers.py
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR
from tzlocal import get_localzone

def start_schedulers(app, shift_mode: str):
    """
    app: callbacks + optional app['on_job_error'](job_id, exc)
    """
    tz = get_localzone()
    sch = BackgroundScheduler(timezone=tz)

    if app.get("oc_refresh"):
        sch.add_job(app["oc_refresh"], "interval", seconds=int(app.get("oc_secs", 10)), id="oc_refresh")

    if shift_mode == "DAY" and app.get("paper_tick"):
        sch.add_job(app["paper_tick"], "interval", seconds=3, id="paper_tick")

    if app.get("heartbeat"):
        sch.add_job(app["heartbeat"], "interval", seconds=60, id="heartbeat")

    if app.get("pre_eod_flatten"):
        sch.add_job(app["pre_eod_flatten"], CronTrigger(hour=15, minute=28), id="pre_eod_flatten")
    if app.get("eod"):
        sch.add_job(app["eod"], CronTrigger(hour=15, minute=31), id="eod")

    if shift_mode == "DAY" and app.get("daily_summary"):
        sch.add_job(app["daily_summary"], CronTrigger(hour=15, minute=35), id="daily_summary")

    if shift_mode == "DAY" and app.get("tele_ops"):
        sch.add_job(app["tele_ops"], "interval", seconds=20, id="tele_ops")

    if shift_mode == "NIGHT" and app.get("nightly"):
        sch.add_job(app["nightly"], CronTrigger(hour=20, minute=0), id="nightly")

    # Error listener -> send to app callback (Telegram)
    if app.get("on_job_error"):
        def _on_err(event):
            try:
                app["on_job_error"](event.job_id, getattr(event, "exception", None))
            except Exception:
                pass
        sch.add_listener(_on_err, EVENT_JOB_ERROR)

    sch.start()
