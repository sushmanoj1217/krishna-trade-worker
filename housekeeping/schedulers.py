from tzlocal import get_localzone
from apscheduler.schedulers.background import BackgroundScheduler

def start_schedulers(app, shift_mode: str):
    sch = BackgroundScheduler(timezone=get_localzone())
    sch.add_job(app['heartbeat'], 'interval', seconds=60)
    if shift_mode.upper()=='DAY':
        sch.add_job(app['oc_refresh'], 'interval', seconds=app['oc_secs'])
        sch.add_job(app['paper_tick'], 'interval', seconds=5)
        sch.add_job(app['pre_eod_flatten'], 'cron', hour=15, minute=29, second=30)
        sch.add_job(app['eod'], 'cron', hour=15, minute=31)
    else:
        sch.add_job(app['oc_refresh'], 'interval', seconds=app['oc_secs'])
        sch.add_job(app['nightly'], 'cron', hour='18-23,0-8/1')
        sch.add_job(app['eod'], 'cron', hour=15, minute=31)
    sch.start()
    return sch
