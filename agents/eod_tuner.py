from utils.logger import log
from integrations import sheets as sh

def run_nightly():
    try:
        sh.append_row("Performance", ["night", "ok"])
        sh.append_row("Params_Override", ['{"note":"auto-tuner stub"}'])
        log.info("EOD tuner wrote overrides stub")
    except Exception as e:
        log.error(f"EOD tuner failed: {e}")
