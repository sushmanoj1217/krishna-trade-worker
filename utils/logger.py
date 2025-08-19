import logging, os, sys, time, hashlib

_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

class DuplicateGuard(logging.Filter):
    def __init__(self, window=5):
        super().__init__()
        self.window = window
        self._seen = {}

    def filter(self, record: logging.LogRecord) -> bool:
        msg = f"{record.levelname}:{record.getMessage()}"
        h = hashlib.md5(msg.encode()).hexdigest()
        now = time.time()
        last = self._seen.get(h, 0)
        self._seen[h] = now
        return (now - last) > self.window

def get_logger(name="krishna"):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(_LEVEL)
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    h.setFormatter(fmt)
    h.addFilter(DuplicateGuard())
    logger.addHandler(h)
    return logger

log = get_logger()
