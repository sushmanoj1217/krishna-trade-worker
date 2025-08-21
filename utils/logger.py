import logging
import sys

class _DupGuardFilter(logging.Filter):
    def __init__(self, window=5):
        super().__init__()
        self.window = window
        self.last = []

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        if self.last and msg == self.last[-1]:
            return False
        self.last.append(msg)
        self.last = self.last[-self.window:]
        return True

log = logging.getLogger("ktw")
log.setLevel(logging.INFO)
h = logging.StreamHandler(sys.stdout)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
h.setFormatter(fmt)
h.addFilter(_DupGuardFilter(window=8))
log.addHandler(h)
