import os

def run(seconds: int):
    os.environ["TICK_SECS"] = str(seconds)
    return f"tick set to {seconds}s (session)"
