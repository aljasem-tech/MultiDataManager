import os

MAX_WORKERS = min(32, (os.cpu_count() or 1) + 4)
