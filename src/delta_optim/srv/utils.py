from datetime import datetime
import time
import os
from pathlib import Path

from deltalake import DeltaTable


def timed(func):

    def wrapper(*args,**kwargs):

        iterations = 1
        durations = []
        for _ in range(iterations):
            start_time = time.time()
            result = func(*args, **kwargs)
            durations.append((time.time() - start_time))
        return (result, sum(durations)/iterations)

    return wrapper

def get_folder_size(folder: Path) -> float:
    """Returns the size of a folder, in MB"""
    total_size = 0
    for dirpath, _, filenames in os.walk(folder):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            if not os.path.islink(file_path):
                total_size += os.path.getsize(file_path)
    return round(total_size / (1024 * 1024), 2)


def print_stats(table:DeltaTable):
    print(f"# Number of parquet files: {len(table.files())}")
    print(f"# Total size of table: {get_folder_size(Path(table.table_uri))}")
