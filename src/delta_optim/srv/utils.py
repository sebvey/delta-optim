import shutil
import os
from pathlib import Path

from deltalake import DeltaTable
import polars as pl


def unlink_path(path: Path):

    if path.is_dir():
        shutil.rmtree(path)

    if path.is_file():
        path.unlink()


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
    print(f"# Total size of table: {get_folder_size(Path(table.table_uri))}")
    print(f"# Number of listed files: {len(table.files())}")

    cnt = len(pl.DataFrame(table.get_add_actions(flatten=True)))
    print(f"# Active files number: {cnt}")
