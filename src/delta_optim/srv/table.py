from enum import Enum, auto
from pathlib import Path
import shutil
from deltalake import DeltaTable, write_deltalake

from ..constant import BASE_PATH
from ..domain import DuplicationMode, schema


def unlinked_table_path(table_name: str) -> Path:

    table_path = BASE_PATH / table_name

    if table_path.is_dir():
        shutil.rmtree(table_path)

    if table_path.is_file():
        table_path.unlink()

    return table_path


def get_table(table_name: str) -> DeltaTable:

    table_path = BASE_PATH / table_name
    table = DeltaTable(table_path)

    if DeltaTable.is_deltatable(table.table_uri):
        return table

    raise Exception(f"No delta table under {repr(table)}")


def create_table(table_name: str) -> DeltaTable:

    table_path = BASE_PATH / table_name
    table_path.mkdir(parents=True)


    return DeltaTable.create(
        table_uri=str(table_path),
        schema=schema,
        name=table_name,
        mode="error",
        partition_by=["year_month"],
    )


def duplicate_table(
        src_table: DeltaTable,
        dst_table_name: str,
        mode: DuplicationMode
    ) -> DeltaTable:

    if mode == DuplicationMode.CloneFolder:
        table_path = unlinked_table_path(dst_table_name)
        shutil.copytree(src=src_table.table_uri,dst=table_path)
        return DeltaTable(table_path)

    if mode == DuplicationMode.ReadWriteTable:
        _ = unlinked_table_path(dst_table_name)
        dst_table = create_table(dst_table_name)
        write_deltalake(dst_table,src_table.to_pyarrow_dataset(), mode="append")
        return dst_table
