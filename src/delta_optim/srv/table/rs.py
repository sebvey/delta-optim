from pathlib import Path
import shutil
from deltalake import DeltaTable

from ...constant import TableConf
from ...domain import schema
from .. import utils


def get(table_conf: TableConf) -> DeltaTable:

    if DeltaTable.is_deltatable(str(table_conf.path)):
        return DeltaTable(table_conf.path)

    raise Exception(f"No delta table under {repr(table_conf.path)}")


def create(table_conf: TableConf) -> DeltaTable:
    "Creates an empty delta table. Removes existing one if any."

    utils.unlink_path(table_conf.path)
    table_conf.path.mkdir(parents=True)

    return DeltaTable.create(
        table_uri=str(table_conf.path),
        schema=schema,
        name=table_conf.name,
        mode="error",
        partition_by=["year_month"],
    )
