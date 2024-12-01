from pathlib import Path
import shutil

import delta as dlio

from ...constant import TableConf


def get(spark, table_conf: TableConf) -> dlio.DeltaTable:

    if dlio.DeltaTable.isDeltaTable(spark,str(table_conf.path)):
        return dlio.DeltaTable.forPath(spark,str(table_conf.path))

    raise Exception(f"No delta table under {repr(table_conf.path)}")


def create(table_conf: TableConf) -> dlio.DeltaTable: ...

