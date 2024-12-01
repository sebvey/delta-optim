from delta_optim.constant import TableConf
from .. import utils
from pathlib import Path
import shutil
from deltalake import DeltaTable


def clone(src_table_conf: TableConf, dst_table_conf: Path) -> DeltaTable:
    "Clones a table by copying files. Erases existing destination folder if any"

    if not DeltaTable.is_deltatable(str(src_table_conf.path)):
        raise Exception(f"No delta table at {src_table_conf.path}")

    utils.unlink_path(dst_table_conf.path)
    shutil.copytree(src=src_table_conf.path,dst=dst_table_conf.path)
    return DeltaTable(str(dst_table_conf.path))
