from deltalake import DeltaTable, write_deltalake

from ..constant import (
    COMPACT_DELTAIO_CONF,
    COMPACT_DELTARS_CONF,
    COMPACT_RW_CONF,
    RAW_TABLE_CONF,
    TableConf,
)
from . import table
from . import utils

#! deltars v0.22.2 - ? bug on active files ?
def with_deltars(raw_table: DeltaTable) -> DeltaTable:

    print()
    print(f"# COMPACTING TO TABLE={COMPACT_DELTARS_CONF.name}")

    new_table = table.clone(RAW_TABLE_CONF, COMPACT_DELTARS_CONF)

    # new_table.create_checkpoint() # no impact
    new_table.optimize.compact()
    new_table.vacuum(
        retention_hours=0,
        enforce_retention_duration=False,
        dry_run=False
    )

    return new_table


def with_read_write(raw_table: DeltaTable) -> DeltaTable:

    print()
    print(f"# COMPACTING TO TABLE={COMPACT_RW_CONF.name}")

    new_table = table.rs.create(COMPACT_RW_CONF)
    write_deltalake(new_table,raw_table.to_pyarrow_table(), mode="append")

    return new_table


def with_deltaio(spark,src_table_conf: TableConf) -> DeltaTable:

    print()
    print(f"# COMPACTING TO TABLE={COMPACT_DELTAIO_CONF.name}")

    table.clone(src_table_conf,COMPACT_DELTAIO_CONF)

    new_table_io = table.io.get(spark,COMPACT_DELTAIO_CONF)
    new_table_io.optimize().executeCompaction()
    new_table_io.vacuum(0)

    return table.rs.get(COMPACT_DELTAIO_CONF)
