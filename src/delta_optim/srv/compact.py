from deltalake import DeltaTable

from ..constant import (
    COMPACT_DELTAIO_CONF,
    COMPACT_DELTARS_CONF,
    RAW_TABLE_CONF,
)
from . import table

def with_deltars() -> DeltaTable:

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


def with_deltaio(spark) -> DeltaTable:

    print()
    print(f"# COMPACTING TO TABLE={COMPACT_DELTAIO_CONF.name}")

    table.clone(RAW_TABLE_CONF,COMPACT_DELTAIO_CONF)

    new_table_io = table.io.get(spark,COMPACT_DELTAIO_CONF)
    new_table_io.optimize().executeCompaction()
    new_table_io.vacuum(0)

    return table.rs.get(COMPACT_DELTAIO_CONF)
