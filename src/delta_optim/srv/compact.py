from deltalake import DeltaTable, write_deltalake

from ..constant import COMPACT_DELTARS_CONF, COMPACT_DELTARS_RW_CONF
from . import table

#! deltars v0.22.2 - bug - list files + vaccum -> remove files are still there
def with_deltars_compact(raw_table: DeltaTable) -> DeltaTable:

    print()
    print(f"# COMPACTING TO TABLE={COMPACT_DELTARS_CONF.name}")

    new_table = table.clone(raw_table, COMPACT_DELTARS_CONF.path)

    new_table.optimize.compact()
    new_table.vacuum(
        retention_hours=0,
        enforce_retention_duration=False,
        dry_run=False
    )

    return new_table


def with_deltars_read_write(raw_table: DeltaTable) -> DeltaTable:

    print()
    print(f"# COMPACTING TO TABLE={COMPACT_DELTARS_RW_CONF.name}")

    new_table = table.create(COMPACT_DELTARS_RW_CONF)
    write_deltalake(new_table,raw_table.to_pyarrow_table(), mode="append")

    return new_table
