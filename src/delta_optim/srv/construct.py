from datetime import date, timedelta

from deltalake import DeltaTable, WriterProperties, write_deltalake

from .table import DuplicationMode, duplicate_table, unlinked_table_path,create_table
from . import produce


def raw_table(
        start_date: date,
        end_date:date,
        locations: list[str]
    ) -> DeltaTable:

    print()
    print(f"# CONSTRUCTING RAW TABLE ...")

    _ = unlinked_table_path("raw")
    raw_table = create_table("raw")

    days = (
        start_date + timedelta(days=i)
        for i in range((end_date - start_date).days)
    )

    for day in days:
        for location in locations:
            write_deltalake(
                raw_table,
                produce.records(day, location).to_arrow(),
                mode="append",
                writer_properties=WriterProperties(compression="ZSTD")
            )

    return raw_table


def compact_table(raw_table: DeltaTable, mode: DuplicationMode) -> DeltaTable:

    print()
    print(f"# CONSTRUCTING COMPACT TABLE ...")

    compact_table = duplicate_table(raw_table,"compact", mode)
    compact_table.optimize.compact()
    compact_table.vacuum(
        retention_hours=0,
        enforce_retention_duration=False,
        dry_run=False
    )

    return compact_table


def zordered_table(raw_table: DeltaTable, mode: DuplicationMode):

    print()
    print(f"# CONSTRUCTING ZORDERED TABLE ... ")

    zordered_table = duplicate_table(raw_table,"zordered", mode)
    zordered_table.optimize.z_order(columns=["location","value"])
    zordered_table.vacuum(
        retention_hours=0,
        enforce_retention_duration=False,
        dry_run=False
    )

    return zordered_table
