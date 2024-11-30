from datetime import date, timedelta
import timeit
from typing import Callable

import pyarrow as pa
from deltalake import DeltaTable, WriterProperties, write_deltalake

from ..domain import DuplicationMode
from .table import duplicate_table, unlinked_table_path,create_table
from . import produce


def raw_table(
        start_date: date,
        end_date:date,
        locations: list[str],
        produce_method: Callable[[date,str],pa.Table],
    ) -> DeltaTable:

    print()
    print(f"## CONSTRUCTING RAW TABLE")
    print(f"# - with: {produce_method.__name__}")

    _ = unlinked_table_path("raw")
    raw_table = create_table("raw")

    days = (
        start_date + timedelta(days=i)
        for i in range((end_date - start_date).days)
    )

    def generate():
        for day in days:
            for location in locations:
                write_deltalake(
                    raw_table,
                    produce_method(day, location),
                    mode="append",
                    writer_properties=WriterProperties(compression="ZSTD")
                )

    generation_time = timeit.timeit(generate, number=1)

    print(f"# - Generation time: {round(generation_time,2)}")

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
