from datetime import date, timedelta
import timeit
from typing import Callable

import pyarrow as pa
from deltalake import DeltaTable, WriterProperties, write_deltalake

from ..constant import RAW_TABLE_CONF
from .table import create
from . import utils


def raw_table(
        start_date: date,
        end_date:date,
        locations: list[str],
        produce_method: Callable[[date,str],pa.Table],
    ) -> DeltaTable:
    " Creates raw table with provided parameters. Erases existing table if any."

    print()
    print(f"## CONSTRUCTING RAW TABLE")
    print(f"# - with: {produce_method.__name__}")


    utils.unlink_path(RAW_TABLE_CONF.path)
    raw_table = create(RAW_TABLE_CONF)

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
