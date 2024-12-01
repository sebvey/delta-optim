from dataclasses import dataclass
from typing import Any, Callable

from deltalake import DeltaTable
import duckdb
import polars as pl
import pyarrow as pa
import timeit
import xfp


@dataclass
class BenchmarkConf:
        iter_nbr: int
        queries: list[Callable[[DeltaTable], Any]]


def _polars_base_query[T: pl.DataFrame | pl.LazyFrame](df: T) -> T:

    return (
        df.filter(
            pl.col("location") == "Lyon",
            pl.col("value") > 0.2,
        )
        .group_by("location","year_month")
        .agg(
            pl.col("value").mean().alias("mean_value"),
            pl.len(),
        )
        .sort("location","year_month")
    )


def polars_eager_query(table: DeltaTable) -> pl.DataFrame:
    return pl.read_delta(table).pipe(_polars_base_query)


def polars_lazy_query(table: DeltaTable) -> pl.DataFrame:
    return pl.scan_delta(table).pipe(_polars_base_query).collect()



def duckdb_query(table: DeltaTable) -> pa.Table:

    return duckdb.sql(
        f"""
        SELECT
            location,
            year_month,
            mean(value) as 'mean_value',
            count(*) as 'row_cnt',
        FROM delta_scan('{table.table_uri}')
        WHERE value > 0.2 and location = 'Lyon'
        GROUP BY location, year_month
        ORDER BY location, year_month
        """
    ).pl()

@xfp.curry
def query_stats(iter_nbr: int, query: Any,table: DeltaTable) -> float:

    duration = timeit.timeit(
        lambda: query(table),
        number=iter_nbr)
    result = query(table)

    print()
    print(f"# QUERY STATS:")
    print(f"# - Query        : {query.__name__}")
    print(f"# - Iterations   : {iter_nbr}")
    print(f"# - Table        : {table.metadata().name}")
    print(f"# => Duration (s): {round(duration,4)}")
    print(result)

    return duration
