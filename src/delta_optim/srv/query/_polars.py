import polars as pl

from ...constant import TableConf
from .. import table


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


def polars_eager_query(table_conf: TableConf) -> pl.DataFrame:
    delta_table = table.rs.get(table_conf)
    return pl.read_delta(delta_table).pipe(_polars_base_query)


def polars_lazy_query(table_conf: TableConf) -> pl.DataFrame:
    delta_table = table.rs.get(table_conf)
    return pl.scan_delta(delta_table).pipe(_polars_base_query).collect()
