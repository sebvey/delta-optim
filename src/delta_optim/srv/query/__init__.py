from ._utils import BenchmarkConf,query_stats
from ._duckdb import duckdb_query
from ._polars import polars_lazy_query, polars_eager_query
from ._spark import spark_query

__all__ = [
    BenchmarkConf,
    query_stats,
    spark_query,
    duckdb_query,
    polars_lazy_query,
    polars_eager_query,
]
