from . import srv
from .constant import (
    RAW_TABLE_CONF,
    COMPACT_DELTARS_CONF,
    COMPACT_DELTAIO_CONF,
)
from .srv.query import BenchmarkConf


def main():

    print("@@@ Spark Local Init ...")
    spark = srv.spark_session.get_local_session()
    print("@@@ Spark Local Init DONE")


    benchmark_conf = BenchmarkConf(
        iter_nbr=1,
        queries= [
            srv.query.spark_query(spark),
            srv.query.duckdb_query,
            srv.query.polars_eager_query,
            srv.query.polars_lazy_query,
        ]
    )

    queried_tables_conf = [
            # RAW_TABLE_CONF,
            COMPACT_DELTAIO_CONF,
            # COMPACT_DELTARS_CONF, #! polars fails on it
        ]

    for table_conf in queried_tables_conf:
        for query in benchmark_conf.queries:
            srv.query.query_stats(benchmark_conf.iter_nbr)(query)(table_conf)
