from xfp import Xlist

from . import srv
from .constant import (
    COMPACT_DELTARS_CONF,
    COMPACT_DELTARS_RW_CONF,
    RAW_TABLE_CONF,
)
from .srv.query import BenchmarkConf


def main():

    print("@@@ Spark Local Init ...")
    spark = srv.spark_session.get_local_session()
    print("@@@ Spark Local DONE")

    benchmark_conf = BenchmarkConf(
        iter_nbr=5,
        queries= [
            srv.query.spark_query(spark),
            srv.query.duckdb_query,
            srv.query.polars_eager_query,
            # srv.query.polars_lazy_query, #! polars bug ?
        ]
    )

    tables_conf = [
            RAW_TABLE_CONF,
            #COMPACT_DELTARS_CONF,
            #COMPACT_DELTARS_RW_CONF,
        ]

    for query in benchmark_conf.queries:
        for table_conf in tables_conf:
            srv.query.query_stats(benchmark_conf.iter_nbr)(query)(table_conf)
