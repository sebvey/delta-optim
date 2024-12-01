from datetime import date

from xfp import Xlist

from .constant import COMPACT_DELTARS_CONF, COMPACT_DELTARS_RW_CONF, RAW_TABLE_CONF

from . import srv
from .srv.query import BenchmarkConf


def main():

    benchmark_conf = BenchmarkConf(
        iter_nbr=25,
        queries= [
            srv.query.duckdb_query,
            srv.query.polars_eager_query,
            srv.query.polars_lazy_query,
        ]
    )

    tables = (
        Xlist([
            #RAW_TABLE_CONF,
            #COMPACT_DELTARS_CONF,
            COMPACT_DELTARS_RW_CONF,
        ])
        .map(srv.table.get)
    )

    for query in benchmark_conf.queries:
        for table in tables:
            srv.query.query_stats(benchmark_conf.iter_nbr)(query)(table)

        # old code
        # perc = lambda a,b: round(a/b,2) * 100

        # print(f"### FINAL STATS FOR {query.__name__}")
        # print(f"# COMPACT/RAW: {perc(compact_duration,raw_duration)} %")
        # print(f"# ZORDERED/RAW: {perc(zordered_duration,raw_duration)}%")
