from datetime import date

from xfp import Xlist

from .domain import DuplicationMode
from . import srv
from .srv.query import BenchmarkConf


def main():

    benchmark_conf = BenchmarkConf(
        iter_nbr=100,
        queries= [
            srv.query.duckdb_query,
            srv.query.polars_eager_query,
            srv.query.polars_lazy_query,
        ]
    )

    raw_table, compact_table, zordered_table =\
        map(srv.table.get_table,("raw","compact","zordered"))

    for query in benchmark_conf.queries:
        raw_duration, compact_duration, zordered_duration = (
            Xlist([raw_table,compact_table,zordered_table])
            .map(srv.query.query_stats(benchmark_conf.iter_nbr)(query))
        )

        perc = lambda a,b: round(a/b,2) * 100

        print(f"### FINAL STATS FOR {query.__name__}")
        print(f"# COMPACT/RAW: {perc(compact_duration,raw_duration)} %")
        print(f"# ZORDERED/RAW: {perc(zordered_duration,raw_duration)}%")
