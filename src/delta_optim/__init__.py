from datetime import date

from xfp import Xlist

from .domain import DuplicationMode
from . import srv
from .srv.query import BenchmarkConf


def main():

    skip_creation = False
    skip_compare = False

    duplication_mode = DuplicationMode.ReadWriteTable
    start_date=date(2024,1,1)
    end_date=date(2024,7,1)
    locations = ("Lyon","Paris","Marseille")

    benchmark_conf = BenchmarkConf(
        iter_nbr=100,
        queries= [
            srv.query.duckdb_query,
            srv.query.polars_eager_query,
            srv.query.polars_lazy_query,
        ]
    )

    if skip_creation:
        raw_table, compact_table, zordered_table =\
            map(srv.table.get_table,("raw","compact","zordered"))

    else:
        raw_table = srv.construct.raw_table(
            start_date=start_date,
            end_date=end_date,
            locations=locations,
        )
        srv.utils.print_stats(raw_table)

        compact_table = srv.construct.compact_table(raw_table, duplication_mode)
        srv.utils.print_stats(compact_table)

        zordered_table = srv.construct.zordered_table(raw_table, duplication_mode)
        srv.utils.print_stats(zordered_table)

    if not skip_compare:
        for query in benchmark_conf.queries:
            raw_duration, compact_duration, zordered_duration = (
                Xlist([raw_table,compact_table,zordered_table])
                .map(srv.query.query_stats(benchmark_conf.iter_nbr)(query))
            )

            perc = lambda a,b: round(a/b,2) * 100

            print(f"### FINAL STATS FOR {query.__name__}")
            print(f"# COMPACT/RAW: {perc(compact_duration,raw_duration)} %")
            print(f"# ZORDERED/RAW: {perc(zordered_duration,raw_duration)}%")
