from datetime import date

from .domain import DuplicationMode
from . import srv


def main():

    duplication_mode = DuplicationMode.ReadWriteTable
    produce_methods   = [
        srv.produce.records_with_polars,
        srv.produce.records_with_duckdb,
    ]

    start_date=date(2024,1,1)
    end_date=date(2024,3,1)
    locations = ("Lyon","Paris","Marseille")

    for produce_method in produce_methods:
        raw_table = srv.construct.raw_table(
            start_date=start_date,
            end_date=end_date,
            locations=locations,
            produce_method=produce_method
        )
        srv.utils.print_stats(raw_table)

    compact_table = srv.construct.compact_table(raw_table, duplication_mode)
    srv.utils.print_stats(compact_table)

    zordered_table = srv.construct.zordered_table(raw_table, duplication_mode)
    srv.utils.print_stats(zordered_table)
