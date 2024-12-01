from datetime import date

from . import srv


def main():

    records_producers = [
        srv.records_producer.from_duckdb,
        srv.records_producer.from_polars_and_pyarrow,
        srv.records_producer.from_polars_pure,
        srv.records_producer.from_polars_and_numpy,
    ]

    start_date=date(2024,1,1)
    end_date=date(2024,3,1)
    locations = ("Lyon","Paris","Marseille")

    for producer in records_producers:
        raw_table = srv.create.raw_table(
            start_date=start_date,
            end_date=end_date,
            locations=locations,
            produce_method=producer
        )
        srv.utils.print_stats(raw_table)
