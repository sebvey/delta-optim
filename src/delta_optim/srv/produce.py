from datetime import date, datetime, timedelta

import duckdb
import polars as pl
import pyarrow as pa
from pyarrow import compute as pc


def records_with_polars(day: date, location: str) -> pa.Table:
    """Produces a DataFrame with 86400 records for given day and location:
    - one 'query' per day -> ~30 files per month.
    - data partitioned by year_month
    - WITH POLARS + PYARROW
    """

    start_dt = datetime(day.year,day.month,day.day)
    end_dt = start_dt + timedelta(days=1, seconds=-1)

    dt_series = pl.datetime_range(start_dt,end_dt,timedelta(seconds=1),eager=True)
    records_nbr = dt_series.len()

    return (
        pl.DataFrame([
            dt_series.alias("datetime"),
            pl.Series(pc.random(records_nbr)).alias("value"),
        ])
        .with_columns(
            pl.col("datetime").dt.to_string("%Y%m").alias("year_month"),
            pl.lit(location).alias("location"),
        )
        .select(
            pl.col("year_month"),
            pl.col("datetime"),
            pl.col("location"),
            pl.col("value"),
        )
    ).to_arrow()

def records_with_duckdb(day: date, location: str) -> pa.Table:
    """Produces a DataFrame with 86400 records for given day and location:
    - one 'query' per day -> ~30 files per month.
    - data partitioned by year_month
    - WITH POLARS + PYARROW
    """

    query = f"""
    with dts as (
        SELECT
            unnest(
                generate_series(
                    date '{day}',
                    date '{day}' + interval 1 day - interval 1 second,
                    interval 1 second
                )
            ) AS datetime
    )

    select
        strftime(datetime,'%Y%m') as 'year_month',
        datetime,
        '{location}' as 'location',
        random() as 'value'
    from dts
    """

    with duckdb.connect() as con:
        return con.sql(query).to_arrow_table()
