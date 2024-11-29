from datetime import date, datetime, timedelta

import polars as pl
from pyarrow import compute as pc


def records(day: date, location: str) -> pl.DataFrame:
    """Produces a DataFrame with 86400 records for given day and location:
    - one 'query' per day -> ~30 files per month.
    - data partitioned by year_month
    """

    start_dt = datetime(day.year,day.month,day.day)
    end_dt = start_dt + timedelta(days=1, seconds=-1)

    dt_series = pl.datetime_range(start_dt,end_dt,timedelta(seconds=1),eager=True)
    records_nbr = 24*60*60

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
    )
