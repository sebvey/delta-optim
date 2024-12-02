import polars as pl
import pyarrow as pa
import pyspark.sql.functions as spkf
import pyspark.sql.types as spkt
import pyspark.sql as sp
import toolz

from delta_optim.constant import TableConf

from .. import table

@toolz.curry
def spark_query(spark,table_conf: TableConf) -> sp.DataFrame:

    delta_table = table.io.get(spark,table_conf)
    spark_df = (
        delta_table.toDF()
        .filter(spkf.col("value") > 0.2)
        .filter(spkf.col("location") == "Lyon")
        .groupBy("location","year_month")
        .agg(
            spkf.mean("value").alias("mean_value"),
            spkf.count("*").alias("row_cnt"),
        )
        .select(
            "location",
            "year_month",
            "mean_value",
            "row_cnt",
        )
        .sort("location","year_month")
    )
    # for printing purpose
    return pl.from_arrow(pa.Table.from_batches(spark_df._collect_as_arrow()))
