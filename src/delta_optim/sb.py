from datetime import date

import delta as dlio
import pyspark.sql.functions as spkf
from delta_optim.constant import RAW_TABLE_CONF
from delta_optim.srv.spark_session import get_local_session

# https://towardsdatascience.com/hands-on-introduction-to-delta-lake-with-py-spark-b39460a4b1ae


spark = get_local_session()

schema = ("int","str","date")
df = spark.createDataFrame([
    (1,"hello",date(2024,1,1)),
    (2,"de",date(2024,1,2)),
    (3,"lu",date(2024,1,3)),
],schema)


delta_table = dlio.DeltaTable.forPath(spark, str(RAW_TABLE_CONF.path))
# delta_table.history().show()

# df = spark.read.format("delta").load("tmp/optim/raw").limit(100).show()
df = (
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
).show()
