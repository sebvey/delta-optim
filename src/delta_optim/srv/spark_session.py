import pyspark
from delta import configure_spark_with_delta_pip

def get_local_session(): # -> SparkSession:
    builder = (
        pyspark.sql.SparkSession.builder.appName("delta-optim")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.executor.memory", "4G")
        .config("spark.driver.memory", "4G")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )

    # TODO - Bring back a efficient local config

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark
