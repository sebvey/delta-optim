import pyspark
from delta import configure_spark_with_delta_pip
import pyspark.sql
import pyspark.sql.functions

def get_local_session(): # -> SparkSession:
    builder = (
        pyspark.sql.SparkSession.builder.appName("delta-optim")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        # .config("spark.executor.memory", "4G")
        # .config("spark.driver.memory", "4G")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.retentionDurationCheck.enabled","false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame([
            (1,"hello"),
            (2,"world"),
            (3,"!!"),
        ],("id","value")
    ).agg(pyspark.sql.functions.array_agg("value")).show()

    return spark
