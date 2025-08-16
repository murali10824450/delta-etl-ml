from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark(app_name: str = "DeltaETL", extra_conf: dict | None = None):
    builder = (
        SparkSession.builder
        .appName(app_name)
        # Delta extensions & catalog
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, v)

    # 🔑 Attach Delta Lake jars when using the pip package
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark
