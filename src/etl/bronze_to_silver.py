from src.utils.spark_session import get_spark
from pyspark.sql.functions import (
    col, to_date, unix_timestamp, when, round as sround
)
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, DoubleType, TimestampType, StringType
)

BRONZE_PATH = "data/bronze/nyc_taxi"
SILVER_PATH = "data/silver/nyc_taxi"

def main():
    spark = get_spark("BronzeToSilver")

    # 1) Read bronze
    df = spark.read.format("delta").load(BRONZE_PATH)

    # 2) Enforce schema (casts)
    df2 = (
        df
        .withColumn("VendorID", col("VendorID").cast(IntegerType()))
        .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", col("trip_distance").cast(DoubleType()))
        .withColumn("RatecodeID", col("RatecodeID").cast(IntegerType()))
        .withColumn("store_and_fwd_flag", col("store_and_fwd_flag").cast(StringType()))
        .withColumn("PULocationID", col("PULocationID").cast(IntegerType()))
        .withColumn("DOLocationID", col("DOLocationID").cast(IntegerType()))
        .withColumn("payment_type", col("payment_type").cast(IntegerType()))
        .withColumn("fare_amount", col("fare_amount").cast(DoubleType()))
        .withColumn("extra", col("extra").cast(DoubleType()))
        .withColumn("mta_tax", col("mta_tax").cast(DoubleType()))
        .withColumn("tip_amount", col("tip_amount").cast(DoubleType()))
        .withColumn("tolls_amount", col("tolls_amount").cast(DoubleType()))
        .withColumn("improvement_surcharge", col("improvement_surcharge").cast(DoubleType()))
        .withColumn("total_amount", col("total_amount").cast(DoubleType()))
        .withColumn("congestion_surcharge", col("congestion_surcharge").cast(DoubleType()))
        .withColumn("airport_fee", col("airport_fee").cast(DoubleType()))
    )

    # 3) Basic data quality filters
    df3 = (
        df2
        .filter(col("trip_distance") >= 0)
        .filter(col("total_amount") >= 0)
        .filter(col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
    )

    # 4) Derived features: duration (min), avg speed (mph)
    duration_min = (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60.0
    df4 = (
        df3
        .withColumn("trip_duration_min", sround(duration_min, 2))
        .withColumn(
            "avg_mph",
            when(duration_min > 0, sround(col("trip_distance") / (duration_min / 60.0), 2)).otherwise(F.lit(None))
        )
        .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
    )

    # 5) Deduplicate conservative subset of columns
    df5 = df4.dropDuplicates([
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "PULocationID", "DOLocationID",
        "total_amount", "passenger_count"
    ])

    # 6) Write Silver (partitioned)
    (df5.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("pickup_date")
        .option("overwriteSchema", "true")
        .save(SILVER_PATH)
    )
    print(f"Silver Delta table written to {SILVER_PATH}")

if __name__ == "__main__":
    main()