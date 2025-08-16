from src.utils.spark_session import get_spark
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType, StringType

BRONZE_PATH = "data/bronze/nyc_taxi"
SILVER_PATH = "data/silver/nyc_taxi"

def transform_to_silver(df):
    # Casts
    df2 = (df
        .withColumn("VendorID", F.col("VendorID").cast(IntegerType()))
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("RatecodeID", F.col("RatecodeID").cast(IntegerType()))
        .withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag").cast(StringType()))
        .withColumn("PULocationID", F.col("PULocationID").cast(IntegerType()))
        .withColumn("DOLocationID", F.col("DOLocationID").cast(IntegerType()))
        .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("extra", F.col("extra").cast(DoubleType()))
        .withColumn("mta_tax", F.col("mta_tax").cast(DoubleType()))
        .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
        .withColumn("tolls_amount", F.col("tolls_amount").cast(DoubleType()))
        .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast(DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
        .withColumn("congestion_surcharge", F.col("congestion_surcharge").cast(DoubleType()))
        .withColumn("airport_fee", F.col("airport_fee").cast(DoubleType()))
    )

    # Filters
    df3 = (df2
        .filter(F.col("trip_distance") >= 0)
        .filter(F.col("total_amount") >= 0)
        .filter(F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
    )

    # Features
    duration_min = (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0
    df4 = (df3
        .withColumn("trip_duration_min", F.round(duration_min, 2))
        .withColumn("avg_mph", F.when(duration_min > 0, F.round(F.col("trip_distance")/(duration_min/60.0), 2)))
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    )

    # De-dup for safety
    df5 = df4.dropDuplicates([
        "tpep_pickup_datetime","tpep_dropoff_datetime","PULocationID","DOLocationID","total_amount","passenger_count"
    ])
    return df5

def main():
    spark = get_spark("SilverUpsertFromBronze")

    bronze = spark.read.format("delta").load(BRONZE_PATH)

    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        # Bootstrap full Silver
        silver_new = transform_to_silver(bronze)
        (silver_new.write.format("delta")
            .mode("overwrite")
            .partitionBy("pickup_date")
            .option("overwriteSchema","true")
            .save(SILVER_PATH))
        print("Initialized Silver table.")
        return

    # Silver exists → Incremental window
    silver = spark.read.format("delta").load(SILVER_PATH)
    max_date = silver.agg(F.max("pickup_date")).first()[0]
    if max_date is None:
        candidates = bronze
    else:
        # small backfill buffer (1 day) to catch late updates
        candidates = bronze.withColumn("pickup_date", F.to_date("tpep_pickup_datetime")) \
                           .filter(F.col("pickup_date") >= F.date_sub(F.lit(max_date), 1)) \
                           .drop("pickup_date")

    to_merge = transform_to_silver(candidates)

    tgt = DeltaTable.forPath(spark, SILVER_PATH)
    cond = """
        t.tpep_pickup_datetime = s.tpep_pickup_datetime AND
        t.tpep_dropoff_datetime = s.tpep_dropoff_datetime AND
        t.PULocationID = s.PULocationID AND
        t.DOLocationID = s.DOLocationID AND
        t.total_amount = s.total_amount AND
        t.passenger_count = s.passenger_count
    """
    (tgt.alias("t")
        .merge(to_merge.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print("Silver upsert complete.")

if __name__ == "__main__":
    main()

