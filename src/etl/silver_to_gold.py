from src.utils.spark_session import get_spark
from pyspark.sql.functions import col, to_date, count as scount, avg, sum as ssum, round as sround

SILVER_PATH = "data/silver/nyc_taxi"
GOLD_PATH = "data/gold/nyc_taxi_kpis_by_pu_date"

def main():
    spark = get_spark("SilverToGold")

    df = spark.read.format("delta").load(SILVER_PATH)

    agg = (
        df.groupBy("pickup_date", "PULocationID")
          .agg(
              scount("*").alias("trips"),
              sround(avg(col("trip_distance")), 2).alias("avg_trip_distance"),
              sround(avg(col("total_amount")), 2).alias("avg_total_amount"),
              sround(ssum(col("total_amount")), 2).alias("revenue_total"),
              sround(avg(col("avg_mph")), 2).alias("avg_mph")
          )
    )

    (agg.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("pickup_date")
        .option("overwriteSchema", "true")
        .save(GOLD_PATH)
    )
    print(f"Gold Delta table written to {GOLD_PATH}")

if __name__ == "__main__":
    main()