from src.utils.spark_session import get_spark

def main():
    spark = get_spark("IngestNYCTaxi")
    df = spark.read.parquet("data/raw/yellow_tripdata_2019-01.parquet")
    # write to Delta (bronze)
    (df.write
       .format("delta")
       .mode("overwrite")
       .save("data/bronze/nyc_taxi"))
    print("Bronze Delta table written to data/bronze/nyc_taxi")

if __name__ == "__main__":
    main()