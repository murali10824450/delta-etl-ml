from src.utils.spark_session import get_spark

LANDING_GLOB = 'data/raw/landing/*.parquet'
BRONZE_PATH = 'data/bronze/nyc_taxi'


def main():
    spark = get_spark("BronzeAppendFromLanding")
    df = spark.read.parquet(LANDING_GLOB)
    (df.write.format("delta").mode("append").save(BRONZE_PATH))
    print(f"Appended landing files into Bronze at {BRONZE_PATH}")

if __name__ == "__main__":
    main()