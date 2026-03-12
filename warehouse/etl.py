from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Silver Layer: Spark Transformations
BRONZE_PATH = "warehouse/data/bronze"
SILVER_PATH = "warehouse/data/silver"

def run_etl():
    spark = SparkSession.builder \
        .appName("F1TelemetryETL") \
        .getOrCreate()
    
    print("Starting Spark ETL cleanup...")
    
    # Example: Load raw telemetry, interpolate coordinates, save as Parquet
    # df = spark.read.json(BRONZE_PATH + "/*.json")
    
    print("ETL transformation complete.")
    spark.stop()

if __name__ == "__main__":
    run_etl()
