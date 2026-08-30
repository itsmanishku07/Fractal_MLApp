"""Spark session utilities for MLApp."""
from pyspark.sql import SparkSession
 
 
def get_spark_session(app_name="MLApp"):
    """Create and return a Spark session."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.default.parallelism", "400") \
        .getOrCreate()
    return spark
