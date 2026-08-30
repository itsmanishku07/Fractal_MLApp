"""PySpark transformations for MLApp."""
from pyspark.sql import DataFrame
 
 
def clean_data(df: DataFrame) -> DataFrame:
    """Clean transaction data."""
    df = df.na.drop(how='any')
    df = df.filter(df['amount'].isNotNull())
    return df
 
 
def calculate_totals(df: DataFrame) -> DataFrame:
    """Calculate total spend per customer."""
    from pyspark.sql import functions as F
    return df.groupBy("customer_id").agg(
        F.sum("amount").alias("total_spend")
    )


def segment_customers(df: DataFrame) -> DataFrame:
    """RFM-based customer segmentation for MLAPP-1234."""
    from pyspark.sql import functions as F
    return df.withColumn(
        "segment",
        F.when(F.col("total_spend").isNull(), "Unknown")
         .when(F.col("total_spend") > 10000, "Premium")
         .when(F.col("total_spend") > 5000, "Gold")
         .when(F.col("total_spend") > 1000, "Silver")
         .otherwise("Bronze")
    )



# MLAPP-2000 ETL Refactoring Pipeline helper
def pipeline_validate(df):
    return df.isNotNull()

