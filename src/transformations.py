"""PySpark transformations for MLApp."""
from pyspark.sql import DataFrame
 
 
def clean_data(df: DataFrame) -> DataFrame:
    """Clean transaction data."""
    # Drop rows where customer_id is null (our requirement)
    df = df.dropna(subset=['customer_id'])
    # Drop rows with any nulls in critical columns (Priya's improvement)
    df = df.na.drop(subset=['amount', 'transaction_date'])
    # Keep only positive amounts
    df = df.filter(df['amount'] > 0)
    return df
 
 
def calculate_totals(df: DataFrame) -> DataFrame:
    """Calculate total spend per customer."""
    from pyspark.sql import functions as F
    return df.groupBy("customer_id").agg(
        F.sum("amount").alias("total_spend")
    )
