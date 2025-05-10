from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FilterFailedTransactions") \
    .getOrCreate()

# Read cleaned, merged data from GCS
df = spark.read.csv("gs://chethana-bucket/merged_cleaned_data.csv", header=True, inferSchema=True)

# Filter failed transactions
failed_df = df.filter(df["Status"] == "FAILED")

# Save the filtered result
failed_df.write.csv("gs://chethana-bucket/output/failed_transactions/", header=True, mode="overwrite")

spark.stop()
