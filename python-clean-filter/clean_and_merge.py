from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CleanAndMergeBankTxnData") \
    .getOrCreate()

# GCS bucket path where all branch CSVs are stored
gcs_input_path = "gs://chethana-bucket/injection/*.csv"
gcs_output_path = "gs://chethana-bucket/merged_cleaned_data.csv"

# Read all CSV files from GCS
df = spark.read.option("header", True).csv(gcs_input_path)

# Display schema for debugging
print("Schema of raw data:")
df.printSchema()

# Clean data:
# 1. Drop rows with any nulls
# 2. Drop rows where required fields are blank strings
required_fields = ["TransactionID", "BranchName", "BranchCity", "Date", "Status", "Amount"]

# Trim whitespace and remove blank strings in required fields
df_trimmed = df.select([trim(col(c)).alias(c) for c in df.columns])

# Filter out rows with blank strings in critical fields
for field in required_fields:
    df_trimmed = df_trimmed.filter((col(field).isNotNull()) & (col(field) != ""))

# Optional: Remove duplicates
df_cleaned = df_trimmed.dropDuplicates()

# Save cleaned data as CSV to GCS
df_cleaned.write.mode("overwrite").csv(gcs_output_path, header=True)

print(f"Cleaned and merged data written to {gcs_output_path}")
spark.stop()
