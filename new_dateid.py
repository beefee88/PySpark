from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, date_format

# Initialize Spark
spark = SparkSession.builder.appName("Add Date ID to usersales").getOrCreate()

# Paths
input_path = r"usersales.csv"
output_path = r"usersales_with_dateid.csv"

# Read CSV
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

print("Sample before:")
df.select("datetime").show(5, truncate=False)

# Convert "yyyy-MM-dd HH:mm:ss" → date → yyyymmdd int
df_with_dateid = df.withColumn(
    "date_id",
    date_format(
        to_date(to_timestamp(col("datetime"))),  # Auto-parses standard format
        "yyyyMMdd"
    ).cast("int")
)

print("Sample with date_id:")
df_with_dateid.select("datetime", "date_id").show(10, truncate=False)

# Write updated CSV
df_with_dateid.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print(f"Done! Added date_id to {df_with_dateid.count()} rows")
spark.stop()
