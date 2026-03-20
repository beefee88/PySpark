from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("Count CSV Records") \
    .getOrCreate()

# Windows-safe input path (edit this)
input_path = r"usersales.csv"  # Your CSV path

# Read CSV and count
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

# Count total records (excluding header)
total_records = df.count()

print(f"Total records in {input_path}: {total_records:,}")
print("Schema:")
df.printSchema()
print("\nSample data:")
df.show(5, truncate=False)

spark.stop()
