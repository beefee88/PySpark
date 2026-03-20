from pyspark.sql import SparkSession
import pyspark.sql.functions
from pyspark.sql.types import TimestampType

# Initialize Spark
spark = SparkSession.builder \
    .appName("Generate UserSales Table - FIXED") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Paths
users_path = r"userdata.parquet"
products_path = r"products-1000.csv"
pcategory_path = r"pcategory.csv"
output_path = r"usersales.csv"

# Read tables
users_df = spark.read.parquet(users_path)
products_df = spark.read.option("header", "true").option("inferSchema", "true").csv(products_path)
pcategory_df = spark.read.option("header", "true").option("inferSchema", "true").csv(pcategory_path)

# Join products → categories
products_with_cat = products_df.join(
    pyspark.sql.functions.broadcast(pcategory_df), 
    products_df.Category == pcategory_df.description, 
    "left"
).select(
    products_df.Pid.alias("pid"),
    pcategory_df.cid,
    products_df.Price
)

# Generate sales: users × 5 products (~100K rows)
users_sales = users_df.select("id") \
    .crossJoin(products_with_cat.limit(5)) \
    .withColumn("quantity", pyspark.sql.functions.floor(pyspark.sql.functions.rand() * 11).cast("int")) \
    .withColumn("datetime", pyspark.sql.functions.to_timestamp(pyspark.sql.functions.expr("timestamp_seconds(1735689600 + (rand() * 31536000)::bigint)")))

# Final columns, filter zero quantity
user_sales_final = users_sales.select(
    "id", "pid", "cid", "Price", "quantity", "datetime"
).filter(pyspark.sql.functions.col("quantity") > 0)

print("Sample sales:")
user_sales_final.show(10)

user_sales_final.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .csv(output_path)

print(f"Saved {user_sales_final.count()} rows to {output_path}")
spark.stop()
