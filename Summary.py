from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, sum as spark_sum, count, to_timestamp
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder \
    .appName("Sales Summary Parquet Tables") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

input_path = r"usersalesprod.csv"
category_path = r"pcategory.csv"
category_output = r"sales_by_category.parquet"
month_output = r"sales_by_month.parquet"

# Read sales data
sales_df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
pcategory_df = spark.read.option("header", "true").option("inferSchema", "true").csv(category_path)

print("Sales data loaded:", sales_df.count(), "rows")
sales_df.select("id", "pid", "cid", "price", "quantity", "date_id").show(5)

# 1. Sales by Category (Revenue = price * quantity)
sales_by_category = (
    sales_df
    .groupBy("cid")
    .agg(
        spark_sum(col("price") * col("quantity")).alias("total_revenue"),
        spark_sum(col("quantity")).alias("total_quantity"),
        count("*").alias("order_count"),
        spark_sum(col("price")).alias("total_price_sum")
    )
    .join(
        pcategory_df.select("cid", "description"),
        "cid",
        "left"
    )
    .orderBy(col("total_revenue").desc())
)

print("=== SALES BY CATEGORY ===")
sales_by_category.show(20, truncate=False)

# Write category summary
sales_by_category.write.mode("overwrite").parquet(category_output)
print(f"Sales by category saved: {category_output}")

# 2. Sales by Month (extract from date_id or datetime)
sales_by_month = (
    sales_df
    .withColumn("year", col("date_id").substr(1, 4).cast("int"))
    .withColumn("month", col("date_id").substr(5, 2).cast("int"))
    .withColumn("ym", (col("date_id").substr(1, 6)).cast("int"))
    .groupBy("ym", "year", "month")
    .agg(
        spark_sum(col("price") * col("quantity")).alias("monthly_revenue"),
        spark_sum(col("quantity")).alias("monthly_quantity"),
        count("*").alias("monthly_orders"),
        spark_sum(col("price")).alias("monthly_price_sum")
    )
    .orderBy("ym")
)

print("\n=== SALES BY MONTH ===")
sales_by_month.show(20, truncate=False)

# Write month summary
sales_by_month.write.mode("overwrite").parquet(month_output)
print(f"Sales by month saved: {month_output}")

spark.stop()
print("Done! Check both Parquet tables.")
