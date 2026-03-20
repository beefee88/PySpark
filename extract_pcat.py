from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, rand, row_number
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder.appName("Extract Categories from Products CSV").getOrCreate()

# Windows-safe paths
input_path = r"products-1000.csv"
output_path = r"categories.csv"

# Read products CSV (header=true)
products_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

print("Original products sample:")
products_df.select("Pid", "Category", "Price").show(10, truncate=False)

# Extract unique categories → assign cid (1,2,3...) + random internalid
categories_df = products_df.select("Category") \
    .distinct() \
    .withColumn("cid", row_number().over(Window.orderBy(col("Category")))) \
    .withColumn("internalid", (rand() * 1000000).cast("int")) \
    .select("cid", "Category", "internalid") \
    .withColumnRenamed("Category", "description") \
    .orderBy("cid")

# Show results
print("Generated Categories Table:")
categories_df.show(truncate=False)
print(f"Generated {categories_df.count()} unique categories")

# Write to CSV (single file, header)
categories_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

spark.stop()
print(f"Categories saved to {output_path}")
