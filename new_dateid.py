from pyspark.sql import SparkSession
from pyspark.sql.functions import col, left, to_timestamp, to_date, date_format

# Initialize Spark
spark = SparkSession.builder \
    .appName("Extract Date ID from String Datetime") \
    .getOrCreate()

# Paths (Windows-safe)
input_path = r"userdata.parquet"
output_path = r"output.parquet"

# Read Parquet
df = spark.read.parquet(input_path)

# Parse string 'yyyy-MM-ddTHH:MM:SS.000000' -> timestamp -> date -> yyyymmdd int
bob = to_timestamp(col("registration_dttm"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
joe =  to_date(bob, "yyyy-MM-dd")  # Get 'yyyy-MM-dd' part
jill = date_format(joe, "yyyyMMdd").cast("int")
df_with_date = df.withColumn("date_id", jill)

# Write output Parquet (all original columns + date_id)
#df_with_date.coalesce(1).write.mode("overwrite").parquet(output_path)
df_with_date.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

spark.stop()
print("Done! Check output.parquet for date_id column.")
