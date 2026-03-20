from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, date_format

# Initialize Spark
spark = SparkSession.builder.appName("Extract Date ID from String Datetime").getOrCreate()

# Paths (Windows-safe)
input_path = r"userdata.parquet"
output_path = r"output.csv"

# Read Parquet
df = spark.read.parquet(input_path)

# Parse string > timestamp -> date -> yyyymmdd
reg_dttm_tmstp = to_timestamp(col("registration_dttm"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
reg_dttm_dt =  to_date(reg_dttm_tmstp, "yyyy-MM-dd")  # Get 'yyyy-MM-dd' part
reg_dttm_dt_int = date_format(reg_dttm_dt, "yyyyMMdd").cast("int")
df_with_date = df.withColumn("date_id", reg_dttm_dt_int)

# Write output Parquet (all original columns + date_id)
#df_with_date.coalesce(1).write.mode("overwrite").parquet(output_path)
df_with_date.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

spark.stop()
print("Done! Added Column date_id.")
