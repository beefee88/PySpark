from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast, col, lower, trim, to_timestamp, to_date, date_format,
    count, when, lit
)

spark = (
    SparkSession.builder
    .appName("Production UserSales Pipeline")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

usersales_path = r"usersalesdtid.csv"
products_path = r"products-1000.csv"
pcategory_path = r"pcategory.csv"
output_path = r"usersales_prod_csv"

def normalize_cols(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower())
    return df

def require_columns(df, required, name):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing columns: {missing}")

usersales = normalize_cols(spark.read.option("header", "true").option("inferSchema", "true").csv(usersales_path))
products = normalize_cols(spark.read.option("header", "true").option("inferSchema", "true").csv(products_path))
pcategory = normalize_cols(spark.read.option("header", "true").option("inferSchema", "true").csv(pcategory_path))

require_columns(usersales, ["id", "pid", "cid", "price", "quantity", "datetime", "date_id"], "usersalesdtid.csv")
require_columns(products, ["pid", "price", "category"], "products-1000.csv")
require_columns(pcategory, ["cid", "description", "internalid"], "pcategory.csv")

products_dim = (
    products.alias("p")
    .join(
        broadcast(pcategory.alias("c")),
        lower(trim(col("p.category"))) == lower(trim(col("c.description"))),
        "left"
    )
    .select(
        col("p.pid").alias("pid"),
        col("p.name").alias("product_name"),
        col("p.category").alias("category_desc"),
        col("p.price").alias("product_price"),
        col("c.cid").alias("category_cid"),  # ← Explicitly named
        col("c.internalid").alias("category_internalid")
    )
)

usersales_clean = (
    usersales
    .withColumn("datetime", to_timestamp(col("datetime")))
    .withColumn("date_id", col("date_id").cast("int"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("price", col("price").cast("double"))
    .withColumn("cid", col("cid").cast("int"))
)

sales_enriched = (
    usersales_clean
    .join(products_dim, "pid", "left")
    .withColumn("price_source", 
        when(col("price").isNotNull(), lit("usersales")).otherwise(lit("products")))
    .withColumn("effective_price", 
        when(col("price").isNotNull(), col("price")).otherwise(col("product_price")))
)

dq_metrics = sales_enriched.agg(
    count("*").alias("row_count"),
    count(when(col("cid").isNull(), 1)).alias("null_cid"),
    count(when(col("category_cid").isNull(), 1)).alias("null_category_cid"),
    count(when(col("datetime").isNull(), 1)).alias("null_datetime"),
    count(when(col("date_id").isNull(), 1)).alias("null_date_id"),
    count(when(col("effective_price").isNull(), 1)).alias("null_price")
)

dq_metrics.show(truncate=False)

final_df = (
    sales_enriched
    .select(
        "id",
        "pid",
        "cid",
        col("effective_price").alias("price"),
        "quantity",
        "datetime",
        "date_id",
        "product_name",
        "category_desc",
        "category_internalid",
        "price_source"
    )
)

final_df.write.mode("overwrite").option("header", "true").csv(output_path)

print(f"Wrote {final_df.count()} rows to {output_path}")
spark.stop()
