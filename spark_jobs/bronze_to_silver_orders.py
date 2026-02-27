from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lower, to_date, round as spark_round
from pyspark.sql.types import IntegerType, DoubleType


# -----------------------------
# 1. Start Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("BronzeToSilverOrders")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n=== Reading Bronze Orders ===")


# -----------------------------
# 2. Read Bronze JSONL
# -----------------------------
df = (
    spark.read
    .option("multiLine", False)
    .json("data/bronze/orders.jsonl")
)

bronze_count = df.count()
print(f"Bronze row count: {bronze_count}")
df.printSchema()


# -----------------------------
# 3. Cleaning + Schema Enforcement
# -----------------------------
VALID_STATUSES = ["completed", "cancelled", "pending"]

clean_df = (
    df
    # Required fields
    .filter(col("order_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .filter(col("product_id").isNotNull())   # every order must have product

    # Timestamp casting
    .withColumn("order_time", to_timestamp(col("order_time")))
    .withColumn("updated_at", to_timestamp(col("updated_at")))
    .filter(col("order_time").isNotNull())
    .filter(col("updated_at").isNotNull())

    # Numeric casting
    .withColumn("quantity", col("quantity").cast(IntegerType()))
    .withColumn("price", col("price").cast(DoubleType()))
    .withColumn("total_amount", col("total_amount").cast(DoubleType()))

    # Business validations
    .filter(col("quantity") > 0)
    .filter(col("price") > 0)
    .filter(col("total_amount") > 0)

    # Normalize status
    .withColumn("order_status", lower(col("order_status")))
    .filter(col("order_status").isin(VALID_STATUSES))

    # Financial integrity check
    .filter(
        spark_round(col("total_amount"), 2) ==
        spark_round(col("price") * col("quantity"), 2)
    )

    # Deduplication
    .dropDuplicates(["order_id"])
)


# -----------------------------
# 4. Explicit Schema Selection
# -----------------------------
silver_df = (
    clean_df.select(
        col("order_id"),
        col("user_id"),
        col("product_id"),
        col("order_time"),
        col("updated_at"),
        col("quantity"),
        col("price"),
        col("total_amount"),
        col("order_status"),
        to_date(col("order_time")).alias("order_date")
    )
)

silver_count = silver_df.count()
dropped = bronze_count - silver_count

print(f"\nSilver row count : {silver_count}")

if bronze_count > 0:
    drop_pct = round(dropped / bronze_count * 100, 2)
else:
    drop_pct = 0.0

print(f"Rows dropped     : {dropped} ({drop_pct}%)")

print("\nCleaned schema:")
silver_df.printSchema()

print("\nSample data:")
silver_df.show(5, truncate=False)


# -----------------------------
# 5. Write Silver Layer
# -----------------------------
print("\n=== Writing Silver Orders ===")

silver_df.write \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .parquet("data/silver/orders")

print(f"\n✅ Bronze → Silver Orders complete! {silver_count} rows written to data/silver/orders")

spark.stop()
