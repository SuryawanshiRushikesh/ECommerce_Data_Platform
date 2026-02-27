from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lower, to_date
from pyspark.sql.types import StringType

# -----------------------------
# 1. Start Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("BronzeToSilverUserEvents") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n=== Reading Bronze Layer ===")

# -----------------------------
# 2. Read Bronze JSONL
# FIX: explicitly disable multiLine so each line is treated as one record
# -----------------------------
df = spark.read \
    .option("multiLine", False) \
    .json("data/bronze/user_events.jsonl")

bronze_count = df.count()
print(f"Bronze row count: {bronze_count}")
print("Raw schema:")
df.printSchema()

# -----------------------------
# 3. Cleaning + Schema Enforcement
# -----------------------------
clean_df = df \
    .filter(col("event_id").isNotNull()) \
    .filter(col("user_id").isNotNull()) \
    .filter(col("session_id").isNotNull()) \
    .withColumn("event_time", to_timestamp(col("event_time"))) \
    .filter(col("event_time").isNotNull()) \
    .withColumn("event_type", lower(col("event_type"))) \
    .withColumn("device", lower(col("device"))) \
    .withColumn("source", lower(col("source"))) \
    .withColumn("order_id", col("order_id").cast(StringType())) \
    .dropDuplicates(["event_id"])

# -----------------------------
# 4. Explicit Column Selection
# FIX: silver should have a known, fixed schema â€” no surprise columns from bronze
# -----------------------------
silver_df = clean_df.select(
    col("event_id"),
    col("user_id"),
    col("session_id"),
    col("event_type"),
    col("product_id"),
    col("order_id"),
    col("device"),
    col("source"),
    col("event_time"),
    to_date(col("event_time")).alias("event_date")    # derived partition column
)

silver_count = silver_df.count()
dropped = bronze_count - silver_count

print(f"\nSilver row count : {silver_count}")
print(f"Rows dropped     : {dropped} ({round(dropped / bronze_count * 100, 2)}%)")
print("\nCleaned schema:")
silver_df.printSchema()
print("\nSample data:")
silver_df.show(5, truncate=False)

# -----------------------------
# 5. Write Silver Layer (Parquet + Partition)
# -----------------------------
print("\n=== Writing Silver Layer ===")

silver_df.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet("data/silver/user_events")

print(f"\nâœ… Bronze â†’ Silver complete! {silver_count} rows written to data/silver/user_events")

spark.stop()
