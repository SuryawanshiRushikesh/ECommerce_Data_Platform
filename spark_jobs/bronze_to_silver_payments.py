from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lower, to_date
from pyspark.sql.types import DoubleType

# -----------------------------
# 1. Start Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("BronzeToSilverPayments")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n=== Reading Bronze Payments ===")

# -----------------------------
# 2. Read Bronze JSONL
# -----------------------------
df = (
    spark.read
    .option("multiLine", False)
    .json("data/bronze/payments.jsonl")
)

bronze_count = df.count()
print(f"Bronze row count: {bronze_count}")
df.printSchema()

# -----------------------------
# 3. Cleaning + Schema Enforcement
# -----------------------------
VALID_STATUSES = ["success", "failed", "refunded"]
VALID_METHODS  = ["card", "upi", "netbanking"]
VALID_CURRENCIES = ["inr"]                                  # FIX: whitelist currencies

clean_df = (
    df
    # Required fields
    .filter(col("transaction_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .filter(col("payment_time").isNotNull())
    .filter(col("currency").isNotNull())                    # FIX: currency must be present

    # Cast types
    .withColumn("payment_time", to_timestamp(col("payment_time")))
    .filter(col("payment_time").isNotNull())
    .withColumn("amount", col("amount").cast(DoubleType()))
    .filter(col("amount").isNotNull())                      # FIX: catch failed casts explicitly

    # Normalize
    .withColumn("payment_status", lower(col("payment_status")))
    .withColumn("payment_method", lower(col("payment_method")))
    .withColumn("currency", lower(col("currency")))

    # Validate allowed values
    .filter(col("payment_status").isin(VALID_STATUSES))
    .filter(col("payment_method").isin(VALID_METHODS))
    .filter(col("currency").isin(VALID_CURRENCIES))         # FIX: reject unknown currencies

    # Financial logic validation
    .filter(
        ((col("payment_status") == "success")  & (col("amount") > 0)) |
        ((col("payment_status") == "failed")   & (col("amount") > 0)) |
        ((col("payment_status") == "refunded") & (col("amount") < 0))
    )

    # Deduplicate
    .dropDuplicates(["transaction_id"])
)

# -----------------------------
# 4. Explicit Schema Selection
# -----------------------------
silver_df = (
    clean_df.select(
        col("transaction_id"),
        col("order_id"),
        col("user_id"),
        col("payment_time"),
        col("amount"),
        col("currency"),
        col("payment_method"),
        col("payment_status"),
        to_date(col("payment_time")).alias("payment_date")
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
print("\n=== Writing Silver Payments ===")

silver_df.write \
    .mode("overwrite") \
    .partitionBy("payment_date") \
    .parquet("data/silver/payments")

print(f"\n✅ Bronze → Silver Payments complete! {silver_count} rows written to data/silver/payments")

spark.stop()
