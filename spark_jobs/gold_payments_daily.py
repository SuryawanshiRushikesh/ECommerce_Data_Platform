from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum,
    round as spark_round, when,
    min as spark_min, max as spark_max
)

# -----------------------------
# 1. Start Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("GoldPaymentsDaily")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n=== Reading Silver Payments ===")

# -----------------------------
# 2. Read Silver Payments
# -----------------------------
df = spark.read.parquet("data/silver/payments")

print("Schema:")
df.printSchema()

# -----------------------------
# 3. Aggregate Daily Payment Metrics
# -----------------------------
payments_daily = (
    df.groupBy("payment_date")
    .agg(
        # Transaction volume
        count("transaction_id").alias("total_transactions"),

        # Status breakdown
        spark_sum(when(col("payment_status") == "success",  1).otherwise(0)).alias("successful_payments"),
        spark_sum(when(col("payment_status") == "failed",   1).otherwise(0)).alias("failed_payments"),
        spark_sum(when(col("payment_status") == "refunded", 1).otherwise(0)).alias("refund_count"),

        # Revenue metrics
        spark_sum(
            when(col("payment_status") == "success", col("amount")).otherwise(0)
        ).alias("total_revenue"),
        spark_sum(
            when(col("payment_status") == "refunded", col("amount")).otherwise(0)
        ).alias("refund_amount"),                           # negative values by design

        # FIX: payment method breakdown — which channels drive volume
        spark_sum(when(col("payment_method") == "card",       1).otherwise(0)).alias("card_transactions"),
        spark_sum(when(col("payment_method") == "upi",        1).otherwise(0)).alias("upi_transactions"),
        spark_sum(when(col("payment_method") == "netbanking", 1).otherwise(0)).alias("netbanking_transactions"),
    )
)

# -----------------------------
# 4. Derived Metrics
# -----------------------------
payments_daily = (
    payments_daily
    .withColumn(
        "net_revenue",
        spark_round(col("total_revenue") + col("refund_amount"), 2)
    )
    .withColumn(                                            # FIX: avg transaction value
        "avg_transaction_value",
        when(
            col("successful_payments") > 0,
            spark_round(col("total_revenue") / col("successful_payments"), 2)
        ).otherwise(0)
    )
    .withColumn(
        "refund_rate",
        when(
            col("successful_payments") > 0,
            spark_round(col("refund_count") / col("successful_payments") * 100, 2)
        ).otherwise(0)
    )
    .withColumn(                                            # FIX: failure rate
        "failure_rate",
        when(
            col("total_transactions") > 0,
            spark_round(col("failed_payments") / col("total_transactions") * 100, 2)
        ).otherwise(0)
    )
    .orderBy("payment_date")
)

gold_count = payments_daily.count()

# FIX: date range log for observability, consistent with orders gold
date_range = df.agg(
    spark_min("payment_date").alias("min_date"),
    spark_max("payment_date").alias("max_date")
).collect()[0]

print(f"\nGold rows (days)  : {gold_count}")
print(f"Date range        : {date_range['min_date']} → {date_range['max_date']}")
print("\nDaily Payments Metrics:")
payments_daily.show(10, truncate=False)

# -----------------------------
# 5. Write Gold Layer
# -----------------------------
print("\n=== Writing Gold Payments Daily ===")

payments_daily.write \
    .mode("overwrite") \
    .partitionBy("payment_date") \
    .parquet("data/gold/payments_daily")

print(f"\n✅ Gold payments daily complete! {gold_count} days written to data/gold/payments_daily")

spark.stop()

