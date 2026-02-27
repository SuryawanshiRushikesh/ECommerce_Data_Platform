from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round

# -----------------------------
# 1. Start Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("GoldDailyBusinessMetrics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n=== Reading Intermediate Gold Tables ===")

# -----------------------------
# 2. Read Intermediate Gold Tables
# -----------------------------
events_df   = spark.read.parquet("data/gold/events_daily")
orders_df   = spark.read.parquet("data/gold/orders_daily")
payments_df = spark.read.parquet("data/gold/payments_daily")


# -----------------------------
# 3. Standardize Date Column
# -----------------------------
events_df   = events_df.withColumnRenamed("event_date", "metric_date")
orders_df   = orders_df.withColumnRenamed("order_date", "metric_date")
payments_df = payments_df.withColumnRenamed("payment_date", "metric_date")


# -----------------------------
# 4. Select Only Executive Metrics (Clean KPI Set)
# -----------------------------
events_df = events_df.select(
    "metric_date",
    "total_users",
    "total_sessions",
    "purchase_count",
    "overall_conversion_rate"
)

orders_df = orders_df.select(
    "metric_date",
    "total_orders",
    "completed_orders",
    "cancellation_rate"
)

# Payments = financial truth layer
payments_df = payments_df.select(
    "metric_date",
    "total_revenue",
    "net_revenue",
    "avg_transaction_value",
    "refund_rate",
    "failure_rate"
)


# -----------------------------
# 5. Join All Tables
# -----------------------------
final_df = (
    events_df
    .join(orders_df, "metric_date", "outer")
    .join(payments_df, "metric_date", "outer")
)


# -----------------------------
# 6. Null Handling
# Volume → 0
# Rates → remain null (null = no data)
# -----------------------------
volume_cols = [
    "total_users",
    "total_sessions",
    "purchase_count",
    "total_orders",
    "completed_orders",
    "total_revenue",
    "net_revenue"
]

final_df = final_df.fillna(0, subset=volume_cols)


# -----------------------------
# 7. Final Cleanup + Rounding (Presentation Layer)
# -----------------------------
final_df = (
    final_df
    .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
    .withColumn("net_revenue", spark_round(col("net_revenue"), 2))
    .withColumn("avg_transaction_value", spark_round(col("avg_transaction_value"), 2))
    .withColumn("overall_conversion_rate", spark_round(col("overall_conversion_rate"), 2))
    .withColumn("cancellation_rate", spark_round(col("cancellation_rate"), 2))
    .withColumn("refund_rate", spark_round(col("refund_rate"), 2))
    .withColumn("failure_rate", spark_round(col("failure_rate"), 2))
    .orderBy("metric_date")
)

# cache to avoid recomputation
final_df.cache()


# -----------------------------
# 8. Show Result
# -----------------------------
row_count = final_df.count()

print(f"\nTotal days processed: {row_count}")
print("\nFinal Daily Business Metrics:")
final_df.show(10, truncate=False)


# -----------------------------
# 9. Write Final Gold Table
# -----------------------------
print("\n=== Writing Final Gold Business Metrics ===")

final_df.write \
    .mode("overwrite") \
    .partitionBy("metric_date") \
    .parquet("data/gold/daily_business_metrics")

print(f"\n✅ Final business metrics table created! {row_count} days written.")

spark.stop()
