from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum,
    when, round as spark_round, coalesce
)

# -----------------------------
# Tunable Constants
# -----------------------------
HIGH_VALUE_THRESHOLD = 5000     # FIX: named constant instead of magic number

# -----------------------------
# 1. Start Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("GoldUserDailyFeatures")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n=== Reading Silver Tables ===")

# -----------------------------
# 2. Read Silver Tables
# FIX: added payments silver for payment-level user features
# -----------------------------
events_df   = spark.read.parquet("data/silver/user_events")
orders_df   = spark.read.parquet("data/silver/orders")
payments_df = spark.read.parquet("data/silver/payments")

# -----------------------------
# 3. User Behavior Features (Events)
# FIX: added checkout_count for full funnel coverage
# -----------------------------
events_features = (
    events_df
    .groupBy("user_id", "event_date")
    .agg(
        count(when(col("event_type") == "session_start", 1)).alias("sessions_count"),
        count(when(col("event_type") == "view_product",  1)).alias("views_count"),
        count(when(col("event_type") == "add_to_cart",   1)).alias("cart_count"),
        count(when(col("event_type") == "checkout",      1)).alias("checkout_count"),  # FIX
        count(when(col("event_type") == "purchase",      1)).alias("purchase_count"),
    )
)

# -----------------------------
# 4. User Transaction Features (Orders)
# -----------------------------
orders_features = (
    orders_df
    .groupBy("user_id", "order_date")
    .agg(
        count("order_id").alias("orders_count"),
        spark_sum(
            when(col("order_status") == "completed", 1).otherwise(0)
        ).alias("completed_orders"),
        spark_sum(
            when(col("order_status") == "cancelled", 1).otherwise(0)
        ).alias("cancelled_orders"),
        spark_sum(
            when(col("order_status") == "completed", col("total_amount")).otherwise(0)
        ).alias("total_spent")
    )
).withColumnRenamed("order_date", "event_date")

# -----------------------------
# 5. User Payment Features (Payments)
# FIX: payment signals are strong ML features — failed payments, refunds etc
# -----------------------------
payments_features = (
    payments_df
    .groupBy("user_id", "payment_date")
    .agg(
        count("transaction_id").alias("total_transactions"),
        spark_sum(
            when(col("payment_status") == "failed",   1).otherwise(0)
        ).alias("failed_payment_count"),
        spark_sum(
            when(col("payment_status") == "refunded", 1).otherwise(0)
        ).alias("refund_count"),
    )
).withColumnRenamed("payment_date", "event_date")

# -----------------------------
# 6. Join All Three Tables
# FIX: outer join so no user-day is lost from any table
# -----------------------------
final_df = (
    events_features
    .join(orders_features,   ["user_id", "event_date"], "outer")
    .join(payments_features, ["user_id", "event_date"], "outer")
)

# -----------------------------
# 7. Null Handling
# -----------------------------
final_df = final_df.fillna(0, subset=[
    "sessions_count", "views_count", "cart_count",
    "checkout_count", "purchase_count",
    "orders_count", "completed_orders", "cancelled_orders", "total_spent",
    "total_transactions", "failed_payment_count", "refund_count"
])

# -----------------------------
# 8. Derived ML Features
# -----------------------------
final_df = (
    final_df
    .withColumn(
        "avg_order_value",
        when(
            col("completed_orders") > 0,
            spark_round(col("total_spent") / col("completed_orders"), 2)
        ).otherwise(0)
    )
    .withColumn(                                            # FIX: ratio feature for ML
        "view_to_cart_rate",
        when(
            col("views_count") > 0,
            spark_round(col("cart_count") / col("views_count"), 4)
        ).otherwise(0)
    )
    .withColumn(                                            # FIX: checkout drop-off signal
        "cart_to_checkout_rate",
        when(
            col("cart_count") > 0,
            spark_round(col("checkout_count") / col("cart_count"), 4)
        ).otherwise(0)
    )
    .withColumn(
        "is_buyer_flag",
        when(col("completed_orders") > 0, 1).otherwise(0)
    )
    .withColumn(
        "conversion_flag",
        when(col("purchase_count") > 0, 1).otherwise(0)
    )
    .withColumn(
        "cart_abandon_flag",
        when(
            (col("cart_count") > 0) & (col("purchase_count") == 0), 1
        ).otherwise(0)
    )
    .withColumn(                                            # FIX: payment risk signal for ML
        "has_failed_payment_flag",
        when(col("failed_payment_count") > 0, 1).otherwise(0)
    )
    .withColumn(
        "has_refund_flag",
        when(col("refund_count") > 0, 1).otherwise(0)
    )
    .withColumn(
        "high_value_user_flag",
        when(col("total_spent") > HIGH_VALUE_THRESHOLD, 1).otherwise(0)
    )
)

final_df = final_df.orderBy("event_date", "user_id")

# Cache before actions
final_df.cache()

# -----------------------------
# 9. Show Result
# -----------------------------
row_count = final_df.count()
print(f"\nTotal user-day feature rows: {row_count}")
print("\nSample User Daily Features:")
final_df.show(10, truncate=False)

# -----------------------------
# 10. Write Gold ML Feature Table
# -----------------------------
print("\n=== Writing ML Feature Table ===")

final_df.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet("data/gold/user_daily_features")

print(f"\n✅ ML feature table created with {row_count} rows → data/gold/user_daily_features")

spark.stop()

