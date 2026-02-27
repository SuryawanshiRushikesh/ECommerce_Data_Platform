from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, count, sum as spark_sum,
    round as spark_round, when
)

# -----------------------------
# 1. Start Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("GoldEventsDaily")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n=== Reading Silver User Events ===")

# -----------------------------
# 2. Read Silver Events
# -----------------------------
df = spark.read.parquet("data/silver/user_events")

print("Schema:")
df.printSchema()

# -----------------------------
# 3. Aggregate Daily Metrics
# FIX: expanded from 2 metrics to full funnel + device/source breakdown
# -----------------------------
events_daily = (
    df.groupBy("event_date")
    .agg(
        # Volume metrics
        countDistinct("user_id").alias("total_users"),
        countDistinct("session_id").alias("total_sessions"),
        count("event_id").alias("total_events"),              # FIX: total event volume per day

        # Funnel metrics                                      # FIX: full funnel counts
        count(when(col("event_type") == "session_start",  1)).alias("session_starts"),
        count(when(col("event_type") == "view_product",   1)).alias("product_views"),
        count(when(col("event_type") == "add_to_cart",    1)).alias("add_to_cart_count"),
        count(when(col("event_type") == "checkout",       1)).alias("checkout_count"),
        count(when(col("event_type") == "purchase",       1)).alias("purchase_count"),

        # Device breakdown                                    # FIX: device split
        count(when(col("device") == "mobile", 1)).alias("mobile_events"),
        count(when(col("device") == "web",    1)).alias("web_events"),

        # Source breakdown                                    # FIX: acquisition channel split
        count(when(col("source") == "organic", 1)).alias("organic_events"),
        count(when(col("source") == "ad",      1)).alias("ad_events"),
        count(when(col("source") == "email",   1)).alias("email_events"),
    )
)

# FIX: derived conversion rates — most valuable gold metrics
events_daily = events_daily.withColumn(
    "cart_to_view_rate",
    spark_round(col("add_to_cart_count") / col("product_views") * 100, 2)
).withColumn(
    "checkout_to_cart_rate",
    spark_round(col("checkout_count") / col("add_to_cart_count") * 100, 2)
).withColumn(
    "purchase_to_checkout_rate",
    spark_round(col("purchase_count") / col("checkout_count") * 100, 2)
).withColumn(
    "overall_conversion_rate",
    spark_round(col("purchase_count") / col("session_starts") * 100, 2)
)

# FIX: sort by date so BI tools get chronologically ordered data
events_daily = events_daily.orderBy("event_date")

gold_count = events_daily.count()
print(f"\nGold rows (days): {gold_count}")
print("\nDaily Events Metrics:")
events_daily.show(10, truncate=False)

# -----------------------------
# 4. Write Gold Layer
# FIX: partitioned by event_date for consistent query performance
# -----------------------------
print("\n=== Writing Gold Events Daily ===")

events_daily.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet("data/gold/events_daily")

print(f"\n✅ Gold events daily complete! {gold_count} days written to data/gold/events_daily")

spark.stop()
