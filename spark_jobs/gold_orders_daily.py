from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    round as spark_round, when, min as spark_min, max as spark_max
)

# -----------------------------
# 1. Start Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("GoldOrdersDaily")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n=== Reading Silver Orders ===")

# -----------------------------
# 2. Read Silver Orders
# -----------------------------
df = spark.read.parquet("data/silver/orders")

print("Schema:")
df.printSchema()

# -----------------------------
# 3. Aggregate Daily Order Metrics
# -----------------------------
orders_daily = (
    df.groupBy("order_date")
    .agg(
        # Order volume
        count("order_id").alias("total_orders"),
        countDistinct("user_id").alias("unique_buyers"),      # FIX: repeat vs new buyer signal

        # Order status breakdown
        spark_sum(when(col("order_status") == "completed", 1).otherwise(0)).alias("completed_orders"),
        spark_sum(when(col("order_status") == "cancelled", 1).otherwise(0)).alias("cancelled_orders"),
        spark_sum(when(col("order_status") == "pending",   1).otherwise(0)).alias("pending_orders"),

        # Units sold (only completed orders)
        spark_sum(
            when(col("order_status") == "completed", col("quantity")).otherwise(0)
        ).alias("units_sold"),

        # Revenue (only completed orders)
        spark_sum(
            when(col("order_status") == "completed", col("total_amount")).otherwise(0)
        ).alias("total_sales_amount"),
    )
)

# -----------------------------
# 4. Derived Business Metrics
# -----------------------------
orders_daily = (
    orders_daily
    .withColumn(
        "avg_order_value",
        when(
            col("completed_orders") > 0,
            spark_round(col("total_sales_amount") / col("completed_orders"), 2)
        ).otherwise(0)
    )
    .withColumn(                                              # FIX: basket size trend
        "avg_units_per_order",
        when(
            col("completed_orders") > 0,
            spark_round(col("units_sold") / col("completed_orders"), 2)
        ).otherwise(0)
    )
    .withColumn(                                             # FIX: key business health metric
        "cancellation_rate",
        when(
            col("total_orders") > 0,
            spark_round(col("cancelled_orders") / col("total_orders") * 100, 2)
        ).otherwise(0)
    )
    .orderBy("order_date")
)

gold_count = orders_daily.count()

# FIX: log date range for observability
date_range = df.agg(
    spark_min("order_date").alias("min_date"),
    spark_max("order_date").alias("max_date")
).collect()[0]

print(f"\nGold rows (days)  : {gold_count}")
print(f"Date range        : {date_range['min_date']} → {date_range['max_date']}")
print("\nDaily Orders Metrics:")
orders_daily.show(10, truncate=False)

# -----------------------------
# 5. Write Gold Layer
# -----------------------------
print("\n=== Writing Gold Orders Daily ===")

orders_daily.write \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .parquet("data/gold/orders_daily")

print(f"\n✅ Gold orders daily complete! {gold_count} days written to data/gold/orders_daily")

spark.stop()

