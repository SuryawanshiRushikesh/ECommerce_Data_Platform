from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round
import json
import os
from datetime import datetime

# -----------------------------
# Ensure folders exist
# -----------------------------
os.makedirs("metrics", exist_ok=True)
os.makedirs("logs", exist_ok=True)

spark = SparkSession.builder.appName("DQ_Silver_Orders").getOrCreate()

print("\n=== Running Data Quality Checks: Silver Orders ===")

df = spark.read.parquet("data/silver/orders")

metrics = {}
errors = []

# -----------------------------
# Row Count
# -----------------------------
metrics["row_count"] = df.count()

# -----------------------------
# Required Fields
# -----------------------------
required_cols = [
    "order_id", "user_id", "product_id",
    "order_time", "quantity", "price",
    "total_amount", "order_status"
]

for c in required_cols:
    null_count = df.filter(col(c).isNull()).count()
    metrics[f"null_{c}"] = null_count

    if null_count > 0:
        errors.append(f"NULL values found in {c}")

# -----------------------------
# Duplicate Order Check
# -----------------------------
metrics["duplicate_order_id"] = (
    df.groupBy("order_id").count().filter("count > 1").count()
)

if metrics["duplicate_order_id"] > 0:
    errors.append("Duplicate order_id found")

# -----------------------------
# Positive Quantity & Price
# -----------------------------
metrics["invalid_quantity"] = df.filter(col("quantity") <= 0).count()
metrics["invalid_price"] = df.filter(col("price") <= 0).count()

if metrics["invalid_quantity"] > 0:
    errors.append("Invalid quantity values")

if metrics["invalid_price"] > 0:
    errors.append("Invalid price values")

# -----------------------------
# Financial Integrity Check
# total_amount == price * quantity
# -----------------------------
metrics["amount_mismatch"] = (
    df.filter(
        spark_round(col("total_amount"), 2) !=
        spark_round(col("price") * col("quantity"), 2)
    ).count()
)

if metrics["amount_mismatch"] > 0:
    errors.append("total_amount mismatch with price * quantity")

# -----------------------------
# Valid Order Status
# -----------------------------
valid_status = ["completed", "cancelled", "pending"]

metrics["invalid_status"] = (
    df.filter(~col("order_status").isin(valid_status)).count()
)

if metrics["invalid_status"] > 0:
    errors.append("Invalid order_status values")

# -----------------------------
# Save Metrics
# -----------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

with open(f"metrics/orders_dq_{timestamp}.json", "w") as f:
    json.dump(metrics, f, indent=4)

# -----------------------------
# Save Logs
# -----------------------------
with open("logs/pipeline.log", "a") as log:
    log.write(f"\n[{timestamp}] SILVER ORDERS DQ\n")
    log.write(json.dumps(metrics))
    log.write("\n")

# -----------------------------
# Final Result
# -----------------------------
if len(errors) == 0:
    print("✅ Silver orders passed all checks")
else:
    print("❌ Data quality failed:", errors)
    raise Exception("Silver orders data quality failed")

spark.stop()
