from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
import os
from datetime import datetime

# -----------------------------
# Ensure folders exist
# -----------------------------
os.makedirs("metrics", exist_ok=True)
os.makedirs("logs", exist_ok=True)

spark = SparkSession.builder.appName("DQ_Silver_Payments").getOrCreate()

print("\n=== Running Data Quality Checks: Silver Payments ===")

df = spark.read.parquet("data/silver/payments")

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
    "transaction_id", "order_id", "user_id",
    "payment_time", "amount", "payment_status"
]

for c in required_cols:
    null_count = df.filter(col(c).isNull()).count()
    metrics[f"null_{c}"] = null_count

    if null_count > 0:
        errors.append(f"NULL values found in {c}")

# -----------------------------
# Duplicate Transaction Check
# -----------------------------
metrics["duplicate_transaction_id"] = (
    df.groupBy("transaction_id").count().filter("count > 1").count()
)

if metrics["duplicate_transaction_id"] > 0:
    errors.append("Duplicate transaction_id found")

# -----------------------------
# Valid Status Values
# -----------------------------
valid_status = ["success", "failed", "refunded"]

metrics["invalid_status"] = (
    df.filter(~col("payment_status").isin(valid_status)).count()
)

if metrics["invalid_status"] > 0:
    errors.append("Invalid payment_status values")

# -----------------------------
# Valid Payment Methods
# -----------------------------
valid_methods = ["card", "upi", "netbanking"]

metrics["invalid_method"] = (
    df.filter(~col("payment_method").isin(valid_methods)).count()
)

if metrics["invalid_method"] > 0:
    errors.append("Invalid payment_method values")

# -----------------------------
# Valid Currency
# -----------------------------
metrics["invalid_currency"] = (
    df.filter(col("currency") != "inr").count()
)

if metrics["invalid_currency"] > 0:
    errors.append("Invalid currency values")

# -----------------------------
# Financial Logic Validation
# -----------------------------
metrics["invalid_success_amount"] = (
    df.filter((col("payment_status") == "success") & (col("amount") <= 0)).count()
)

metrics["invalid_failed_amount"] = (
    df.filter((col("payment_status") == "failed") & (col("amount") <= 0)).count()
)

metrics["invalid_refund_amount"] = (
    df.filter((col("payment_status") == "refunded") & (col("amount") >= 0)).count()
)

if metrics["invalid_success_amount"] > 0:
    errors.append("Invalid success payment amounts")

if metrics["invalid_failed_amount"] > 0:
    errors.append("Invalid failed payment amounts")

if metrics["invalid_refund_amount"] > 0:
    errors.append("Invalid refund amounts")

# -----------------------------
# Save Metrics
# -----------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

with open(f"metrics/payments_dq_{timestamp}.json", "w") as f:
    json.dump(metrics, f, indent=4)

# -----------------------------
# Save Logs
# -----------------------------
with open("logs/pipeline.log", "a") as log:
    log.write(f"\n[{timestamp}] SILVER PAYMENTS DQ\n")
    log.write(json.dumps(metrics))
    log.write("\n")

# -----------------------------
# Final Result
# -----------------------------
if len(errors) == 0:
    print("✅ Silver payments passed all checks")
else:
    print("❌ Data quality failed:", errors)
    raise Exception("Silver payments data quality failed")

spark.stop()
