from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
import os
from datetime import datetime

# -----------------------------
# Ensure folders exist (production safety)
# -----------------------------
os.makedirs("metrics", exist_ok=True)
os.makedirs("logs", exist_ok=True)

# -----------------------------
# Start Spark
# -----------------------------
spark = SparkSession.builder.appName("DQ_Silver_UserEvents").getOrCreate()

print("\n=== Running Data Quality Checks: Silver User Events ===")

df = spark.read.parquet("data/silver/user_events")

metrics = {}
errors = []

# -----------------------------
# Row Count
# -----------------------------
metrics["row_count"] = df.count()

# -----------------------------
# Required Field Checks
# -----------------------------
required_cols = [
    "event_id",
    "user_id",
    "session_id",
    "event_time",
    "event_type"
]

for c in required_cols:
    null_count = df.filter(col(c).isNull()).count()
    metrics[f"null_{c}"] = null_count

    if null_count > 0:
        errors.append(f"NULL values found in {c}")

# -----------------------------
# Duplicate Check
# -----------------------------
metrics["duplicate_event_id"] = (
    df.groupBy("event_id").count().filter("count > 1").count()
)

if metrics["duplicate_event_id"] > 0:
    errors.append("Duplicate event_id found")

# -----------------------------
# Valid Event Types
# -----------------------------
valid_events = [
    "session_start",
    "view_product",
    "add_to_cart",
    "checkout",
    "purchase"
]

metrics["invalid_event_types"] = (
    df.filter(~col("event_type").isin(valid_events)).count()
)

if metrics["invalid_event_types"] > 0:
    errors.append("Invalid event_type values found")

# -----------------------------
# Valid Device Values
# -----------------------------
valid_devices = ["mobile", "web"]

metrics["invalid_devices"] = (
    df.filter(~col("device").isin(valid_devices)).count()
)

if metrics["invalid_devices"] > 0:
    errors.append("Invalid device values found")

# -----------------------------
# Valid Source Values
# -----------------------------
valid_sources = ["organic", "ad", "email"]

metrics["invalid_sources"] = (
    df.filter(~col("source").isin(valid_sources)).count()
)

if metrics["invalid_sources"] > 0:
    errors.append("Invalid source values found")

# -----------------------------
# Save Metrics
# -----------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

with open(f"metrics/user_events_dq_{timestamp}.json", "w") as f:
    json.dump(metrics, f, indent=4)

# -----------------------------
# Save Logs
# -----------------------------
with open("logs/pipeline.log", "a") as log:
    log.write(f"\n[{timestamp}] SILVER USER EVENTS DQ\n")
    log.write(json.dumps(metrics))
    log.write("\n")

# -----------------------------
# Final Result
# -----------------------------
if len(errors) == 0:
    print("✅ Silver user events passed all checks")
else:
    print("❌ Data quality failed:", errors)
    raise Exception("Silver user events data quality failed")

spark.stop()
