import subprocess
import sys
import os
import logging
from datetime import datetime

# -----------------------------
# Logging Setup
# FIX: logs to both console and file for debugging failed runs
# -----------------------------
os.makedirs("logs", exist_ok=True)
log_file = f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file)
    ]
)
log = logging.getLogger(__name__)

# -----------------------------
# Config
# -----------------------------
MAX_RETRIES = 1     # FIX: retry once on transient failures before giving up

# -----------------------------
# Step Runner
# FIX: retry logic, list-based commands, full summary tracking
# -----------------------------
step_results = []   # FIX: collect per-step stats for final summary

def run_step(name, command):
    log.info(f"===== Running: {name} =====")
    start = datetime.now()

    # FIX: split command into list to avoid shell=True security risk
    cmd = command if isinstance(command, list) else command.split()

    for attempt in range(1, MAX_RETRIES + 2):  # +2 → initial try + retries
        result = subprocess.run(cmd)
        duration = round((datetime.now() - start).total_seconds(), 2)

        if result.returncode == 0:
            log.info(f"✅ Completed: {name} ({duration}s)")
            step_results.append({"step": name, "status": "SUCCESS", "duration": duration})
            return

        if attempt <= MAX_RETRIES:
            log.warning(f"⚠️  Attempt {attempt} failed for: {name} — retrying...")
        else:
            log.error(f"❌ FAILED: {name} after {attempt} attempt(s) ({duration}s)")
            step_results.append({"step": name, "status": "FAILED", "duration": duration})
            _print_summary()
            sys.exit(1)


def _print_summary():
    """FIX: print per-step duration table at end for bottleneck analysis."""
    log.info("\n========== PIPELINE SUMMARY ==========")
    log.info(f"{'Step':<45} {'Status':<10} {'Duration':>10}")
    log.info("-" * 68)
    for r in step_results:
        log.info(f"{r['step']:<45} {r['status']:<10} {r['duration']:>8.2f}s")
    total = sum(r["duration"] for r in step_results)
    log.info("-" * 68)
    log.info(f"{'TOTAL':<45} {'':<10} {total:>8.2f}s")
    log.info(f"Log saved to: {log_file}")


# -----------------------------
# PIPELINE EXECUTION ORDER
# -----------------------------
pipeline_start = datetime.now()
log.info(f"\n🚀 Pipeline started at {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}\n")

# 1️⃣ Ingestion
run_step("Generate User Events", "python ingestion/event_generator.py")
run_step("Generate Orders",      "python ingestion/orders_generator.py")
run_step("Generate Payments",    "python ingestion/payments_generator.py")

# 2️⃣ Bronze → Silver
run_step("Silver User Events", "spark-submit spark_jobs/bronze_to_silver_users.py")
run_step("Silver Orders",      "spark-submit spark_jobs/bronze_to_silver_orders.py")
run_step("Silver Payments",    "spark-submit spark_jobs/bronze_to_silver_payments.py")

# 3️⃣ Data Quality Checks
run_step("DQ User Events", "spark-submit data_quality/check_user_events.py")
run_step("DQ Orders",      "spark-submit data_quality/check_orders.py")
run_step("DQ Payments",    "spark-submit data_quality/check_payments.py")

# 4️⃣ Gold Layer
run_step("Gold Events Daily",      "spark-submit spark_jobs/gold_events_daily.py")
run_step("Gold Orders Daily",      "spark-submit spark_jobs/gold_orders_daily.py")
run_step("Gold Payments Daily",    "spark-submit spark_jobs/gold_payments_daily.py")
run_step("Gold Business Metrics",  "spark-submit spark_jobs/gold_daily_business_metrics.py")
run_step("Gold ML Features",       "spark-submit spark_jobs/gold_user_daily_features.py")

# -----------------------------
# Final Summary
# -----------------------------
total_duration = round((datetime.now() - pipeline_start).total_seconds(), 2)
_print_summary()
log.info(f"\n🏁 PIPELINE COMPLETED SUCCESSFULLY in {total_duration}s\n")

