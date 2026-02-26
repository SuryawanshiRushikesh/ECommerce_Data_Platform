import json
import uuid
import random
import os
from datetime import datetime, timedelta

INPUT_PATH = "data/bronze/orders.jsonl"
OUTPUT_PATH = "data/bronze/payments.jsonl"

PAYMENT_METHODS = ["card", "upi", "netbanking"]


def generate_payment_from_order(order):
    order_status = order["order_status"]
    order_time = order["order_time"]
    total_amount = order["total_amount"]

    # Completed orders â†’ successful payment
    if order_status == "completed":
        payment_status = "success"
        amount = total_amount
        payment_time = (
            datetime.fromisoformat(order_time) +
            timedelta(minutes=random.randint(1, 30))
        ).isoformat()

    # Cancelled orders â†’ payment failed before capture, no refund needed
    # FIX: refunded doesn't make sense here â€” payment never went through
    elif order_status == "cancelled":
        payment_status = "failed"
        amount = total_amount
        payment_time = (
            datetime.fromisoformat(order_time) +
            timedelta(minutes=random.randint(1, 15))
        ).isoformat()

    # Pending orders â†’ no payment record yet
    else:
        return None

    # FIX: ~8% of successful payments later get refunded (returned orders)
    # This is a separate follow-up record, not a replacement
    records = []

    base_payment = {
        "transaction_id": str(uuid.uuid4()),
        "order_id": order["order_id"],
        "user_id": order["user_id"],
        "payment_time": payment_time,
        "amount": amount,
        "currency": "INR",                          # FIX: added currency
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": payment_status
    }
    records.append(base_payment)

    # FIX: generate a separate refund record for ~8% of successful payments
    if payment_status == "success" and random.random() < 0.08:
        refund_amount = round(
            total_amount * random.choice([1.0, 0.75, 0.5]),  # full or partial refund
            2
        )
        refund_time = (
            datetime.fromisoformat(payment_time) +
            timedelta(hours=random.randint(12, 72))
        ).isoformat()
        records.append({
            "transaction_id": str(uuid.uuid4()),
            "order_id": order["order_id"],
            "user_id": order["user_id"],
            "payment_time": refund_time,
            "amount": -refund_amount,               # negative to signal refund
            "currency": "INR",
            "payment_method": base_payment["payment_method"],
            "payment_status": "refunded"
        })

    return records


def main():
    # FIX: ensure output dir exists
    os.makedirs("data/bronze", exist_ok=True)

    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}\nRun generate_orders.py first."
        )

    payments = []

    with open(INPUT_PATH, "r") as f:
        for line in f:
            order = json.loads(line.strip())
            records = generate_payment_from_order(order)
            if records:
                payments.extend(records)

    with open(OUTPUT_PATH, "w") as f:
        for p in payments:
            f.write(json.dumps(p) + "\n")

    print(f"Generated {len(payments)} payment records â†’ {OUTPUT_PATH}")

    from collections import Counter
    status_counts = Counter(p["payment_status"] for p in payments)
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")


if __name__ == "__main__":
    main()
