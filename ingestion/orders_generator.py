import json
import random
import os
from datetime import datetime, timedelta

INPUT_PATH = "data/bronze/user_events.jsonl"
OUTPUT_PATH = "data/bronze/orders.jsonl"

# FIX: Consistent base price per product so price analysis is meaningful
# product_1 will always be near the same price across orders
NUM_PRODUCTS = 1000
PRODUCT_BASE_PRICES = {
    f"product_{i}": round(random.uniform(100, 2000), 2)
    for i in range(1, NUM_PRODUCTS + 1)
}


def get_product_price(product_id):
    """Return base price Â± 5% to simulate minor price fluctuations."""
    base = PRODUCT_BASE_PRICES.get(product_id, 500.0)
    variation = base * random.uniform(-0.05, 0.05)
    return round(base + variation, 2)


def generate_order_from_purchase(event):
    quantity = random.randint(1, 3)
    price = get_product_price(event["product_id"])
    order_time = event["event_time"]

    order_status = random.choices(
        ["completed", "cancelled", "pending"],
        weights=[0.80, 0.10, 0.10]
    )[0]

    # FIX: updated_at reflects when the status was last changed
    # pending orders have the same updated_at as order_time
    if order_status == "completed":
        updated_at = (
            datetime.fromisoformat(order_time) + timedelta(minutes=random.randint(30, 1440))
        ).isoformat()
    elif order_status == "cancelled":
        updated_at = (
            datetime.fromisoformat(order_time) + timedelta(minutes=random.randint(5, 120))
        ).isoformat()
    else:
        updated_at = order_time

    return {
        "order_id": event["order_id"],
        "user_id": event["user_id"],
        "product_id": event["product_id"],
        "order_time": order_time,
        "updated_at": updated_at,          # FIX: added for SCD / audit trail
        "quantity": quantity,
        "price": price,
        "total_amount": round(price * quantity, 2),   # FIX: computed here for payments linkage
        "order_status": order_status
    }


def main():
    os.makedirs("data/bronze", exist_ok=True)

    # FIX: clear error message if upstream file is missing
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}\n"
            "Run generate_user_events.py first."
        )

    orders = []

    with open(INPUT_PATH, "r") as f:
        for line in f:
            event = json.loads(line.strip())
            if event.get("event_type") == "purchase":
                order = generate_order_from_purchase(event)
                orders.append(order)

    with open(OUTPUT_PATH, "w") as f:
        for order in orders:
            f.write(json.dumps(order) + "\n")

    print(f"Generated {len(orders)} orders â†’ {OUTPUT_PATH}")

    # Summary breakdown
    from collections import Counter
    status_counts = Counter(o["order_status"] for o in orders)
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")


if __name__ == "__main__":
    main()
