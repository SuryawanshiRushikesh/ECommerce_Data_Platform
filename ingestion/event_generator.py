import json
import random
import uuid
from datetime import datetime, timedelta
import os
from collections import Counter

OUTPUT_PATH = "data/bronze/user_events.jsonl"

# Tuned for ~70k–80k rows
NUM_USERS = 8000
NUM_PRODUCTS = 1000

USERS = [f"user_{i}" for i in range(1, NUM_USERS + 1)]
PRODUCTS = [f"product_{i}" for i in range(1, NUM_PRODUCTS + 1)]

DEVICES = ["mobile", "web"]
SOURCES = ["organic", "ad", "email"]

# Historical data across last 90 days
BASE_TIME = datetime.utcnow() - timedelta(days=90)


def generate_session_events(user_id, base_time):
    session_id = str(uuid.uuid4())
    device = random.choice(DEVICES)
    source = random.choice(SOURCES)

    events = []
    current_time = base_time

    # session start
    current_time += timedelta(seconds=random.randint(1, 30))
    events.append({
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "session_id": session_id,
        "event_type": "session_start",
        "product_id": None,
        "event_time": current_time.isoformat(),
        "device": device,
        "source": source
    })

    # 2–4 product views
    num_views = random.randint(2, 4)
    for _ in range(num_views):
        product = random.choice(PRODUCTS)
        current_time += timedelta(seconds=random.randint(10, 120))
        events.append({
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": "view_product",
            "product_id": product,
            "event_time": current_time.isoformat(),
            "device": device,
            "source": source
        })

    # 60% chance add_to_cart
    if random.random() < 0.6:
        product = random.choice(PRODUCTS)
        current_time += timedelta(seconds=random.randint(10, 120))
        events.append({
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": "add_to_cart",
            "product_id": product,
            "event_time": current_time.isoformat(),
            "device": device,
            "source": source
        })

        # 40% chance checkout
        if random.random() < 0.4:
            current_time += timedelta(seconds=random.randint(10, 120))
            events.append({
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "session_id": session_id,
                "event_type": "checkout",
                "product_id": product,
                "event_time": current_time.isoformat(),
                "device": device,
                "source": source
            })

            # 70% of checkouts become purchases
            if random.random() < 0.7:
                order_id = str(uuid.uuid4())
                current_time += timedelta(seconds=random.randint(5, 60))

                event = {
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "session_id": session_id,
                    "event_type": "purchase",
                    "product_id": product,
                    "order_id": order_id,
                    "event_time": current_time.isoformat(),
                    "device": device,
                    "source": source
                }
                events.append(event)

    return events


def main():
    os.makedirs("data/bronze", exist_ok=True)

    all_events = []

    for user in USERS:
        # 1–2 sessions per user
        num_sessions = random.randint(1, 2)

        for _ in range(num_sessions):
            session_time = BASE_TIME + timedelta(
                days=random.randint(0, 90),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )

            session_events = generate_session_events(user, session_time)
            all_events.extend(session_events)

    # sort chronologically
    all_events.sort(key=lambda e: e["event_time"])

    # overwrite file each run
    with open(OUTPUT_PATH, "w") as f:
        for event in all_events:
            f.write(json.dumps(event) + "\n")

    print(f"\nGenerated {len(all_events)} user events -> {OUTPUT_PATH}")

    # funnel summary
    counts = Counter(e["event_type"] for e in all_events)
    print("\nEvent distribution:")
    for event_type, count in sorted(counts.items()):
        print(f"{event_type}: {count}")


if __name__ == "__main__":
    main()
