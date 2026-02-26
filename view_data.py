import pandas as pd

print("\n===== USER EVENTS SAMPLE =====")

df = pd.read_json("data/bronze/user_events.jsonl", lines=True)

print(df.head(10))   # show first 10 rows
print("\nTotal rows:", len(df))

print("\nColumns:")
print(df.columns)

print("\nEvent type distribution:")
print(df["event_type"].value_counts())
