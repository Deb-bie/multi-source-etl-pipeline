import os
import random
from faker import Faker # type: ignore
import csv

fake = Faker()
rows = [
    [
        "entity_id",
        "entity_type",
        "reason",
        "listed_date"
    ]
]

entity_types = ["ip", "device", "account"]
reasons = [
    "Known botnet activity",
    "Previous fraud attempt",
    "Suspicious login pattern",
    "Chargeback abuse"
]

for i in range(2000):
    entity_type = random.choice(entity_types)
    reason = random.choice(reasons)
    listed_date = fake.date_between(start_date="-180d", end_date="-60d")

    if entity_type == 'ip':
        entity_id = fake.ipv4()
    elif entity_type == 'device':
        entity_id = f"dev_{fake.random_number(digits=6)}"
    else:
        entity_id = f"acc_{fake.random_number(digits=6)}"
    
    

    rows.append([
        entity_id,
        entity_type,
        reason,
        listed_date
    ])

os.makedirs('data', exist_ok=True)
with open('data/sample_fraud_watchlist.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerows(rows)

print(f"Generated {len(rows)-1} rows in data/sample_fraud_watchlist.csv")



