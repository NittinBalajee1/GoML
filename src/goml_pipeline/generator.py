"""Deterministic CSV and JSON mock data generation."""

import csv
import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID


ENTITY_FIELDS = [
    "entity_id",
    "first_name",
    "last_name",
    "contact_email",
    "registration_date",
    "region",
]
TX_FIELDS = [
    "transaction_id",
    "entity_id",
    "category",
    "transaction_amount",
    "currency",
    "timestamp",
]
REGIONS = ["north", "South", "east", "West", "central"]


def stable_uuid(seed: int, kind: str, index: int) -> str:
    return str(UUID(int=random.Random(f"{seed}:{kind}:{index}").getrandbits(128), version=4))


def generate_data(
    entity_path: Path,
    transaction_path: Path,
    entity_count: int = 200,
    transaction_count: int = 1000,
    seed: int = 42,
) -> dict[str, int]:
    """Generate base counts plus deterministic injected defects; all parent dirs are created."""
    if entity_count < 1 or transaction_count < 1:
        raise ValueError("entity_count and transaction_count must be positive")
    rng = random.Random(seed)
    entity_path.parent.mkdir(parents=True, exist_ok=True)
    transaction_path.parent.mkdir(parents=True, exist_ok=True)
    names = [("Avery", "Shah"), ("Jordan", "Kim"), ("Morgan", "Singh"), ("Taylor", "Patel")]
    entities = []
    for i in range(entity_count):
        first, last = names[i % len(names)]
        entities.append(
            {
                "entity_id": stable_uuid(seed, "entity", i),
                "first_name": first,
                "last_name": last,
                "contact_email": f"person{i}@example.com",
                "registration_date": str(date(2022, 1, 1) + timedelta(days=i % 1400)),
                "region": REGIONS[i % len(REGIONS)],
            }
        )
    # Count is based on clean base rows: for each complete 100, append 5 exact duplicates and
    # 5 malformed-email rows. For small samples inject one each so defects remain demonstrable.
    dup_count = max(1, 5 * (entity_count // 100))
    bad_email_count = max(1, 5 * (entity_count // 100))
    duplicates = [entities[i].copy() for i in range(dup_count)]
    bad_emails = []
    for i in range(bad_email_count):
        row = entities[(i + dup_count) % entity_count].copy()
        row["entity_id"] = stable_uuid(seed, "bad-entity", i)
        row["contact_email"] = f"invalid{i}.example.com"
        bad_emails.append(row)
    all_entities = entities + duplicates + bad_emails
    with entity_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=ENTITY_FIELDS)
        writer.writeheader()
        writer.writerows(all_entities)

    transactions = []
    current_month_start = date.today().replace(day=1)
    anchor_date = current_month_start - timedelta(days=150)
    for i in range(transaction_count):
        entity = entities[rng.randrange(len(entities))]
        ts = datetime.combine(anchor_date, datetime.min.time(), timezone.utc) + timedelta(
            hours=i * 5
        )
        transactions.append(
            {
                "transaction_id": stable_uuid(seed, "transaction", i),
                "entity_id": entity["entity_id"],
                "category": ["Retail", "Travel", "Health", "Digital"][i % 4],
                "transaction_amount": round(rng.uniform(5, 1200), 2),
                "currency": ["USD", "EUR", "GBP"][i % 3],
                "timestamp": ts.isoformat(),
            }
        )
    # One deterministic defect of each category for any dataset with at least three rows.
    if transaction_count >= 3:
        transactions[0]["transaction_amount"] = -12.5
        transactions[1]["entity_id"] = None
        transactions[2]["currency"] = "CAD"
    elif transaction_count == 2:
        transactions[0]["transaction_amount"] = -12.5
        transactions[1]["entity_id"] = None
        transactions[1]["currency"] = "CAD"
    else:
        transactions[0]["transaction_amount"] = -12.5
        transactions[0]["entity_id"] = None
        transactions[0]["currency"] = "CAD"
    transaction_path.write_text(json.dumps(transactions, indent=2), encoding="utf-8")
    return {
        "entity_base": entity_count,
        "entity_rows": len(all_entities),
        "entity_duplicates": dup_count,
        "invalid_emails": bad_email_count,
        "transaction_rows": transaction_count,
        "transaction_defect_types": 3,
    }
