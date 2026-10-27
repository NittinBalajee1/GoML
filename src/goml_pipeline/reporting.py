"""Machine-readable run quality and SQL analytics exports."""

import csv
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from .database import query_analytics

LOG = logging.getLogger(__name__)


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def export_analytics(
    db_path: Path, reports_dir: Path, run_id: str, as_of: date | None = None
) -> dict[str, str]:
    reports_dir.mkdir(parents=True, exist_ok=True)
    results = query_analytics(db_path, as_of)
    manifest = {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "queries": {},
    }
    for name, rows in results.items():
        path = reports_dir / f"{name}.csv"
        headers = list(rows[0]) if rows else _empty_headers(name)
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        manifest["queries"][name] = {"file": path.name, "rows": len(rows)}
    write_json(reports_dir / "analytics_manifest.json", manifest)
    LOG.info(
        "Analytics CSV exports complete",
        extra={
            "run_id": run_id,
            "stage": "reporting",
            "event": "exports_written",
            "count": len(results),
        },
    )
    return {name: str(reports_dir / f"{name}.csv") for name in results}


def _empty_headers(name: str) -> list[str]:
    mapping = {
        "monthly_revenue_by_category": ["category", "currency", "total_transaction_amount"],
        "top_regions_average_per_entity": [
            "region",
            "currency",
            "average_transaction_amount_per_entity",
        ],
        "transaction_count_amount_by_region": [
            "region",
            "currency",
            "transaction_count",
            "total_amount",
        ],
        "monthly_transaction_volume": [
            "year",
            "month",
            "month_name",
            "currency",
            "transaction_count",
        ],
        "top_entities_by_amount": [
            "entity_id",
            "first_name",
            "last_name",
            "currency",
            "total_amount",
        ],
        "average_amount_and_count_by_entity": [
            "entity_id",
            "region",
            "currency",
            "average_transaction_amount",
            "transaction_count",
        ],
        "category_distribution": ["category", "currency", "transaction_count", "total_amount"],
        "daily_transaction_activity": [
            "full_date",
            "currency",
            "transaction_count",
            "total_amount",
        ],
        "new_entity_registrations_by_month": ["year", "month", "region", "new_entities"],
        "transaction_amounts_by_currency": [
            "currency",
            "transaction_count",
            "total_amount",
            "average_amount",
        ],
        "entity_activity_distribution": [
            "region",
            "entity_count",
            "active_entities",
            "transaction_count",
        ],
        "data_quality_summary": ["status", "run_count"],
    }
    return mapping[name]
