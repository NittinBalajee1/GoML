"""Explicit end-to-end stage orchestration."""

import csv
import json
import logging
import hashlib
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .config import Settings
from .contracts import Entity, Transaction, validate_records
from .database import connect, load_and_transform
from .dlq import write_dlq
from .generator import generate_data
from .reporting import export_analytics, write_json

LOG = logging.getLogger(__name__)


def run_pipeline(
    settings: Settings, generate: bool = True, incremental: bool | None = None
) -> dict[str, Any]:
    settings.ensure_directories()
    run_id = settings.run_id or str(uuid4())
    batch_id = "pending"
    started = datetime.now(timezone.utc)
    mode = settings.incremental if incremental is None else incremental
    LOG.info(
        "Pipeline started",
        extra={"run_id": run_id, "stage": "orchestration", "event": "run_started"},
    )
    try:
        generated = (
            generate_data(
                settings.entities_path,
                settings.transactions_path,
                settings.entity_count,
                settings.transaction_count,
                settings.seed,
            )
            if generate
            else None
        )
        with settings.entities_path.open(newline="", encoding="utf-8") as stream:
            entity_records = list(csv.DictReader(stream))
        try:
            tx_records = json.loads(settings.transactions_path.read_text(encoding="utf-8"))
            if not isinstance(tx_records, list):
                raise ValueError("transactions JSON must contain an array")
        except json.JSONDecodeError as exc:
            raise ValueError(f"Malformed transaction JSON at line {exc.lineno}: {exc.msg}") from exc
        valid_entities, invalid_entities = validate_records(Entity, entity_records)
        valid_transactions, invalid_transactions = validate_records(Transaction, tx_records)
        entity_hash = hashlib.sha256(settings.entities_path.read_bytes()).hexdigest()
        transaction_hash = hashlib.sha256(settings.transactions_path.read_bytes()).hexdigest()
        batch_id = hashlib.sha256((entity_hash + transaction_hash).encode()).hexdigest()
        entities_dlq = write_dlq(
            settings.dlq_dir / "invalid_entities.json", invalid_entities, "entity", run_id, batch_id
        )
        transactions_dlq = write_dlq(
            settings.dlq_dir / "invalid_transactions.json",
            invalid_transactions,
            "transaction",
            run_id,
            batch_id,
        )
        metrics = load_and_transform(
            settings.database_path,
            valid_entities,
            valid_transactions,
            settings.entities_path,
            settings.transactions_path,
            run_id,
            mode,
        )
        metrics.update(
            {
                "run_id": run_id,
                "mode": "incremental" if mode else "full",
                "input_entities": len(entity_records),
                "valid_entities": len(valid_entities),
                "rejected_entities": len(invalid_entities),
                "input_transactions": len(tx_records),
                "valid_transactions": len(valid_transactions),
                "rejected_transactions": len(invalid_transactions),
                "duplicate_entity_rows": len(valid_entities)
                - len({str(item.entity_id) for item in valid_entities}),
                "duplicate_transaction_ids": len(valid_transactions)
                - len({str(item.transaction_id) for item in valid_transactions}),
                "invalid_email_records": sum(
                    any(error.get("loc", ())[0:1] == ("contact_email",) for error in item["errors"])
                    for item in invalid_entities
                ),
                "invalid_amount_records": sum(
                    any(
                        error.get("loc", ())[0:1] == ("transaction_amount",)
                        for error in item["errors"]
                    )
                    for item in invalid_transactions
                ),
                "missing_entity_id_records": sum(
                    any(error.get("loc", ())[0:1] == ("entity_id",) for error in item["errors"])
                    for item in invalid_transactions
                ),
                "invalid_currency_records": sum(
                    any(error.get("loc", ())[0:1] == ("currency",) for error in item["errors"])
                    for item in invalid_transactions
                ),
                "invalid_uuid_records": sum(
                    any(error.get("type") == "uuid_parsing" for error in item["errors"])
                    for item in invalid_entities + invalid_transactions
                ),
                "missing_required_field_errors": sum(
                    error.get("type") == "missing"
                    for item in invalid_entities + invalid_transactions
                    for error in item["errors"]
                ),
                "dlq_entities_written": entities_dlq,
                "dlq_transactions_written": transactions_dlq,
                "generated": generated,
                "started_at": started.isoformat(),
                "finished_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        write_json(settings.reports_dir / "data_quality_report.json", metrics)
        metrics["exports"] = export_analytics(settings.database_path, settings.reports_dir, run_id)
        con = connect(settings.database_path)
        try:
            con.execute(
                "UPDATE pipeline_runs SET details=?::JSON WHERE run_id=?",
                [json.dumps(metrics), run_id],
            )
        finally:
            con.close()
        write_json(settings.reports_dir / "latest_run.json", metrics)
        LOG.info(
            "Pipeline completed",
            extra={
                "run_id": run_id,
                "batch_id": metrics.get("batch_id"),
                "stage": "orchestration",
                "event": "run_completed",
            },
        )
        return metrics
    except Exception as exc:
        if settings.database_path.exists():
            con = None
            try:
                con = connect(settings.database_path)
                con.execute(
                    "UPDATE pipeline_runs SET finished_at=?, status='FAILED', details=?::JSON WHERE run_id=?",
                    [datetime.now(timezone.utc), json.dumps({"error": str(exc)}), run_id],
                )
            except Exception:
                LOG.exception(
                    "Could not persist failed run status",
                    extra={"run_id": run_id, "stage": "audit"},
                )
            finally:
                if con is not None:
                    con.close()
        LOG.exception(
            "Pipeline failed",
            extra={"run_id": run_id, "stage": "orchestration", "event": "run_failed"},
        )
        raise
