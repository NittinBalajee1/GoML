"""DuckDB loading, transformations, star schema checks and analytics."""

import hashlib
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
from .contracts import Entity, Transaction

LOG = logging.getLogger(__name__)
_SOURCE_SQL_DIR = Path(__file__).resolve().parents[2] / "sql"
# In a source checkout, SQL lives at the repository root. In the Docker image,
# the package is installed under site-packages while SQL is copied to /app/sql.
SQL_DIR = _SOURCE_SQL_DIR if _SOURCE_SQL_DIR.is_dir() else Path("/app/sql")


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def connect(path: Path) -> duckdb.DuckDBPyConnection:
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def _exec_sql_file(con: duckdb.DuckDBPyConnection, name: str) -> None:
    con.execute((SQL_DIR / name).read_text(encoding="utf-8"))


def load_and_transform(
    db_path: Path,
    entities: list[Entity],
    transactions: list[Transaction],
    entity_source: Path,
    tx_source: Path,
    run_id: str,
    incremental: bool,
) -> dict[str, Any]:
    con = connect(db_path)
    try:
        _exec_sql_file(con, "bronze.sql")
        con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs(run_id VARCHAR PRIMARY KEY, batch_id VARCHAR,
          started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, mode VARCHAR, status VARCHAR, details JSON)""")
        con.execute(
            "CREATE TABLE IF NOT EXISTS processed_batches(batch_id VARCHAR PRIMARY KEY, entity_hash VARCHAR, transaction_hash VARCHAR, processed_at TIMESTAMPTZ)"
        )
        batch_id = hashlib.sha256(
            (file_hash(entity_source) + file_hash(tx_source)).encode()
        ).hexdigest()
        if (
            incremental
            and con.execute(
                "SELECT count(*) FROM processed_batches WHERE batch_id=?", [batch_id]
            ).fetchone()[0]
        ):
            started = datetime.now(timezone.utc)
            metrics = {
                "batch_id": batch_id,
                "skipped_batch": True,
                "inserted_entities": 0,
                "inserted_transactions": 0,
                "silver_entities": con.execute("SELECT count(*) FROM silver_entities").fetchone()[
                    0
                ],
                "silver_transactions": con.execute(
                    "SELECT count(*) FROM silver_transactions"
                ).fetchone()[0],
                "orphan_transactions": con.execute(
                    "SELECT count(*) FROM rejected_orphan_transactions"
                ).fetchone()[0],
                "fact_transactions": con.execute(
                    "SELECT count(*) FROM fact_transactions"
                ).fetchone()[0],
                "integrity": validate_star(con),
            }
            con.execute(
                "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, 'incremental', 'SUCCEEDED', ?::JSON)",
                [run_id, batch_id, started, datetime.now(timezone.utc), json.dumps(metrics)],
            )
            return metrics
        eh, th = file_hash(entity_source), file_hash(tx_source)
        start = datetime.now(timezone.utc)
        con.execute(
            "INSERT INTO pipeline_runs VALUES (?, ?, ?, NULL, ?, 'RUNNING', '{}'::JSON)",
            [run_id, batch_id, start, "incremental" if incremental else "full"],
        )
        try:
            con.execute("BEGIN TRANSACTION")
            if not incremental:
                for table in ("bronze_entities", "bronze_transactions"):
                    con.execute(f"DELETE FROM {table}")
                con.execute("DELETE FROM processed_batches")
            before_e = con.execute("SELECT count(*) FROM bronze_entities").fetchone()[0]
            before_t = con.execute("SELECT count(*) FROM bronze_transactions").fetchone()[0]
            ordered_entities = sorted(
                entities,
                key=lambda row: hashlib.sha256(
                    json.dumps(row.model_dump(mode="json"), sort_keys=True).encode()
                ).hexdigest(),
            )
            for row in ordered_entities:
                data = row.model_dump(mode="json")
                h = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
                existing = con.execute(
                    "SELECT source_hash FROM bronze_entities WHERE entity_id=? LIMIT 1",
                    [str(row.entity_id)],
                ).fetchone()
                if existing and existing[0] != h and incremental:
                    raise ValueError(f"Conflicting content for entity_id {row.entity_id}")
                if not existing or not incremental:
                    con.execute(
                        "INSERT INTO bronze_entities VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        [
                            str(row.entity_id),
                            row.first_name,
                            row.last_name,
                            str(row.contact_email),
                            row.registration_date,
                            row.region,
                            h,
                            start.replace(tzinfo=None),
                        ],
                    )
            for row in transactions:
                data = row.model_dump(mode="json")
                h = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
                existing = con.execute(
                    "SELECT source_hash FROM bronze_transactions WHERE transaction_id=? LIMIT 1",
                    [str(row.transaction_id)],
                ).fetchone()
                if existing and existing[0] != h:
                    raise ValueError(f"Conflicting content for transaction_id {row.transaction_id}")
                if not existing or not incremental:
                    con.execute(
                        "INSERT INTO bronze_transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        [
                            str(row.transaction_id),
                            str(row.entity_id),
                            row.category,
                            row.transaction_amount,
                            row.currency,
                            row.timestamp,
                            h,
                            start.replace(tzinfo=None),
                        ],
                    )
            _exec_sql_file(con, "silver.sql")
            _exec_sql_file(con, "gold.sql")
            integrity = validate_star(con)
            after_e = con.execute("SELECT count(*) FROM bronze_entities").fetchone()[0]
            after_t = con.execute("SELECT count(*) FROM bronze_transactions").fetchone()[0]
            metrics = {
                "batch_id": batch_id,
                "skipped_batch": False,
                "inserted_entities": after_e - before_e,
                "inserted_transactions": after_t - before_t,
                "silver_entities": con.execute("SELECT count(*) FROM silver_entities").fetchone()[
                    0
                ],
                "silver_transactions": con.execute(
                    "SELECT count(*) FROM silver_transactions"
                ).fetchone()[0],
                "orphan_transactions": con.execute(
                    "SELECT count(*) FROM rejected_orphan_transactions"
                ).fetchone()[0],
                "fact_transactions": con.execute(
                    "SELECT count(*) FROM fact_transactions"
                ).fetchone()[0],
                "integrity": integrity,
            }
            con.execute(
                "INSERT OR REPLACE INTO processed_batches VALUES (?, ?, ?, ?)",
                [batch_id, eh, th, datetime.now(timezone.utc)],
            )
            con.execute(
                "UPDATE pipeline_runs SET finished_at=?,status='SUCCEEDED',details=?::JSON WHERE run_id=?",
                [datetime.now(timezone.utc), json.dumps(metrics), run_id],
            )
            con.execute("COMMIT")
            return metrics
        except Exception as exc:
            con.execute("ROLLBACK")
            con.execute(
                "UPDATE pipeline_runs SET finished_at=?,status='FAILED',details=?::JSON WHERE run_id=?",
                [datetime.now(timezone.utc), json.dumps({"error": str(exc)}), run_id],
            )
            raise
    finally:
        con.close()


def validate_star(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    checks = {
        "duplicate_entity_business_keys": "SELECT count(*) FROM (SELECT entity_id FROM dim_entity GROUP BY entity_id HAVING count(*)>1)",
        "duplicate_entity_surrogate_keys": "SELECT count(*) FROM (SELECT entity_key FROM dim_entity GROUP BY entity_key HAVING count(*)>1)",
        "duplicate_date_keys": "SELECT count(*) FROM (SELECT date_key FROM dim_date GROUP BY date_key HAVING count(*)>1)",
        "orphan_entity_facts": "SELECT count(*) FROM fact_transactions f LEFT JOIN dim_entity e USING(entity_key) WHERE e.entity_key IS NULL",
        "orphan_date_facts": "SELECT count(*) FROM fact_transactions f LEFT JOIN dim_date d USING(date_key) WHERE d.date_key IS NULL",
        "null_fact_keys": "SELECT count(*) FROM fact_transactions WHERE entity_key IS NULL OR date_key IS NULL",
        "fact_reconciliation": "SELECT abs((SELECT count(*) FROM fact_transactions)-(SELECT count(*) FROM silver_transactions s JOIN silver_entities e USING(entity_id)))",
    }
    result = {key: int(con.execute(query).fetchone()[0]) for key, query in checks.items()}
    if any(result.values()):
        raise ValueError(f"Gold star-schema integrity check failed: {result}")
    return result


def _named_analytics_from_sql() -> dict[str, str]:
    """Read named mandatory analytics from the canonical SQL file."""
    named: dict[str, str] = {}
    current: str | None = None
    statement: list[str] = []
    for line in (SQL_DIR / "analytics.sql").read_text(encoding="utf-8").splitlines():
        marker = line.strip()
        if marker.lower().startswith("-- query:"):
            if current is not None:
                named[current] = " ".join(statement).strip().rstrip(";")
            current = marker.split(":", 1)[1].strip()
            statement = []
        elif current is not None and marker and not marker.startswith("--"):
            statement.append(marker)
    if current is not None:
        named[current] = " ".join(statement).strip().rstrip(";")
    if not {"monthly_revenue_by_category", "top_regions_average_per_entity"}.issubset(named):
        raise ValueError("sql/analytics.sql is missing one of the mandatory named queries")
    return named


ANALYTICS: dict[str, str] = {
    **_named_analytics_from_sql(),
    "transaction_count_amount_by_region": "SELECT e.region,f.currency,count(*) transaction_count,sum(f.transaction_amount) total_amount FROM fact_transactions f JOIN dim_entity e USING(entity_key) GROUP BY e.region,f.currency ORDER BY e.region,f.currency",
    "monthly_transaction_volume": 'SELECT d."year",d."month",d.month_name,f.currency,count(*) transaction_count FROM fact_transactions f JOIN dim_date d USING(date_key) GROUP BY d."year",d."month",d.month_name,f.currency ORDER BY d."year",d."month",f.currency',
    "top_entities_by_amount": "WITH amounts AS (SELECT e.entity_id,e.first_name,e.last_name,f.currency,sum(f.transaction_amount) total_amount FROM fact_transactions f JOIN dim_entity e USING(entity_key) GROUP BY e.entity_id,e.first_name,e.last_name,f.currency) SELECT entity_id,first_name,last_name,currency,total_amount FROM amounts QUALIFY row_number() OVER (PARTITION BY currency ORDER BY total_amount DESC,entity_id)<=5 ORDER BY currency,total_amount DESC",
    "category_distribution": "SELECT category,currency,count(*) transaction_count,sum(transaction_amount) total_amount FROM fact_transactions GROUP BY category,currency ORDER BY category,currency",
    "daily_transaction_activity": "SELECT d.full_date,f.currency,count(*) transaction_count,sum(f.transaction_amount) total_amount FROM fact_transactions f JOIN dim_date d USING(date_key) GROUP BY d.full_date,f.currency ORDER BY d.full_date,f.currency",
    "new_entity_registrations_by_month": 'SELECT year(registration_date) AS "year",month(registration_date) AS "month",region,count(*) new_entities FROM dim_entity GROUP BY 1,2,3 ORDER BY 1,2,3',
    "transaction_amounts_by_currency": "SELECT currency,count(*) transaction_count,sum(transaction_amount) total_amount,avg(transaction_amount) average_amount FROM fact_transactions GROUP BY currency ORDER BY currency",
    "average_amount_and_count_by_entity": "SELECT e.entity_id,e.region,f.currency,avg(f.transaction_amount) average_transaction_amount,count(*) transaction_count FROM fact_transactions f JOIN dim_entity e USING(entity_key) GROUP BY e.entity_id,e.region,f.currency ORDER BY e.entity_id,f.currency",
    "entity_activity_distribution": "SELECT e.region,count(DISTINCT e.entity_key) entity_count,count(DISTINCT f.entity_key) active_entities,count(f.transaction_id) transaction_count FROM dim_entity e LEFT JOIN fact_transactions f USING(entity_key) GROUP BY e.region ORDER BY e.region",
    "data_quality_summary": "SELECT status,count(*) run_count FROM pipeline_runs GROUP BY status ORDER BY status",
}


def query_analytics(db_path: Path, as_of: date | None = None) -> dict[str, list[dict[str, Any]]]:
    con = connect(db_path)
    try:
        result = {}
        for name, sql in ANALYTICS.items():
            cursor = con.execute(
                sql,
                [as_of or date.today(), as_of or date.today()]
                if name == "monthly_revenue_by_category"
                else [],
            )
            cols = [item[0] for item in cursor.description]
            result[name] = [dict(zip(cols, row)) for row in cursor.fetchall()]
        return result
    finally:
        con.close()
