import csv
import json
from collections import Counter
from datetime import date
from pathlib import Path
import pytest
from pydantic import ValidationError
from goml_pipeline.contracts import Entity, Transaction, validate_records
from goml_pipeline.generator import generate_data
from goml_pipeline.orchestration import run_pipeline
from goml_pipeline.database import connect, query_analytics
from goml_pipeline.dlq import write_dlq
from goml_pipeline.database import ANALYTICS, validate_star
import duckdb


def entity_payload(**overrides):
    value = {
        "entity_id": "550e8400-e29b-41d4-a716-446655440000",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "contact_email": "ada@example.com",
        "registration_date": "2025-01-01",
        "region": "north",
    }
    value.update(overrides)
    return value


def transaction_payload(**overrides):
    value = {
        "transaction_id": "550e8400-e29b-41d4-a716-446655440010",
        "entity_id": "550e8400-e29b-41d4-a716-446655440000",
        "category": "Retail",
        "transaction_amount": "12.50",
        "currency": "USD",
        "timestamp": "2026-10-05T12:00:00Z",
    }
    value.update(overrides)
    return value


def test_contract_rejections():
    with pytest.raises(ValidationError):
        Entity.model_validate(entity_payload(contact_email="bad"))
    for bad in (
        {"transaction_amount": -1},
        {"entity_id": None},
        {"transaction_amount": True},
        {"transaction_amount": "NaN"},
        {"currency": "CAD"},
    ):
        with pytest.raises(ValidationError):
            Transaction.model_validate(transaction_payload(**bad))
    for bad in (
        {"transaction_id": "invalid"},
        {"category": ""},
        {"timestamp": "not-a-date"},
        {"transaction_amount": 0},
        {"transaction_amount": "Infinity"},
    ):
        with pytest.raises(ValidationError):
            Transaction.model_validate(transaction_payload(**bad))
    for bad in (
        {"entity_id": "not-a-uuid"},
        {"first_name": " "},
        {"last_name": ""},
        {"region": " "},
        {"registration_date": "not-a-date"},
    ):
        with pytest.raises(ValidationError):
            Entity.model_validate(entity_payload(**bad))


def test_generator_contract_defects_and_repeatability(tmp_path):
    e1, t1 = tmp_path / "a/e.csv", tmp_path / "a/t.json"
    e2, t2 = tmp_path / "b/e.csv", tmp_path / "b/t.json"
    counts = generate_data(e1, t1, 200, 20, 17)
    generate_data(e2, t2, 200, 20, 17)
    assert e1.read_bytes() == e2.read_bytes() and t1.read_bytes() == t2.read_bytes()
    with e1.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 220
    assert Counter(tuple(r[k] for k in rows[0]) for r in rows).most_common(1)[0][1] >= 2
    assert (
        sum("@" not in r["contact_email"] or r["contact_email"].count("@") != 1 for r in rows) >= 10
    )
    tx = json.loads(t1.read_text(encoding="utf-8"))
    assert any(x["transaction_amount"] < 0 for x in tx)
    assert any(x["entity_id"] is None for x in tx)
    assert any(x["currency"] == "CAD" for x in tx)
    assert counts["entity_rows"] == len(rows)


def test_generator_small_inputs_keep_all_defect_types(tmp_path):
    entities, transactions = tmp_path / "entities.csv", tmp_path / "transactions.json"
    generate_data(entities, transactions, 1, 1, 1)
    row = json.loads(transactions.read_text(encoding="utf-8"))[0]
    assert row["transaction_amount"] < 0
    assert row["entity_id"] is None
    assert row["currency"] == "CAD"


def test_dlq_preserves_missing_null_and_accumulates(settings):
    p = settings.root / "dlq/invalid.json"
    _, invalid = validate_records(
        Transaction, [transaction_payload(entity_id=None), {"transaction_id": "bad"}]
    )
    assert write_dlq(p, invalid, "transaction", "run-a", "batch-a") == 2
    write_dlq(p, invalid[:1], "transaction", "run-b", "batch-a")
    result = json.loads(p.read_text(encoding="utf-8"))
    assert len(result) == 2 and result[0]["record"]["entity_id"] is None
    assert "entity_id" not in result[1]["record"] and result[0]["errors"]


def test_pipeline_end_to_end_and_incremental(settings):
    settings = type(settings)(root=settings.root, entity_count=100, transaction_count=50, seed=33)
    result = run_pipeline(settings)
    assert result["rejected_entities"] >= 5 and result["rejected_transactions"] == 3
    assert result["integrity"] and result["fact_transactions"] > 0
    second = run_pipeline(settings, incremental=True)
    assert second["skipped_batch"] is True
    assert second["fact_transactions"] == 47
    assert len(query_analytics(settings.database_path)) == 12
    con = connect(settings.database_path)
    try:
        assert con.execute("select count(*) from bronze_entities").fetchone()[0] == 105
        assert con.execute("select count(*) from bronze_transactions").fetchone()[0] == 47
        assert con.execute("select count(*) from fact_transactions").fetchone()[0] == 47
        assert con.execute("select count(*) from silver_entities").fetchone()[0] == 100
        assert con.execute("select count(*) from pipeline_runs").fetchone()[0] == 2
        audit = json.loads(
            con.execute(
                "select details from pipeline_runs order by started_at desc limit 1"
            ).fetchone()[0]
        )
        assert len(audit["exports"]) == 12
        sql = (Path(__file__).parents[1] / "sql/silver.sql").read_text(encoding="utf-8")
        assert "row_number()" in sql.lower()
    finally:
        con.close()

    quality = json.loads(
        (settings.reports_dir / "data_quality_report.json").read_text(encoding="utf-8")
    )
    assert quality["duplicate_entity_rows"] == 5
    assert quality["invalid_email_records"] == 5
    dlq_entries = json.loads(
        (settings.dlq_dir / "invalid_entities.json").read_text(encoding="utf-8")
    )
    assert all(
        entry["batch_id"] == quality["batch_id"]
        for entry in dlq_entries
        if entry["run_id"] == quality["run_id"]
    )
    assert (
        len(
            list(
                csv.DictReader(
                    (settings.reports_dir / "category_distribution.csv").open(encoding="utf-8")
                )
            )
        )
        > 0
    )


def test_required_analytics_known_answers():
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE dim_entity(entity_key INTEGER, region VARCHAR)")
        con.execute("INSERT INTO dim_entity VALUES (1,'R1'),(2,'R1'),(3,'R2')")
        con.execute(
            "CREATE TABLE fact_transactions(entity_key INTEGER, category VARCHAR, currency VARCHAR, transaction_amount DECIMAL(10,2), timestamp TIMESTAMPTZ)"
        )
        con.execute("""INSERT INTO fact_transactions VALUES
            (1,'Retail','USD',10,'2026-10-01T00:00:00Z'), (1,'Retail','USD',30,'2026-10-31T23:00:00Z'),
            (2,'Retail','USD',100,'2026-10-15T00:00:00Z'), (3,'Retail','USD',40,'2026-10-10T00:00:00Z'),
            (3,'Retail','USD',60,'2026-10-20T00:00:00Z'), (2,'Retail','USD',500,'2026-09-30T23:00:00Z'),
            (2,'Retail','USD',700,'2026-11-01T00:00:00Z')""")
        monthly = con.execute(
            ANALYTICS["monthly_revenue_by_category"], [date(2026, 10, 27), date(2026, 10, 27)]
        ).fetchall()
        assert monthly == [("Retail", "USD", 240)]
        top = con.execute(ANALYTICS["top_regions_average_per_entity"]).fetchall()
        assert top[0][0] == "R1" and float(top[0][2]) == pytest.approx(680 / 3)
        assert float(top[1][2]) == 50
    finally:
        con.close()


def test_incremental_new_batch_and_conflicting_entity(settings):
    from goml_pipeline.generator import generate_data

    settings = type(settings)(root=settings.root, entity_count=40, transaction_count=20, seed=51)
    first = run_pipeline(settings)
    generate_data(settings.entities_path, settings.transactions_path, 30, 12, 52)
    second = run_pipeline(settings, generate=False, incremental=True)
    assert second["inserted_entities"] == 30
    assert second["inserted_transactions"] == 9
    assert second["fact_transactions"] == first["fact_transactions"] + 9

    with settings.entities_path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    rows[0]["first_name"] = "Changed"
    with settings.entities_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    failed_settings = type(settings)(root=settings.root, run_id="conflict-run")
    with pytest.raises(ValueError, match="Conflicting content for entity_id"):
        run_pipeline(failed_settings, generate=False, incremental=True)
    con = connect(settings.database_path)
    try:
        assert (
            con.execute("SELECT status FROM pipeline_runs WHERE run_id='conflict-run'").fetchone()[
                0
            ]
            == "FAILED"
        )
    finally:
        con.close()


def test_star_integrity_detects_corrupt_mappings():
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE dim_entity(entity_key INTEGER, entity_id INTEGER)")
        con.execute("INSERT INTO dim_entity VALUES (1,10),(2,10)")
        con.execute("CREATE TABLE dim_date(date_key INTEGER)")
        con.execute("INSERT INTO dim_date VALUES (20261001),(20261001)")
        con.execute(
            "CREATE TABLE fact_transactions(transaction_id INTEGER, entity_key INTEGER, date_key INTEGER)"
        )
        con.execute("INSERT INTO fact_transactions VALUES (1,99,99999999)")
        con.execute("CREATE TABLE silver_transactions(transaction_id INTEGER, entity_id INTEGER)")
        con.execute("CREATE TABLE silver_entities(entity_id INTEGER)")
        with pytest.raises(ValueError, match="integrity check failed"):
            validate_star(con)
    finally:
        con.close()


def test_configurable_path_environment(settings, monkeypatch):
    monkeypatch.setenv("GOML_RAW_DIR", "custom/raw")
    monkeypatch.setenv("GOML_DATABASE_PATH", "custom/warehouse.duckdb")
    assert settings.raw_dir == settings.root / "custom/raw"
    assert settings.database_path == settings.root / "custom/warehouse.duckdb"


def test_contract_valid_orphan_is_audited_and_excluded(settings):
    settings = type(settings)(root=settings.root, entity_count=20, transaction_count=10, seed=71)
    from goml_pipeline.generator import generate_data

    generate_data(settings.entities_path, settings.transactions_path, 20, 10, 71)
    transactions = json.loads(settings.transactions_path.read_text(encoding="utf-8"))
    transactions[-1]["entity_id"] = "550e8400-e29b-41d4-a716-446655440099"
    settings.transactions_path.write_text(json.dumps(transactions), encoding="utf-8")
    result = run_pipeline(settings, generate=False)
    assert result["orphan_transactions"] == 1
    assert result["fact_transactions"] == result["valid_transactions"] - 1
    con = connect(settings.database_path)
    try:
        reason = con.execute("SELECT reason FROM rejected_orphan_transactions").fetchone()[0]
        assert reason == "entity_id_not_found_in_silver_entities"
    finally:
        con.close()
