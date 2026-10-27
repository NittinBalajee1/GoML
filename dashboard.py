"""Local Streamlit dashboard for the goML DuckDB pipeline."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from goml_pipeline.config import Settings
from goml_pipeline.database import ANALYTICS, connect, query_analytics
from goml_pipeline.orchestration import run_pipeline


ROOT = Path(__file__).resolve().parent
SETTINGS = Settings(root=ROOT)
REPORTS = SETTINGS.reports_dir
DATABASE = SETTINGS.database_path

QUERY_HELP = {
    "monthly_revenue_by_category": "Monthly amount by category and currency.",
    "top_regions_average_per_entity": "Ranks regions by the average of each entity's transaction average.",
    "transaction_count_amount_by_region": "Transaction count and total amount for each region and currency.",
    "monthly_transaction_volume": "Transaction counts by month and currency.",
    "top_entities_by_amount": "Top entities by amount, ranked separately for each currency.",
    "category_distribution": "Transaction mix and amount by category and currency.",
    "daily_transaction_activity": "Daily transaction counts and amounts.",
    "new_entity_registrations_by_month": "New entity registrations by month and region.",
    "transaction_amounts_by_currency": "Transaction count, total, and average amount by currency.",
    "average_amount_and_count_by_entity": "Average transaction amount and count for each entity.",
    "entity_activity_distribution": "Active entities and transaction volume by region.",
    "data_quality_summary": "Successful and failed pipeline run counts.",
}

st.set_page_config(
    page_title="goML | Data Pipeline",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
      .block-container {max-width: 1440px; padding-top: 2rem; padding-bottom: 3rem;}
      [data-testid="stSidebar"] {border-right: 1px solid #e5eaf0;}
      [data-testid="stMetric"] {background:#fff; border:1px solid #e5eaf0;
        padding:18px 20px; border-radius:14px; box-shadow:0 2px 8px rgba(16,42,67,.035);}
      .eyebrow {color:#0f766e; font-size:.75rem; font-weight:700; letter-spacing:.12em;
        text-transform:uppercase; margin-bottom:.4rem;}
      .muted {color:#627d98; font-size:.95rem;}
      .flow-step {background:#fff; border:1px solid #e5eaf0; border-radius:12px;
        padding:14px; text-align:center; font-weight:650; color:#243b53;}
      div[data-testid="stDataFrame"] {border:1px solid #e5eaf0; border-radius:12px;}
      div.stButton > button[kind="primary"] {border-radius:10px; font-weight:650;}
    </style>
    """,
    unsafe_allow_html=True,
)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def get_counts() -> dict[str, int]:
    if not DATABASE.exists():
        return {}
    con = connect(DATABASE)
    try:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        names = {
            "Bronze entities": "bronze_entities",
            "Bronze transactions": "bronze_transactions",
            "Silver entities": "silver_entities",
            "Silver transactions": "silver_transactions",
            "Fact transactions": "fact_transactions",
        }
        return {
            label: int(con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
            for label, table in names.items()
            if table in tables
        }
    except Exception:
        return {}
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_analytics(database_path: str, as_of: str, database_mtime: float) -> dict[str, list[dict[str, Any]]]:
    del database_mtime  # Cache key changes when DuckDB changes.
    return query_analytics(Path(database_path), date.fromisoformat(as_of))


latest = read_json(REPORTS / "latest_run.json")
quality = read_json(REPORTS / "data_quality_report.json")
counts = get_counts()
has_database = bool(counts)

st.sidebar.markdown("<div class='eyebrow'>LOCAL DATA PLATFORM</div>", unsafe_allow_html=True)
st.sidebar.markdown("# ◈ goML")
st.sidebar.caption("Pipeline monitor · DuckDB warehouse")
page = st.sidebar.radio("Workspace", ["Overview", "Query explorer", "Data quality"], label_visibility="collapsed")
st.sidebar.divider()
if st.sidebar.button("▶  Run pipeline", type="primary", use_container_width=True):
    try:
        with st.spinner("Running ingestion, transformations, quality checks, and exports…"):
            run_result = run_pipeline(SETTINGS)
        st.session_state["last_dashboard_run"] = run_result
        st.sidebar.success("Pipeline completed")
        st.rerun()
    except Exception as exc:  # Display the pipeline failure while preserving its traceback in logs.
        st.sidebar.error(f"Pipeline failed: {exc}")
st.sidebar.caption("Local only · results stay in `data/`")

st.markdown("<div class='eyebrow'>GOML DATA ENGINEERING CASE STUDY</div>", unsafe_allow_html=True)
st.title("Pipeline overview" if page == "Overview" else page)
st.markdown(
    "<div class='muted'>Follow each batch from raw files through validation and the Gold analytics layer.</div>",
    unsafe_allow_html=True,
)
st.write("")

if not has_database:
    st.info("No warehouse found yet. Use **Run pipeline** in the sidebar to generate data and build the dashboard.")
else:
    last_run_time = latest.get("finished_at", "—")
    status_col, run_col = st.columns([1, 3])
    with status_col:
        st.success("Warehouse online")
    with run_col:
        st.caption(f"Last completed run · {last_run_time} · mode: {latest.get('mode', 'unknown')}")

    if page == "Overview":
        accepted = int(latest.get("valid_entities", 0)) + int(latest.get("valid_transactions", 0))
        rejected = int(latest.get("rejected_entities", 0)) + int(latest.get("rejected_transactions", 0))
        metric_cols = st.columns(4)
        metric_cols[0].metric("Bronze transactions", f"{counts.get('Bronze transactions', 0):,}")
        metric_cols[1].metric("Silver entities", f"{counts.get('Silver entities', 0):,}")
        metric_cols[2].metric("Gold fact rows", f"{counts.get('Fact transactions', 0):,}")
        metric_cols[3].metric("Rows in DLQ", f"{rejected:,}", help=f"Accepted source records in this run: {accepted:,}")

        st.subheader("Data journey")
        flow = st.columns(5)
        for col, label in zip(flow, ["RAW FILES", "VALIDATION", "BRONZE", "SILVER", "GOLD + SQL"]):
            col.markdown(f"<div class='flow-step'>{label}</div>", unsafe_allow_html=True)

        st.write("")
        chart_left, chart_right = st.columns(2)
        if DATABASE.exists():
            try:
                results = load_analytics(str(DATABASE), date.today().isoformat(), DATABASE.stat().st_mtime)
                currency_df = pd.DataFrame(results.get("transaction_amounts_by_currency", []))
                with chart_left:
                    st.subheader("Transaction amount by currency")
                    if not currency_df.empty:
                        st.bar_chart(currency_df.set_index("currency")["total_amount"], color="#0F766E")
                        st.caption("Each total stays in its original currency; no FX conversion is applied.")
                    else:
                        st.caption("No transaction totals available.")
                monthly_df = pd.DataFrame(results.get("monthly_transaction_volume", []))
                with chart_right:
                    st.subheader("Monthly transaction volume")
                    if not monthly_df.empty:
                        monthly_df["period"] = monthly_df["year"].astype(str) + "-" + monthly_df["month"].astype(str).str.zfill(2)
                        pivot = monthly_df.pivot_table(index="period", columns="currency", values="transaction_count", aggfunc="sum").sort_index()
                        st.bar_chart(pivot, color=["#0F766E", "#5B8DEF", "#E9A23B"])
                    else:
                        st.caption("No monthly activity available.")
            except Exception as exc:
                st.warning(f"Analytics preview is unavailable: {exc}")

        with st.expander("Latest run details"):
            st.json(latest)

    elif page == "Query explorer":
        st.markdown("Explore and download the results of the 12 warehouse analytics queries.")
        query_name = st.selectbox("Choose an analysis", list(ANALYTICS), format_func=lambda name: name.replace("_", " ").title())
        st.caption(QUERY_HELP.get(query_name, "Business analytics query."))
        as_of = st.date_input("As-of date", value=date.today(), help="Used by the current-month revenue query.")
        with st.expander("View SQL"):
            st.code(ANALYTICS[query_name], language="sql")
        try:
            results = load_analytics(str(DATABASE), as_of.isoformat(), DATABASE.stat().st_mtime)
            frame = pd.DataFrame(results.get(query_name, []))
            st.markdown(f"**{len(frame):,} rows**")
            if not frame.empty:
                st.dataframe(frame, hide_index=True, width="stretch")
                st.download_button(
                    "Download CSV",
                    data=frame.to_csv(index=False).encode("utf-8"),
                    file_name=f"{query_name}.csv",
                    mime="text/csv",
                )
            else:
                st.info("The query returned no rows for the current warehouse.")
        except Exception as exc:
            st.error(f"Could not run this query: {exc}")

    else:
        st.markdown("Record-level validation and warehouse integrity for the most recent run.")
        metrics = [
            ("Entity input", "input_entities"),
            ("Valid entities", "valid_entities"),
            ("Rejected entities", "rejected_entities"),
            ("Transaction input", "input_transactions"),
            ("Valid transactions", "valid_transactions"),
            ("Rejected transactions", "rejected_transactions"),
        ]
        cards = st.columns(3)
        for index, (label, key) in enumerate(metrics):
            cards[index % 3].metric(label, f"{int(quality.get(key, latest.get(key, 0))):,}")
        st.subheader("Validation findings")
        defect_keys = [
            "duplicate_entity_rows",
            "duplicate_transaction_ids",
            "invalid_email_records",
            "invalid_amount_records",
            "missing_entity_id_records",
            "invalid_currency_records",
            "invalid_uuid_records",
            "missing_required_field_errors",
            "orphan_transactions",
        ]
        defect_rows = [{"check": key.replace("_", " ").title(), "records": int(quality.get(key, latest.get(key, 0)))} for key in defect_keys]
        st.dataframe(pd.DataFrame(defect_rows), hide_index=True, width="stretch")
        integrity = quality.get("integrity", latest.get("integrity", {}))
        if integrity:
            st.subheader("Gold integrity checks")
            integrity_frame = pd.DataFrame([{"check": key.replace("_", " ").title(), "failures": value} for key, value in integrity.items()])
            st.dataframe(integrity_frame, hide_index=True, width="stretch")
            if all(value == 0 for value in integrity.values()):
                st.success("All recorded integrity checks passed.")
        report_bytes = json.dumps(quality, indent=2).encode("utf-8")
        st.download_button("Download quality report", report_bytes, "data_quality_report.json", "application/json")

st.divider()
st.caption("goML · Local analytics workspace · DuckDB · Bronze → Silver → Gold")
