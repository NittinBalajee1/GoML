"""Command-line interface."""

import argparse
import json
import os
from datetime import date
from pathlib import Path

from .config import Settings
from .database import connect, query_analytics
from .generator import generate_data
from .logging_config import configure_logging
from .orchestration import run_pipeline
from .reporting import export_analytics


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="goml-pipeline")
    p.add_argument("command", choices=["generate", "run", "analytics", "health", "latest-run"])
    p.add_argument("--root", type=Path, default=Path.cwd())
    p.add_argument("--entities", type=int, default=200)
    p.add_argument("--transactions", type=int, default=1000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--incremental", action="store_true")
    p.add_argument("--no-generate", action="store_true", help="run against existing input files")
    p.add_argument("--as-of", type=date.fromisoformat)
    p.add_argument("--run-id")
    p.add_argument("--log-level", default=os.getenv("GOML_LOG_LEVEL", "INFO"))
    return p


def main() -> None:
    args = parser().parse_args()
    settings = Settings(
        root=args.root,
        entity_count=args.entities,
        transaction_count=args.transactions,
        seed=args.seed,
        incremental=args.incremental,
        run_id=args.run_id,
        log_level=args.log_level,
    )
    configure_logging(settings.log_level)
    if args.command == "generate":
        settings.ensure_directories()
        print(
            json.dumps(
                generate_data(
                    settings.entities_path,
                    settings.transactions_path,
                    args.entities,
                    args.transactions,
                    args.seed,
                ),
                indent=2,
            )
        )
    elif args.command == "run":
        print(
            json.dumps(run_pipeline(settings, generate=not args.no_generate), indent=2, default=str)
        )
    elif args.command == "analytics":
        output = query_analytics(settings.database_path, args.as_of)
        print(json.dumps(output, indent=2, default=str))
        export_analytics(
            settings.database_path, settings.reports_dir, "manual-analytics", args.as_of
        )
    elif args.command == "health":
        con = connect(settings.database_path)
        try:
            table_count = con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name='fact_transactions'"
            ).fetchone()[0]
            if not table_count:
                raise SystemExit("health check failed: fact_transactions table missing")
            print(
                json.dumps(
                    {
                        "status": "healthy",
                        "fact_transactions": con.execute(
                            "SELECT count(*) FROM fact_transactions"
                        ).fetchone()[0],
                    }
                )
            )
        finally:
            con.close()
    else:
        path = settings.reports_dir / "latest_run.json"
        print(path.read_text(encoding="utf-8") if path.exists() else "No run report exists")


if __name__ == "__main__":
    main()
