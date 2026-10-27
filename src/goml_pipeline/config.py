"""Typed configuration and path resolution."""

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    root: Path = Path.cwd()
    entity_count: int = 200
    transaction_count: int = 1000
    seed: int = 42
    incremental: bool = False
    run_id: str | None = None
    log_level: str = "INFO"

    def _path(self, key: str, default: Path) -> Path:
        value = Path(os.getenv(key, str(default)))
        return value if value.is_absolute() else self.root / value

    @property
    def raw_dir(self) -> Path:
        return self._path("GOML_RAW_DIR", Path("data/raw"))

    @property
    def entities_path(self) -> Path:
        return self._path("GOML_ENTITIES_PATH", self.raw_dir / "entities.csv")

    @property
    def transactions_path(self) -> Path:
        return self._path("GOML_TRANSACTIONS_PATH", self.raw_dir / "transactions.json")

    @property
    def dlq_dir(self) -> Path:
        return self._path("GOML_DLQ_DIR", Path("data/dlq"))

    @property
    def database_path(self) -> Path:
        return self._path("GOML_DATABASE_PATH", Path("data/warehouse.duckdb"))

    @property
    def reports_dir(self) -> Path:
        return self._path("GOML_REPORTS_DIR", Path("data/reports"))

    def ensure_directories(self) -> None:
        for path in (
            self.entities_path.parent,
            self.transactions_path.parent,
            self.dlq_dir,
            self.database_path.parent,
            self.reports_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
