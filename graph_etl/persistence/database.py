from __future__ import annotations

import sqlite3
from pathlib import Path

from graph_etl.persistence.clock import Clock, timestamp, utc_now
from graph_etl.persistence.extraction_store import ExtractionStore
from graph_etl.persistence.ingestion_store import IngestionStore
from graph_etl.persistence.migrations import (
    MIGRATIONS,
    SCHEMA_VERSION,
    apply_migrations,
)
from graph_etl.persistence.resolution_store import ResolutionStore

__all__ = [
    "Database",
    "MIGRATIONS",
    "SCHEMA_VERSION",
    "timestamp",
    "utc_now",
]


class Database(IngestionStore, ExtractionStore, ResolutionStore):
    def __init__(self, path: str | Path, *, clock: Clock = utc_now) -> None:
        self.connection = sqlite3.connect(str(path), timeout=30)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.clock = clock
        try:
            apply_migrations(self.connection, clock, timestamp)
        except Exception:
            self.connection.close()
            raise

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *_: object) -> None:
        self.connection.rollback()
        self.close()

    def close(self) -> None:
        self.connection.close()