from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

from config.settings import AppSettings

try:
    import psycopg
except Exception:  # pragma: no cover - fallback defensivo para ambientes sem driver
    psycopg = None


class DatabaseUnavailableError(RuntimeError):
    pass


@dataclass(frozen=True)
class DatabaseManager:
    dsn: str
    connect_timeout: int = 5

    def _connect(self):
        if psycopg is None:
            raise DatabaseUnavailableError("Dependencia psycopg nao instalada no ambiente.")
        if not self.dsn:
            raise DatabaseUnavailableError("DATABASE_URL nao configurada.")
        return psycopg.connect(self.dsn, connect_timeout=self.connect_timeout)

    @contextmanager
    def connection(self) -> Iterator[object]:
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()


def build_database_manager(settings: AppSettings) -> DatabaseManager | None:
    if not settings.db_enabled:
        return None
    if not str(settings.database_url or "").strip():
        return None
    return DatabaseManager(dsn=settings.database_url, connect_timeout=settings.db_connect_timeout)
