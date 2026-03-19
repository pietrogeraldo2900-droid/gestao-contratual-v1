from __future__ import annotations

from typing import Any, List

from app.database.connection import DatabaseManager


def _dict_row_factory():
    try:
        from psycopg.rows import dict_row

        return dict_row
    except Exception:
        return None


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    if row is None:
        return {}
    values = list(row)
    keys = ["id", "contract_id", "file_name", "created_at"]
    return {k: values[idx] if idx < len(values) else None for idx, k in enumerate(keys)}


class ReportRepository:
    def __init__(self, db: DatabaseManager):
        self._db = db

    def create_report(self, contract_id: int, file_name: str) -> dict[str, Any]:
        sql = """
        INSERT INTO reports (contract_id, file_name)
        VALUES (%s, %s)
        RETURNING id, contract_id, file_name, created_at
        """
        with self._db.connection() as conn:
            try:
                cursor_kwargs = {}
                dict_factory = _dict_row_factory()
                if dict_factory is not None:
                    cursor_kwargs["row_factory"] = dict_factory
                with conn.cursor(**cursor_kwargs) as cur:
                    cur.execute(sql, (int(contract_id), file_name))
                    row = cur.fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        out = _row_to_dict(row)
        if not out:
            raise RuntimeError("Falha ao criar registro de relatorio.")
        return out

    def list_reports_by_contract(self, contract_id: int) -> List[dict[str, Any]]:
        sql = """
        SELECT id, contract_id, file_name, created_at
        FROM reports
        WHERE contract_id = %s
        ORDER BY created_at DESC, id DESC
        """
        with self._db.connection() as conn:
            cursor_kwargs = {}
            dict_factory = _dict_row_factory()
            if dict_factory is not None:
                cursor_kwargs["row_factory"] = dict_factory
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(sql, (int(contract_id),))
                rows = cur.fetchall() or []
        return [_row_to_dict(row) for row in rows]
