from __future__ import annotations

from datetime import datetime
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


def _recent_row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    if row is None:
        return {}
    values = list(row)
    keys = [
        "id",
        "contract_id",
        "file_name",
        "created_at",
        "numero_contrato",
        "nome_contrato",
        "contratante_nome",
        "contratada_nome",
        "municipios_atendidos",
        "objeto_contrato",
        "status_contrato",
    ]
    return {k: values[idx] if idx < len(values) else None for idx, k in enumerate(keys)}


class ReportRepository:
    def __init__(self, db: DatabaseManager):
        self._db = db

    def count_reports(self) -> int:
        sql = "SELECT COUNT(*) FROM reports"
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
        if not row:
            return 0
        if isinstance(row, dict):
            return int(row.get("count", 0) or 0)
        return int(row[0] or 0)

    def count_recent_reports(self, since: datetime) -> int:
        sql = "SELECT COUNT(*) FROM reports WHERE created_at >= %s"
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (since,))
                row = cur.fetchone()
        if not row:
            return 0
        if isinstance(row, dict):
            return int(row.get("count", 0) or 0)
        return int(row[0] or 0)

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

    def list_recent_reports(self, limit: int = 20) -> List[dict[str, Any]]:
        sql = """
        SELECT
            r.id,
            r.contract_id,
            r.file_name,
            r.created_at,
            COALESCE(NULLIF(c.numero_contrato, ''), c.contract_code) AS numero_contrato,
            COALESCE(NULLIF(c.nome_contrato, ''), c.title) AS nome_contrato,
            COALESCE(NULLIF(c.contratante_nome, ''), '') AS contratante_nome,
            COALESCE(NULLIF(c.contratada_nome, ''), '') AS contratada_nome,
            COALESCE(NULLIF(c.municipios_atendidos, ''), '') AS municipios_atendidos,
            COALESCE(NULLIF(c.objeto_contrato, ''), c.description) AS objeto_contrato,
            COALESCE(NULLIF(c.status_contrato, ''), c.status) AS status_contrato
        FROM reports r
        LEFT JOIN contracts c ON c.id = r.contract_id
        ORDER BY r.created_at DESC, r.id DESC
        LIMIT %s
        """
        with self._db.connection() as conn:
            cursor_kwargs = {}
            dict_factory = _dict_row_factory()
            if dict_factory is not None:
                cursor_kwargs["row_factory"] = dict_factory
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(sql, (int(limit),))
                rows = cur.fetchall() or []
        return [_recent_row_to_dict(row) for row in rows]
