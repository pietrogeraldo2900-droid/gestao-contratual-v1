from __future__ import annotations

from typing import Any, List

from app.database.connection import DatabaseManager
from app.models.contract import Contract, ContractCreateInput


class ContractConflictError(RuntimeError):
    pass


def _dict_row_factory():
    try:
        from psycopg.rows import dict_row

        return dict_row
    except Exception:
        return None


def _is_unique_violation(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    message = str(exc).lower()
    return "uniqueviolation" in name or "duplicate key value" in message


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    if row is None:
        return {}
    values = list(row)
    keys = ["id", "contract_code", "title", "description", "status", "created_at", "updated_at"]
    return {k: values[idx] if idx < len(values) else None for idx, k in enumerate(keys)}


class ContractRepository:
    def __init__(self, db: DatabaseManager):
        self._db = db

    def exists_contract(self, contract_id: int) -> bool:
        sql = "SELECT 1 FROM contracts WHERE id = %s LIMIT 1"
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(contract_id),))
                row = cur.fetchone()
        return bool(row)

    def create_contract(self, payload: ContractCreateInput) -> Contract:
        sql = """
        INSERT INTO contracts (contract_code, title, description, status)
        VALUES (%s, %s, %s, %s)
        RETURNING id, contract_code, title, description, status, created_at, updated_at
        """
        with self._db.connection() as conn:
            try:
                cursor_kwargs = {}
                dict_factory = _dict_row_factory()
                if dict_factory is not None:
                    cursor_kwargs["row_factory"] = dict_factory
                with conn.cursor(**cursor_kwargs) as cur:
                    cur.execute(
                        sql,
                        (
                            payload.contract_code,
                            payload.title,
                            payload.description,
                            payload.status,
                        ),
                    )
                    row = cur.fetchone()
                conn.commit()
            except Exception as exc:
                conn.rollback()
                if _is_unique_violation(exc):
                    raise ContractConflictError("Ja existe contrato com este codigo.") from exc
                raise

        contract_row = _row_to_dict(row)
        if not contract_row:
            raise RuntimeError("Falha ao criar contrato: resposta vazia do banco.")
        return Contract.from_row(contract_row)

    def list_contracts(self, limit: int = 100) -> List[Contract]:
        sql = """
        SELECT id, contract_code, title, description, status, created_at, updated_at
        FROM contracts
        ORDER BY created_at DESC, id DESC
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
        return [Contract.from_row(_row_to_dict(row)) for row in rows]
