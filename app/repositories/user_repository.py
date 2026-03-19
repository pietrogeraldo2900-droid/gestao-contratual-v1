from __future__ import annotations

from typing import Any

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
    keys = ["id", "email", "password", "created_at"]
    return {k: values[idx] if idx < len(values) else None for idx, k in enumerate(keys)}


def _is_unique_violation(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    message = str(exc).lower()
    return "uniqueviolation" in name or "duplicate key value" in message


class UserAlreadyExistsError(RuntimeError):
    pass


class UserRepository:
    def __init__(self, db: DatabaseManager):
        self._db = db

    def create_user(self, email: str, password_hash: str) -> dict[str, Any]:
        sql = """
        INSERT INTO users (email, password)
        VALUES (%s, %s)
        RETURNING id, email, password, created_at
        """
        with self._db.connection() as conn:
            try:
                cursor_kwargs = {}
                dict_factory = _dict_row_factory()
                if dict_factory is not None:
                    cursor_kwargs["row_factory"] = dict_factory
                with conn.cursor(**cursor_kwargs) as cur:
                    cur.execute(sql, (email, password_hash))
                    row = cur.fetchone()
                conn.commit()
            except Exception as exc:
                conn.rollback()
                if _is_unique_violation(exc):
                    raise UserAlreadyExistsError("Email ja cadastrado.") from exc
                raise
        out = _row_to_dict(row)
        if not out:
            raise RuntimeError("Falha ao criar usuario.")
        return out

    def get_user_by_email(self, email: str) -> dict[str, Any] | None:
        sql = "SELECT id, email, password, created_at FROM users WHERE email = %s LIMIT 1"
        with self._db.connection() as conn:
            cursor_kwargs = {}
            dict_factory = _dict_row_factory()
            if dict_factory is not None:
                cursor_kwargs["row_factory"] = dict_factory
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(sql, (email,))
                row = cur.fetchone()
        out = _row_to_dict(row)
        return out or None

    def get_user_by_id(self, user_id: int) -> dict[str, Any] | None:
        sql = "SELECT id, email, password, created_at FROM users WHERE id = %s LIMIT 1"
        with self._db.connection() as conn:
            cursor_kwargs = {}
            dict_factory = _dict_row_factory()
            if dict_factory is not None:
                cursor_kwargs["row_factory"] = dict_factory
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(sql, (int(user_id),))
                row = cur.fetchone()
        out = _row_to_dict(row)
        return out or None
