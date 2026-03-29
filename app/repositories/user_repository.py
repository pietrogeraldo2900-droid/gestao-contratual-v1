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
    keys = [
        "id",
        "email",
        "password",
        "role",
        "status",
        "approved_by",
        "approved_at",
        "last_login_at",
        "created_at",
    ]
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

    def create_user(self, email: str, password_hash: str, *, status: str, role: str | None = None) -> dict[str, Any]:
        sql = """
        INSERT INTO users (email, password, status, role)
        VALUES (%s, %s, %s, %s)
        RETURNING id, email, password, role, status, approved_by, approved_at, last_login_at, created_at
        """
        with self._db.connection() as conn:
            try:
                cursor_kwargs = {}
                dict_factory = _dict_row_factory()
                if dict_factory is not None:
                    cursor_kwargs["row_factory"] = dict_factory
                with conn.cursor(**cursor_kwargs) as cur:
                    cur.execute(sql, (email, password_hash, status, role))
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
        sql = """
        SELECT id, email, password, role, status, approved_by, approved_at, last_login_at, created_at
        FROM users
        WHERE email = %s
        LIMIT 1
        """
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
        sql = """
        SELECT id, email, password, role, status, approved_by, approved_at, last_login_at, created_at
        FROM users
        WHERE id = %s
        LIMIT 1
        """
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

    def list_users(self, status: str | None = None) -> list[dict[str, Any]]:
        sql = """
        SELECT id, email, role, status, approved_by, approved_at, last_login_at, created_at
        FROM users
        {where_clause}
        ORDER BY created_at DESC
        """
        params: tuple[Any, ...] = ()
        where_clause = ""
        if status:
            where_clause = "WHERE status = %s"
            params = (status,)
        sql = sql.format(where_clause=where_clause)
        with self._db.connection() as conn:
            cursor_kwargs = {}
            dict_factory = _dict_row_factory()
            if dict_factory is not None:
                cursor_kwargs["row_factory"] = dict_factory
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        return [_row_to_dict(row) for row in rows]

    def update_user_status(
        self,
        user_id: int,
        status: str,
        *,
        approved_by: int | None = None,
        set_approved: bool = False,
    ) -> dict[str, Any]:
        sql = """
        UPDATE users
        SET status = %s,
            approved_by = CASE WHEN %s THEN %s ELSE approved_by END,
            approved_at = CASE WHEN %s THEN NOW() ELSE approved_at END
        WHERE id = %s
        RETURNING id, email, password, role, status, approved_by, approved_at, last_login_at, created_at
        """
        with self._db.connection() as conn:
            cursor_kwargs = {}
            dict_factory = _dict_row_factory()
            if dict_factory is not None:
                cursor_kwargs["row_factory"] = dict_factory
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(sql, (status, set_approved, approved_by, set_approved, int(user_id)))
                row = cur.fetchone()
            conn.commit()
        out = _row_to_dict(row)
        if not out:
            raise RuntimeError("Falha ao atualizar status do usuario.")
        return out

    def update_user_role(self, user_id: int, role: str) -> dict[str, Any]:
        sql = """
        UPDATE users
        SET role = %s
        WHERE id = %s
        RETURNING id, email, password, role, status, approved_by, approved_at, last_login_at, created_at
        """
        with self._db.connection() as conn:
            cursor_kwargs = {}
            dict_factory = _dict_row_factory()
            if dict_factory is not None:
                cursor_kwargs["row_factory"] = dict_factory
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(sql, (role, int(user_id)))
                row = cur.fetchone()
            conn.commit()
        out = _row_to_dict(row)
        if not out:
            raise RuntimeError("Falha ao atualizar papel do usuario.")
        return out

    def update_last_login(self, user_id: int) -> None:
        sql = "UPDATE users SET last_login_at = NOW() WHERE id = %s"
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(user_id),))
            conn.commit()
