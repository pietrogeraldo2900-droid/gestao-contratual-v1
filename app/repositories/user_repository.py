from __future__ import annotations

from typing import Any

from app.database.connection import DatabaseManager


_USER_KEYS = [
    "id",
    "email",
    "password",
    "role",
    "status",
    "approved_by",
    "approved_at",
    "last_login_at",
    "created_at",
    "contractor_name",
]

_USER_SELECT_FIELDS = """
    id,
    email,
    password,
    role,
    status,
    approved_by,
    approved_at,
    last_login_at,
    created_at,
    contractor_name
"""

_USER_LIST_SELECT_FIELDS = """
    id,
    email,
    password,
    role,
    status,
    approved_by,
    approved_at,
    last_login_at,
    created_at,
    contractor_name
"""


def _dict_row_factory():
    try:
        from psycopg.rows import dict_row

        return dict_row
    except Exception:
        return None


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if row is None:
        return {}
    values = list(row)
    return {k: values[idx] if idx < len(values) else None for idx, k in enumerate(_USER_KEYS)}


def _is_unique_violation(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    message = str(exc).lower()
    return "uniqueviolation" in name or "duplicate key value" in message


class UserAlreadyExistsError(RuntimeError):
    pass


class UserRepository:
    def __init__(self, db: DatabaseManager):
        self._db = db

    def _cursor_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        dict_factory = _dict_row_factory()
        if dict_factory is not None:
            kwargs["row_factory"] = dict_factory
        return kwargs

    def _list_authorized_contract_ids(self, conn, user_id: int) -> list[int]:
        sql = """
        SELECT contract_id
        FROM user_contract_permissions
        WHERE user_id = %s
        ORDER BY contract_id ASC
        """
        try:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, (int(user_id),))
                rows = cur.fetchall() or []
        except Exception:
            return []
        output: list[int] = []
        for row in rows:
            if isinstance(row, dict):
                raw = row.get("contract_id")
            else:
                raw = row[0] if row else None
            try:
                contract_id = int(raw)
            except Exception:
                continue
            if contract_id > 0:
                output.append(contract_id)
        return output

    def _attach_scope(self, conn, user_row: dict[str, Any]) -> dict[str, Any]:
        out = dict(user_row or {})
        try:
            user_id = int(out.get("id", 0) or 0)
        except Exception:
            user_id = 0
        if user_id <= 0:
            out["authorized_contract_ids"] = []
            return out
        out["authorized_contract_ids"] = self._list_authorized_contract_ids(conn, user_id)
        return out

    def create_user(self, email: str, password_hash: str, *, status: str, role: str | None = None) -> dict[str, Any]:
        sql = f"""
        INSERT INTO users (email, password, status, role)
        VALUES (%s, %s, %s, %s)
        RETURNING {_USER_SELECT_FIELDS}
        """
        with self._db.connection() as conn:
            try:
                with conn.cursor(**self._cursor_kwargs()) as cur:
                    cur.execute(sql, (email, password_hash, status, role))
                    row = cur.fetchone()
                conn.commit()
            except Exception as exc:
                conn.rollback()
                if _is_unique_violation(exc):
                    raise UserAlreadyExistsError("Email ja cadastrado.") from exc
                raise
            out = self._attach_scope(conn, _row_to_dict(row))
        if not out:
            raise RuntimeError("Falha ao criar usuario.")
        return out

    def get_user_by_email(self, email: str) -> dict[str, Any] | None:
        sql = f"""
        SELECT {_USER_SELECT_FIELDS}
        FROM users
        WHERE email = %s
        LIMIT 1
        """
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, (email,))
                row = cur.fetchone()
            out = self._attach_scope(conn, _row_to_dict(row))
        return out or None

    def get_user_by_id(self, user_id: int) -> dict[str, Any] | None:
        sql = f"""
        SELECT {_USER_SELECT_FIELDS}
        FROM users
        WHERE id = %s
        LIMIT 1
        """
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, (int(user_id),))
                row = cur.fetchone()
            out = self._attach_scope(conn, _row_to_dict(row))
        return out or None

    def list_users(self, status: str | None = None) -> list[dict[str, Any]]:
        sql = f"""
        SELECT {_USER_LIST_SELECT_FIELDS}
        FROM users
        {{where_clause}}
        ORDER BY created_at DESC
        """
        params: tuple[Any, ...] = ()
        where_clause = ""
        if status:
            where_clause = "WHERE status = %s"
            params = (status,)
        sql = sql.format(where_clause=where_clause)
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall() or []
            users = [self._attach_scope(conn, _row_to_dict(row)) for row in rows]
        return users

    def update_user_status(
        self,
        user_id: int,
        status: str,
        *,
        approved_by: int | None = None,
        set_approved: bool = False,
    ) -> dict[str, Any]:
        sql = f"""
        UPDATE users
        SET status = %s,
            approved_by = CASE WHEN %s THEN %s ELSE approved_by END,
            approved_at = CASE WHEN %s THEN NOW() ELSE approved_at END
        WHERE id = %s
        RETURNING {_USER_SELECT_FIELDS}
        """
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, (status, set_approved, approved_by, set_approved, int(user_id)))
                row = cur.fetchone()
            conn.commit()
            out = self._attach_scope(conn, _row_to_dict(row))
        if not out:
            raise RuntimeError("Falha ao atualizar status do usuario.")
        return out

    def update_user_role(self, user_id: int, role: str) -> dict[str, Any]:
        sql = f"""
        UPDATE users
        SET role = %s
        WHERE id = %s
        RETURNING {_USER_SELECT_FIELDS}
        """
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, (role, int(user_id)))
                row = cur.fetchone()
            conn.commit()
            out = self._attach_scope(conn, _row_to_dict(row))
        if not out:
            raise RuntimeError("Falha ao atualizar papel do usuario.")
        return out

    def update_user_contractor(self, user_id: int, contractor_name: str | None) -> dict[str, Any]:
        sql = f"""
        UPDATE users
        SET contractor_name = NULLIF(%s, '')
        WHERE id = %s
        RETURNING {_USER_SELECT_FIELDS}
        """
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, (str(contractor_name or "").strip(), int(user_id)))
                row = cur.fetchone()
            conn.commit()
            out = self._attach_scope(conn, _row_to_dict(row))
        if not out:
            raise RuntimeError("Falha ao atualizar contratada do usuario.")
        return out

    def replace_user_authorized_contracts(self, user_id: int, contract_ids: list[int]) -> list[int]:
        normalized: list[int] = []
        seen: set[int] = set()
        for value in list(contract_ids or []):
            try:
                contract_id = int(value)
            except Exception:
                continue
            if contract_id <= 0 or contract_id in seen:
                continue
            seen.add(contract_id)
            normalized.append(contract_id)

        with self._db.connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM user_contract_permissions WHERE user_id = %s", (int(user_id),))
                    for contract_id in normalized:
                        cur.execute(
                            """
                            INSERT INTO user_contract_permissions (user_id, contract_id)
                            VALUES (%s, %s)
                            ON CONFLICT (user_id, contract_id) DO NOTHING
                            """,
                            (int(user_id), int(contract_id)),
                        )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return normalized

    def get_user_authorized_contract_ids(self, user_id: int) -> list[int]:
        with self._db.connection() as conn:
            return self._list_authorized_contract_ids(conn, int(user_id))

    def update_last_login(self, user_id: int) -> None:
        sql = "UPDATE users SET last_login_at = NOW() WHERE id = %s"
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(user_id),))
            conn.commit()
