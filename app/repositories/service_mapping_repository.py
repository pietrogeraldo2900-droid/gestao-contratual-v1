from __future__ import annotations

from typing import Any

from app.core.input_layer import normalizar_texto
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
    return dict(row)


def _normalize_alias(value: object) -> str:
    return normalizar_texto(str(value or "")).strip()


class ServiceMappingRepository:
    def __init__(self, db: DatabaseManager):
        self._db = db

    def list_services(self, search: str = "", limit: int = 500) -> list[dict[str, Any]]:
        limit_value = max(1, min(int(limit or 500), 2000))
        search_value = str(search or "").strip()
        where_sql = ""
        params: list[Any] = []
        if search_value:
            where_sql = "WHERE s.servico_oficial ILIKE %s OR s.categoria ILIKE %s"
            like = f"%{search_value}%"
            params.extend([like, like])

        sql = f"""
        SELECT
            s.id,
            s.servico_oficial,
            s.categoria,
            s.unidade_padrao,
            s.ativo,
            s.created_at,
            s.updated_at,
            COALESCE(COUNT(a.id) FILTER (WHERE a.ativo), 0) AS aliases_ativos
        FROM service_registry s
        LEFT JOIN service_aliases a
            ON a.service_id = s.id
        {where_sql}
        GROUP BY s.id
        ORDER BY LOWER(s.servico_oficial), s.id
        LIMIT %s
        """
        params.append(limit_value)

        with self._db.connection() as conn:
            cursor_kwargs = {}
            dict_factory = _dict_row_factory()
            if dict_factory is not None:
                cursor_kwargs["row_factory"] = dict_factory
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
        return [_row_to_dict(row) for row in rows]

    def get_service_by_name(self, servico_oficial: str) -> dict[str, Any] | None:
        sql = """
        SELECT id, servico_oficial, categoria, unidade_padrao, ativo, created_at, updated_at
        FROM service_registry
        WHERE servico_oficial = %s
        LIMIT 1
        """
        with self._db.connection() as conn:
            cursor_kwargs = {}
            dict_factory = _dict_row_factory()
            if dict_factory is not None:
                cursor_kwargs["row_factory"] = dict_factory
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(sql, (str(servico_oficial or "").strip(),))
                row = cur.fetchone()
        output = _row_to_dict(row)
        return output or None

    def upsert_service(
        self,
        servico_oficial: str,
        categoria: str,
        unidade_padrao: str = "",
        ativo: bool = True,
    ) -> dict[str, Any]:
        sql = """
        INSERT INTO service_registry (servico_oficial, categoria, unidade_padrao, ativo)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (servico_oficial)
        DO UPDATE
            SET categoria = EXCLUDED.categoria,
                unidade_padrao = EXCLUDED.unidade_padrao,
                ativo = EXCLUDED.ativo,
                updated_at = NOW()
        RETURNING id, servico_oficial, categoria, unidade_padrao, ativo, created_at, updated_at
        """
        params = (
            str(servico_oficial or "").strip(),
            str(categoria or "").strip() or "servico_nao_mapeado",
            str(unidade_padrao or "").strip(),
            bool(ativo),
        )
        with self._db.connection() as conn:
            cursor_kwargs = {}
            dict_factory = _dict_row_factory()
            if dict_factory is not None:
                cursor_kwargs["row_factory"] = dict_factory
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
            conn.commit()
        output = _row_to_dict(row)
        if not output:
            raise RuntimeError("Falha ao salvar servico.")
        return output

    def list_aliases(self, search: str = "", limit: int = 500) -> list[dict[str, Any]]:
        limit_value = max(1, min(int(limit or 500), 2000))
        search_value = str(search or "").strip()
        where_sql = ""
        params: list[Any] = []
        if search_value:
            where_sql = "WHERE a.alias_text ILIKE %s OR s.servico_oficial ILIKE %s"
            like = f"%{search_value}%"
            params.extend([like, like])

        sql = f"""
        SELECT
            a.id,
            a.alias_text,
            a.alias_norm,
            a.source,
            a.ativo,
            a.created_at,
            a.updated_at,
            s.id AS service_id,
            s.servico_oficial,
            s.categoria
        FROM service_aliases a
        INNER JOIN service_registry s
            ON s.id = a.service_id
        {where_sql}
        ORDER BY a.updated_at DESC, a.id DESC
        LIMIT %s
        """
        params.append(limit_value)
        with self._db.connection() as conn:
            cursor_kwargs = {}
            dict_factory = _dict_row_factory()
            if dict_factory is not None:
                cursor_kwargs["row_factory"] = dict_factory
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
        return [_row_to_dict(row) for row in rows]

    def upsert_alias(
        self,
        alias_text: str,
        service_id: int,
        *,
        source: str = "manual",
        ativo: bool = True,
    ) -> dict[str, Any]:
        alias_clean = str(alias_text or "").strip()
        alias_norm = _normalize_alias(alias_clean)
        if not alias_clean or not alias_norm:
            raise ValueError("Alias invalido.")

        sql = """
        INSERT INTO service_aliases (alias_text, alias_norm, service_id, source, ativo)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (alias_norm)
        DO UPDATE
            SET alias_text = EXCLUDED.alias_text,
                service_id = EXCLUDED.service_id,
                source = EXCLUDED.source,
                ativo = EXCLUDED.ativo,
                updated_at = NOW()
        RETURNING id, alias_text, alias_norm, service_id, source, ativo, created_at, updated_at
        """
        params = (
            alias_clean,
            alias_norm,
            int(service_id),
            str(source or "manual").strip() or "manual",
            bool(ativo),
        )
        with self._db.connection() as conn:
            cursor_kwargs = {}
            dict_factory = _dict_row_factory()
            if dict_factory is not None:
                cursor_kwargs["row_factory"] = dict_factory
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
            conn.commit()

        output = _row_to_dict(row)
        if not output:
            raise RuntimeError("Falha ao salvar alias.")
        return output

    def list_alias_map(self) -> dict[str, dict[str, Any]]:
        sql = """
        SELECT
            a.alias_norm,
            a.alias_text,
            s.id AS service_id,
            s.servico_oficial,
            s.categoria
        FROM service_aliases a
        INNER JOIN service_registry s
            ON s.id = a.service_id
        WHERE a.ativo = TRUE AND s.ativo = TRUE
        """
        with self._db.connection() as conn:
            cursor_kwargs = {}
            dict_factory = _dict_row_factory()
            if dict_factory is not None:
                cursor_kwargs["row_factory"] = dict_factory
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(sql)
                rows = cur.fetchall()

        alias_map: dict[str, dict[str, Any]] = {}
        for row in rows:
            item = _row_to_dict(row)
            alias_norm = _normalize_alias(item.get("alias_norm", ""))
            if not alias_norm:
                continue
            alias_map[alias_norm] = {
                "alias_norm": alias_norm,
                "alias_text": str(item.get("alias_text", "") or "").strip(),
                "service_id": int(item.get("service_id", 0) or 0),
                "servico_oficial": str(item.get("servico_oficial", "") or "").strip(),
                "categoria": str(item.get("categoria", "") or "").strip() or "servico_nao_mapeado",
            }
        return alias_map
