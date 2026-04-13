from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from app.database.connection import DatabaseManager
from app.models.inspection import Inspection


def _dict_row_factory():
    try:
        from psycopg.rows import dict_row

        return dict_row
    except Exception:
        return None


def _to_decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value or "0").replace(",", "."))
    except Exception:
        return Decimal("0")


def _to_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


class ConferenceRepository:
    def __init__(self, db: DatabaseManager):
        self._db = db

    def _cursor_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        dict_factory = _dict_row_factory()
        if dict_factory is not None:
            kwargs["row_factory"] = dict_factory
        return kwargs

    def list_pending_inspections_for_user(self, *, user_id: int, role: str, limit: int = 150) -> list[Inspection]:
        role_norm = str(role or "").strip().lower()
        where_sql = """
        WHERE LOWER(COALESCE(i.status, '')) IN ('aberta', 'em_andamento')
          AND NOT EXISTS (
              SELECT 1
              FROM inspection_conference_sessions s
              WHERE s.inspection_id = i.id
                AND LOWER(COALESCE(s.status, '')) = 'concluida'
          )
        """
        params: list[Any] = []
        scoped_join = ""
        if role_norm == "fiscal":
            scoped_join = """
            INNER JOIN user_contract_permissions ucp
                ON ucp.contract_id = i.contract_id
               AND ucp.user_id = %s
            """
            params.append(int(user_id))

        sql = f"""
        SELECT
            i.id,
            i.contract_id,
            i.titulo,
            i.data_vistoria,
            i.periodo,
            i.nucleo,
            i.municipio,
            i.local_vistoria,
            i.equipe,
            i.fiscal_nome,
            i.fiscal_contato,
            i.responsavel_nome,
            i.responsavel_contato,
            i.status,
            i.prioridade,
            i.resultado,
            i.score_geral,
            i.observacoes,
            i.created_by,
            i.created_at,
            i.updated_at,
            c.numero_contrato,
            c.nome_contrato
        FROM inspections i
        LEFT JOIN contracts c ON c.id = i.contract_id
        {scoped_join}
        {where_sql}
        ORDER BY i.data_vistoria ASC NULLS LAST, i.id ASC
        LIMIT %s
        """
        params.append(max(1, min(int(limit), 1000)))

        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall() or []
        return [Inspection.from_row(row if isinstance(row, dict) else {}) for row in rows]

    def can_user_access_inspection(self, *, user_id: int, role: str, inspection_id: int) -> bool:
        role_norm = str(role or "").strip().lower()
        if role_norm != "fiscal":
            sql_any = "SELECT 1 FROM inspections WHERE id = %s LIMIT 1"
            with self._db.connection() as conn:
                with conn.cursor(**self._cursor_kwargs()) as cur:
                    cur.execute(sql_any, (int(inspection_id),))
                    return bool(cur.fetchone())

        sql = """
        SELECT 1
        FROM inspections i
        INNER JOIN user_contract_permissions ucp
            ON ucp.contract_id = i.contract_id
           AND ucp.user_id = %s
        WHERE i.id = %s
        LIMIT 1
        """
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, (int(user_id), int(inspection_id)))
                return bool(cur.fetchone())

    def get_session_by_inspection_and_fiscal(self, *, inspection_id: int, fiscal_user_id: int) -> dict[str, Any] | None:
        sql = """
        SELECT
            id,
            inspection_id,
            contract_id,
            fiscal_user_id,
            status,
            technical_notes,
            location_verified,
            summary_json,
            started_at,
            completed_at,
            created_at,
            updated_at
        FROM inspection_conference_sessions
        WHERE inspection_id = %s
          AND fiscal_user_id = %s
        LIMIT 1
        """
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, (int(inspection_id), int(fiscal_user_id)))
                row = cur.fetchone()
        return dict(row) if isinstance(row, dict) else None

    def get_or_create_session(self, *, inspection_id: int, contract_id: int | None, fiscal_user_id: int) -> dict[str, Any]:
        sql = """
        INSERT INTO inspection_conference_sessions (
            inspection_id,
            contract_id,
            fiscal_user_id,
            status
        )
        VALUES (%s, %s, %s, 'rascunho')
        ON CONFLICT (inspection_id, fiscal_user_id)
        DO UPDATE
            SET contract_id = EXCLUDED.contract_id,
                updated_at = NOW()
        RETURNING
            id,
            inspection_id,
            contract_id,
            fiscal_user_id,
            status,
            technical_notes,
            location_verified,
            summary_json,
            started_at,
            completed_at,
            created_at,
            updated_at
        """
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(
                    sql,
                    (
                        int(inspection_id),
                        int(contract_id) if contract_id else None,
                        int(fiscal_user_id),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        if not isinstance(row, dict):
            raise RuntimeError("Falha ao abrir sessao de conferencia.")
        return dict(row)

    def save_session_draft(self, *, session_id: int, technical_notes: str, location_verified: str) -> dict[str, Any]:
        sql = """
        UPDATE inspection_conference_sessions
        SET technical_notes = %s,
            location_verified = %s,
            updated_at = NOW()
        WHERE id = %s
        RETURNING
            id,
            inspection_id,
            contract_id,
            fiscal_user_id,
            status,
            technical_notes,
            location_verified,
            summary_json,
            started_at,
            completed_at,
            created_at,
            updated_at
        """
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(
                    sql,
                    (
                        str(technical_notes or "").strip(),
                        str(location_verified or "").strip(),
                        int(session_id),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        if not isinstance(row, dict):
            raise RuntimeError("Falha ao salvar rascunho da conferencia.")
        return dict(row)

    def list_session_item_results(self, *, session_id: int) -> list[dict[str, Any]]:
        sql = """
        SELECT
            id,
            session_id,
            inspection_item_id,
            ordem,
            area,
            item_titulo,
            descricao,
            quantidade_verificada,
            verificado_informado,
            status,
            observacao_tecnica,
            local_verificado,
            evidencia_ref,
            from_field,
            created_at,
            updated_at
        FROM inspection_conference_item_results
        WHERE session_id = %s
        ORDER BY from_field ASC, ordem ASC, id ASC
        """
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, (int(session_id),))
                rows = cur.fetchall() or []
        return [dict(row) if isinstance(row, dict) else {} for row in rows]

    def upsert_planned_item_results(self, *, session_id: int, items: list[dict[str, Any]]) -> None:
        sql = """
        INSERT INTO inspection_conference_item_results (
            session_id,
            inspection_item_id,
            ordem,
            area,
            item_titulo,
            descricao,
            quantidade_verificada,
            verificado_informado,
            status,
            observacao_tecnica,
            local_verificado,
            evidencia_ref,
            from_field
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE)
        ON CONFLICT (session_id, inspection_item_id)
        DO UPDATE
            SET ordem = EXCLUDED.ordem,
                area = EXCLUDED.area,
                item_titulo = EXCLUDED.item_titulo,
                descricao = EXCLUDED.descricao,
                quantidade_verificada = EXCLUDED.quantidade_verificada,
                verificado_informado = EXCLUDED.verificado_informado,
                status = EXCLUDED.status,
                observacao_tecnica = EXCLUDED.observacao_tecnica,
                local_verificado = EXCLUDED.local_verificado,
                evidencia_ref = EXCLUDED.evidencia_ref,
                updated_at = NOW()
        """
        with self._db.connection() as conn:
            try:
                with conn.cursor() as cur:
                    for item in list(items or []):
                        cur.execute(
                            sql,
                            (
                                int(session_id),
                                int(item.get("inspection_item_id", 0) or 0),
                                int(item.get("ordem", 0) or 0),
                                str(item.get("area", "") or "").strip(),
                                str(item.get("item_titulo", "") or "").strip(),
                                str(item.get("descricao", "") or "").strip(),
                                _to_decimal(item.get("quantidade_verificada")),
                                bool(item.get("verificado_informado", False)),
                                str(item.get("status", "pendente") or "pendente").strip().lower(),
                                str(item.get("observacao_tecnica", "") or "").strip(),
                                str(item.get("local_verificado", "") or "").strip(),
                                str(item.get("evidencia_ref", "") or "").strip(),
                            ),
                        )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def add_field_item_result(self, *, session_id: int, item: dict[str, Any]) -> None:
        sql = """
        INSERT INTO inspection_conference_item_results (
            session_id,
            inspection_item_id,
            ordem,
            area,
            item_titulo,
            descricao,
            quantidade_verificada,
            verificado_informado,
            status,
            observacao_tecnica,
            local_verificado,
            evidencia_ref,
            from_field
        )
        VALUES (%s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        int(session_id),
                        int(item.get("ordem", 0) or 0),
                        str(item.get("area", "") or "").strip(),
                        str(item.get("item_titulo", "") or "").strip(),
                        str(item.get("descricao", "") or "").strip(),
                        _to_decimal(item.get("quantidade_verificada")),
                        bool(item.get("verificado_informado", True)),
                        str(item.get("status", "observacao") or "observacao").strip().lower(),
                        str(item.get("observacao_tecnica", "") or "").strip(),
                        str(item.get("local_verificado", "") or "").strip(),
                        str(item.get("evidencia_ref", "") or "").strip(),
                    ),
                )
            conn.commit()

    def conclude_and_publish(
        self,
        *,
        session_id: int,
        technical_notes: str,
        location_verified: str,
        summary: dict[str, Any],
        inspection_resultado: str,
    ) -> dict[str, Any]:
        with self._db.connection() as conn:
            try:
                with conn.cursor(**self._cursor_kwargs()) as cur:
                    cur.execute(
                        """
                        UPDATE inspection_conference_sessions
                        SET status = 'concluida',
                            technical_notes = %s,
                            location_verified = %s,
                            summary_json = %s::jsonb,
                            completed_at = NOW(),
                            updated_at = NOW()
                        WHERE id = %s
                        RETURNING
                            id,
                            inspection_id,
                            contract_id,
                            fiscal_user_id,
                            status,
                            technical_notes,
                            location_verified,
                            summary_json,
                            started_at,
                            completed_at,
                            created_at,
                            updated_at
                        """,
                        (
                            str(technical_notes or "").strip(),
                            str(location_verified or "").strip(),
                            json.dumps(summary or {}, ensure_ascii=False),
                            int(session_id),
                        ),
                    )
                    session_row = cur.fetchone()
                    if not isinstance(session_row, dict):
                        raise RuntimeError("Sessao de conferencia nao encontrada para conclusao.")

                    cur.execute(
                        """
                        SELECT
                            id,
                            session_id,
                            inspection_item_id,
                            ordem,
                            area,
                            item_titulo,
                            descricao,
                            quantidade_verificada,
                            verificado_informado,
                            status,
                            observacao_tecnica,
                            local_verificado,
                            evidencia_ref,
                            from_field
                        FROM inspection_conference_item_results
                        WHERE session_id = %s
                        ORDER BY from_field ASC, ordem ASC, id ASC
                        """,
                        (int(session_id),),
                    )
                    item_rows = cur.fetchall() or []
                    if not item_rows:
                        raise RuntimeError("Nao ha resultados de conferencia para publicar na base oficial.")

                    inspection_id = _to_int(session_row.get("inspection_id"), 0)
                    contract_id = _to_int(session_row.get("contract_id"), 0)
                    fiscal_user_id = _to_int(session_row.get("fiscal_user_id"), 0)

                    cur.execute("DELETE FROM official_conference_results WHERE inspection_id = %s", (inspection_id,))

                    insert_sql = """
                    INSERT INTO official_conference_results (
                        conference_session_id,
                        inspection_id,
                        contract_id,
                        fiscal_user_id,
                        source,
                        item_origem,
                        inspection_item_id,
                        ordem,
                        area,
                        item_titulo,
                        descricao,
                        quantidade_verificada,
                        verificado_informado,
                        status,
                        observacao_tecnica,
                        local_verificado,
                        evidencia_ref
                    )
                    VALUES (
                        %s, %s, %s, %s, 'fiscal_conferencia', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    for item in item_rows:
                        row = dict(item) if isinstance(item, dict) else {}
                        from_field = bool(row.get("from_field"))
                        cur.execute(
                            insert_sql,
                            (
                                int(session_id),
                                inspection_id,
                                contract_id if contract_id > 0 else None,
                                fiscal_user_id,
                                "campo" if from_field else "planejado",
                                _to_int(row.get("inspection_item_id"), 0) or None,
                                _to_int(row.get("ordem"), 0),
                                str(row.get("area", "") or "").strip(),
                                str(row.get("item_titulo", "") or "").strip(),
                                str(row.get("descricao", "") or "").strip(),
                                _to_decimal(row.get("quantidade_verificada")),
                                bool(row.get("verificado_informado", False)),
                                str(row.get("status", "pendente") or "pendente").strip().lower(),
                                str(row.get("observacao_tecnica", "") or "").strip(),
                                str(row.get("local_verificado", "") or "").strip(),
                                str(row.get("evidencia_ref", "") or "").strip(),
                            ),
                        )

                    cur.execute(
                        """
                        UPDATE inspection_items ii
                        SET quantidade_verificada = r.quantidade_verificada,
                            quantidade_oficial = r.quantidade_verificada,
                            verificado_informado = COALESCE(r.verificado_informado, FALSE),
                            evidencia_ref = COALESCE(NULLIF(r.evidencia_ref, ''), ii.evidencia_ref),
                            status = COALESCE(NULLIF(r.status, ''), ii.status),
                            updated_at = NOW()
                        FROM inspection_conference_item_results r
                        WHERE r.session_id = %s
                          AND r.inspection_item_id = ii.id
                        """,
                        (int(session_id),),
                    )

                    cur.execute(
                        """
                        UPDATE inspections
                        SET status = 'concluida',
                            resultado = %s,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (str(inspection_resultado or "pendente").strip().lower(), inspection_id),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return dict(session_row)
