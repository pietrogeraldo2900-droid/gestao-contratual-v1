from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Iterable

from app.database.connection import DatabaseManager
from app.models.declaration import DailyExecutionDeclaration, DailyExecutionDeclarationItem


def _dict_row_factory():
    try:
        from psycopg.rows import dict_row

        return dict_row
    except Exception:
        return None


def _ensure_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _ensure_decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value or "0").replace(",", "."))
    except Exception:
        return Decimal("0")


class DailyExecutionDeclarationRepository:
    def __init__(self, db: DatabaseManager):
        self._db = db

    def _cursor_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        dict_factory = _dict_row_factory()
        if dict_factory is not None:
            kwargs["row_factory"] = dict_factory
        return kwargs

    def list_declarations(
        self,
        *,
        limit: int = 200,
        contract_id: int | None = None,
        created_by: int | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> list[DailyExecutionDeclaration]:
        clauses = []
        params: list[Any] = []
        if contract_id:
            clauses.append("d.contract_id = %s")
            params.append(int(contract_id))
        if created_by:
            clauses.append("d.created_by = %s")
            params.append(int(created_by))
        if date_from:
            clauses.append("d.declaration_date >= %s")
            params.append(date_from)
        if date_to:
            clauses.append("d.declaration_date <= %s")
            params.append(date_to)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
        SELECT
            d.id,
            d.contract_id,
            d.declaration_date,
            d.periodo,
            d.nucleo,
            d.municipio,
            d.logradouro,
            d.equipe,
            d.responsavel_nome,
            d.responsavel_contato,
            d.observacoes,
            d.is_official_base,
            d.generated_inspection_id,
            d.created_by,
            d.created_at,
            d.updated_at,
            c.numero_contrato,
            c.nome_contrato
        FROM daily_execution_declarations d
        LEFT JOIN contracts c ON c.id = d.contract_id
        {where_sql}
        ORDER BY d.declaration_date DESC, d.created_at DESC, d.id DESC
        LIMIT %s
        """
        params.append(max(1, min(int(limit), 1000)))

        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall() or []
        return [DailyExecutionDeclaration.from_row(row if isinstance(row, dict) else {}) for row in rows]

    def get_declaration(self, declaration_id: int) -> DailyExecutionDeclaration | None:
        sql = """
        SELECT
            d.id,
            d.contract_id,
            d.declaration_date,
            d.periodo,
            d.nucleo,
            d.municipio,
            d.logradouro,
            d.equipe,
            d.responsavel_nome,
            d.responsavel_contato,
            d.observacoes,
            d.is_official_base,
            d.generated_inspection_id,
            d.created_by,
            d.created_at,
            d.updated_at,
            c.numero_contrato,
            c.nome_contrato
        FROM daily_execution_declarations d
        LEFT JOIN contracts c ON c.id = d.contract_id
        WHERE d.id = %s
        LIMIT 1
        """
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, (int(declaration_id),))
                row = cur.fetchone()
        if not row:
            return None
        return DailyExecutionDeclaration.from_row(row if isinstance(row, dict) else {})

    def list_items(self, declaration_id: int) -> list[DailyExecutionDeclarationItem]:
        sql = """
        SELECT
            id,
            declaration_id,
            ordem,
            servico_oficial,
            servico_label,
            categoria,
            quantidade,
            unidade,
            local_execucao,
            descricao,
            item_status,
            created_at,
            updated_at
        FROM daily_execution_declaration_items
        WHERE declaration_id = %s
        ORDER BY ordem ASC, id ASC
        """
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, (int(declaration_id),))
                rows = cur.fetchall() or []
        return [DailyExecutionDeclarationItem.from_row(row if isinstance(row, dict) else {}) for row in rows]

    def create_declaration(
        self,
        *,
        contract_id: int,
        declaration_date: date,
        periodo: str,
        nucleo: str,
        municipio: str,
        logradouro: str,
        equipe: str,
        responsavel_nome: str,
        responsavel_contato: str,
        observacoes: str,
        created_by: int | None,
        items: Iterable[dict[str, Any]],
    ) -> DailyExecutionDeclaration:
        sql_header = """
        INSERT INTO daily_execution_declarations (
            contract_id,
            declaration_date,
            periodo,
            nucleo,
            municipio,
            logradouro,
            equipe,
            responsavel_nome,
            responsavel_contato,
            observacoes,
            is_official_base,
            created_by
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, %s
        )
        RETURNING id
        """
        sql_item = """
        INSERT INTO daily_execution_declaration_items (
            declaration_id,
            ordem,
            servico_oficial,
            servico_label,
            categoria,
            quantidade,
            unidade,
            local_execucao,
            descricao,
            item_status
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        with self._db.connection() as conn:
            try:
                with conn.cursor(**self._cursor_kwargs()) as cur:
                    cur.execute(
                        sql_header,
                        (
                            int(contract_id),
                            declaration_date,
                            str(periodo or "").strip(),
                            str(nucleo or "").strip(),
                            str(municipio or "").strip(),
                            str(logradouro or "").strip(),
                            str(equipe or "").strip(),
                            str(responsavel_nome or "").strip(),
                            str(responsavel_contato or "").strip(),
                            str(observacoes or "").strip(),
                            int(created_by) if created_by else None,
                        ),
                    )
                    created = cur.fetchone()
                    if not created:
                        raise RuntimeError("Falha ao criar declaracao diaria.")
                    declaration_id = _ensure_int(created.get("id") if isinstance(created, dict) else created[0], 0)

                    for idx, item in enumerate(list(items or [])):
                        service_name = str((item or {}).get("servico_oficial", "") or "").strip()
                        if not service_name:
                            continue
                        cur.execute(
                            sql_item,
                            (
                                declaration_id,
                                idx + 1,
                                service_name,
                                str((item or {}).get("servico_label", "") or "").strip(),
                                str((item or {}).get("categoria", "") or "").strip(),
                                _ensure_decimal((item or {}).get("quantidade")),
                                str((item or {}).get("unidade", "") or "").strip(),
                                str((item or {}).get("local_execucao", "") or "").strip(),
                                str((item or {}).get("descricao", "") or "").strip(),
                                str((item or {}).get("item_status", "declarado") or "declarado").strip().lower(),
                            ),
                        )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        loaded = self.get_declaration(declaration_id)
        if loaded is None:
            raise RuntimeError("Falha ao carregar declaracao criada.")
        return loaded

    def link_generated_inspection(self, declaration_id: int, inspection_id: int) -> bool:
        sql = """
        UPDATE daily_execution_declarations
        SET generated_inspection_id = %s, updated_at = NOW()
        WHERE id = %s
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(inspection_id), int(declaration_id)))
                changed = int(getattr(cur, "rowcount", 0) or 0)
            conn.commit()
        return changed > 0

    def delete_declaration(self, declaration_id: int) -> bool:
        sql = "DELETE FROM daily_execution_declarations WHERE id = %s"
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(declaration_id),))
                changed = int(getattr(cur, "rowcount", 0) or 0)
            conn.commit()
        return changed > 0

