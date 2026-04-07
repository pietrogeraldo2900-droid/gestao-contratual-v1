from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Iterable

from app.database.connection import DatabaseManager
from app.models.inspection import Inspection, InspectionItem


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


class InspectionRepository:
    def __init__(self, db: DatabaseManager):
        self._db = db

    def _cursor_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        dict_factory = _dict_row_factory()
        if dict_factory is not None:
            kwargs["row_factory"] = dict_factory
        return kwargs

    def list_inspections(
        self,
        *,
        limit: int = 200,
        contract_id: int | None = None,
        status: str = "",
        date_from: date | None = None,
        date_to: date | None = None,
        query: str = "",
    ) -> list[Inspection]:
        clauses = []
        params: list[Any] = []

        if contract_id:
            clauses.append("i.contract_id = %s")
            params.append(int(contract_id))
        if status:
            clauses.append("LOWER(i.status) = %s")
            params.append(str(status or "").strip().lower())
        if date_from:
            clauses.append("i.data_vistoria >= %s")
            params.append(date_from)
        if date_to:
            clauses.append("i.data_vistoria <= %s")
            params.append(date_to)
        if query:
            clauses.append(
                "("
                "i.titulo ILIKE %s OR i.nucleo ILIKE %s OR i.municipio ILIKE %s OR "
                "i.local_vistoria ILIKE %s OR i.equipe ILIKE %s"
                ")"
            )
            like = f"%{str(query).strip()}%"
            params.extend([like, like, like, like, like])

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
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
        {where_sql}
        ORDER BY i.data_vistoria DESC, i.created_at DESC, i.id DESC
        LIMIT %s
        """
        params.append(max(1, min(int(limit), 1000)))

        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall() or []
        return [Inspection.from_row(row if isinstance(row, dict) else {}) for row in rows]

    def count_inspections(
        self,
        *,
        contract_id: int | None = None,
        status: str = "",
    ) -> int:
        clauses = []
        params: list[Any] = []
        if contract_id:
            clauses.append("contract_id = %s")
            params.append(int(contract_id))
        if status:
            clauses.append("LOWER(status) = %s")
            params.append(str(status or "").strip().lower())
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT COUNT(*) AS total FROM inspections {where_sql}"
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, tuple(params))
                row = cur.fetchone()
        if isinstance(row, dict):
            return _ensure_int(row.get("total"), 0)
        if row:
            return _ensure_int(row[0], 0)
        return 0

    def get_inspection(self, inspection_id: int) -> Inspection | None:
        sql = """
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
        WHERE i.id = %s
        LIMIT 1
        """
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, (int(inspection_id),))
                row = cur.fetchone()
        if not row:
            return None
        return Inspection.from_row(row if isinstance(row, dict) else {})

    def list_items(self, inspection_id: int) -> list[InspectionItem]:
        sql = """
        SELECT
            id,
            inspection_id,
            ordem,
            area,
            item_titulo,
            descricao,
            status,
            severidade,
            prazo_ajuste,
            responsavel_ajuste,
            valor_multa,
            evidencia_ref,
            created_at,
            updated_at
        FROM inspection_items
        WHERE inspection_id = %s
        ORDER BY ordem ASC, id ASC
        """
        with self._db.connection() as conn:
            with conn.cursor(**self._cursor_kwargs()) as cur:
                cur.execute(sql, (int(inspection_id),))
                rows = cur.fetchall() or []
        return [InspectionItem.from_row(row if isinstance(row, dict) else {}) for row in rows]

    def create_inspection(
        self,
        *,
        contract_id: int | None,
        titulo: str,
        data_vistoria: date,
        periodo: str,
        nucleo: str,
        municipio: str,
        local_vistoria: str,
        equipe: str,
        fiscal_nome: str,
        fiscal_contato: str,
        responsavel_nome: str,
        responsavel_contato: str,
        status: str,
        prioridade: str,
        resultado: str,
        score_geral: Decimal,
        observacoes: str,
        created_by: int | None,
        items: Iterable[dict[str, Any]],
    ) -> Inspection:
        sql_header = """
        INSERT INTO inspections (
            contract_id,
            titulo,
            data_vistoria,
            periodo,
            nucleo,
            municipio,
            local_vistoria,
            equipe,
            fiscal_nome,
            fiscal_contato,
            responsavel_nome,
            responsavel_contato,
            status,
            prioridade,
            resultado,
            score_geral,
            observacoes,
            created_by
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        RETURNING id
        """
        sql_item = """
        INSERT INTO inspection_items (
            inspection_id,
            ordem,
            area,
            item_titulo,
            descricao,
            status,
            severidade,
            prazo_ajuste,
            responsavel_ajuste,
            valor_multa,
            evidencia_ref
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        with self._db.connection() as conn:
            try:
                with conn.cursor(**self._cursor_kwargs()) as cur:
                    cur.execute(
                        sql_header,
                        (
                            int(contract_id) if contract_id else None,
                            str(titulo or "").strip(),
                            data_vistoria,
                            str(periodo or "").strip(),
                            str(nucleo or "").strip(),
                            str(municipio or "").strip(),
                            str(local_vistoria or "").strip(),
                            str(equipe or "").strip(),
                            str(fiscal_nome or "").strip(),
                            str(fiscal_contato or "").strip(),
                            str(responsavel_nome or "").strip(),
                            str(responsavel_contato or "").strip(),
                            str(status or "").strip(),
                            str(prioridade or "").strip(),
                            str(resultado or "").strip(),
                            _ensure_decimal(score_geral),
                            str(observacoes or "").strip(),
                            int(created_by) if created_by else None,
                        ),
                    )
                    created = cur.fetchone()
                    if not created:
                        raise RuntimeError("Falha ao criar vistoria.")
                    inspection_id = _ensure_int(created.get("id") if isinstance(created, dict) else created[0], 0)
                    for idx, item in enumerate(list(items or [])):
                        title = str((item or {}).get("item_titulo", "") or "").strip()
                        if not title:
                            continue
                        prazo = (item or {}).get("prazo_ajuste")
                        cur.execute(
                            sql_item,
                            (
                                inspection_id,
                                idx + 1,
                                str((item or {}).get("area", "") or "").strip(),
                                title,
                                str((item or {}).get("descricao", "") or "").strip(),
                                str((item or {}).get("status", "pendente") or "pendente").strip().lower(),
                                str((item or {}).get("severidade", "baixa") or "baixa").strip().lower(),
                                prazo,
                                str((item or {}).get("responsavel_ajuste", "") or "").strip(),
                                _ensure_decimal((item or {}).get("valor_multa")),
                                str((item or {}).get("evidencia_ref", "") or "").strip(),
                            ),
                        )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        loaded = self.get_inspection(inspection_id)
        if loaded is None:
            raise RuntimeError("Falha ao carregar vistoria criada.")
        return loaded

    def update_inspection_status(self, inspection_id: int, status: str) -> bool:
        sql = """
        UPDATE inspections
        SET status = %s, updated_at = NOW()
        WHERE id = %s
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (str(status or "").strip().lower(), int(inspection_id)))
                changed = int(getattr(cur, "rowcount", 0) or 0)
            conn.commit()
        return changed > 0

