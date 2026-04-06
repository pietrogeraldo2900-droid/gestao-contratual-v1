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
    keys = [
        "id",
        "nome_contrato",
        "numero_contrato",
        "objeto_contrato",
        "data_assinatura",
        "vigencia_inicio",
        "vigencia_fim",
        "prazo_dias",
        "valor_contrato",
        "contratante_nome",
        "contratante_cnpj",
        "contratada_nome",
        "contratada_cnpj",
        "regional",
        "diretoria",
        "municipios_atendidos",
        "status_contrato",
        "reajuste_indice",
        "prazo_pagamento_dias",
        "possui_ordem_servico",
        "observacoes",
        "created_at",
        "updated_at",
    ]
    return {k: values[idx] if idx < len(values) else None for idx, k in enumerate(keys)}


def _to_legacy_status(status_contrato: str) -> str:
    normalized = str(status_contrato or "").strip().lower()
    mapping = {
        "em_implantacao": "rascunho",
        "ativo": "ativo",
        "suspenso": "cancelado",
        "encerrado": "encerrado",
    }
    return mapping.get(normalized, "rascunho")


class ContractRepository:
    def __init__(self, db: DatabaseManager):
        self._db = db

    def count_contracts(self) -> int:
        sql = "SELECT COUNT(*) FROM contracts"
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
        if not row:
            return 0
        if isinstance(row, dict):
            return int(row.get("count", 0) or 0)
        return int(row[0] or 0)

    def exists_contract(self, contract_id: int) -> bool:
        sql = "SELECT 1 FROM contracts WHERE id = %s LIMIT 1"
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(contract_id),))
                row = cur.fetchone()
        return bool(row)

    def create_contract(self, payload: ContractCreateInput) -> Contract:
        legacy_status = _to_legacy_status(payload.status_contrato)
        sql = """
        INSERT INTO contracts (
            contract_code,
            title,
            description,
            status,
            nome_contrato,
            numero_contrato,
            objeto_contrato,
            data_assinatura,
            vigencia_inicio,
            vigencia_fim,
            prazo_dias,
            valor_contrato,
            contratante_nome,
            contratante_cnpj,
            contratada_nome,
            contratada_cnpj,
            regional,
            diretoria,
            municipios_atendidos,
            status_contrato,
            reajuste_indice,
            prazo_pagamento_dias,
            possui_ordem_servico,
            observacoes
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        RETURNING
            id,
            nome_contrato,
            numero_contrato,
            objeto_contrato,
            data_assinatura,
            vigencia_inicio,
            vigencia_fim,
            prazo_dias,
            valor_contrato,
            contratante_nome,
            contratante_cnpj,
            contratada_nome,
            contratada_cnpj,
            regional,
            diretoria,
            municipios_atendidos,
            status_contrato,
            reajuste_indice,
            prazo_pagamento_dias,
            possui_ordem_servico,
            observacoes,
            created_at,
            updated_at
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
                            payload.numero_contrato,
                            payload.nome_contrato,
                            payload.objeto_contrato,
                            legacy_status,
                            payload.nome_contrato,
                            payload.numero_contrato,
                            payload.objeto_contrato,
                            payload.data_assinatura,
                            payload.vigencia_inicio,
                            payload.vigencia_fim,
                            payload.prazo_dias,
                            payload.valor_contrato,
                            payload.contratante_nome,
                            payload.contratante_cnpj,
                            payload.contratada_nome,
                            payload.contratada_cnpj,
                            payload.regional,
                            payload.diretoria,
                            payload.municipios_atendidos,
                            payload.status_contrato,
                            payload.reajuste_indice,
                            payload.prazo_pagamento_dias,
                            payload.possui_ordem_servico,
                            payload.observacoes,
                        ),
                    )
                    row = cur.fetchone()
                conn.commit()
            except Exception as exc:
                conn.rollback()
                if _is_unique_violation(exc):
                    raise ContractConflictError("Ja existe contrato com este numero.") from exc
                raise

        contract_row = _row_to_dict(row)
        if not contract_row:
            raise RuntimeError("Falha ao criar contrato: resposta vazia do banco.")
        return Contract.from_row(contract_row)

    def list_contracts(self, limit: int = 100) -> List[Contract]:
        sql = """
        SELECT
            id,
            nome_contrato,
            numero_contrato,
            objeto_contrato,
            data_assinatura,
            vigencia_inicio,
            vigencia_fim,
            prazo_dias,
            valor_contrato,
            contratante_nome,
            contratante_cnpj,
            contratada_nome,
            contratada_cnpj,
            regional,
            diretoria,
            municipios_atendidos,
            status_contrato,
            reajuste_indice,
            prazo_pagamento_dias,
            possui_ordem_servico,
            observacoes,
            created_at,
            updated_at
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

    def get_contract_report_context(self, contract_id: int) -> dict[str, Any] | None:
        sql = """
        SELECT
            id,
            COALESCE(NULLIF(numero_contrato, ''), contract_code) AS numero_contrato,
            COALESCE(NULLIF(nome_contrato, ''), title) AS nome_contrato,
            COALESCE(NULLIF(contratante_nome, ''), '') AS contratante_nome,
            COALESCE(NULLIF(contratada_nome, ''), '') AS contratada_nome,
            COALESCE(NULLIF(municipios_atendidos, ''), '') AS municipios_atendidos,
            COALESCE(NULLIF(objeto_contrato, ''), description) AS objeto_contrato,
            COALESCE(NULLIF(status_contrato, ''), status) AS status_contrato
        FROM contracts
        WHERE id = %s
        LIMIT 1
        """
        with self._db.connection() as conn:
            cursor_kwargs = {}
            dict_factory = _dict_row_factory()
            if dict_factory is not None:
                cursor_kwargs["row_factory"] = dict_factory
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(sql, (int(contract_id),))
                row = cur.fetchone()
        if not row:
            return None
        parsed = row if isinstance(row, dict) else {
            "id": row[0],
            "numero_contrato": row[1],
            "nome_contrato": row[2],
            "contratante_nome": row[3],
            "contratada_nome": row[4],
            "municipios_atendidos": row[5],
            "objeto_contrato": row[6],
            "status_contrato": row[7],
        }
        return {
            "id": int(parsed.get("id", 0) or 0),
            "numero_contrato": str(parsed.get("numero_contrato", "") or ""),
            "nome_contrato": str(parsed.get("nome_contrato", "") or ""),
            "contratante_nome": str(parsed.get("contratante_nome", "") or ""),
            "contratada_nome": str(parsed.get("contratada_nome", "") or ""),
            "municipios_atendidos": str(parsed.get("municipios_atendidos", "") or ""),
            "objeto_contrato": str(parsed.get("objeto_contrato", "") or ""),
            "status_contrato": str(parsed.get("status_contrato", "") or ""),
        }
