from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return datetime.min
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return datetime.min


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        return None


def _parse_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _parse_optional_int(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


def _parse_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    text = str(value or "").strip().replace(",", ".")
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "on", "yes", "sim", "s"}


def _fmt_date(value: date | None) -> str:
    if value is None:
        return ""
    return value.isoformat()


@dataclass(frozen=True)
class DailyExecutionDeclaration:
    id: int
    contract_id: int | None
    contract_label: str
    declaration_date: date | None
    periodo: str
    nucleo: str
    municipio: str
    logradouro: str
    equipe: str
    responsavel_nome: str
    responsavel_contato: str
    observacoes: str
    is_official_base: bool
    generated_inspection_id: int | None
    created_by: int | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "DailyExecutionDeclaration":
        contract_id = _parse_optional_int(row.get("contract_id"))
        numero = str(row.get("numero_contrato", "") or "").strip()
        nome = str(row.get("nome_contrato", "") or "").strip()
        if numero and nome:
            contract_label = f"{numero} - {nome}"
        elif nome:
            contract_label = nome
        elif numero:
            contract_label = numero
        else:
            contract_label = ""

        return cls(
            id=_parse_int(row.get("id")),
            contract_id=contract_id,
            contract_label=contract_label,
            declaration_date=_parse_date(row.get("declaration_date")),
            periodo=str(row.get("periodo", "") or "").strip(),
            nucleo=str(row.get("nucleo", "") or "").strip(),
            municipio=str(row.get("municipio", "") or "").strip(),
            logradouro=str(row.get("logradouro", "") or "").strip(),
            equipe=str(row.get("equipe", "") or "").strip(),
            responsavel_nome=str(row.get("responsavel_nome", "") or "").strip(),
            responsavel_contato=str(row.get("responsavel_contato", "") or "").strip(),
            observacoes=str(row.get("observacoes", "") or "").strip(),
            is_official_base=_parse_bool(row.get("is_official_base")),
            generated_inspection_id=_parse_optional_int(row.get("generated_inspection_id")),
            created_by=_parse_optional_int(row.get("created_by")),
            created_at=_parse_datetime(row.get("created_at")),
            updated_at=_parse_datetime(row.get("updated_at")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "contract_id": self.contract_id,
            "contract_label": self.contract_label,
            "declaration_date": _fmt_date(self.declaration_date),
            "periodo": self.periodo,
            "nucleo": self.nucleo,
            "municipio": self.municipio,
            "logradouro": self.logradouro,
            "equipe": self.equipe,
            "responsavel_nome": self.responsavel_nome,
            "responsavel_contato": self.responsavel_contato,
            "observacoes": self.observacoes,
            "is_official_base": self.is_official_base,
            "generated_inspection_id": self.generated_inspection_id,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at != datetime.min else "",
            "updated_at": self.updated_at.isoformat() if self.updated_at != datetime.min else "",
        }


@dataclass(frozen=True)
class DailyExecutionDeclarationItem:
    id: int
    declaration_id: int
    ordem: int
    servico_oficial: str
    servico_label: str
    categoria: str
    quantidade: Decimal
    unidade: str
    local_execucao: str
    descricao: str
    item_status: str
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "DailyExecutionDeclarationItem":
        return cls(
            id=_parse_int(row.get("id")),
            declaration_id=_parse_int(row.get("declaration_id")),
            ordem=_parse_int(row.get("ordem")),
            servico_oficial=str(row.get("servico_oficial", "") or "").strip(),
            servico_label=str(row.get("servico_label", "") or "").strip(),
            categoria=str(row.get("categoria", "") or "").strip(),
            quantidade=_parse_decimal(row.get("quantidade")),
            unidade=str(row.get("unidade", "") or "").strip(),
            local_execucao=str(row.get("local_execucao", "") or "").strip(),
            descricao=str(row.get("descricao", "") or "").strip(),
            item_status=str(row.get("item_status", "") or "").strip(),
            created_at=_parse_datetime(row.get("created_at")),
            updated_at=_parse_datetime(row.get("updated_at")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "declaration_id": self.declaration_id,
            "ordem": self.ordem,
            "servico_oficial": self.servico_oficial,
            "servico_label": self.servico_label,
            "categoria": self.categoria,
            "quantidade": str(self.quantidade),
            "unidade": self.unidade,
            "local_execucao": self.local_execucao,
            "descricao": self.descricao,
            "item_status": self.item_status,
            "created_at": self.created_at.isoformat() if self.created_at != datetime.min else "",
            "updated_at": self.updated_at.isoformat() if self.updated_at != datetime.min else "",
        }


__all__ = ["DailyExecutionDeclaration", "DailyExecutionDeclarationItem"]
