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


def _parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"1", "true", "on", "yes", "sim", "s"}:
        return True
    if text in {"0", "false", "off", "no", "nao", "n"}:
        return False
    return None


def _date_to_str(value: date | None) -> str:
    if value is None:
        return ""
    return value.isoformat()


def _decimal_to_str(value: Decimal) -> str:
    try:
        return f"{value:.2f}"
    except Exception:
        return "0.00"


@dataclass(frozen=True)
class Contract:
    id: int
    nome_contrato: str
    numero_contrato: str
    objeto_contrato: str
    data_assinatura: date | None
    vigencia_inicio: date | None
    vigencia_fim: date | None
    prazo_dias: int
    valor_contrato: Decimal
    contratante_nome: str
    contratante_cnpj: str
    contratada_nome: str
    contratada_cnpj: str
    regional: str
    diretoria: str
    municipios_atendidos: str
    status_contrato: str
    reajuste_indice: str
    prazo_pagamento_dias: int | None
    possui_ordem_servico: bool | None
    observacoes: str
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Contract":
        numero_contrato = str(
            row.get("numero_contrato")
            or row.get("contract_code")
            or ""
        ).strip()
        nome_contrato = str(
            row.get("nome_contrato")
            or row.get("title")
            or ""
        ).strip()
        objeto_contrato = str(
            row.get("objeto_contrato")
            or row.get("description")
            or ""
        ).strip()
        status_contrato = str(
            row.get("status_contrato")
            or row.get("status")
            or ""
        ).strip()
        return cls(
            id=int(row.get("id", 0) or 0),
            nome_contrato=nome_contrato,
            numero_contrato=numero_contrato,
            objeto_contrato=objeto_contrato,
            data_assinatura=_parse_date(row.get("data_assinatura")),
            vigencia_inicio=_parse_date(row.get("vigencia_inicio")),
            vigencia_fim=_parse_date(row.get("vigencia_fim")),
            prazo_dias=_parse_int(row.get("prazo_dias")),
            valor_contrato=_parse_decimal(row.get("valor_contrato")),
            contratante_nome=str(row.get("contratante_nome", "") or "").strip(),
            contratante_cnpj=str(row.get("contratante_cnpj", "") or "").strip(),
            contratada_nome=str(row.get("contratada_nome", "") or "").strip(),
            contratada_cnpj=str(row.get("contratada_cnpj", "") or "").strip(),
            regional=str(row.get("regional", "") or "").strip(),
            diretoria=str(row.get("diretoria", "") or "").strip(),
            municipios_atendidos=str(row.get("municipios_atendidos", "") or "").strip(),
            status_contrato=status_contrato,
            reajuste_indice=str(row.get("reajuste_indice", "") or "").strip(),
            prazo_pagamento_dias=_parse_optional_int(row.get("prazo_pagamento_dias")),
            possui_ordem_servico=_parse_bool(row.get("possui_ordem_servico")),
            observacoes=str(row.get("observacoes", "") or "").strip(),
            created_at=_parse_datetime(row.get("created_at")),
            updated_at=_parse_datetime(row.get("updated_at")),
        )

    @property
    def contract_code(self) -> str:
        return self.numero_contrato

    @property
    def title(self) -> str:
        return self.nome_contrato

    @property
    def description(self) -> str:
        return self.objeto_contrato

    @property
    def status(self) -> str:
        return self.status_contrato

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "nome_contrato": self.nome_contrato,
            "numero_contrato": self.numero_contrato,
            "objeto_contrato": self.objeto_contrato,
            "data_assinatura": _date_to_str(self.data_assinatura),
            "vigencia_inicio": _date_to_str(self.vigencia_inicio),
            "vigencia_fim": _date_to_str(self.vigencia_fim),
            "prazo_dias": self.prazo_dias,
            "valor_contrato": _decimal_to_str(self.valor_contrato),
            "contratante_nome": self.contratante_nome,
            "contratante_cnpj": self.contratante_cnpj,
            "contratada_nome": self.contratada_nome,
            "contratada_cnpj": self.contratada_cnpj,
            "regional": self.regional,
            "diretoria": self.diretoria,
            "municipios_atendidos": self.municipios_atendidos,
            "status_contrato": self.status_contrato,
            "reajuste_indice": self.reajuste_indice,
            "prazo_pagamento_dias": self.prazo_pagamento_dias,
            "possui_ordem_servico": self.possui_ordem_servico,
            "observacoes": self.observacoes,
            # Aliases legados para compatibilidade com API/fluxos existentes.
            "contract_code": self.numero_contrato,
            "title": self.nome_contrato,
            "description": self.objeto_contrato,
            "status": self.status_contrato,
            "created_at": self.created_at.isoformat() if self.created_at != datetime.min else "",
            "updated_at": self.updated_at.isoformat() if self.updated_at != datetime.min else "",
        }


@dataclass(frozen=True)
class ContractCreateInput:
    nome_contrato: str
    numero_contrato: str
    objeto_contrato: str
    data_assinatura: date
    prazo_dias: int
    valor_contrato: Decimal
    contratante_nome: str
    contratante_cnpj: str
    contratada_nome: str
    contratada_cnpj: str
    regional: str
    diretoria: str
    municipios_atendidos: str
    status_contrato: str
    vigencia_inicio: date | None = None
    vigencia_fim: date | None = None
    reajuste_indice: str = ""
    prazo_pagamento_dias: int | None = None
    possui_ordem_servico: bool | None = None
    observacoes: str = ""
