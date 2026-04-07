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


def _fmt_date(value: date | None) -> str:
    if not value:
        return ""
    return value.isoformat()


@dataclass(frozen=True)
class Inspection:
    id: int
    contract_id: int | None
    contract_label: str
    titulo: str
    data_vistoria: date | None
    periodo: str
    nucleo: str
    municipio: str
    local_vistoria: str
    equipe: str
    fiscal_nome: str
    fiscal_contato: str
    responsavel_nome: str
    responsavel_contato: str
    status: str
    prioridade: str
    resultado: str
    score_geral: Decimal
    observacoes: str
    created_by: int | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Inspection":
        contract_id_raw = row.get("contract_id")
        contract_id = _parse_int(contract_id_raw) if str(contract_id_raw or "").strip() else None
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

        created_by_raw = row.get("created_by")
        created_by = _parse_int(created_by_raw) if str(created_by_raw or "").strip() else None

        return cls(
            id=_parse_int(row.get("id")),
            contract_id=contract_id,
            contract_label=contract_label,
            titulo=str(row.get("titulo", "") or "").strip(),
            data_vistoria=_parse_date(row.get("data_vistoria")),
            periodo=str(row.get("periodo", "") or "").strip(),
            nucleo=str(row.get("nucleo", "") or "").strip(),
            municipio=str(row.get("municipio", "") or "").strip(),
            local_vistoria=str(row.get("local_vistoria", "") or "").strip(),
            equipe=str(row.get("equipe", "") or "").strip(),
            fiscal_nome=str(row.get("fiscal_nome", "") or "").strip(),
            fiscal_contato=str(row.get("fiscal_contato", "") or "").strip(),
            responsavel_nome=str(row.get("responsavel_nome", "") or "").strip(),
            responsavel_contato=str(row.get("responsavel_contato", "") or "").strip(),
            status=str(row.get("status", "") or "").strip(),
            prioridade=str(row.get("prioridade", "") or "").strip(),
            resultado=str(row.get("resultado", "") or "").strip(),
            score_geral=_parse_decimal(row.get("score_geral")),
            observacoes=str(row.get("observacoes", "") or "").strip(),
            created_by=created_by,
            created_at=_parse_datetime(row.get("created_at")),
            updated_at=_parse_datetime(row.get("updated_at")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "contract_id": self.contract_id,
            "contract_label": self.contract_label,
            "titulo": self.titulo,
            "data_vistoria": _fmt_date(self.data_vistoria),
            "periodo": self.periodo,
            "nucleo": self.nucleo,
            "municipio": self.municipio,
            "local_vistoria": self.local_vistoria,
            "equipe": self.equipe,
            "fiscal_nome": self.fiscal_nome,
            "fiscal_contato": self.fiscal_contato,
            "responsavel_nome": self.responsavel_nome,
            "responsavel_contato": self.responsavel_contato,
            "status": self.status,
            "prioridade": self.prioridade,
            "resultado": self.resultado,
            "score_geral": str(self.score_geral),
            "observacoes": self.observacoes,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at != datetime.min else "",
            "updated_at": self.updated_at.isoformat() if self.updated_at != datetime.min else "",
        }


@dataclass(frozen=True)
class InspectionItem:
    id: int
    inspection_id: int
    ordem: int
    area: str
    item_titulo: str
    descricao: str
    status: str
    severidade: str
    prazo_ajuste: date | None
    responsavel_ajuste: str
    valor_multa: Decimal
    evidencia_ref: str
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "InspectionItem":
        return cls(
            id=_parse_int(row.get("id")),
            inspection_id=_parse_int(row.get("inspection_id")),
            ordem=_parse_int(row.get("ordem")),
            area=str(row.get("area", "") or "").strip(),
            item_titulo=str(row.get("item_titulo", "") or "").strip(),
            descricao=str(row.get("descricao", "") or "").strip(),
            status=str(row.get("status", "") or "").strip(),
            severidade=str(row.get("severidade", "") or "").strip(),
            prazo_ajuste=_parse_date(row.get("prazo_ajuste")),
            responsavel_ajuste=str(row.get("responsavel_ajuste", "") or "").strip(),
            valor_multa=_parse_decimal(row.get("valor_multa")),
            evidencia_ref=str(row.get("evidencia_ref", "") or "").strip(),
            created_at=_parse_datetime(row.get("created_at")),
            updated_at=_parse_datetime(row.get("updated_at")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "inspection_id": self.inspection_id,
            "ordem": self.ordem,
            "area": self.area,
            "item_titulo": self.item_titulo,
            "descricao": self.descricao,
            "status": self.status,
            "severidade": self.severidade,
            "prazo_ajuste": _fmt_date(self.prazo_ajuste),
            "responsavel_ajuste": self.responsavel_ajuste,
            "valor_multa": str(self.valor_multa),
            "evidencia_ref": self.evidencia_ref,
            "created_at": self.created_at.isoformat() if self.created_at != datetime.min else "",
            "updated_at": self.updated_at.isoformat() if self.updated_at != datetime.min else "",
        }

