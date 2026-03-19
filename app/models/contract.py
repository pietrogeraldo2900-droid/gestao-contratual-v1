from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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


@dataclass(frozen=True)
class Contract:
    id: int
    contract_code: str
    title: str
    description: str
    status: str
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Contract":
        return cls(
            id=int(row.get("id", 0) or 0),
            contract_code=str(row.get("contract_code", "") or "").strip(),
            title=str(row.get("title", "") or "").strip(),
            description=str(row.get("description", "") or "").strip(),
            status=str(row.get("status", "") or "").strip(),
            created_at=_parse_datetime(row.get("created_at")),
            updated_at=_parse_datetime(row.get("updated_at")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "contract_code": self.contract_code,
            "title": self.title,
            "description": self.description,
            "status": self.status,
            "created_at": self.created_at.isoformat() if self.created_at != datetime.min else "",
            "updated_at": self.updated_at.isoformat() if self.updated_at != datetime.min else "",
        }


@dataclass(frozen=True)
class ContractCreateInput:
    contract_code: str
    title: str
    description: str = ""
    status: str = "rascunho"
