from __future__ import annotations

from typing import Any, List

from app.models.contract import Contract, ContractCreateInput
from app.repositories.contract_repository import ContractConflictError, ContractRepository


class ContractValidationError(ValueError):
    pass


class ContractService:
    _ALLOWED_STATUS = {"rascunho", "ativo", "encerrado", "cancelado"}

    def __init__(self, repository: ContractRepository):
        self._repository = repository

    def init_schema(self) -> None:
        # Mantido por compatibilidade de interface. Inicializacao agora ocorre em app.database.init_db.
        return None

    def list_contracts(self, limit: int = 100) -> List[Contract]:
        limit = max(1, min(int(limit), 500))
        return self._repository.list_contracts(limit=limit)

    def create_contract(self, payload: dict[str, Any]) -> Contract:
        contract_code = " ".join(str(payload.get("contract_code", "") or "").split()).strip()
        title = " ".join(str(payload.get("title", "") or "").split()).strip()
        description = str(payload.get("description", "") or "").strip()
        status = str(payload.get("status", "rascunho") or "rascunho").strip().lower()
        if not status:
            status = "rascunho"

        if not contract_code:
            raise ContractValidationError("Informe o codigo do contrato.")
        if not title:
            raise ContractValidationError("Informe o titulo do contrato.")
        if len(contract_code) > 80:
            raise ContractValidationError("Codigo do contrato deve ter no maximo 80 caracteres.")
        if len(title) > 255:
            raise ContractValidationError("Titulo do contrato deve ter no maximo 255 caracteres.")
        if status not in self._ALLOWED_STATUS:
            raise ContractValidationError(
                "Status invalido. Use: rascunho, ativo, encerrado ou cancelado."
            )

        clean_payload = ContractCreateInput(
            contract_code=contract_code,
            title=title,
            description=description,
            status=status,
        )
        return self._repository.create_contract(clean_payload)


__all__ = ["ContractConflictError", "ContractService", "ContractValidationError"]
