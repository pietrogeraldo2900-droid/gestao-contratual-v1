from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, List

from app.repositories.contract_repository import ContractRepository
from app.repositories.report_repository import ReportRepository


class ReportValidationError(ValueError):
    pass


class ReportService:
    def __init__(self, report_repository: ReportRepository, contract_repository: ContractRepository):
        self._report_repository = report_repository
        self._contract_repository = contract_repository

    def create_report(self, contract_id: int, file_name: str) -> dict[str, Any]:
        try:
            contract_id_int = int(contract_id)
        except Exception:
            raise ReportValidationError("contract_id invalido.")
        if contract_id_int <= 0:
            raise ReportValidationError("contract_id invalido.")

        normalized_name = Path(str(file_name or "").strip()).name
        if not normalized_name:
            raise ReportValidationError("file_name obrigatorio.")
        if len(normalized_name) > 255:
            raise ReportValidationError("file_name excede 255 caracteres.")

        if not self._contract_repository.exists_contract(contract_id_int):
            raise ReportValidationError("Contrato nao encontrado para o contract_id informado.")

        created = self._report_repository.create_report(contract_id_int, normalized_name)
        created_at = created.get("created_at")
        if isinstance(created_at, datetime):
            created["created_at"] = created_at.isoformat()
        else:
            created["created_at"] = str(created_at or "")
        created["id"] = int(created.get("id", 0) or 0)
        created["contract_id"] = int(created.get("contract_id", 0) or 0)
        created["file_name"] = str(created.get("file_name", "") or "")
        return created

    def list_reports_by_contract(self, contract_id: int) -> List[dict[str, Any]]:
        try:
            contract_id_int = int(contract_id)
        except Exception:
            raise ReportValidationError("contract_id invalido.")
        if contract_id_int <= 0:
            raise ReportValidationError("contract_id invalido.")
        if not self._contract_repository.exists_contract(contract_id_int):
            raise ReportValidationError("Contrato nao encontrado para o contract_id informado.")

        rows = self._report_repository.list_reports_by_contract(contract_id_int)
        out: List[dict[str, Any]] = []
        for row in rows:
            created_at = row.get("created_at")
            out.append(
                {
                    "id": int(row.get("id", 0) or 0),
                    "contract_id": int(row.get("contract_id", 0) or 0),
                    "file_name": str(row.get("file_name", "") or ""),
                    "created_at": created_at.isoformat() if isinstance(created_at, datetime) else str(created_at or ""),
                }
            )
        return out


def create_report_service(service: ReportService, contract_id: int, file_name: str) -> dict[str, Any]:
    return service.create_report(contract_id, file_name)


def list_reports_by_contract_service(service: ReportService, contract_id: int) -> List[dict[str, Any]]:
    return service.list_reports_by_contract(contract_id)
