from __future__ import annotations

from datetime import datetime, timedelta, timezone
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

    def _to_iso(self, value: Any) -> str:
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value or "")

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
        created["created_at"] = self._to_iso(created.get("created_at"))
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
            out.append(
                {
                    "id": int(row.get("id", 0) or 0),
                    "contract_id": int(row.get("contract_id", 0) or 0),
                    "file_name": str(row.get("file_name", "") or ""),
                    "created_at": self._to_iso(row.get("created_at")),
                }
            )
        return out

    def get_contract_context(self, contract_id: int) -> dict[str, Any]:
        try:
            contract_id_int = int(contract_id)
        except Exception:
            raise ReportValidationError("contract_id invalido.")
        if contract_id_int <= 0:
            raise ReportValidationError("contract_id invalido.")
        context = self._contract_repository.get_contract_report_context(contract_id_int)
        if not context:
            raise ReportValidationError("Contrato nao encontrado para o contract_id informado.")
        return context

    def count_reports(self) -> int:
        return max(0, int(self._report_repository.count_reports()))

    def count_recent_reports(self, days: int = 7) -> int:
        try:
            window_days = int(days)
        except Exception:
            window_days = 7
        window_days = max(1, min(window_days, 3650))
        since = datetime.now(timezone.utc) - timedelta(days=window_days)
        return max(0, int(self._report_repository.count_recent_reports(since)))

    def list_recent_reports(self, limit: int = 20) -> List[dict[str, Any]]:
        try:
            safe_limit = int(limit)
        except Exception:
            safe_limit = 20
        safe_limit = max(1, min(safe_limit, 100))

        rows = self._report_repository.list_recent_reports(limit=safe_limit)
        out: List[dict[str, Any]] = []
        for row in rows:
            numero_contrato = str(row.get("numero_contrato", row.get("contract_code", "")) or "")
            nome_contrato = str(row.get("nome_contrato", row.get("contract_title", "")) or "")
            out.append(
                {
                    "id": int(row.get("id", 0) or 0),
                    "contract_id": int(row.get("contract_id", 0) or 0),
                    "file_name": str(row.get("file_name", "") or ""),
                    "created_at": self._to_iso(row.get("created_at")),
                    "numero_contrato": numero_contrato,
                    "nome_contrato": nome_contrato,
                    "contratante_nome": str(row.get("contratante_nome", "") or ""),
                    "contratada_nome": str(row.get("contratada_nome", "") or ""),
                    "municipios_atendidos": str(row.get("municipios_atendidos", "") or ""),
                    "objeto_contrato": str(row.get("objeto_contrato", "") or ""),
                    "status_contrato": str(row.get("status_contrato", "") or ""),
                    # Aliases legados para compatibilidade.
                    "contract_code": numero_contrato,
                    "contract_title": nome_contrato,
                }
            )
        return out


def create_report_service(service: ReportService, contract_id: int, file_name: str) -> dict[str, Any]:
    return service.create_report(contract_id, file_name)


def list_reports_by_contract_service(service: ReportService, contract_id: int) -> List[dict[str, Any]]:
    return service.list_reports_by_contract(contract_id)
