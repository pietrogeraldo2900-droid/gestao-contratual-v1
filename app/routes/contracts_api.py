from __future__ import annotations

from typing import Any

from flask import Flask, jsonify, request

from app.repositories.contract_repository import ContractConflictError
from app.services.contract_service import ContractService, ContractValidationError
from app.services.report_service import (
    ReportService,
    ReportValidationError,
    list_reports_by_contract_service,
)


def register_contract_routes(
    app: Flask,
    contract_service: ContractService | None,
    report_service: ReportService | None = None,
) -> None:
    def _service_or_503() -> tuple[ContractService | None, tuple[Any, int] | None]:
        if contract_service is not None:
            return contract_service, None
        return None, (
            jsonify(
                {
                    "success": False,
                    "data": None,
                    "error": "Modulo de contratos indisponivel. Habilite DB_ENABLED e configure conexao Postgres.",
                }
            ),
            503,
        )

    def _report_service_or_503() -> tuple[ReportService | None, tuple[Any, int] | None]:
        if report_service is not None:
            return report_service, None
        return None, (
            jsonify(
                {
                    "success": False,
                    "data": None,
                    "error": "Modulo de relatorios indisponivel. Habilite DB_ENABLED e configure conexao Postgres.",
                }
            ),
            503,
        )

    @app.get("/contracts")
    def list_contracts():
        service, error_response = _service_or_503()
        if error_response is not None:
            return error_response

        try:
            limit = int(str(request.args.get("limit", "100") or "100").strip())
        except Exception:
            limit = 100

        try:
            contracts = service.list_contracts(limit=limit)  # type: ignore[union-attr]
        except Exception as exc:
            app.logger.exception("Erro ao listar contratos")
            return jsonify({"success": False, "data": None, "error": "Falha ao listar contratos.", "detail": str(exc)}), 500

        items = [contract.to_dict() for contract in contracts]
        return jsonify({"success": True, "data": {"count": len(items), "items": items}}), 200

    @app.post("/contracts")
    def create_contract():
        service, error_response = _service_or_503()
        if error_response is not None:
            return error_response

        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            payload = request.form.to_dict()

        try:
            contract = service.create_contract(payload)  # type: ignore[union-attr]
        except ContractValidationError as exc:
            return jsonify({"success": False, "data": None, "error": str(exc)}), 400
        except ContractConflictError as exc:
            return jsonify({"success": False, "data": None, "error": str(exc)}), 409
        except Exception as exc:
            app.logger.exception("Erro ao criar contrato")
            return jsonify({"success": False, "data": None, "error": "Falha ao criar contrato.", "detail": str(exc)}), 500

        return jsonify({"success": True, "data": contract.to_dict()}), 201

    @app.get("/contracts/<int:contract_id>/reports")
    def list_reports_by_contract(contract_id: int):
        service, error_response = _report_service_or_503()
        if error_response is not None:
            return error_response

        try:
            rows = list_reports_by_contract_service(service, contract_id)  # type: ignore[arg-type]
            contract_context = None
            get_context = getattr(service, "get_contract_context", None)
            if callable(get_context):
                contract_context = get_context(contract_id)
        except ReportValidationError as exc:
            return jsonify({"success": False, "data": None, "error": str(exc)}), 400
        except Exception as exc:
            app.logger.exception("Erro ao listar relatorios por contrato")
            return jsonify({"success": False, "data": None, "error": "Falha ao listar relatorios.", "detail": str(exc)}), 500

        return (
            jsonify(
                {
                    "success": True,
                    "data": {
                        "contract_id": contract_id,
                        "contract_context": contract_context,
                        "count": len(rows),
                        "items": rows,
                    },
                }
            ),
            200,
        )
