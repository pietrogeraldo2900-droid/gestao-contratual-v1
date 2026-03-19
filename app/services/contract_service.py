from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, List

from app.models.contract import Contract, ContractCreateInput
from app.repositories.contract_repository import ContractConflictError, ContractRepository


class ContractValidationError(ValueError):
    pass


class ContractService:
    _ALLOWED_STATUS = {"em_implantacao", "ativo", "suspenso", "encerrado"}
    _STATUS_ALIASES = {"rascunho": "em_implantacao", "cancelado": "suspenso"}

    def __init__(self, repository: ContractRepository):
        self._repository = repository

    def init_schema(self) -> None:
        # Mantido por compatibilidade de interface. Inicializacao agora ocorre em app.database.init_db.
        return None

    def list_contracts(self, limit: int = 100) -> List[Contract]:
        limit = max(1, min(int(limit), 500))
        return self._repository.list_contracts(limit=limit)

    def count_contracts(self) -> int:
        return max(0, int(self._repository.count_contracts()))

    def _normalize_status(self, raw: object) -> str:
        status = str(raw or "em_implantacao").strip().lower() or "em_implantacao"
        return self._STATUS_ALIASES.get(status, status)

    def _parse_required_date(self, raw: object, field_label: str) -> date:
        text = str(raw or "").strip()
        if not text:
            raise ContractValidationError(f"Informe {field_label}.")
        try:
            return date.fromisoformat(text[:10])
        except Exception:
            raise ContractValidationError(f"{field_label} deve estar no formato YYYY-MM-DD.")

    def _parse_optional_date(self, raw: object, field_label: str) -> date | None:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            return date.fromisoformat(text[:10])
        except Exception:
            raise ContractValidationError(f"{field_label} deve estar no formato YYYY-MM-DD.")

    def _parse_required_int(self, raw: object, field_label: str, min_value: int = 0) -> int:
        text = str(raw or "").strip()
        if not text:
            raise ContractValidationError(f"Informe {field_label}.")
        try:
            value = int(text)
        except Exception:
            raise ContractValidationError(f"{field_label} deve ser um numero inteiro.")
        if value < min_value:
            raise ContractValidationError(f"{field_label} deve ser maior ou igual a {min_value}.")
        return value

    def _parse_optional_int(self, raw: object, field_label: str, min_value: int = 0) -> int | None:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            value = int(text)
        except Exception:
            raise ContractValidationError(f"{field_label} deve ser um numero inteiro.")
        if value < min_value:
            raise ContractValidationError(f"{field_label} deve ser maior ou igual a {min_value}.")
        return value

    def _parse_required_decimal(self, raw: object, field_label: str, min_value: Decimal) -> Decimal:
        text = str(raw or "").strip().replace(",", ".")
        if not text:
            raise ContractValidationError(f"Informe {field_label}.")
        try:
            value = Decimal(text)
        except (InvalidOperation, ValueError):
            raise ContractValidationError(f"{field_label} deve ser um valor numerico valido.")
        if value < min_value:
            raise ContractValidationError(f"{field_label} deve ser maior ou igual a {min_value}.")
        return value

    def _parse_optional_bool(self, raw: object) -> bool | None:
        if isinstance(raw, bool):
            return raw
        text = str(raw or "").strip().lower()
        if not text:
            return None
        if text in {"1", "true", "on", "yes", "sim", "s"}:
            return True
        if text in {"0", "false", "off", "no", "nao", "n"}:
            return False
        raise ContractValidationError("Campo possui_ordem_servico invalido.")

    def _compact(self, raw: object) -> str:
        return " ".join(str(raw or "").split()).strip()

    def _first_non_empty(self, payload: dict[str, Any], *keys: str, default: str = "") -> str:
        for key in keys:
            if key in payload:
                value = self._compact(payload.get(key))
                if value:
                    return value
        return default

    def create_contract(self, payload: dict[str, Any]) -> Contract:
        legacy_payload = (
            ("contract_code" in payload or "title" in payload)
            and "numero_contrato" not in payload
            and "nome_contrato" not in payload
        )
        default_text = "Nao informado"

        numero_contrato = self._first_non_empty(payload, "numero_contrato", "contract_code")
        nome_contrato = self._first_non_empty(payload, "nome_contrato", "title")
        objeto_contrato = self._first_non_empty(payload, "objeto_contrato", "description")
        contratante_nome = self._first_non_empty(
            payload, "contratante_nome", default=default_text if legacy_payload else ""
        )
        contratada_nome = self._first_non_empty(
            payload, "contratada_nome", default=default_text if legacy_payload else ""
        )
        regional = self._first_non_empty(
            payload, "regional", default=default_text if legacy_payload else ""
        )
        municipios_atendidos = self._first_non_empty(
            payload, "municipios_atendidos", default=default_text if legacy_payload else ""
        )

        status_contrato = self._normalize_status(
            payload.get("status_contrato", payload.get("status", "em_implantacao"))
        )
        if status_contrato not in self._ALLOWED_STATUS:
            raise ContractValidationError(
                "Status invalido. Use: em_implantacao, ativo, suspenso ou encerrado."
            )

        if not numero_contrato:
            raise ContractValidationError("Informe numero_contrato.")
        if not nome_contrato:
            raise ContractValidationError("Informe nome_contrato.")
        if not objeto_contrato:
            raise ContractValidationError("Informe objeto_contrato.")
        if not contratante_nome:
            raise ContractValidationError("Informe contratante_nome.")
        if not contratada_nome:
            raise ContractValidationError("Informe contratada_nome.")
        if not regional:
            raise ContractValidationError("Informe regional.")
        if not municipios_atendidos:
            raise ContractValidationError("Informe municipios_atendidos.")

        if len(numero_contrato) > 80:
            raise ContractValidationError("numero_contrato deve ter no maximo 80 caracteres.")
        if len(nome_contrato) > 255:
            raise ContractValidationError("nome_contrato deve ter no maximo 255 caracteres.")
        if len(contratante_nome) > 255:
            raise ContractValidationError("contratante_nome deve ter no maximo 255 caracteres.")
        if len(contratada_nome) > 255:
            raise ContractValidationError("contratada_nome deve ter no maximo 255 caracteres.")
        if len(regional) > 120:
            raise ContractValidationError("regional deve ter no maximo 120 caracteres.")

        data_assinatura_raw = payload.get("data_assinatura")
        if legacy_payload and not str(data_assinatura_raw or "").strip():
            data_assinatura_raw = date.today().isoformat()
        data_assinatura = self._parse_required_date(data_assinatura_raw, "data_assinatura")

        prazo_dias_raw = payload.get("prazo_dias")
        if legacy_payload and not str(prazo_dias_raw or "").strip():
            prazo_dias_raw = "0"
        prazo_dias = self._parse_required_int(prazo_dias_raw, "prazo_dias", min_value=0)

        valor_contrato_raw = payload.get("valor_contrato")
        if legacy_payload and not str(valor_contrato_raw or "").strip():
            valor_contrato_raw = "0"
        valor_contrato = self._parse_required_decimal(
            valor_contrato_raw,
            "valor_contrato",
            min_value=Decimal("0"),
        )

        vigencia_inicio = self._parse_optional_date(payload.get("vigencia_inicio"), "vigencia_inicio")
        vigencia_fim = self._parse_optional_date(payload.get("vigencia_fim"), "vigencia_fim")
        if vigencia_inicio and vigencia_fim and vigencia_fim < vigencia_inicio:
            raise ContractValidationError("vigencia_fim nao pode ser anterior a vigencia_inicio.")

        prazo_pagamento_dias = self._parse_optional_int(
            payload.get("prazo_pagamento_dias"),
            "prazo_pagamento_dias",
            min_value=0,
        )
        possui_ordem_servico = self._parse_optional_bool(payload.get("possui_ordem_servico"))

        clean_payload = ContractCreateInput(
            nome_contrato=nome_contrato,
            numero_contrato=numero_contrato,
            objeto_contrato=objeto_contrato,
            data_assinatura=data_assinatura,
            vigencia_inicio=vigencia_inicio,
            vigencia_fim=vigencia_fim,
            prazo_dias=prazo_dias,
            valor_contrato=valor_contrato,
            contratante_nome=contratante_nome,
            contratante_cnpj=self._first_non_empty(payload, "contratante_cnpj"),
            contratada_nome=contratada_nome,
            contratada_cnpj=self._first_non_empty(payload, "contratada_cnpj"),
            regional=regional,
            diretoria=self._first_non_empty(payload, "diretoria"),
            municipios_atendidos=municipios_atendidos,
            status_contrato=status_contrato,
            reajuste_indice=self._first_non_empty(payload, "reajuste_indice"),
            prazo_pagamento_dias=prazo_pagamento_dias,
            possui_ordem_servico=possui_ordem_servico,
            observacoes=str(payload.get("observacoes", "") or "").strip(),
        )
        return self._repository.create_contract(clean_payload)

    def get_contract_report_context(self, contract_id: int) -> dict[str, Any] | None:
        try:
            contract_id_int = int(contract_id)
        except Exception:
            raise ContractValidationError("contract_id invalido.")
        if contract_id_int <= 0:
            raise ContractValidationError("contract_id invalido.")
        return self._repository.get_contract_report_context(contract_id_int)


__all__ = ["ContractConflictError", "ContractService", "ContractValidationError"]
