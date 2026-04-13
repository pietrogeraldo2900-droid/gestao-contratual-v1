from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from app.models.inspection import Inspection, InspectionItem
from app.repositories.inspection_repository import InspectionRepository


class InspectionValidationError(ValueError):
    pass


class InspectionService:
    _ALLOWED_STATUS = {"aberta", "em_andamento", "concluida", "cancelada"}
    _ALLOWED_RESULT = {"pendente", "conforme", "parcial", "nao_conforme"}
    _ALLOWED_PRIORITY = {"baixa", "media", "alta", "critica"}
    _ALLOWED_ITEM_STATUS = {"pendente", "conforme", "nao_conforme", "observacao"}
    _ALLOWED_ITEM_SEVERITY = {"baixa", "media", "alta", "critica"}
    _ALLOWED_DIVERGENCE_STATUS = {
        "sem_divergencia",
        "a_maior",
        "a_menor",
        "nao_verificado",
        "novo_nao_declarado",
        "nao_executado",
    }

    def __init__(self, repository: InspectionRepository):
        self._repository = repository

    def _parse_optional_date(self, raw: object, field_label: str) -> date | None:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            return date.fromisoformat(text[:10])
        except Exception:
            raise InspectionValidationError(f"{field_label} deve estar no formato YYYY-MM-DD.")

    def _parse_required_date(self, raw: object, field_label: str) -> date:
        parsed = self._parse_optional_date(raw, field_label)
        if parsed is None:
            raise InspectionValidationError(f"Informe {field_label}.")
        return parsed

    def _normalize_enum(self, raw: object, allowed: set[str], field_label: str, default: str) -> str:
        value = str(raw or default).strip().lower() or default
        if value not in allowed:
            raise InspectionValidationError(
                f"{field_label} invalido. Opcoes: {', '.join(sorted(allowed))}."
            )
        return value

    def _parse_decimal(self, raw: object, field_label: str, default: str = "0") -> Decimal:
        text = str(raw if raw is not None else default).strip().replace(",", ".")
        if not text:
            text = default
        try:
            return Decimal(text)
        except (InvalidOperation, ValueError):
            raise InspectionValidationError(f"{field_label} deve ser numerico.")

    def _parse_optional_decimal(self, raw: object, field_label: str) -> Decimal | None:
        text = str(raw if raw is not None else "").strip().replace(",", ".")
        if not text:
            return None
        try:
            return Decimal(text)
        except (InvalidOperation, ValueError):
            raise InspectionValidationError(f"{field_label} deve ser numerico.")

    def _parse_optional_int(self, raw: object) -> int | None:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            return int(text)
        except Exception:
            raise InspectionValidationError("Contrato invalido.")

    def _build_divergence_metrics(
        self,
        *,
        quantidade_declarada: Decimal,
        quantidade_verificada: Decimal,
        verificado_informado: bool,
    ) -> tuple[Decimal, Decimal, str]:
        diff = quantidade_verificada - quantidade_declarada
        abs_diff = abs(diff)
        if not verificado_informado:
            status = "nao_verificado"
        elif quantidade_declarada == quantidade_verificada:
            status = "sem_divergencia"
        elif quantidade_declarada == Decimal("0") and quantidade_verificada > Decimal("0"):
            status = "novo_nao_declarado"
        elif quantidade_declarada > Decimal("0") and quantidade_verificada == Decimal("0"):
            status = "nao_executado"
        elif quantidade_verificada > quantidade_declarada:
            status = "a_maior"
        else:
            status = "a_menor"

        if quantidade_declarada == Decimal("0"):
            percentual = Decimal("100") if quantidade_verificada != Decimal("0") else Decimal("0")
        else:
            percentual = (abs_diff / abs(quantidade_declarada)) * Decimal("100")
        percentual = percentual.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        if status not in self._ALLOWED_DIVERGENCE_STATUS:
            status = "sem_divergencia"
        return abs_diff, percentual, status

    def _build_item_payloads(self, raw_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for raw in list(raw_items or []):
            item_titulo = str((raw or {}).get("item_titulo", "") or "").strip()
            area = str((raw or {}).get("area", "") or "").strip()
            descricao = str((raw or {}).get("descricao", "") or "").strip()
            if not item_titulo and not (area or descricao):
                continue
            if not item_titulo:
                raise InspectionValidationError("Cada item de vistoria precisa de um titulo.")

            status = self._normalize_enum(
                (raw or {}).get("status"),
                self._ALLOWED_ITEM_STATUS,
                "Status do item",
                "pendente",
            )
            severidade = self._normalize_enum(
                (raw or {}).get("severidade"),
                self._ALLOWED_ITEM_SEVERITY,
                "Severidade do item",
                "baixa",
            )
            prazo_ajuste = self._parse_optional_date((raw or {}).get("prazo_ajuste"), "Prazo do item")
            valor_multa = self._parse_decimal((raw or {}).get("valor_multa"), "Valor da multa", default="0")
            quantidade_declarada = self._parse_decimal(
                (raw or {}).get("quantidade_declarada"),
                "Quantidade declarada",
                default="0",
            )
            quantidade_verificada_raw = self._parse_optional_decimal(
                (raw or {}).get("quantidade_verificada"),
                "Quantidade verificada",
            )
            verificado_informado = quantidade_verificada_raw is not None
            quantidade_verificada = quantidade_verificada_raw if quantidade_verificada_raw is not None else Decimal("0")
            if quantidade_declarada < 0:
                raise InspectionValidationError("Quantidade declarada nao pode ser negativa.")
            if quantidade_verificada < 0:
                raise InspectionValidationError("Quantidade verificada nao pode ser negativa.")

            # Regra central: a base oficial e sempre derivada da conferencia do fiscal.
            quantidade_oficial = quantidade_verificada
            divergencia_absoluta, divergencia_percentual, divergencia_status = self._build_divergence_metrics(
                quantidade_declarada=quantidade_declarada,
                quantidade_verificada=quantidade_verificada,
                verificado_informado=verificado_informado,
            )
            items.append(
                {
                    "area": area,
                    "item_titulo": item_titulo,
                    "descricao": descricao,
                    "status": status,
                    "severidade": severidade,
                    "prazo_ajuste": prazo_ajuste,
                    "responsavel_ajuste": str((raw or {}).get("responsavel_ajuste", "") or "").strip(),
                    "valor_multa": valor_multa,
                    "evidencia_ref": str((raw or {}).get("evidencia_ref", "") or "").strip(),
                    "quantidade_declarada": quantidade_declarada,
                    "quantidade_verificada": quantidade_verificada,
                    "quantidade_oficial": quantidade_oficial,
                    "verificado_informado": verificado_informado,
                    "divergencia_absoluta": divergencia_absoluta,
                    "divergencia_percentual": divergencia_percentual,
                    "divergencia_status": divergencia_status,
                }
            )
        return items

    def list_inspections(
        self,
        *,
        limit: int = 200,
        contract_id: int | None = None,
        contract_ids: list[int] | None = None,
        status: str = "",
        date_from: object = "",
        date_to: object = "",
        query: str = "",
    ) -> list[Inspection]:
        status_norm = str(status or "").strip().lower()
        if status_norm and status_norm not in self._ALLOWED_STATUS:
            status_norm = ""
        normalized_contract_ids: list[int] | None = None
        if contract_ids is not None:
            normalized_contract_ids = []
            for value in list(contract_ids or []):
                try:
                    parsed = int(value)
                except Exception:
                    continue
                if parsed > 0:
                    normalized_contract_ids.append(parsed)
        return self._repository.list_inspections(
            limit=max(1, min(int(limit), 1000)),
            contract_id=int(contract_id) if contract_id else None,
            contract_ids=normalized_contract_ids,
            status=status_norm,
            date_from=self._parse_optional_date(date_from, "Data inicial"),
            date_to=self._parse_optional_date(date_to, "Data final"),
            query=str(query or "").strip(),
        )

    def count_inspections(self, *, contract_id: int | None = None, contract_ids: list[int] | None = None, status: str = "") -> int:
        status_norm = str(status or "").strip().lower()
        if status_norm and status_norm not in self._ALLOWED_STATUS:
            status_norm = ""
        normalized_contract_ids: list[int] | None = None
        if contract_ids is not None:
            normalized_contract_ids = []
            for value in list(contract_ids or []):
                try:
                    parsed = int(value)
                except Exception:
                    continue
                if parsed > 0:
                    normalized_contract_ids.append(parsed)
        return self._repository.count_inspections(
            contract_id=contract_id,
            contract_ids=normalized_contract_ids,
            status=status_norm,
        )

    def get_inspection_with_items(self, inspection_id: int) -> tuple[Inspection | None, list[InspectionItem]]:
        inspection = self._repository.get_inspection(int(inspection_id))
        if inspection is None:
            return None, []
        items = self._repository.list_items(inspection.id)
        return inspection, items

    def create_inspection(
        self,
        payload: dict[str, Any],
        raw_items: list[dict[str, Any]],
        *,
        created_by: int | None = None,
    ) -> Inspection:
        titulo = str(payload.get("titulo", "") or "").strip()
        if not titulo:
            raise InspectionValidationError("Informe o titulo da vistoria.")

        status = self._normalize_enum(payload.get("status"), self._ALLOWED_STATUS, "Status", "aberta")
        prioridade = self._normalize_enum(
            payload.get("prioridade"),
            self._ALLOWED_PRIORITY,
            "Prioridade",
            "media",
        )
        resultado = self._normalize_enum(
            payload.get("resultado"),
            self._ALLOWED_RESULT,
            "Resultado",
            "pendente",
        )
        data_vistoria = self._parse_required_date(payload.get("data_vistoria"), "data_vistoria")
        score_geral = self._parse_decimal(payload.get("score_geral"), "Score geral", default="0")
        if score_geral < 0:
            raise InspectionValidationError("Score geral nao pode ser negativo.")

        contract_id = self._parse_optional_int(payload.get("contract_id"))
        if not contract_id:
            raise InspectionValidationError("Informe o contrato da vistoria.")
        items = self._build_item_payloads(list(raw_items or []))
        if not items:
            raise InspectionValidationError("Inclua pelo menos um item na vistoria.")

        return self._repository.create_inspection(
            contract_id=contract_id,
            titulo=titulo,
            data_vistoria=data_vistoria,
            periodo=str(payload.get("periodo", "") or "").strip(),
            nucleo=str(payload.get("nucleo", "") or "").strip(),
            municipio=str(payload.get("municipio", "") or "").strip(),
            local_vistoria=str(payload.get("local_vistoria", "") or "").strip(),
            equipe=str(payload.get("equipe", "") or "").strip(),
            fiscal_nome=str(payload.get("fiscal_nome", "") or "").strip(),
            fiscal_contato=str(payload.get("fiscal_contato", "") or "").strip(),
            responsavel_nome=str(payload.get("responsavel_nome", "") or "").strip(),
            responsavel_contato=str(payload.get("responsavel_contato", "") or "").strip(),
            status=status,
            prioridade=prioridade,
            resultado=resultado,
            score_geral=score_geral,
            observacoes=str(payload.get("observacoes", "") or "").strip(),
            created_by=created_by,
            items=items,
        )

    def update_inspection_status(self, inspection_id: int, status: str) -> bool:
        status_norm = self._normalize_enum(status, self._ALLOWED_STATUS, "Status", "aberta")
        return self._repository.update_inspection_status(int(inspection_id), status_norm)

    def delete_inspection(self, inspection_id: int) -> bool:
        delete_inspection = getattr(self._repository, "delete_inspection", None)
        if not callable(delete_inspection):
            return False
        return bool(delete_inspection(int(inspection_id)))


__all__ = ["InspectionService", "InspectionValidationError"]
