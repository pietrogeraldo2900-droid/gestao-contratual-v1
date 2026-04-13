from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from app.models.declaration import DailyExecutionDeclaration, DailyExecutionDeclarationItem
from app.models.inspection import Inspection
from app.repositories.declaration_repository import DailyExecutionDeclarationRepository
from app.services.inspection_service import InspectionService, InspectionValidationError


class DeclarationValidationError(ValueError):
    pass


class DeclarationService:
    _ALLOWED_ITEM_STATUS = {"declarado", "ajustado", "cancelado"}

    def __init__(
        self,
        repository: DailyExecutionDeclarationRepository,
        inspection_service: InspectionService,
        *,
        service_catalog_provider: Callable[[], dict[str, Any]] | None = None,
    ):
        self._repository = repository
        self._inspection_service = inspection_service
        self._service_catalog_provider = service_catalog_provider

    def _normalize_text(self, raw: object) -> str:
        return " ".join(str(raw or "").split()).strip()

    def _normalize_key(self, raw: object) -> str:
        text = self._normalize_text(raw).casefold()
        return " ".join(text.split())

    def _parse_optional_date(self, raw: object, field_label: str) -> date | None:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            return date.fromisoformat(text[:10])
        except Exception:
            raise DeclarationValidationError(f"{field_label} deve estar no formato YYYY-MM-DD.")

    def _parse_required_date(self, raw: object, field_label: str) -> date:
        parsed = self._parse_optional_date(raw, field_label)
        if parsed is None:
            raise DeclarationValidationError(f"Informe {field_label}.")
        return parsed

    def _parse_required_int(self, raw: object, field_label: str) -> int:
        text = str(raw or "").strip()
        if not text:
            raise DeclarationValidationError(f"Informe {field_label}.")
        try:
            value = int(text)
        except Exception:
            raise DeclarationValidationError(f"{field_label} invalido.")
        if value <= 0:
            raise DeclarationValidationError(f"{field_label} invalido.")
        return value

    def _parse_decimal(self, raw: object, field_label: str) -> Decimal:
        text = str(raw or "").strip().replace(",", ".")
        if not text:
            raise DeclarationValidationError(f"Informe {field_label}.")
        try:
            value = Decimal(text)
        except (InvalidOperation, ValueError):
            raise DeclarationValidationError(f"{field_label} deve ser numerico.")
        if value <= 0:
            raise DeclarationValidationError(f"{field_label} deve ser maior que zero.")
        return value

    def _normalize_enum(
        self,
        raw: object,
        *,
        allowed: set[str],
        default: str,
        field_label: str,
    ) -> str:
        value = str(raw or default).strip().lower() or default
        if value not in allowed:
            raise DeclarationValidationError(
                f"{field_label} invalido. Opcoes: {', '.join(sorted(allowed))}."
            )
        return value

    def _service_catalog_maps(self) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
        by_servico: dict[str, dict[str, str]] = {}
        by_label: dict[str, str] = {}
        provider = self._service_catalog_provider
        if not callable(provider):
            return by_servico, by_label
        try:
            catalog = dict(provider() or {})
        except Exception:
            return by_servico, by_label

        for raw in list(catalog.get("options", []) or []):
            row = dict(raw or {})
            servico = self._normalize_text(row.get("servico"))
            if not servico:
                continue
            categoria = self._normalize_text(row.get("categoria")) or "servico_nao_mapeado"
            label = self._normalize_text(row.get("label")) or servico.replace("_", " ")
            by_servico[servico] = {
                "servico_oficial": servico,
                "servico_label": label,
                "categoria": categoria,
            }
            for alias in {servico, servico.replace("_", " "), label}:
                key = self._normalize_key(alias)
                if key:
                    by_label[key] = servico
        return by_servico, by_label

    def _resolve_service_meta(
        self,
        *,
        servico_oficial: str,
        servico_label: str,
        categoria: str,
        by_servico: dict[str, dict[str, str]],
        by_label: dict[str, str],
    ) -> dict[str, str]:
        direct = by_servico.get(servico_oficial)
        if direct:
            return dict(direct)

        for probe in (servico_oficial, servico_label):
            mapped_key = by_label.get(self._normalize_key(probe))
            if not mapped_key:
                continue
            mapped = by_servico.get(mapped_key)
            if mapped:
                return dict(mapped)

        normalized_service = servico_oficial or servico_label
        normalized_service = normalized_service or "servico_nao_mapeado"
        normalized_label = servico_label or normalized_service.replace("_", " ")
        normalized_category = categoria or "servico_nao_mapeado"
        return {
            "servico_oficial": self._normalize_text(normalized_service),
            "servico_label": self._normalize_text(normalized_label),
            "categoria": self._normalize_text(normalized_category),
        }

    def _build_declaration_items(self, raw_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        by_servico, by_label = self._service_catalog_maps()
        items: list[dict[str, Any]] = []
        for raw in list(raw_items or []):
            row = dict(raw or {})
            servico_oficial = self._normalize_text(row.get("servico_oficial"))
            servico_label = self._normalize_text(row.get("servico_label"))
            categoria = self._normalize_text(row.get("categoria"))
            local_execucao = self._normalize_text(row.get("local_execucao"))
            descricao = self._normalize_text(row.get("descricao"))
            quantidade_text = str(row.get("quantidade", "") or "").strip()

            has_any_data = any(
                [
                    servico_oficial,
                    servico_label,
                    categoria,
                    local_execucao,
                    descricao,
                    quantidade_text,
                ]
            )
            if not has_any_data:
                continue
            if not (servico_oficial or servico_label):
                raise DeclarationValidationError("Cada item deve informar um servico padronizado.")

            quantidade = self._parse_decimal(quantidade_text, "Quantidade do item")
            meta = self._resolve_service_meta(
                servico_oficial=servico_oficial,
                servico_label=servico_label,
                categoria=categoria,
                by_servico=by_servico,
                by_label=by_label,
            )
            item_status = self._normalize_enum(
                row.get("item_status"),
                allowed=self._ALLOWED_ITEM_STATUS,
                default="declarado",
                field_label="Status do item",
            )
            items.append(
                {
                    "servico_oficial": str(meta.get("servico_oficial", "") or "").strip(),
                    "servico_label": str(meta.get("servico_label", "") or "").strip(),
                    "categoria": str(meta.get("categoria", "") or "").strip(),
                    "quantidade": quantidade,
                    "unidade": self._normalize_text(row.get("unidade")),
                    "local_execucao": local_execucao,
                    "descricao": descricao,
                    "item_status": item_status,
                }
            )
        if not items:
            raise DeclarationValidationError("Inclua pelo menos um item na declaracao diaria.")
        return items

    def _build_generated_inspection_payload(
        self,
        declaration: DailyExecutionDeclaration,
    ) -> dict[str, Any]:
        data_iso = declaration.declaration_date.isoformat() if declaration.declaration_date else ""
        location_hint = declaration.nucleo or declaration.municipio or declaration.contract_label or "campo"
        title = f"Ficha de Conferencia - Declaracao diaria {data_iso} - {location_hint}"
        return {
            "contract_id": str(declaration.contract_id or ""),
            "titulo": title[:255],
            "data_vistoria": data_iso,
            "periodo": declaration.periodo or "Diurno",
            "nucleo": declaration.nucleo,
            "municipio": declaration.municipio,
            "local_vistoria": declaration.logradouro,
            "equipe": declaration.equipe,
            "fiscal_nome": "Conferencia automatica",
            "fiscal_contato": "",
            "responsavel_nome": declaration.responsavel_nome,
            "responsavel_contato": declaration.responsavel_contato,
            "status": "aberta",
            "prioridade": "media",
            "resultado": "pendente",
            "score_geral": "0",
            "observacoes": (
                "Ficha gerada automaticamente a partir da declaracao diaria de execucao "
                f"#{declaration.id}. Esta declaracao nao compoe a base oficial."
            ),
        }

    def _fmt_qty(self, value: Decimal) -> str:
        text = f"{value:.3f}"
        return text.rstrip("0").rstrip(".")

    def _build_generated_inspection_items(self, declaration_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in declaration_items:
            service_label = (
                str(item.get("servico_label", "") or "").strip()
                or str(item.get("servico_oficial", "") or "").strip()
                or "servico_nao_mapeado"
            )
            categoria = str(item.get("categoria", "") or "").strip() or "servico_nao_mapeado"
            qtd = self._fmt_qty(Decimal(str(item.get("quantidade", "0") or "0")))
            unidade = str(item.get("unidade", "") or "").strip()
            local_execucao = str(item.get("local_execucao", "") or "").strip()
            descricao_item = str(item.get("descricao", "") or "").strip()

            detail_parts = [f"Quantidade declarada: {qtd}{(' ' + unidade) if unidade else ''}"]
            if local_execucao:
                detail_parts.append(f"Local declarado: {local_execucao}")
            if descricao_item:
                detail_parts.append(f"Descricao declarada: {descricao_item}")

            rows.append(
                {
                    "area": categoria,
                    "item_titulo": f"Conferir execucao declarada de {service_label}"[:255],
                    "descricao": " | ".join(detail_parts),
                    "quantidade_declarada": str(item.get("quantidade", "0") or "0"),
                    "quantidade_verificada": "",
                    "status": "pendente",
                    "severidade": "baixa",
                    "prazo_ajuste": None,
                    "responsavel_ajuste": "",
                    "valor_multa": "0",
                    "evidencia_ref": "",
                }
            )
        if not rows:
            raise DeclarationValidationError("Nao foi possivel gerar itens de conferencia.")
        return rows

    def list_declarations(
        self,
        *,
        limit: int = 200,
        contract_id: int | None = None,
        created_by: int | None = None,
        date_from: object = "",
        date_to: object = "",
    ) -> list[DailyExecutionDeclaration]:
        return self._repository.list_declarations(
            limit=max(1, min(int(limit), 1000)),
            contract_id=int(contract_id) if contract_id else None,
            created_by=int(created_by) if created_by else None,
            date_from=self._parse_optional_date(date_from, "Data inicial"),
            date_to=self._parse_optional_date(date_to, "Data final"),
        )

    def get_declaration_with_items(
        self,
        declaration_id: int,
    ) -> tuple[DailyExecutionDeclaration | None, list[DailyExecutionDeclarationItem]]:
        declaration = self._repository.get_declaration(int(declaration_id))
        if declaration is None:
            return None, []
        items = self._repository.list_items(declaration.id)
        return declaration, items

    def create_declaration_and_generate_inspection(
        self,
        payload: dict[str, Any],
        raw_items: list[dict[str, Any]],
        *,
        created_by: int | None = None,
    ) -> tuple[DailyExecutionDeclaration, Inspection]:
        contract_id = self._parse_required_int(payload.get("contract_id"), "Contrato")
        declaration_date = self._parse_required_date(payload.get("declaration_date"), "data da declaracao")
        items = self._build_declaration_items(list(raw_items or []))

        declaration = self._repository.create_declaration(
            contract_id=contract_id,
            declaration_date=declaration_date,
            periodo=self._normalize_text(payload.get("periodo")) or "Diurno",
            nucleo=self._normalize_text(payload.get("nucleo")),
            municipio=self._normalize_text(payload.get("municipio")),
            logradouro=self._normalize_text(payload.get("logradouro")),
            equipe=self._normalize_text(payload.get("equipe")),
            responsavel_nome=self._normalize_text(payload.get("responsavel_nome")),
            responsavel_contato=self._normalize_text(payload.get("responsavel_contato")),
            observacoes=self._normalize_text(payload.get("observacoes")),
            created_by=int(created_by) if created_by else None,
            items=items,
        )

        inspection_payload = self._build_generated_inspection_payload(declaration)
        inspection_items = self._build_generated_inspection_items(items)
        try:
            inspection = self._inspection_service.create_inspection(
                inspection_payload,
                inspection_items,
                created_by=int(created_by) if created_by else None,
            )
        except InspectionValidationError as exc:
            self._repository.delete_declaration(declaration.id)
            raise DeclarationValidationError(str(exc))
        except Exception:
            self._repository.delete_declaration(declaration.id)
            raise

        try:
            linked = self._repository.link_generated_inspection(declaration.id, inspection.id)
        except Exception:
            delete_inspection = getattr(self._inspection_service, "delete_inspection", None)
            if callable(delete_inspection):
                delete_inspection(int(inspection.id))
            self._repository.delete_declaration(declaration.id)
            raise
        if not linked:
            delete_inspection = getattr(self._inspection_service, "delete_inspection", None)
            if callable(delete_inspection):
                delete_inspection(int(inspection.id))
            self._repository.delete_declaration(declaration.id)
            raise DeclarationValidationError("Nao foi possivel vincular a ficha gerada a declaracao diaria.")
        refreshed = self._repository.get_declaration(declaration.id) or declaration
        return refreshed, inspection


__all__ = ["DeclarationService", "DeclarationValidationError"]
