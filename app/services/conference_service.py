from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from app.models.inspection import Inspection, InspectionItem
from app.repositories.conference_repository import ConferenceRepository
from app.services.inspection_service import InspectionService


class ConferenceValidationError(ValueError):
    pass


class ConferenceAccessError(PermissionError):
    pass


class ConferenceNotFoundError(LookupError):
    pass


class ConferenceService:
    _ALLOWED_ITEM_STATUS = {"pendente", "conforme", "nao_conforme", "observacao"}

    def __init__(
        self,
        repository: ConferenceRepository,
        inspection_service: InspectionService,
        evidence_root: Path,
    ):
        self._repository = repository
        self._inspection_service = inspection_service
        self._evidence_root = Path(evidence_root)

    def _normalize_status(self, raw: object, *, default: str = "pendente") -> str:
        value = str(raw or default).strip().lower() or default
        if value not in self._ALLOWED_ITEM_STATUS:
            raise ConferenceValidationError(
                f"Status do item invalido. Opcoes: {', '.join(sorted(self._ALLOWED_ITEM_STATUS))}."
            )
        return value

    def _parse_decimal(self, raw: object, *, field: str, default: str = "0") -> Decimal:
        text = str(raw if raw is not None else default).strip().replace(",", ".")
        if not text:
            text = default
        try:
            value = Decimal(text)
        except (InvalidOperation, ValueError):
            raise ConferenceValidationError(f"{field} deve ser numerico.")
        if value < 0:
            raise ConferenceValidationError(f"{field} nao pode ser negativo.")
        return value

    def _parse_optional_decimal(self, raw: object, *, field: str) -> Decimal | None:
        text = str(raw or "").strip()
        if not text:
            return None
        return self._parse_decimal(text, field=field)

    def _ensure_access(self, *, inspection_id: int, user_id: int, role: str) -> tuple[Inspection, list[InspectionItem]]:
        if not self._repository.can_user_access_inspection(
            user_id=int(user_id),
            role=str(role or "").strip().lower(),
            inspection_id=int(inspection_id),
        ):
            raise ConferenceAccessError("Usuario sem permissao para acessar esta ficha.")
        inspection, items = self._inspection_service.get_inspection_with_items(int(inspection_id))
        if inspection is None:
            raise ConferenceNotFoundError("Ficha nao encontrada.")
        return inspection, items

    def list_pending_queue(self, *, user_id: int, role: str, limit: int = 150) -> list[Inspection]:
        return self._repository.list_pending_inspections_for_user(
            user_id=int(user_id),
            role=str(role or "").strip().lower(),
            limit=max(1, min(int(limit), 1000)),
        )

    def get_ficha_detail(self, *, inspection_id: int, user_id: int, role: str) -> dict[str, Any]:
        inspection, items = self._ensure_access(
            inspection_id=int(inspection_id),
            user_id=int(user_id),
            role=role,
        )
        session = self._repository.get_session_by_inspection_and_fiscal(
            inspection_id=inspection.id,
            fiscal_user_id=int(user_id),
        )
        result_rows = []
        if isinstance(session, dict) and int(session.get("id", 0) or 0) > 0:
            result_rows = self._repository.list_session_item_results(session_id=int(session.get("id", 0) or 0))
        payload = self._build_payload(
            inspection=inspection,
            items=items,
            session=session or {},
            result_rows=result_rows,
        )
        rows = list(payload.get("planned_items", []) or [])
        return {
            "inspection": inspection.to_dict(),
            "items": rows,
            "session": session or {},
            "item_totals": {
                "total": len(rows),
                "pendente": sum(1 for row in rows if str(row.get("status", "") or "") == "pendente"),
                "nao_conforme": sum(1 for row in rows if str(row.get("status", "") or "") == "nao_conforme"),
                "conforme": sum(1 for row in rows if str(row.get("status", "") or "") == "conforme"),
            },
        }

    def _build_payload(
        self,
        *,
        inspection: Inspection,
        items: list[InspectionItem],
        session: dict[str, Any],
        result_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        result_map: dict[int, dict[str, Any]] = {}
        field_items: list[dict[str, Any]] = []
        for row in list(result_rows or []):
            inspection_item_id = int(row.get("inspection_item_id", 0) or 0)
            if inspection_item_id > 0:
                result_map[inspection_item_id] = row
            else:
                field_items.append(
                    {
                        "id": int(row.get("id", 0) or 0),
                        "ordem": int(row.get("ordem", 0) or 0),
                        "area": str(row.get("area", "") or "").strip(),
                        "item_titulo": str(row.get("item_titulo", "") or "").strip(),
                        "descricao": str(row.get("descricao", "") or "").strip(),
                        "quantidade_verificada": str(row.get("quantidade_verificada", "0") or "0"),
                        "verificado_informado": bool(row.get("verificado_informado", True)),
                        "status": str(row.get("status", "observacao") or "observacao").strip().lower(),
                        "observacao_tecnica": str(row.get("observacao_tecnica", "") or "").strip(),
                        "local_verificado": str(row.get("local_verificado", "") or "").strip(),
                        "evidencia_ref": str(row.get("evidencia_ref", "") or "").strip(),
                    }
                )

        planned_items: list[dict[str, Any]] = []
        for idx, item in enumerate(list(items or []), start=1):
            base = item.to_dict()
            current = result_map.get(item.id, {})
            current_informed = bool(current.get("verificado_informado", False))
            quantity_value = ""
            if current_informed:
                quantity_value = str(current.get("quantidade_verificada", "") or "").strip()
            planned_items.append(
                {
                    "inspection_item_id": item.id,
                    "ordem": int(base.get("ordem", idx) or idx),
                    "area": str(base.get("area", "") or "").strip(),
                    "item_titulo": str(base.get("item_titulo", "") or "").strip(),
                    "descricao": str(base.get("descricao", "") or "").strip(),
                    "quantidade_declarada": str(base.get("quantidade_declarada", "0") or "0"),
                    "quantidade_verificada": quantity_value,
                    "verificado_informado": current_informed,
                    "status": str(current.get("status", "pendente") or "pendente").strip().lower(),
                    "observacao_tecnica": str(current.get("observacao_tecnica", "") or "").strip(),
                    "local_verificado": str(current.get("local_verificado", "") or "").strip(),
                    "evidencia_ref": str(current.get("evidencia_ref", base.get("evidencia_ref", "")) or "").strip(),
                }
            )

        return {
            "inspection": inspection.to_dict(),
            "session": dict(session or {}),
            "planned_items": planned_items,
            "field_items": field_items,
        }

    def open_conference(self, *, inspection_id: int, user_id: int, role: str) -> dict[str, Any]:
        inspection, items = self._ensure_access(
            inspection_id=int(inspection_id),
            user_id=int(user_id),
            role=role,
        )
        session = self._repository.get_or_create_session(
            inspection_id=inspection.id,
            contract_id=inspection.contract_id,
            fiscal_user_id=int(user_id),
        )
        result_rows = self._repository.list_session_item_results(session_id=int(session.get("id", 0) or 0))
        payload = self._build_payload(
            inspection=inspection,
            items=items,
            session=session,
            result_rows=result_rows,
        )
        payload["readonly"] = str(session.get("status", "") or "").strip().lower() == "concluida"
        return payload

    def save_evidence_file(
        self,
        *,
        file_storage: FileStorage,
        inspection_id: int,
        session_id: int,
        slot: str,
    ) -> str:
        if not isinstance(file_storage, FileStorage):
            raise ConferenceValidationError("Arquivo de evidencia invalido.")
        filename = secure_filename(str(file_storage.filename or "").strip())
        if not filename:
            raise ConferenceValidationError("Selecione um arquivo valido para evidencia.")

        extension = Path(filename).suffix.lower()
        if extension not in {".jpg", ".jpeg", ".png", ".webp", ".pdf"}:
            raise ConferenceValidationError("Extensao de evidencia nao suportada. Use JPG, PNG, WEBP ou PDF.")

        self._evidence_root.mkdir(parents=True, exist_ok=True)
        target_dir = self._evidence_root / f"ficha_{int(inspection_id)}" / f"sessao_{int(session_id)}"
        target_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        safe_slot = secure_filename(str(slot or "item").strip()) or "item"
        target_name = f"{stamp}_{safe_slot}_{filename}"
        target_path = target_dir / target_name
        file_storage.save(target_path)
        return str(target_path)

    def save_draft(
        self,
        *,
        inspection_id: int,
        user_id: int,
        role: str,
        planned_updates: list[dict[str, Any]],
        technical_notes: str,
        location_verified: str,
    ) -> dict[str, Any]:
        payload = self.open_conference(inspection_id=inspection_id, user_id=user_id, role=role)
        session = dict(payload.get("session", {}) or {})
        session_id = int(session.get("id", 0) or 0)
        if session_id <= 0:
            raise ConferenceValidationError("Sessao de conferencia invalida.")
        if str(session.get("status", "") or "").strip().lower() == "concluida":
            raise ConferenceValidationError("A conferencia ja foi concluida e nao pode ser alterada.")

        normalized_updates: list[dict[str, Any]] = []
        for raw in list(planned_updates or []):
            inspection_item_id = int(raw.get("inspection_item_id", 0) or 0)
            if inspection_item_id <= 0:
                continue
            quantidade_verificada_raw = self._parse_optional_decimal(
                raw.get("quantidade_verificada"),
                field="Quantidade verificada",
            )
            normalized_updates.append(
                {
                    "inspection_item_id": inspection_item_id,
                    "ordem": int(raw.get("ordem", 0) or 0),
                    "area": str(raw.get("area", "") or "").strip(),
                    "item_titulo": str(raw.get("item_titulo", "") or "").strip(),
                    "descricao": str(raw.get("descricao", "") or "").strip(),
                    "quantidade_verificada": quantidade_verificada_raw if quantidade_verificada_raw is not None else Decimal("0"),
                    "verificado_informado": quantidade_verificada_raw is not None,
                    "status": self._normalize_status(raw.get("status"), default="pendente"),
                    "observacao_tecnica": str(raw.get("observacao_tecnica", "") or "").strip(),
                    "local_verificado": str(raw.get("local_verificado", "") or "").strip(),
                    "evidencia_ref": str(raw.get("evidencia_ref", "") or "").strip(),
                }
            )
        self._repository.upsert_planned_item_results(
            session_id=session_id,
            items=normalized_updates,
        )
        return self._repository.save_session_draft(
            session_id=session_id,
            technical_notes=str(technical_notes or "").strip(),
            location_verified=str(location_verified or "").strip(),
        )

    def add_field_item(
        self,
        *,
        inspection_id: int,
        user_id: int,
        role: str,
        payload: dict[str, Any],
    ) -> None:
        conference_payload = self.open_conference(inspection_id=inspection_id, user_id=user_id, role=role)
        session = dict(conference_payload.get("session", {}) or {})
        if str(session.get("status", "") or "").strip().lower() == "concluida":
            raise ConferenceValidationError("A conferencia ja foi concluida e nao pode receber novos itens.")
        session_id = int(session.get("id", 0) or 0)
        if session_id <= 0:
            raise ConferenceValidationError("Sessao de conferencia invalida.")

        item_titulo = str(payload.get("item_titulo", "") or "").strip()
        if not item_titulo:
            raise ConferenceValidationError("Informe o titulo do item encontrado em campo.")

        all_current = list(conference_payload.get("planned_items", []) or []) + list(conference_payload.get("field_items", []) or [])
        next_ordem = max([int(row.get("ordem", 0) or 0) for row in all_current] + [0]) + 1
        self._repository.add_field_item_result(
            session_id=session_id,
            item={
                "ordem": next_ordem,
                "area": str(payload.get("area", "") or "").strip(),
                "item_titulo": item_titulo,
                "descricao": str(payload.get("descricao", "") or "").strip(),
                "quantidade_verificada": self._parse_decimal(
                    payload.get("quantidade_verificada"),
                    field="Quantidade verificada",
                    default="0",
                ),
                "verificado_informado": True,
                "status": self._normalize_status(payload.get("status"), default="observacao"),
                "observacao_tecnica": str(payload.get("observacao_tecnica", "") or "").strip(),
                "local_verificado": str(payload.get("local_verificado", "") or "").strip(),
                "evidencia_ref": str(payload.get("evidencia_ref", "") or "").strip(),
            },
        )

    def _build_summary_from_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        planned_items = list(payload.get("planned_items", []) or [])
        field_items = list(payload.get("field_items", []) or [])
        all_items = planned_items + field_items

        declared_total = Decimal("0")
        verified_total = Decimal("0")
        pending_count = 0
        nao_conforme_count = 0
        conforme_count = 0
        observacao_count = 0
        for item in all_items:
            status = str(item.get("status", "pendente") or "pendente").strip().lower()
            if status == "pendente":
                pending_count += 1
            elif status == "nao_conforme":
                nao_conforme_count += 1
            elif status == "conforme":
                conforme_count += 1
            elif status == "observacao":
                observacao_count += 1
            verified_total += self._parse_decimal(
                item.get("quantidade_verificada"),
                field="Quantidade verificada",
                default="0",
            )
            declared_total += self._parse_decimal(
                item.get("quantidade_declarada"),
                field="Quantidade declarada",
                default="0",
            )

        ready_to_conclude = all(
            (
                str(item.get("status", "pendente") or "pendente").strip().lower() != "pendente"
                and bool(item.get("verificado_informado", False))
            )
            for item in planned_items
        ) and bool(planned_items)

        if nao_conforme_count > 0:
            resultado = "nao_conforme"
        elif pending_count > 0 or observacao_count > 0:
            resultado = "parcial"
        else:
            resultado = "conforme"

        return {
            "planned_count": len(planned_items),
            "field_found_count": len(field_items),
            "total_count": len(all_items),
            "pending_count": pending_count,
            "nao_conforme_count": nao_conforme_count,
            "conforme_count": conforme_count,
            "observacao_count": observacao_count,
            "declared_total": str(declared_total),
            "verified_total": str(verified_total),
            "delta_total": str(verified_total - declared_total),
            "ready_to_conclude": ready_to_conclude,
            "resultado_final": resultado,
        }

    def get_summary(self, *, inspection_id: int, user_id: int, role: str) -> dict[str, Any]:
        payload = self.open_conference(inspection_id=inspection_id, user_id=user_id, role=role)
        summary = self._build_summary_from_payload(payload)
        payload["summary"] = summary
        return payload

    def conclude(
        self,
        *,
        inspection_id: int,
        user_id: int,
        role: str,
        technical_notes: str,
        location_verified: str,
    ) -> dict[str, Any]:
        payload = self.get_summary(inspection_id=inspection_id, user_id=user_id, role=role)
        session = dict(payload.get("session", {}) or {})
        session_id = int(session.get("id", 0) or 0)
        if session_id <= 0:
            raise ConferenceValidationError("Sessao de conferencia invalida.")
        if str(session.get("status", "") or "").strip().lower() == "concluida":
            return payload

        summary = dict(payload.get("summary", {}) or {})
        if not bool(summary.get("ready_to_conclude")):
            raise ConferenceValidationError(
                "Todos os itens planejados devem ter status definido e quantidade verificada informada antes da conclusao."
            )

        concluded_session = self._repository.conclude_and_publish(
            session_id=session_id,
            technical_notes=str(technical_notes or "").strip(),
            location_verified=str(location_verified or "").strip(),
            summary=summary,
            inspection_resultado=str(summary.get("resultado_final", "pendente") or "pendente"),
        )
        refreshed = self.get_summary(inspection_id=inspection_id, user_id=user_id, role=role)
        refreshed["session"] = concluded_session
        return refreshed


__all__ = [
    "ConferenceAccessError",
    "ConferenceNotFoundError",
    "ConferenceService",
    "ConferenceValidationError",
]
