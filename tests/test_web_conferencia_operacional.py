from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.routes.web_app import create_app
from app.services.conference_service import ConferenceValidationError
from app.services.user_service import UserAuthError, UserValidationError


class FakeUserService:
    def __init__(self) -> None:
        self._users_by_email: dict[str, dict[str, object]] = {}
        self._next_id = 1

    def register_user(self, email: str, password: str) -> dict[str, object]:
        clean_email = str(email or "").strip().lower()
        if not clean_email:
            raise UserValidationError("Email obrigatorio.")
        if len(str(password or "")) < 8:
            raise UserValidationError("Senha deve ter no minimo 8 caracteres.")
        if clean_email in self._users_by_email:
            raise UserValidationError("Email ja cadastrado.")
        user = {
            "id": self._next_id,
            "email": clean_email,
            "role": "fiscal",
            "status": "active",
            "authorized_contract_ids": [1],
            "contract_ids": [1],
            "created_at": "2026-04-01T00:00:00+00:00",
        }
        self._users_by_email[clean_email] = {"user": user, "password": str(password or "")}
        self._next_id += 1
        return dict(user)

    def authenticate_user(self, email: str, password: str) -> dict[str, object]:
        clean_email = str(email or "").strip().lower()
        row = self._users_by_email.get(clean_email)
        if not row or str(row.get("password", "") or "") != str(password or ""):
            raise UserAuthError("Credenciais invalidas.")
        return dict(row["user"])  # type: ignore[index]

    def get_user_by_id(self, user_id: int) -> dict[str, object] | None:
        for row in self._users_by_email.values():
            user = row.get("user")
            if isinstance(user, dict) and int(user.get("id", 0) or 0) == int(user_id):
                return dict(user)
        return None


@dataclass
class _FakeInspection:
    id: int
    contract_id: int | None
    contract_label: str
    titulo: str
    data_vistoria: str
    status: str
    nucleo: str
    municipio: str
    equipe: str
    fiscal_nome: str
    fiscal_contato: str
    responsavel_nome: str
    responsavel_contato: str
    local_vistoria: str
    prioridade: str
    resultado: str
    observacoes: str

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "contract_id": self.contract_id,
            "contract_label": self.contract_label,
            "titulo": self.titulo,
            "data_vistoria": self.data_vistoria,
            "status": self.status,
            "nucleo": self.nucleo,
            "municipio": self.municipio,
            "equipe": self.equipe,
            "fiscal_nome": self.fiscal_nome,
            "fiscal_contato": self.fiscal_contato,
            "responsavel_nome": self.responsavel_nome,
            "responsavel_contato": self.responsavel_contato,
            "local_vistoria": self.local_vistoria,
            "prioridade": self.prioridade,
            "resultado": self.resultado,
            "observacoes": self.observacoes,
        }


class FakeConferenceService:
    def __init__(self) -> None:
        self._inspection = _FakeInspection(
            id=1,
            contract_id=1,
            contract_label="CTR-001 - Contrato Centro",
            titulo="Conferencia semanal Mississipi",
            data_vistoria="2026-04-05",
            status="aberta",
            nucleo="Mississipi",
            municipio="Carapicuiba",
            equipe="Equipe 01",
            fiscal_nome="Fiscal 01",
            fiscal_contato="11999990000",
            responsavel_nome="Responsavel local",
            responsavel_contato="11999998888",
            local_vistoria="Rua das Flores",
            prioridade="media",
            resultado="pendente",
            observacoes="",
        )
        self._session = {
            "id": 90,
            "status": "rascunho",
            "technical_notes": "",
            "location_verified": "",
        }
        self._planned_items: list[dict[str, Any]] = [
            {
                "inspection_item_id": 11,
                "ordem": 1,
                "area": "Rede",
                "item_titulo": "Verificar sinalizacao",
                "descricao": "",
                "quantidade_declarada": "2",
                "quantidade_verificada": "0",
                "status": "pendente",
                "observacao_tecnica": "",
                "local_verificado": "",
                "evidencia_ref": "",
            }
        ]
        self._field_items: list[dict[str, Any]] = []
        self.concluded = False
        self.last_saved_draft: dict[str, Any] = {}

    def list_pending_queue(self, *, user_id: int, role: str, limit: int = 150) -> list[_FakeInspection]:
        _ = (user_id, role, limit)
        return [self._inspection]

    def get_ficha_detail(self, *, inspection_id: int, user_id: int, role: str) -> dict[str, Any]:
        _ = (inspection_id, user_id, role)
        return {
            "inspection": self._inspection.to_dict(),
            "items": [
                {
                    "id": 11,
                    "ordem": 1,
                    "area": "Rede",
                    "item_titulo": "Verificar sinalizacao",
                    "descricao": "",
                    "status": "pendente",
                    "quantidade_declarada": "2",
                }
            ],
            "session": dict(self._session),
            "item_totals": {"total": 1, "pendente": 1, "nao_conforme": 0, "conforme": 0},
        }

    def open_conference(self, *, inspection_id: int, user_id: int, role: str) -> dict[str, Any]:
        _ = (inspection_id, user_id, role)
        return {
            "inspection": self._inspection.to_dict(),
            "session": dict(self._session),
            "planned_items": list(self._planned_items),
            "field_items": list(self._field_items),
            "readonly": str(self._session.get("status", "") or "").strip().lower() == "concluida",
        }

    def save_evidence_file(self, **kwargs) -> str:  # noqa: ANN003
        _ = kwargs
        return "/tmp/evidencia_fake.jpg"

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
        _ = (inspection_id, user_id, role)
        self.last_saved_draft = {
            "planned_updates": list(planned_updates),
            "technical_notes": technical_notes,
            "location_verified": location_verified,
        }
        mapped = {int(item.get("inspection_item_id", 0) or 0): item for item in list(planned_updates or [])}
        new_rows: list[dict[str, Any]] = []
        for row in self._planned_items:
            current = mapped.get(int(row.get("inspection_item_id", 0) or 0), {})
            updated = dict(row)
            if current:
                updated["quantidade_verificada"] = str(current.get("quantidade_verificada", "0") or "0")
                updated["status"] = str(current.get("status", "pendente") or "pendente")
                updated["observacao_tecnica"] = str(current.get("observacao_tecnica", "") or "")
                updated["local_verificado"] = str(current.get("local_verificado", "") or "")
                updated["evidencia_ref"] = str(current.get("evidencia_ref", "") or "")
            new_rows.append(updated)
        self._planned_items = new_rows
        self._session["technical_notes"] = str(technical_notes or "")
        self._session["location_verified"] = str(location_verified or "")
        return dict(self._session)

    def add_field_item(self, *, inspection_id: int, user_id: int, role: str, payload: dict[str, Any]) -> None:
        _ = (inspection_id, user_id, role)
        self._field_items.append(
            {
                "inspection_item_id": 0,
                "ordem": len(self._planned_items) + len(self._field_items) + 1,
                "area": str(payload.get("area", "") or ""),
                "item_titulo": str(payload.get("item_titulo", "") or ""),
                "descricao": str(payload.get("descricao", "") or ""),
                "quantidade_declarada": "0",
                "quantidade_verificada": str(payload.get("quantidade_verificada", "0") or "0"),
                "status": str(payload.get("status", "observacao") or "observacao"),
                "observacao_tecnica": str(payload.get("observacao_tecnica", "") or ""),
                "local_verificado": str(payload.get("local_verificado", "") or ""),
                "evidencia_ref": str(payload.get("evidencia_ref", "") or ""),
            }
        )

    def get_summary(self, *, inspection_id: int, user_id: int, role: str) -> dict[str, Any]:
        _ = (inspection_id, user_id, role)
        planned_pending = sum(1 for item in self._planned_items if str(item.get("status", "pendente")) == "pendente")
        ready = bool(self._planned_items) and planned_pending == 0
        return {
            "inspection": self._inspection.to_dict(),
            "session": dict(self._session),
            "planned_items": list(self._planned_items),
            "field_items": list(self._field_items),
            "summary": {
                "planned_count": len(self._planned_items),
                "field_found_count": len(self._field_items),
                "total_count": len(self._planned_items) + len(self._field_items),
                "pending_count": planned_pending,
                "nao_conforme_count": sum(1 for item in self._planned_items if str(item.get("status", "")) == "nao_conforme"),
                "conforme_count": sum(1 for item in self._planned_items if str(item.get("status", "")) == "conforme"),
                "observacao_count": sum(1 for item in self._planned_items if str(item.get("status", "")) == "observacao"),
                "declared_total": "2",
                "verified_total": str(sum(float(item.get("quantidade_verificada", "0") or "0") for item in self._planned_items)),
                "delta_total": "0",
                "ready_to_conclude": ready,
                "resultado_final": "conforme" if ready else "parcial",
            },
        }

    def conclude(
        self,
        *,
        inspection_id: int,
        user_id: int,
        role: str,
        technical_notes: str,
        location_verified: str,
    ) -> dict[str, Any]:
        _ = (inspection_id, user_id, role)
        summary_payload = self.get_summary(inspection_id=inspection_id, user_id=user_id, role=role)
        if not bool(summary_payload.get("summary", {}).get("ready_to_conclude")):
            raise ConferenceValidationError("Conferencia ainda nao pode ser concluida.")
        self._session["technical_notes"] = str(technical_notes or "")
        self._session["location_verified"] = str(location_verified or "")
        self._session["status"] = "concluida"
        self.concluded = True
        return self.get_summary(inspection_id=inspection_id, user_id=user_id, role=role)


class WebConferenciaOperacionalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)

        outputs_root = self.tmp_path / "saidas"
        master_dir = self.tmp_path / "BASE_MESTRA"
        history_file = self.tmp_path / "data" / "runtime" / "processing_history.csv"
        draft_dir = self.tmp_path / "data" / "drafts" / "web"
        nucleo_reference_file = self.tmp_path / "config" / "nucleo_reference.json"
        nucleo_reference_file.parent.mkdir(parents=True, exist_ok=True)
        nucleo_reference_file.write_text(json.dumps({"version": "test", "nucleos": []}), encoding="utf-8")

        self.app = create_app(
            {
                "TESTING": True,
                "SECRET_KEY": "test-secret",
                "OUTPUTS_ROOT": str(outputs_root),
                "MASTER_DIR": str(master_dir),
                "HISTORY_FILE": str(history_file),
                "DRAFT_DIR": str(draft_dir),
                "NUCLEO_REFERENCE_FILE": str(nucleo_reference_file),
            }
        )
        self.app.config["USER_SERVICE"] = FakeUserService()
        self.fake_conference_service = FakeConferenceService()
        self.app.config["CONFERENCE_SERVICE"] = self.fake_conference_service
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _authenticate(self) -> None:
        register_response = self.client.post(
            "/cadastro",
            data={"email": "fiscal@empresa.com", "password": "12345678", "confirm_password": "12345678"},
            follow_redirects=False,
        )
        self.assertIn(register_response.status_code, (302, 303))
        login_response = self.client.post(
            "/login",
            data={"email": "fiscal@empresa.com", "password": "12345678"},
            follow_redirects=False,
        )
        self.assertIn(login_response.status_code, (302, 303))

    def test_conferencia_queue_requires_login(self) -> None:
        response = self.client.get("/conferencia-operacional/pendentes", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login?next=/conferencia-operacional/pendentes", str(response.headers.get("Location", "") or ""))

    def test_conferencia_queue_and_detail_render(self) -> None:
        self._authenticate()
        queue_response = self.client.get("/conferencia-operacional/pendentes")
        self.assertEqual(queue_response.status_code, 200)
        queue_html = queue_response.data.decode("utf-8")
        self.assertIn("Fila do fiscal", queue_html)
        self.assertIn("Conferencia semanal Mississipi", queue_html)

        detail_response = self.client.get("/conferencia-operacional/fichas/1")
        self.assertEqual(detail_response.status_code, 200)
        detail_html = detail_response.data.decode("utf-8")
        self.assertIn("Checklist previsto", detail_html)
        self.assertIn("Verificar sinalizacao", detail_html)

        field_response = self.client.get("/conferencia-operacional/fichas/1/campo")
        self.assertEqual(field_response.status_code, 200)
        self.assertIn("Conferencia em campo", field_response.data.decode("utf-8"))

    def test_conferencia_save_draft_add_field_item_and_conclude(self) -> None:
        self._authenticate()

        draft_response = self.client.post(
            "/conferencia-operacional/fichas/1/rascunho",
            data={
                "qtd_11": "2",
                "status_11": "conforme",
                "obs_11": "Conferido sem ressalvas",
                "local_11": "Rua das Flores",
                "evidencia_existing_11": "/tmp/evidencia_existente.jpg",
                "technical_notes": "Conferencia completa",
                "location_verified": "Trecho norte",
                "next_action": "resumo",
            },
            follow_redirects=False,
        )
        self.assertEqual(draft_response.status_code, 302)
        self.assertIn("/conferencia-operacional/fichas/1/resumo", str(draft_response.headers.get("Location", "") or ""))
        self.assertEqual(self.fake_conference_service.last_saved_draft.get("technical_notes"), "Conferencia completa")

        field_item_response = self.client.post(
            "/conferencia-operacional/fichas/1/itens-campo",
            data={
                "area": "Rede",
                "item_titulo": "Caixa adicional",
                "descricao": "Item encontrado no trecho",
                "quantidade_verificada": "1",
                "status": "observacao",
                "observacao_tecnica": "Sem risco imediato",
                "local_verificado": "Rua B",
            },
            follow_redirects=False,
        )
        self.assertEqual(field_item_response.status_code, 302)
        self.assertIn("/conferencia-operacional/fichas/1/campo", str(field_item_response.headers.get("Location", "") or ""))

        summary_response = self.client.get("/conferencia-operacional/fichas/1/resumo")
        self.assertEqual(summary_response.status_code, 200)
        summary_html = summary_response.data.decode("utf-8")
        self.assertIn("Resumo final da conferencia", summary_html)
        self.assertIn("Concluir e publicar na base oficial", summary_html)

        conclude_response = self.client.post(
            "/conferencia-operacional/fichas/1/concluir",
            data={"technical_notes": "Publicado", "location_verified": "Trecho norte"},
            follow_redirects=True,
        )
        self.assertEqual(conclude_response.status_code, 200)
        self.assertTrue(self.fake_conference_service.concluded)
        self.assertIn("Conferencia concluida e publicada na base oficial", conclude_response.data.decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
