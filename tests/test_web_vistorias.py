from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

from app.routes.web_app import create_app
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
            "role": "admin_operacional",
            "status": "active",
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
    prioridade: str
    resultado: str
    nucleo: str
    municipio: str
    equipe: str
    observacoes: str

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "contract_id": self.contract_id,
            "contract_label": self.contract_label,
            "titulo": self.titulo,
            "data_vistoria": self.data_vistoria,
            "status": self.status,
            "prioridade": self.prioridade,
            "resultado": self.resultado,
            "nucleo": self.nucleo,
            "municipio": self.municipio,
            "equipe": self.equipe,
            "observacoes": self.observacoes,
            "periodo": "",
            "local_vistoria": "",
            "fiscal_nome": "",
            "fiscal_contato": "",
            "responsavel_nome": "",
            "responsavel_contato": "",
            "score_geral": "0",
            "created_by": None,
            "created_at": "",
            "updated_at": "",
        }


class FakeInspectionService:
    def __init__(self) -> None:
        self._rows: list[_FakeInspection] = [
            _FakeInspection(
                id=1,
                contract_id=1,
                contract_label="CTR-001 - Contrato Centro",
                titulo="Inspecao inicial",
                data_vistoria="2026-04-01",
                status="aberta",
                prioridade="media",
                resultado="pendente",
                nucleo="Mississipi",
                municipio="Carapicuiba",
                equipe="Equipe 01",
                observacoes="",
            )
        ]
        self._items: dict[int, list[dict[str, object]]] = {
            1: [
                {
                    "id": 1,
                    "inspection_id": 1,
                    "ordem": 1,
                    "area": "Rede",
                    "item_titulo": "Verificar sinalizacao",
                    "descricao": "",
                    "status": "pendente",
                    "severidade": "media",
                    "prazo_ajuste": "",
                    "responsavel_ajuste": "",
                    "valor_multa": "0",
                    "evidencia_ref": "",
                    "created_at": "",
                    "updated_at": "",
                }
            ]
        }
        self._next_id = 2

    def list_inspections(self, **kwargs):  # noqa: ANN003
        _ = kwargs
        return list(self._rows)

    def count_inspections(self, *, contract_id: int | None = None, status: str = "") -> int:
        rows = list(self._rows)
        if contract_id:
            rows = [row for row in rows if int(row.contract_id or 0) == int(contract_id)]
        if status:
            rows = [row for row in rows if row.status == status]
        return len(rows)

    def create_inspection(self, payload, raw_items, *, created_by=None):  # noqa: ANN001, ANN003
        _ = created_by
        if not payload.get("titulo"):
            raise ValueError("titulo obrigatorio")
        new = _FakeInspection(
            id=self._next_id,
            contract_id=int(payload.get("contract_id")) if str(payload.get("contract_id") or "").isdigit() else None,
            contract_label="",
            titulo=str(payload.get("titulo", "") or ""),
            data_vistoria=str(payload.get("data_vistoria", "") or ""),
            status=str(payload.get("status", "aberta") or "aberta"),
            prioridade=str(payload.get("prioridade", "media") or "media"),
            resultado=str(payload.get("resultado", "pendente") or "pendente"),
            nucleo=str(payload.get("nucleo", "") or ""),
            municipio=str(payload.get("municipio", "") or ""),
            equipe=str(payload.get("equipe", "") or ""),
            observacoes=str(payload.get("observacoes", "") or ""),
        )
        self._rows.append(new)
        items = list(raw_items or [])
        self._items[new.id] = [
            {
                "id": idx + 1,
                "inspection_id": new.id,
                "ordem": idx + 1,
                "area": str(item.get("area", "") or ""),
                "item_titulo": str(item.get("item_titulo", "") or ""),
                "descricao": str(item.get("descricao", "") or ""),
                "status": str(item.get("status", "pendente") or "pendente"),
                "severidade": str(item.get("severidade", "baixa") or "baixa"),
                "prazo_ajuste": "",
                "responsavel_ajuste": str(item.get("responsavel_ajuste", "") or ""),
                "valor_multa": str(item.get("valor_multa", "0") or "0"),
                "evidencia_ref": str(item.get("evidencia_ref", "") or ""),
                "created_at": "",
                "updated_at": "",
            }
            for idx, item in enumerate(items)
            if str(item.get("item_titulo", "") or "").strip()
        ]
        self._next_id += 1
        return new

    def get_inspection_with_items(self, inspection_id: int):
        for row in self._rows:
            if int(row.id) == int(inspection_id):
                items = [type("FakeItem", (), {"to_dict": lambda self, d=item: d})() for item in self._items.get(row.id, [])]
                return row, items
        return None, []

    def update_inspection_status(self, inspection_id: int, status: str) -> bool:
        for row in self._rows:
            if int(row.id) == int(inspection_id):
                row.status = str(status or "").strip().lower()
                return True
        return False


class WebVistoriasTests(unittest.TestCase):
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
        self.app.config["INSPECTION_SERVICE"] = FakeInspectionService()
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _authenticate(self) -> None:
        register_response = self.client.post(
            "/cadastro",
            data={"email": "vistoria@empresa.com", "password": "12345678", "confirm_password": "12345678"},
            follow_redirects=False,
        )
        self.assertIn(register_response.status_code, (302, 303))
        login_response = self.client.post(
            "/login",
            data={"email": "vistoria@empresa.com", "password": "12345678"},
            follow_redirects=False,
        )
        self.assertIn(login_response.status_code, (302, 303))

    def test_vistorias_requires_login(self) -> None:
        response = self.client.get("/vistorias", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login?next=/vistorias", str(response.headers.get("Location", "") or ""))

    def test_vistorias_list_render_for_authenticated_user(self) -> None:
        self._authenticate()
        response = self.client.get("/vistorias")
        self.assertEqual(response.status_code, 200)
        html = response.data.decode("utf-8")
        self.assertIn("Vistorias de campo", html)
        self.assertIn("Inspecao inicial", html)

    def test_create_vistoria_and_open_detail(self) -> None:
        self._authenticate()
        response = self.client.post(
            "/vistorias",
            data={
                "titulo": "Vistoria ponte oeste",
                "data_vistoria": "2026-04-02",
                "status": "aberta",
                "prioridade": "alta",
                "resultado": "pendente",
                "nucleo": "Bonanca",
                "municipio": "Osasco",
                "item_area[]": ["Rede"],
                "item_titulo[]": ["Isolar trecho"],
                "item_descricao[]": ["Avaliar segurança local"],
                "item_status[]": ["pendente"],
                "item_severidade[]": ["alta"],
                "item_prazo[]": [""],
                "item_responsavel[]": [""],
                "item_multa[]": [""],
                "item_evidencia[]": [""],
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        self.assertIn("/vistorias/", str(response.headers.get("Location", "") or ""))

        detail = self.client.get(str(response.headers.get("Location", "") or ""))
        self.assertEqual(detail.status_code, 200)
        html = detail.data.decode("utf-8")
        self.assertIn("Vistoria ponte oeste", html)
        self.assertIn("Isolar trecho", html)

    def test_update_status(self) -> None:
        self._authenticate()
        response = self.client.post("/vistorias/1/status", data={"status": "concluida"}, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Status atualizado com sucesso", response.data.decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
