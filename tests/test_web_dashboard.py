from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

from app.repositories.user_repository import UserAlreadyExistsError
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
            raise UserAlreadyExistsError("Email ja cadastrado.")
        user = {"id": self._next_id, "email": clean_email, "created_at": "2026-01-01T00:00:00+00:00"}
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


@dataclass(frozen=True)
class _FakeContract:
    id: int
    numero_contrato: str
    nome_contrato: str
    municipios_atendidos: str
    status_contrato: str
    created_at: str

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "numero_contrato": self.numero_contrato,
            "nome_contrato": self.nome_contrato,
            "municipios_atendidos": self.municipios_atendidos,
            "status_contrato": self.status_contrato,
            "contract_code": self.numero_contrato,
            "title": self.nome_contrato,
            "description": "",
            "status": self.status_contrato,
            "created_at": self.created_at,
            "updated_at": self.created_at,
        }


class FakeContractService:
    def __init__(self) -> None:
        self._contracts = [
            _FakeContract(
                id=1,
                numero_contrato="CTR-001",
                nome_contrato="Contrato Centro",
                municipios_atendidos="Sao Paulo",
                status_contrato="ativo",
                created_at="2026-03-10",
            ),
            _FakeContract(
                id=2,
                numero_contrato="CTR-002",
                nome_contrato="Contrato Oeste",
                municipios_atendidos="Osasco",
                status_contrato="em_implantacao",
                created_at="2026-03-11",
            ),
        ]

    def count_contracts(self) -> int:
        return len(self._contracts)

    def list_contracts(self, limit: int = 100):
        return list(self._contracts)[: max(1, int(limit))]


class FakeReportService:
    def __init__(self) -> None:
        self._reports = [
            {
                "id": 10,
                "contract_id": 1,
                "file_name": "relatorio_ctr001.pdf",
                "created_at": "2026-03-15T10:00:00+00:00",
                "numero_contrato": "CTR-001",
                "nome_contrato": "Contrato Centro",
            },
            {
                "id": 11,
                "contract_id": 2,
                "file_name": "relatorio_ctr002.pdf",
                "created_at": "2026-03-16T10:00:00+00:00",
                "numero_contrato": "CTR-002",
                "nome_contrato": "Contrato Oeste",
            },
        ]

    def count_reports(self) -> int:
        return len(self._reports)

    def count_recent_reports(self, days: int = 7) -> int:
        _ = days
        return len(self._reports)

    def list_recent_reports(self, limit: int = 20):
        return list(self._reports)[: max(1, int(limit))]


class WebDashboardTests(unittest.TestCase):
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
        self.app.config["CONTRACTS_SERVICE"] = FakeContractService()
        self.app.config["REPORT_SERVICE"] = FakeReportService()
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _authenticate(self) -> None:
        response = self.client.post(
            "/cadastro",
            data={"email": "painel@empresa.com", "password": "12345678", "confirm_password": "12345678"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual("/dashboard", str(response.headers.get("Location", "") or ""))

    def test_dashboard_redirects_to_login_when_not_authenticated(self) -> None:
        response = self.client.get("/dashboard", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login?next=/dashboard", str(response.headers.get("Location", "") or ""))

    def test_root_redirects_to_dashboard_when_authenticated(self) -> None:
        self._authenticate()
        response = self.client.get("/", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertEqual("/dashboard", str(response.headers.get("Location", "") or ""))

    def test_dashboard_renders_summary_cards_and_tables(self) -> None:
        self._authenticate()
        response = self.client.get("/dashboard")
        self.assertEqual(response.status_code, 200)
        html = response.data.decode("utf-8")
        self.assertIn("Dashboard operacional", html)
        self.assertIn("Relatorios recentes", html)
        self.assertIn("CTR-001", html)
        self.assertIn("relatorio_ctr001.pdf", html)

    def test_dashboard_shows_required_navigation_links(self) -> None:
        self._authenticate()
        response = self.client.get("/dashboard")
        self.assertEqual(response.status_code, 200)
        html = response.data.decode("utf-8")
        self.assertIn('href="/dashboard"', html)
        self.assertIn('href="/contratos"', html)
        self.assertIn('href="/dashboard#reports"', html)
        self.assertIn('href="/logout"', html)


if __name__ == "__main__":
    unittest.main()
