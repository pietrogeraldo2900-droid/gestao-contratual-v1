from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from app.repositories.user_repository import UserAlreadyExistsError
from app.routes.web_app import create_app
from app.services.user_service import UserAuthError, UserValidationError


class FakeUserService:
    def __init__(self) -> None:
        self._users_by_email: dict[str, dict[str, object]] = {}
        self._next_id = 1

    def _normalize_email(self, email: str) -> str:
        return str(email or "").strip().lower()

    def register_user(self, email: str, password: str) -> dict[str, object]:
        clean_email = self._normalize_email(email)
        if not clean_email:
            raise UserValidationError("Email obrigatorio.")
        if len(str(password or "")) < 8:
            raise UserValidationError("Senha deve ter no minimo 8 caracteres.")
        if clean_email in self._users_by_email:
            raise UserAlreadyExistsError("Email ja cadastrado.")

        user = {
            "id": self._next_id,
            "email": clean_email,
            "status": "pending",
            "role": "operador",
            "created_at": "2026-01-01T00:00:00+00:00",
        }
        self._users_by_email[clean_email] = {"user": user, "password": str(password or "")}
        self._next_id += 1
        return dict(user)

    def authenticate_user(self, email: str, password: str) -> dict[str, object]:
        clean_email = self._normalize_email(email)
        row = self._users_by_email.get(clean_email)
        if not row:
            raise UserAuthError("Credenciais invalidas.")
        status = str(row["user"].get("status", "") or "").strip().lower()
        if status != "active":
            if status == "pending":
                raise UserAuthError("Seu cadastro foi recebido e esta aguardando aprovacao.")
            if status == "rejected":
                raise UserAuthError("Seu cadastro nao foi aprovado. Entre em contato com o administrador.")
            if status == "disabled":
                raise UserAuthError("Sua conta esta desativada no momento. Entre em contato com o administrador.")
            raise UserAuthError("Sua conta nao esta ativa. Entre em contato com o administrador.")
        if str(row.get("password", "") or "") != str(password or ""):
            raise UserAuthError("Credenciais invalidas.")
        return dict(row["user"])  # type: ignore[index]

    def get_user_by_id(self, user_id: int) -> dict[str, object] | None:
        try:
            uid = int(user_id)
        except Exception:
            return None
        for row in self._users_by_email.values():
            user = row.get("user")
            if isinstance(user, dict) and int(user.get("id", 0) or 0) == uid:
                return dict(user)
        return None

    def set_status(self, email: str, status: str) -> None:
        clean_email = self._normalize_email(email)
        if clean_email in self._users_by_email:
            self._users_by_email[clean_email]["user"]["status"] = status

    def set_role(self, email: str, role: str) -> None:
        clean_email = self._normalize_email(email)
        if clean_email in self._users_by_email:
            self._users_by_email[clean_email]["user"]["role"] = role


class WebAuthUITests(unittest.TestCase):
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
        self.fake_user_service = FakeUserService()
        self.app.config["USER_SERVICE"] = self.fake_user_service
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_redirects_to_login_when_not_authenticated(self) -> None:
        response = self.client.get("/", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        location = str(response.headers.get("Location", "") or "")
        self.assertIn("/login?next=/", location)

    def test_register_login_logout_flow(self) -> None:
        register_response = self.client.post(
            "/cadastro",
            data={
                "email": "usuario@empresa.com",
                "password": "12345678",
                "confirm_password": "12345678",
            },
            follow_redirects=False,
        )
        self.assertEqual(register_response.status_code, 302)
        self.assertIn("/login", str(register_response.headers.get("Location", "") or ""))

        self.fake_user_service.set_status("usuario@empresa.com", "active")
        login_response = self.client.post(
            "/login",
            data={"email": "usuario@empresa.com", "password": "12345678"},
            follow_redirects=False,
        )
        self.assertEqual(login_response.status_code, 302)
        self.assertEqual("/dashboard", str(login_response.headers.get("Location", "") or ""))

        home_response = self.client.get("/dashboard")
        self.assertEqual(home_response.status_code, 200)
        self.assertIn("Dashboard operacional", home_response.data.decode("utf-8"))

        root_response = self.client.get("/", follow_redirects=False)
        self.assertEqual(root_response.status_code, 302)
        self.assertEqual("/dashboard", str(root_response.headers.get("Location", "") or ""))

        logout_response = self.client.get("/logout", follow_redirects=False)
        self.assertEqual(logout_response.status_code, 302)
        self.assertIn("/login", str(logout_response.headers.get("Location", "") or ""))

        redirected_response = self.client.get("/", follow_redirects=False)
        self.assertEqual(redirected_response.status_code, 302)
        self.assertIn("/login?next=/", str(redirected_response.headers.get("Location", "") or ""))

    def test_login_invalid_shows_friendly_message(self) -> None:
        self.fake_user_service.register_user("admin@empresa.com", "12345678")
        self.fake_user_service.set_status("admin@empresa.com", "active")

        response = self.client.post(
            "/login",
            data={"email": "admin@empresa.com", "password": "senha-errada", "next": "/"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 401)
        html = response.data.decode("utf-8")
        self.assertIn("Credenciais invalidas.", html)

    def test_register_existing_user_shows_friendly_message(self) -> None:
        self.fake_user_service.register_user("admin@empresa.com", "12345678")

        response = self.client.post(
            "/cadastro",
            data={
                "email": "admin@empresa.com",
                "password": "12345678",
                "confirm_password": "12345678",
                "next": "/",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 409)
        html = response.data.decode("utf-8")
        self.assertIn("Usuario ja existe para este email.", html)

    def test_login_respects_next_parameter(self) -> None:
        self.fake_user_service.register_user("admin@empresa.com", "12345678")
        self.fake_user_service.set_status("admin@empresa.com", "active")

        response = self.client.post(
            "/login",
            data={"email": "admin@empresa.com", "password": "12345678", "next": "/history"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual("/history", str(response.headers.get("Location", "") or ""))

    def test_login_default_redirects_to_dashboard(self) -> None:
        self.fake_user_service.register_user("admin@empresa.com", "12345678")
        self.fake_user_service.set_status("admin@empresa.com", "active")

        response = self.client.post(
            "/login",
            data={"email": "admin@empresa.com", "password": "12345678"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual("/dashboard", str(response.headers.get("Location", "") or ""))

    def test_login_pending_shows_pending_message(self) -> None:
        self.fake_user_service.register_user("pendente@empresa.com", "12345678")

        response = self.client.post(
            "/login",
            data={"email": "pendente@empresa.com", "password": "12345678"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 401)
        html = response.data.decode("utf-8")
        self.assertIn("aguardando aprovacao", html.lower())


if __name__ == "__main__":
    unittest.main()
