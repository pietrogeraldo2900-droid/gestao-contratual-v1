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


class WebResultsUITests(unittest.TestCase):
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
        self.client = self.app.test_client()

        self.pipeline_service = self.app.config["PIPELINE_SERVICE"]
        self.output_dir = outputs_root / "saida_web_20260319_120000"
        reports_dir = self.output_dir / "relatorios_nucleos"
        reports_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "base_gerencial.xlsx").write_text("base", encoding="utf-8")
        (self.output_dir / "execucao.csv").write_text("nucleo\nCentro\n", encoding="utf-8")
        (reports_dir / "relatorio_final.pdf").write_text("pdf fake", encoding="utf-8")

        self.pipeline_service._append_history(
            {
                "processed_at": "19/03/2026 12:00:00",
                "obra_data": "19/03/2026",
                "nucleo": "Centro",
                "nucleo_detectado_texto": "Centro",
                "nucleo_oficial": "Centro",
                "logradouro": "Av. Central",
                "municipio": "Sao Paulo",
                "municipio_detectado_texto": "Sao Paulo",
                "municipio_oficial": "Sao Paulo",
                "nucleo_status_cadastro": "ativo",
                "equipe": "Equipe A",
                "status": "sucesso",
                "contract_id": "12",
                "contract_label": "CTR-012 - Contrato Centro",
                "output_dir": str(self.output_dir),
                "base_gerencial_path": str(self.output_dir / "base_gerencial.xlsx"),
                "master_dir": str(master_dir),
                "nao_mapeados": "0",
                "alertas": "",
                "mensagem": "Mensagem de teste",
            }
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _authenticate(self) -> None:
        response = self.client.post(
            "/cadastro",
            data={"email": "resultados@empresa.com", "password": "12345678", "confirm_password": "12345678"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

    def test_results_requires_authentication(self) -> None:
        response = self.client.get("/resultados", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login?next=/resultados", str(response.headers.get("Location", "") or ""))

    def test_results_list_renders_generated_outputs(self) -> None:
        self._authenticate()
        response = self.client.get("/resultados")
        self.assertEqual(response.status_code, 200)
        html = response.data.decode("utf-8")
        self.assertIn("Resultados gerados", html)
        self.assertIn("CTR-012 - Contrato Centro", html)
        self.assertIn("relatorio_final.pdf", html)
        self.assertIn('href="/resultados"', html)

    def test_result_detail_renders_files(self) -> None:
        self._authenticate()
        response = self.client.get("/resultados/saida_web_20260319_120000")
        self.assertEqual(response.status_code, 200)
        html = response.data.decode("utf-8")
        self.assertIn("Resultado do processamento", html)
        self.assertIn("base_gerencial.xlsx", html)
        self.assertIn("relatorio_final.pdf", html)

    def test_result_file_downloads_from_outputs_root(self) -> None:
        self._authenticate()
        response = self.client.get("/resultados/arquivo/saida_web_20260319_120000/relatorios_nucleos/relatorio_final.pdf?download=1")
        try:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.data.decode("utf-8"), "pdf fake")
        finally:
            response.close()

    def test_result_file_rejects_path_outside_outputs_root(self) -> None:
        self._authenticate()
        response = self.client.get("/resultados/arquivo/../../windows/system32/drivers/etc/hosts")
        self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
