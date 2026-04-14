from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from app.routes.web_app import create_app


class AuthApiTests(unittest.TestCase):
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
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_auth_register_returns_503_when_database_disabled(self) -> None:
        response = self.client.post("/auth/register", json={"email": "a@b.com", "password": "12345678"})
        self.assertEqual(response.status_code, 503)
        payload = response.get_json() or {}
        self.assertFalse(payload.get("success", True))

    def test_auth_login_returns_503_when_database_disabled(self) -> None:
        response = self.client.post("/auth/login", json={"email": "a@b.com", "password": "12345678"})
        self.assertEqual(response.status_code, 503)
        payload = response.get_json() or {}
        self.assertFalse(payload.get("success", True))

    def test_auth_me_returns_503_when_database_disabled(self) -> None:
        response = self.client.get("/auth/me")
        self.assertEqual(response.status_code, 503)
        payload = response.get_json() or {}
        self.assertFalse(payload.get("success", True))


if __name__ == "__main__":
    unittest.main()
