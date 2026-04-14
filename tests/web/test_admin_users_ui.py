from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from app.routes.web_app import create_app


class FakeUserService:
    def __init__(self, user: dict[str, object]):
        self.user = dict(user)

    def get_user_by_id(self, user_id: int) -> dict[str, object] | None:
        if int(self.user.get("id", 0) or 0) != int(user_id):
            return None
        if str(self.user.get("status", "") or "").lower() != "active":
            return None
        return dict(self.user)


class FakeUserRepository:
    def __init__(self, users: dict[int, dict[str, object]]):
        self.users = {int(k): dict(v) for k, v in users.items()}
        for user in self.users.values():
            user.setdefault("authorized_contract_ids", [])
            user.setdefault("contractor_name", "")

    def list_users(self, status: str | None = None):
        if status:
            return [dict(v) for v in self.users.values() if str(v.get("status", "") or "").lower() == status]
        return [dict(v) for v in self.users.values()]

    def get_user_by_id(self, user_id: int):
        user = self.users.get(int(user_id))
        return dict(user) if user else None

    def update_user_status(self, user_id: int, status: str, *, approved_by=None, set_approved=False):
        user = dict(self.users.get(int(user_id), {}))
        user["status"] = status
        if set_approved:
            user["approved_by"] = approved_by
            user["approved_at"] = "now"
        self.users[int(user_id)] = user
        return dict(user)

    def update_user_role(self, user_id: int, role: str):
        user = dict(self.users.get(int(user_id), {}))
        user["role"] = role
        self.users[int(user_id)] = user
        return dict(user)

    def update_user_contractor(self, user_id: int, contractor_name: str | None):
        user = dict(self.users.get(int(user_id), {}))
        user["contractor_name"] = str(contractor_name or "").strip()
        self.users[int(user_id)] = user
        return dict(user)

    def replace_user_authorized_contracts(self, user_id: int, contract_ids: list[int]):
        user = dict(self.users.get(int(user_id), {}))
        user["authorized_contract_ids"] = list(contract_ids or [])
        self.users[int(user_id)] = user
        return list(contract_ids or [])


class FakeAuditRepository:
    def __init__(self):
        self.actions: list[dict[str, object]] = []

    def log_action(self, actor_user_id: int, target_user_id, action: str, metadata=None):
        self.actions.append(
            {
                "actor_user_id": actor_user_id,
                "target_user_id": target_user_id,
                "action": action,
                "metadata": metadata or {},
            }
        )


def _build_app_with_user(role: str):
    tmp = tempfile.TemporaryDirectory()
    tmp_path = Path(tmp.name)

    outputs_root = tmp_path / "saidas"
    master_dir = tmp_path / "BASE_MESTRA"
    history_file = tmp_path / "data" / "runtime" / "processing_history.csv"
    draft_dir = tmp_path / "data" / "drafts" / "web"
    nucleo_reference_file = tmp_path / "config" / "nucleo_reference.json"
    nucleo_reference_file.parent.mkdir(parents=True, exist_ok=True)
    nucleo_reference_file.write_text(json.dumps({"version": "test", "nucleos": []}), encoding="utf-8")

    app = create_app(
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

    user = {"id": 1, "email": "user@empresa.com", "status": "active", "role": role, "created_at": ""}
    app.config["USER_SERVICE"] = FakeUserService(user)
    return app, tmp


class AdminUsersUITests(unittest.TestCase):
    def test_admin_users_blocked_for_operador(self):
        app, tmp = _build_app_with_user("operador")
        client = app.test_client()
        with client.session_transaction() as sess:
            sess["web_user_id"] = 1
            sess["web_user_email"] = "user@empresa.com"
        response = client.get("/admin/usuarios")
        self.assertEqual(response.status_code, 403)
        tmp.cleanup()

    def test_admin_users_blocked_for_leitor(self):
        app, tmp = _build_app_with_user("leitor")
        client = app.test_client()
        with client.session_transaction() as sess:
            sess["web_user_id"] = 1
            sess["web_user_email"] = "user@empresa.com"
        response = client.get("/admin/usuarios")
        self.assertEqual(response.status_code, 403)
        tmp.cleanup()

    def test_superadmin_can_approve_user_and_audit(self):
        app, tmp = _build_app_with_user("superadmin")
        repo = FakeUserRepository(
            {
                2: {
                    "id": 2,
                    "email": "novo@empresa.com",
                    "status": "pending",
                    "role": "",
                    "created_at": "",
                }
            }
        )
        audit = FakeAuditRepository()
        app.config["USER_REPOSITORY"] = repo
        app.config["ADMIN_AUDIT_REPOSITORY"] = audit
        client = app.test_client()
        with client.session_transaction() as sess:
            sess["web_user_id"] = 1
            sess["web_user_email"] = "super@empresa.com"
        response = client.post(
            "/admin/usuarios/2/update",
            data={"action": "approve", "role": "operador"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        updated = repo.users.get(2, {})
        self.assertEqual(updated.get("status"), "active")
        self.assertEqual(updated.get("role"), "operador")
        self.assertTrue(any(action["action"] == "user_approved" for action in audit.actions))
        tmp.cleanup()

    def test_superadmin_can_approve_contratada_with_scope(self):
        app, tmp = _build_app_with_user("superadmin")

        class FakeContract:
            def __init__(self, contract_id: int):
                self._id = contract_id

            def to_dict(self):
                return {
                    "id": self._id,
                    "numero_contrato": f"CTR-{self._id:03d}",
                    "nome_contrato": f"Contrato {self._id}",
                    "contratada_nome": "Prestadora A",
                }

        class FakeContractService:
            def list_contracts(self, limit: int = 100):
                _ = limit
                return [FakeContract(1), FakeContract(2)]

            def count_contracts(self):
                return 2

        repo = FakeUserRepository(
            {
                2: {
                    "id": 2,
                    "email": "contratada@empresa.com",
                    "status": "pending",
                    "role": "",
                    "created_at": "",
                }
            }
        )
        audit = FakeAuditRepository()
        app.config["USER_REPOSITORY"] = repo
        app.config["ADMIN_AUDIT_REPOSITORY"] = audit
        app.config["CONTRACTS_SERVICE"] = FakeContractService()
        client = app.test_client()
        with client.session_transaction() as sess:
            sess["web_user_id"] = 1
            sess["web_user_email"] = "super@empresa.com"

        response = client.post(
            "/admin/usuarios/2/update",
            data={
                "action": "approve",
                "role": "contratada",
                "contractor_name": "Prestadora A",
                "contract_ids": ["1", "2"],
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        updated = repo.users.get(2, {})
        self.assertEqual(updated.get("status"), "active")
        self.assertEqual(updated.get("role"), "contratada")
        self.assertEqual(updated.get("contractor_name"), "Prestadora A")
        self.assertEqual(updated.get("authorized_contract_ids"), [1, 2])
        self.assertTrue(any(action["action"] == "user_approved" for action in audit.actions))
        tmp.cleanup()

    def test_superadmin_sees_premium_user_management_layout(self):
        app, tmp = _build_app_with_user("superadmin")

        class FakeContract:
            def __init__(self, contract_id: int):
                self._id = contract_id

            def to_dict(self):
                return {
                    "id": self._id,
                    "numero_contrato": f"CTR-{self._id:03d}",
                    "nome_contrato": f"Contrato {self._id}",
                    "contratada_nome": "Prestadora A",
                }

        class FakeContractService:
            def list_contracts(self, limit: int = 100):
                _ = limit
                return [FakeContract(1), FakeContract(2), FakeContract(3)]

            def count_contracts(self):
                return 3

        repo = FakeUserRepository(
            {
                2: {
                    "id": 2,
                    "email": "pendente@empresa.com",
                    "status": "pending",
                    "role": "",
                    "created_at": "",
                },
                3: {
                    "id": 3,
                    "email": "ativo@empresa.com",
                    "status": "active",
                    "role": "operador",
                    "created_at": "",
                    "last_login_at": "",
                    "contractor_name": "Prestadora A",
                    "authorized_contract_ids": [1, 2],
                },
            }
        )
        app.config["USER_REPOSITORY"] = repo
        app.config["CONTRACTS_SERVICE"] = FakeContractService()
        client = app.test_client()
        with client.session_transaction() as sess:
            sess["web_user_id"] = 1
            sess["web_user_email"] = "super@empresa.com"

        response = client.get("/admin/usuarios?user_id=2")
        self.assertEqual(response.status_code, 200)
        html = response.data.decode("utf-8")
        self.assertIn("admin-users-page", html)
        self.assertIn("admin-users-layout", html)
        self.assertIn("Tabela principal", html)
        self.assertIn("Painel lateral", html)
        self.assertIn("pendente@empresa.com", html)
        self.assertIn("Aprovar usuario", html)
        tmp.cleanup()

    def test_role_change_reflects_route_access(self):
        app, tmp = _build_app_with_user("leitor")
        client = app.test_client()
        with client.session_transaction() as sess:
            sess["web_user_id"] = 1
            sess["web_user_email"] = "user@empresa.com"
        response = client.get("/nucleos", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertEqual("/dashboard", str(response.headers.get("Location", "") or ""))
        tmp.cleanup()

    def test_sidebar_hides_restricted_items_for_leitor(self):
        app, tmp = _build_app_with_user("leitor")
        client = app.test_client()
        with client.session_transaction() as sess:
            sess["web_user_id"] = 1
            sess["web_user_email"] = "user@empresa.com"
        response = client.get("/dashboard")
        self.assertEqual(response.status_code, 200)
        html = response.data.decode("utf-8")
        self.assertNotIn('nav-link-label">Contratos<', html)
        self.assertNotIn('nav-link-label">Nucleos<', html)
        tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
