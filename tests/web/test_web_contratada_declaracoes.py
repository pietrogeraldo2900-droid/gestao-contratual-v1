from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

from app.routes.web_app import create_app
from app.services.user_service import UserAuthError, UserValidationError


@dataclass
class _FakeContract:
    id: int
    numero_contrato: str
    nome_contrato: str

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "numero_contrato": self.numero_contrato,
            "nome_contrato": self.nome_contrato,
        }


@dataclass
class _FakeDeclaration:
    id: int
    contract_id: int
    contract_label: str
    declaration_date: str
    periodo: str
    nucleo: str
    municipio: str
    logradouro: str
    equipe: str
    responsavel_nome: str
    responsavel_contato: str
    observacoes: str
    generated_inspection_id: int
    created_by: int | None

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "contract_id": self.contract_id,
            "contract_label": self.contract_label,
            "declaration_date": self.declaration_date,
            "periodo": self.periodo,
            "nucleo": self.nucleo,
            "municipio": self.municipio,
            "logradouro": self.logradouro,
            "equipe": self.equipe,
            "responsavel_nome": self.responsavel_nome,
            "responsavel_contato": self.responsavel_contato,
            "observacoes": self.observacoes,
            "is_official_base": False,
            "generated_inspection_id": self.generated_inspection_id,
            "created_by": self.created_by,
            "created_at": "2026-04-08T00:00:00+00:00",
            "updated_at": "2026-04-08T00:00:00+00:00",
        }


@dataclass
class _FakeDeclarationItem:
    declaration_id: int
    ordem: int
    servico_oficial: str
    servico_label: str
    categoria: str
    quantidade: str
    unidade: str
    local_execucao: str
    descricao: str
    item_status: str

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.ordem,
            "declaration_id": self.declaration_id,
            "ordem": self.ordem,
            "servico_oficial": self.servico_oficial,
            "servico_label": self.servico_label,
            "categoria": self.categoria,
            "quantidade": self.quantidade,
            "unidade": self.unidade,
            "local_execucao": self.local_execucao,
            "descricao": self.descricao,
            "item_status": self.item_status,
            "created_at": "",
            "updated_at": "",
        }


class FakeUserService:
    def __init__(self, *, role: str, authorized_contract_ids: list[int] | None = None) -> None:
        self._users_by_email: dict[str, dict[str, object]] = {}
        self._next_id = 1
        self._default_role = role
        self._authorized_contract_ids = list(authorized_contract_ids or [])

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
            "role": self._default_role,
            "status": "active",
            "authorized_contract_ids": list(self._authorized_contract_ids),
            "created_at": "2026-04-08T00:00:00+00:00",
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


class FakeContractService:
    def list_contracts(self, limit: int = 100):  # noqa: ANN001
        _ = limit
        return [
            _FakeContract(1, "CTR-001", "Contrato Centro"),
            _FakeContract(2, "CTR-002", "Contrato Norte"),
        ]

    def count_contracts(self) -> int:
        return 2


class FakeDeclarationService:
    def __init__(self) -> None:
        self._declarations: list[_FakeDeclaration] = []
        self._items: dict[int, list[_FakeDeclarationItem]] = {}
        self._next_declaration_id = 1
        self._next_inspection_id = 100

    def list_declarations(self, *, limit=200, contract_id=None, created_by=None, date_from="", date_to=""):  # noqa: ANN001, ANN003
        _ = (date_from, date_to)
        rows = list(self._declarations)
        if contract_id:
            rows = [row for row in rows if int(row.contract_id) == int(contract_id)]
        if created_by:
            rows = [row for row in rows if int(row.created_by or 0) == int(created_by)]
        return rows[: max(1, int(limit))]

    def get_declaration_with_items(self, declaration_id: int):
        for row in self._declarations:
            if int(row.id) == int(declaration_id):
                return row, list(self._items.get(row.id, []))
        return None, []

    def create_declaration_and_generate_inspection(self, payload, raw_items, *, created_by=None):  # noqa: ANN001, ANN003
        contract_id = int(str(payload.get("contract_id", "") or "0"))
        if contract_id <= 0:
            raise ValueError("Contrato invalido")
        if not list(raw_items or []):
            raise ValueError("Sem itens")

        declaration = _FakeDeclaration(
            id=self._next_declaration_id,
            contract_id=contract_id,
            contract_label=f"CTR-{contract_id:03d} - Contrato Teste",
            declaration_date=str(payload.get("declaration_date", "") or ""),
            periodo=str(payload.get("periodo", "") or ""),
            nucleo=str(payload.get("nucleo", "") or ""),
            municipio=str(payload.get("municipio", "") or ""),
            logradouro=str(payload.get("logradouro", "") or ""),
            equipe=str(payload.get("equipe", "") or ""),
            responsavel_nome=str(payload.get("responsavel_nome", "") or ""),
            responsavel_contato=str(payload.get("responsavel_contato", "") or ""),
            observacoes=str(payload.get("observacoes", "") or ""),
            generated_inspection_id=self._next_inspection_id,
            created_by=int(created_by) if created_by else None,
        )
        self._declarations.append(declaration)

        parsed_items: list[_FakeDeclarationItem] = []
        for idx, raw in enumerate(list(raw_items or [])):
            service_name = str(raw.get("servico_oficial", "") or "").strip()
            qty = str(raw.get("quantidade", "") or "").strip()
            if not service_name and not qty:
                continue
            parsed_items.append(
                _FakeDeclarationItem(
                    declaration_id=declaration.id,
                    ordem=idx + 1,
                    servico_oficial=service_name or "servico_nao_mapeado",
                    servico_label=str(raw.get("servico_label", "") or "").strip(),
                    categoria=str(raw.get("categoria", "") or "").strip(),
                    quantidade=qty or "0",
                    unidade=str(raw.get("unidade", "") or "").strip(),
                    local_execucao=str(raw.get("local_execucao", "") or "").strip(),
                    descricao=str(raw.get("descricao", "") or "").strip(),
                    item_status=str(raw.get("item_status", "declarado") or "declarado"),
                )
            )
        self._items[declaration.id] = parsed_items

        inspection = type("FakeInspection", (), {"id": self._next_inspection_id})()
        self._next_declaration_id += 1
        self._next_inspection_id += 1
        return declaration, inspection


class WebContratadaDeclaracoesTests(unittest.TestCase):
    def _build_client(self, *, role: str, contract_ids: list[int]) -> tuple[object, object, tempfile.TemporaryDirectory]:
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
        app.config["USER_SERVICE"] = FakeUserService(role=role, authorized_contract_ids=contract_ids)
        app.config["CONTRACTS_SERVICE"] = FakeContractService()
        app.config["DECLARATION_SERVICE"] = FakeDeclarationService()
        client = app.test_client()
        return app, client, tmp

    def _authenticate(self, client) -> None:  # noqa: ANN001
        register_response = client.post(
            "/cadastro",
            data={"email": "contratada@empresa.com", "password": "12345678", "confirm_password": "12345678"},
            follow_redirects=False,
        )
        self.assertIn(register_response.status_code, (302, 303))
        login_response = client.post(
            "/login",
            data={"email": "contratada@empresa.com", "password": "12345678"},
            follow_redirects=False,
        )
        self.assertIn(login_response.status_code, (302, 303))

    def test_contratada_screen_requires_login(self) -> None:
        _, client, tmp = self._build_client(role="contratada", contract_ids=[1])
        response = client.get("/conferencia-operacional/contratada/declaracoes", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            "/login?next=/conferencia-operacional/contratada/declaracoes",
            str(response.headers.get("Location", "") or ""),
        )
        tmp.cleanup()

    def test_contratada_can_create_declaration_and_auto_generate_ficha(self) -> None:
        _, client, tmp = self._build_client(role="contratada", contract_ids=[1])
        self._authenticate(client)

        new_page = client.get("/conferencia-operacional/contratada/declaracoes/nova")
        self.assertEqual(new_page.status_code, 200)
        self.assertIn("Declaracao diaria da contratada", new_page.data.decode("utf-8"))

        response = client.post(
            "/conferencia-operacional/contratada/declaracoes",
            data={
                "contract_id": "1",
                "declaration_date": "2026-04-08",
                "periodo": "Diurno",
                "nucleo": "Centro",
                "municipio": "Osasco",
                "logradouro": "Rua A",
                "equipe": "Equipe 01",
                "responsavel_nome": "Responsavel Teste",
                "responsavel_contato": "11999990000",
                "observacoes": "Teste",
                "item_servico_oficial[]": ["prolongamento_rede_agua"],
                "item_servico_label[]": ["Prolongamento rede agua"],
                "item_categoria[]": ["rede_agua"],
                "item_quantidade[]": ["12.5"],
                "item_unidade[]": ["m"],
                "item_local_execucao[]": ["Trecho 01"],
                "item_descricao[]": ["Execucao diaria"],
                "item_status[]": ["declarado"],
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        location = str(response.headers.get("Location", "") or "")
        self.assertIn("/conferencia-operacional/contratada/declaracoes/", location)

        detail = client.get(location)
        self.assertEqual(detail.status_code, 200)
        html = detail.data.decode("utf-8")
        self.assertIn("Declaracao diaria #1", html)
        self.assertIn("Abrir ficha vinculada", html)
        self.assertIn("#100", html)
        tmp.cleanup()

    def test_operator_cannot_access_contratada_screen(self) -> None:
        _, client, tmp = self._build_client(role="operador", contract_ids=[1])
        self._authenticate(client)
        response = client.get("/conferencia-operacional/contratada/declaracoes", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertEqual("/dashboard", str(response.headers.get("Location", "") or ""))
        tmp.cleanup()

    def test_contratada_cannot_submit_unauthorized_contract(self) -> None:
        _, client, tmp = self._build_client(role="contratada", contract_ids=[1])
        self._authenticate(client)
        response = client.post(
            "/conferencia-operacional/contratada/declaracoes",
            data={
                "contract_id": "2",
                "declaration_date": "2026-04-08",
                "item_servico_oficial[]": ["prolongamento_rede_agua"],
                "item_servico_label[]": ["Prolongamento rede agua"],
                "item_categoria[]": ["rede_agua"],
                "item_quantidade[]": ["12.5"],
                "item_unidade[]": ["m"],
                "item_local_execucao[]": ["Trecho 01"],
                "item_descricao[]": ["Execucao diaria"],
                "item_status[]": ["declarado"],
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 403)
        self.assertIn("Contrato fora do escopo", response.data.decode("utf-8"))
        tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
