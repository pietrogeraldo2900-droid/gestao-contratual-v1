from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

from app.repositories.contract_repository import ContractConflictError
from app.repositories.user_repository import UserAlreadyExistsError
from app.routes.web_app import create_app
from app.services.contract_service import ContractValidationError
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
    objeto_contrato: str
    data_assinatura: str
    prazo_dias: int
    valor_contrato: str
    contratante_nome: str
    contratada_nome: str
    regional: str
    municipios_atendidos: str
    status_contrato: str
    created_at: str

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "numero_contrato": self.numero_contrato,
            "nome_contrato": self.nome_contrato,
            "objeto_contrato": self.objeto_contrato,
            "data_assinatura": self.data_assinatura,
            "prazo_dias": self.prazo_dias,
            "valor_contrato": self.valor_contrato,
            "contratante_nome": self.contratante_nome,
            "contratada_nome": self.contratada_nome,
            "regional": self.regional,
            "municipios_atendidos": self.municipios_atendidos,
            "status_contrato": self.status_contrato,
            "contract_code": self.numero_contrato,
            "title": self.nome_contrato,
            "description": self.objeto_contrato,
            "status": self.status_contrato,
            "created_at": self.created_at,
            "updated_at": self.created_at,
        }


class FakeContractService:
    def __init__(self) -> None:
        self._next_id = 2
        self._contracts: list[_FakeContract] = [
            _FakeContract(
                id=1,
                numero_contrato="CTR-001",
                nome_contrato="Contrato Inicial",
                objeto_contrato="Objeto inicial",
                data_assinatura="2026-03-10",
                prazo_dias=365,
                valor_contrato="15000.00",
                contratante_nome="Sabesp",
                contratada_nome="Prestadora A",
                regional="Capital",
                municipios_atendidos="Sao Paulo",
                status_contrato="ativo",
                created_at="2026-03-10T10:00:00+00:00",
            )
        ]

    def count_contracts(self) -> int:
        return len(self._contracts)

    def list_contracts(self, limit: int = 100):
        safe_limit = max(1, int(limit))
        return list(self._contracts)[:safe_limit]

    def create_contract(self, payload: dict[str, object]):
        numero = str(payload.get("numero_contrato", "") or "").strip()
        nome = str(payload.get("nome_contrato", "") or "").strip()
        objeto = str(payload.get("objeto_contrato", "") or "").strip()
        data_assinatura = str(payload.get("data_assinatura", "") or "").strip()
        prazo_dias = str(payload.get("prazo_dias", "") or "").strip()
        valor_contrato = str(payload.get("valor_contrato", "") or "").strip()
        contratante_nome = str(payload.get("contratante_nome", "") or "").strip()
        contratada_nome = str(payload.get("contratada_nome", "") or "").strip()
        regional = str(payload.get("regional", "") or "").strip()
        municipios = str(payload.get("municipios_atendidos", "") or "").strip()
        status = str(payload.get("status_contrato", "em_implantacao") or "em_implantacao").strip().lower()

        if not numero:
            raise ContractValidationError("Informe numero_contrato.")
        if not nome:
            raise ContractValidationError("Informe nome_contrato.")
        if not objeto:
            raise ContractValidationError("Informe objeto_contrato.")
        if not data_assinatura:
            raise ContractValidationError("Informe data_assinatura.")
        if not prazo_dias:
            raise ContractValidationError("Informe prazo_dias.")
        if not valor_contrato:
            raise ContractValidationError("Informe valor_contrato.")
        if not contratante_nome:
            raise ContractValidationError("Informe contratante_nome.")
        if not contratada_nome:
            raise ContractValidationError("Informe contratada_nome.")
        if not regional:
            raise ContractValidationError("Informe regional.")
        if not municipios:
            raise ContractValidationError("Informe municipios_atendidos.")
        if any(item.numero_contrato.lower() == numero.lower() for item in self._contracts):
            raise ContractConflictError("Ja existe contrato com este numero.")
        if status not in {"em_implantacao", "ativo", "suspenso", "encerrado"}:
            raise ContractValidationError("Status invalido.")

        created = _FakeContract(
            id=self._next_id,
            numero_contrato=numero,
            nome_contrato=nome,
            objeto_contrato=objeto,
            data_assinatura=data_assinatura,
            prazo_dias=int(prazo_dias),
            valor_contrato=valor_contrato,
            contratante_nome=contratante_nome,
            contratada_nome=contratada_nome,
            regional=regional,
            municipios_atendidos=municipios,
            status_contrato=status,
            created_at="2026-03-20T10:00:00+00:00",
        )
        self._next_id += 1
        self._contracts.insert(0, created)
        return created


class WebContractsUITests(unittest.TestCase):
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
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _authenticate(self) -> None:
        response = self.client.post(
            "/cadastro",
            data={"email": "contratos@empresa.com", "password": "12345678", "confirm_password": "12345678"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual("/dashboard", str(response.headers.get("Location", "") or ""))

    def test_contracts_page_requires_authentication(self) -> None:
        response = self.client.get("/contratos", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login?next=/contratos", str(response.headers.get("Location", "") or ""))

    def test_contracts_list_and_create_flow(self) -> None:
        self._authenticate()

        list_response = self.client.get("/contratos")
        self.assertEqual(list_response.status_code, 200)
        html_list = list_response.data.decode("utf-8")
        self.assertIn("Contratos cadastrados", html_list)
        self.assertIn("CTR-001", html_list)

        form_response = self.client.get("/contratos/novo")
        self.assertEqual(form_response.status_code, 200)
        self.assertIn("Cadastrar contrato", form_response.data.decode("utf-8"))

        create_response = self.client.post(
            "/contratos",
            data={
                "numero_contrato": "CTR-002",
                "nome_contrato": "Contrato Oeste",
                "objeto_contrato": "Objeto novo",
                "data_assinatura": "2026-03-20",
                "prazo_dias": "180",
                "valor_contrato": "19999.90",
                "contratante_nome": "Sabesp",
                "contratada_nome": "Prestadora B",
                "regional": "ABC",
                "municipios_atendidos": "Santo Andre, Sao Bernardo",
                "status_contrato": "ativo",
            },
            follow_redirects=False,
        )
        self.assertEqual(create_response.status_code, 302)
        self.assertEqual("/contratos", str(create_response.headers.get("Location", "") or ""))

        updated_list = self.client.get("/contratos")
        self.assertEqual(updated_list.status_code, 200)
        html_updated = updated_list.data.decode("utf-8")
        self.assertIn("CTR-002", html_updated)
        self.assertIn("Contrato Oeste", html_updated)

    def test_contracts_create_shows_validation_error(self) -> None:
        self._authenticate()
        response = self.client.post(
            "/contratos",
            data={
                "numero_contrato": "",
                "nome_contrato": "Sem numero",
                "objeto_contrato": "Objeto",
                "data_assinatura": "2026-03-20",
                "prazo_dias": "30",
                "valor_contrato": "1000",
                "contratante_nome": "Sabesp",
                "contratada_nome": "Prestadora",
                "regional": "Capital",
                "municipios_atendidos": "Sao Paulo",
                "status_contrato": "em_implantacao",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 400)
        html = response.data.decode("utf-8")
        self.assertIn("Informe numero_contrato.", html)


if __name__ == "__main__":
    unittest.main()
