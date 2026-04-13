from __future__ import annotations

import unittest
from dataclasses import dataclass
from decimal import Decimal

from app.services.inspection_service import InspectionService, InspectionValidationError


@dataclass
class _DummyInspection:
    id: int = 1


class _FakeInspectionRepository:
    def __init__(self) -> None:
        self.last_payload: dict | None = None

    def create_inspection(self, **kwargs):
        self.last_payload = kwargs
        return _DummyInspection(id=99)

    def list_inspections(self, **kwargs):
        _ = kwargs
        return []

    def count_inspections(self, **kwargs):
        _ = kwargs
        return 0

    def get_inspection(self, inspection_id: int):
        _ = inspection_id
        return None

    def list_items(self, inspection_id: int):
        _ = inspection_id
        return []

    def update_inspection_status(self, inspection_id: int, status: str):
        _ = inspection_id
        _ = status
        return True

    def delete_inspection(self, inspection_id: int):
        _ = inspection_id
        return True


class InspectionServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = _FakeInspectionRepository()
        self.service = InspectionService(self.repo)  # type: ignore[arg-type]
        self.base_payload = {
            "titulo": "Conferencia operacional - lote 1",
            "data_vistoria": "2026-04-08",
            "status": "aberta",
            "prioridade": "media",
            "resultado": "pendente",
            "score_geral": "0",
        }

    def _create(self, items: list[dict]):
        return self.service.create_inspection(dict(self.base_payload), items, created_by=1)

    def test_base_oficial_usa_apenas_quantidade_verificada(self) -> None:
        self._create(
            [
                {
                    "item_titulo": "Executar travessia",
                    "quantidade_declarada": "10",
                    "quantidade_verificada": "7",
                }
            ]
        )
        assert self.repo.last_payload is not None
        item = self.repo.last_payload["items"][0]
        self.assertEqual(item["quantidade_declarada"], Decimal("10"))
        self.assertEqual(item["quantidade_verificada"], Decimal("7"))
        self.assertEqual(item["quantidade_oficial"], Decimal("7"))
        self.assertEqual(item["divergencia_absoluta"], Decimal("3"))
        self.assertEqual(item["divergencia_status"], "a_menor")
        self.assertTrue(item["verificado_informado"])

    def test_sem_quantidade_verificada_marca_nao_verificado(self) -> None:
        self._create(
            [
                {
                    "item_titulo": "Executar compactacao",
                    "quantidade_declarada": "5",
                    "quantidade_verificada": "",
                }
            ]
        )
        assert self.repo.last_payload is not None
        item = self.repo.last_payload["items"][0]
        self.assertEqual(item["quantidade_oficial"], Decimal("0"))
        self.assertEqual(item["divergencia_status"], "nao_verificado")
        self.assertFalse(item["verificado_informado"])

    def test_classifica_novo_nao_declarado(self) -> None:
        self._create(
            [
                {
                    "item_titulo": "Nova rede identificada",
                    "quantidade_declarada": "0",
                    "quantidade_verificada": "12",
                }
            ]
        )
        assert self.repo.last_payload is not None
        item = self.repo.last_payload["items"][0]
        self.assertEqual(item["divergencia_status"], "novo_nao_declarado")
        self.assertEqual(item["quantidade_oficial"], Decimal("12"))

    def test_rejeita_quantidade_verificada_negativa(self) -> None:
        with self.assertRaises(InspectionValidationError):
            self._create(
                [
                    {
                        "item_titulo": "Regularizar trecho",
                        "quantidade_declarada": "4",
                        "quantidade_verificada": "-1",
                    }
                ]
            )

    def test_exige_contrato_na_criacao_da_vistoria(self) -> None:
        payload = dict(self.base_payload)
        payload["contract_id"] = ""
        with self.assertRaises(InspectionValidationError):
            self.service.create_inspection(payload, [{"item_titulo": "Executar compactacao"}], created_by=1)


if __name__ == "__main__":
    unittest.main()
