from __future__ import annotations

import unittest
from pathlib import Path

from report_system import ServiceDictionary, WhatsAppReportParser


class LegacyParserMappingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        base_dir = Path(__file__).resolve().parents[1]
        dict_path = base_dir / "config" / "service_dictionary.csv"
        cls.dictionary = ServiceDictionary(dict_path)
        cls.parser = WhatsAppReportParser(cls.dictionary)

    def test_servico_complementar_e_embutida_mureta(self):
        mensagem = """
RDO - AL 33 - Oeste 1
27/02/2026

Nucleo: Mississipi (Equipe Weslyn - Viela Mississipi)
Servico complementar: Instalacao de Caixa de Inspecao (0,80 x 0,80)
Embutida: 7
Mureta: 9
""".strip()

        parsed = self.parser.parse_text(mensagem, source_name="legado_servicos.txt")
        execucao = parsed.get("execucao", [])
        self.assertGreaterEqual(len(execucao), 3)

        by_name = {str(row.get("item_original", "")).strip().lower(): row for row in execucao}

        caixa_inspecao_item = None
        for row in execucao:
            nome = str(row.get("item_original", "")).strip().lower()
            if "caixa de inspecao" in nome:
                caixa_inspecao_item = row
                break

        self.assertIsNotNone(caixa_inspecao_item)
        self.assertEqual(caixa_inspecao_item.get("categoria_item"), "caixa_inspecao")
        self.assertEqual(float(caixa_inspecao_item.get("quantidade", 0)), 1.0)

        self.assertIn("embutida", by_name)
        self.assertEqual(by_name["embutida"].get("categoria_item"), "caixa_uma")
        self.assertEqual(float(by_name["embutida"].get("quantidade", 0)), 7.0)

        self.assertIn("mureta", by_name)
        self.assertEqual(by_name["mureta"].get("categoria_item"), "caixa_uma")
        self.assertEqual(float(by_name["mureta"].get("quantidade", 0)), 9.0)


    def test_mojibake_nao_quebra_mapeamento_de_ramais_agua(self):
        mensagem = """
ðŸ“ *NÃºcleo: PatrocÃ­nio (Equipe Sidomar â€” Tv. Rancharia / Rua Rio Tocantins)*
âœ… Ramais Ã¡gua: 3
""".strip()

        parsed = self.parser.parse_text(mensagem, source_name="legado_mojibake.txt")
        execucao = parsed.get("execucao", [])
        self.assertGreaterEqual(len(execucao), 1)

        item = execucao[0]
        self.assertEqual(item.get("categoria_item"), "ramal_agua")
        self.assertIn("ramais", str(item.get("item_original", "")).lower())

    def test_nucleo_com_municipio_no_cabecalho(self):
        mensagem = """
RDO - AL 33 - Oeste 1
27/02/2026

Nucleo: Cerejas - Jandira (Equipe Reggis - Tv. Haitiana)
Intradomiciliares: 8
""".strip()

        parsed = self.parser.parse_text(mensagem, source_name="legado_municipio.txt")
        self.assertGreaterEqual(len(parsed.get("frentes", [])), 1)
        self.assertGreaterEqual(len(parsed.get("execucao", [])), 1)

        frente = parsed["frentes"][0]
        item = parsed["execucao"][0]

        self.assertEqual(frente.get("nucleo"), "Cerejas")
        self.assertEqual(frente.get("municipio"), "Jandira")
        self.assertEqual(item.get("municipio"), "Jandira")


    def test_normaliza_nucleo_operacional_e_nao_inferir_viela(self):
        mensagem = """
RDO - AL 33 - Oeste 1
27/02/2026

Nucleo: Mississipi - Esgoto (Equipe Weslyn - Viela Mississipi)
Ramais esgoto: 3

Nucleo: Viela 1 / Viela 7 (Equipe Xavier)
Prolongamento rede: 19 m
""".strip()

        parsed = self.parser.parse_text(mensagem, source_name="legado_nucleo_normalizado.txt")
        frentes = parsed.get("frentes", [])
        self.assertGreaterEqual(len(frentes), 2)

        self.assertEqual(frentes[0].get("nucleo"), "Mississipi")
        self.assertEqual(frentes[1].get("nucleo"), "")
        self.assertEqual(frentes[1].get("logradouro"), "Viela 1 / Viela 7")

    def test_legado_mantem_apenas_primeira_equipe_por_linha(self):
        mensagem = """
RDO - AL 33 - Oeste 1
27/02/2026

Nucleo: Mississipi (Equipe Carlos / Wesley - Viela 6)
Ramais agua: 2
""".strip()
        parsed = self.parser.parse_text(mensagem, source_name="legado_equipe_primeira.txt")
        self.assertTrue(parsed.get("frentes"))
        self.assertTrue(parsed.get("execucao"))
        self.assertEqual(parsed["frentes"][0].get("equipe"), "Carlos")
        self.assertEqual(parsed["execucao"][0].get("equipe"), "Carlos")

if __name__ == "__main__":
    unittest.main()


