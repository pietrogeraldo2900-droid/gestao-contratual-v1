from __future__ import annotations

import unittest
from pathlib import Path

from app.core.input_layer import (
    OfficialMessageParser,
    ServiceDictionaryV2,
    normalizar_unidade,
)


class InputLayerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        base_dir = Path(__file__).resolve().parents[1]
        dict_path = base_dir / "config" / "service_dictionary_v2.json"
        cls.dictionary = ServiceDictionaryV2(dict_path)
        cls.parser = OfficialMessageParser(cls.dictionary)

    def test_parsing_basico_modelo_oficial(self):
        mensagem = """
RDO - 11/03/2026

NUCLEO: Mississipi
LOGRADOURO: Viela 04 - acesso pela Rua Sao Jorge
MUNICIPIO: Carapicuiba
EQUIPE: Equipe 03

EXECUCAO:
- 35 m rede de agua
- 12 un hidrometro
- 2 un cavalete
- 1 un reparo de vazamento
- 18 m recomposicao de passeio

FRENTES:
- assentamento de rede
- instalacao de ligacoes

OCORRENCIAS:
- interferencia com rede de esgoto
- imovel fechado em 3 tentativas

OBS:
- trecho com acesso estreito
""".strip()
        parsed = self.parser.parse_text(mensagem, source_name="teste.txt")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["data_referencia"], "11/03/2026")
        self.assertEqual(len(parsed["execucao"]), 5)
        self.assertEqual(len(parsed["frentes"]), 2)
        self.assertEqual(len(parsed["observacoes"]), 1)

    def test_separacao_nucleo_logradouro(self):
        mensagem = """
RDO - 11/03/2026
NUCLEO: Mississipi
LOGRADOURO: Viela 04 - acesso pela Rua Sao Jorge
MUNICIPIO: Carapicuiba
EQUIPE: Equipe 03
EXECUCAO:
- 12 un hidrometro
FRENTES:
- frente unica
OCORRENCIAS:
- nenhuma
""".strip()
        parsed = self.parser.parse_text(mensagem, source_name="teste2.txt")
        self.assertIsNotNone(parsed)
        exec_item = parsed["execucao"][0]
        self.assertEqual(exec_item["nucleo"], "Mississipi")
        self.assertEqual(exec_item["logradouro"], "Viela 04 - acesso pela Rua Sao Jorge")

    def test_hidrometro_variacoes(self):
        entradas = [
            "hidrometro",
            "hidr\u00f4metro",
            "hidr\u00f4metros",
            "instala\u00e7\u00e3o de hidr\u00f4metro",
        ]
        for entrada in entradas:
            mapeado = self.dictionary.mapear_servico(entrada)
            self.assertEqual(mapeado["servico_oficial"], "hidrometro")
            self.assertEqual(mapeado["categoria"], "hidrometro")

    def test_unidades_normalizadas(self):
        self.assertEqual(normalizar_unidade("metros"), "m")
        self.assertEqual(normalizar_unidade("metro"), "m")
        self.assertEqual(normalizar_unidade("m"), "m")
        self.assertEqual(normalizar_unidade("unidades"), "un")
        self.assertEqual(normalizar_unidade("und"), "un")
        self.assertEqual(normalizar_unidade("un"), "un")

    def test_nao_inferir_nucleo(self):
        mensagem = """
RDO - 11/03/2026
LOGRADOURO: Viela 04 - acesso pela Rua Sao Jorge
MUNICIPIO: Carapicuiba
EQUIPE: Equipe 03
EXECUCAO:
- 10 m rede de agua
FRENTES:
- assentamento de rede
OCORRENCIAS:
- sem ocorrencias
""".strip()
        parsed = self.parser.parse_text(mensagem, source_name="sem_nucleo.txt")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["frentes"][0]["nucleo"], "")
        self.assertEqual(parsed["frentes"][0]["logradouro"], "Viela 04 - acesso pela Rua Sao Jorge")

    def test_servico_nao_mapeado(self):
        mensagem = """
RDO - 11/03/2026
NUCLEO: Mississipi
LOGRADOURO: Rua A
MUNICIPIO: Carapicuiba
EQUIPE: Equipe 03
EXECUCAO:
- 4 un servico ultradesconhecido xyz
FRENTES:
- frente unica
OCORRENCIAS:
- sem ocorrencias
""".strip()
        parsed = self.parser.parse_text(mensagem, source_name="nao_mapeado.txt")
        self.assertIsNotNone(parsed)
        exec_item = parsed["execucao"][0]
        self.assertEqual(exec_item["servico_oficial"], "servico_nao_mapeado")
        self.assertEqual(len(parsed["servicos_nao_mapeados"]), 1)

    def test_aliases_operacionais_novos_reduzem_nao_mapeado(self):
        casos = {
            "capeamento de rede": "rede_agua",
            "ligacao intradomiciliar": "intradomiciliar",
            "caps utilizados": "caps",
            "interligacao de rede": "interligacao",
            "concretagem de vala": "concretagem_vala",
            "embutida": "caixa_uma",
            "mureta": "caixa_uma",
            "instalacao de caixa de inspecao": "caixa_inspecao",
            "pi instalado": "caixa_inspecao",
            "ramais agua executados": "ramal_agua",
            "ramais de esgoto executados": "ramal_esgoto",
            "prolongamento rede agua pead 63": "rede_agua",
            "rede 200mm esgoto": "rede_esgoto",
            "luvas 63": "luvas",
            "cortes d agua": "corte_agua",
            "vistoria tecnica": "vistoria",
            "retirada de big bag": "retirada_entulho",
        }
        for termo, esperado in casos.items():
            mapeado = self.dictionary.mapear_servico(termo)
            self.assertEqual(mapeado["servico_oficial"], esperado)


    def test_servico_complementar_usa_texto_apos_dois_pontos(self):
        mensagem = """
RDO - 27/02/2026
NUCLEO: Mississipi
LOGRADOURO: Viela Mississipi
MUNICIPIO: Carapicuiba
EQUIPE: Weslyn
EXECUCAO:
- servico complementar: instalacao de caixa de inspecao (0,80 x 0,80)
FRENTES:
- esgoto
OCORRENCIAS:
- sem ocorrencias
""".strip()
        parsed = self.parser.parse_text(mensagem, source_name="servico_complementar.txt")
        self.assertIsNotNone(parsed)
        self.assertEqual(len(parsed["execucao"]), 1)
        item = parsed["execucao"][0]
        self.assertEqual(item["servico_oficial"], "caixa_inspecao")

    def test_oficial_mantem_apenas_primeira_equipe(self):
        mensagem = """
RDO - 12/03/2026
NUCLEO: Mississipi
LOGRADOURO: Viela 6
MUNICIPIO: Carapicuiba
EQUIPE: Equipe 01 / Equipe 02
EXECUCAO:
- 3 un hidrometro
FRENTES:
- ligacao
OCORRENCIAS:
- sem ocorrencias
OBS:
- observacao
""".strip()
        parsed = self.parser.parse_text(mensagem, source_name="equipe_primeira.txt")
        self.assertIsNotNone(parsed)
        self.assertTrue(parsed["frentes"])
        self.assertTrue(parsed["execucao"])
        self.assertTrue(parsed["ocorrencias"])
        self.assertTrue(parsed["observacoes"])
        self.assertEqual(parsed["frentes"][0]["equipe"], "Equipe 01")
        self.assertEqual(parsed["execucao"][0]["equipe"], "Equipe 01")
        self.assertEqual(parsed["ocorrencias"][0]["equipe"], "Equipe 01")
        self.assertEqual(parsed["observacoes"][0]["equipe"], "Equipe 01")

    def test_modelo_novo_rdo_data_local_servicos_ocorrencias(self):
        mensagem = """
RDO - Oeste 1
DATA: 25/03/2026

NUCLEO: Savoy
EQUIPE: Sidney
LOCAL:
Viela 100
Viela 376

SERVICOS:
hidrometro: 10 un
intradomiciliar: 10 un
caixa_inspecao: 7 un
ramal_esgoto: 1 un

OCORRENCIAS:
equipe_reduzida
falha_equipamento

OBS:
(opcional)
""".strip()
        parsed = self.parser.parse_text(mensagem, source_name="modelo_novo.txt")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["contrato"], "Oeste 1")
        self.assertEqual(parsed["data_referencia"], "25/03/2026")
        self.assertEqual(parsed["frentes"][0]["logradouro"], "Viela 100 / Viela 376")
        self.assertEqual(len(parsed["execucao"]), 4)
        self.assertEqual(parsed["execucao"][0]["servico_oficial"], "hidrometro")
        self.assertEqual(parsed["execucao"][0]["quantidade"], 10.0)
        self.assertEqual(parsed["execucao"][0]["unidade"], "un")
        self.assertEqual(parsed["ocorrencias"][0]["tipo_ocorrencia"], "equipe_reduzida")
        self.assertEqual(parsed["ocorrencias"][1]["tipo_ocorrencia"], "falha_equipamento")
        self.assertEqual(len(parsed["observacoes"]), 0)

    def test_modelo_novo_ignora_placeholders_vazios_em_servicos(self):
        mensagem = """
RDO - Oeste 1
DATA: 25/03/2026

NUCLEO: Bonanca
EQUIPE: Mario
LOCAL:
Rua A

SERVICOS:
-
APLICACAO DE PAVIMENTO CBUQ: 18.75 m2
-
INSTALACAO DE CAIXA DAGUA: 1 un
-

OCORRENCIAS:
solo_rochoso
""".strip()
        parsed = self.parser.parse_text(mensagem, source_name="modelo_placeholders.txt")
        self.assertIsNotNone(parsed)
        self.assertEqual(len(parsed["execucao"]), 2)
        self.assertEqual(parsed["execucao"][0]["id_item"], "I0001")
        self.assertEqual(parsed["execucao"][1]["id_item"], "I0002")
        self.assertTrue(all(str(row.get("servico_bruto", "") or "").strip() for row in parsed["execucao"]))
        self.assertTrue(all(str(row.get("servico_bruto", "") or "").strip() not in {"-"} for row in parsed["execucao"]))

    def test_modelo_novo_multiplos_nucleos_preserva_contexto_por_bloco(self):
        mensagem = """
RDO - Oeste 1
DATA: 02/04/2026

NUCLEO: VILA MENCK
EQUIPE: ROGERIO
LOCAL: LOURENCO BELOLI

SERVICOS:
APLICACAO DE PAVIMENTO CBUQ: 18.75 m2

OCORRENCIAS:
EQUIPE REDUZIDA

NUCLEO: JARDIM OLINDA
EQUIPE: TIAGO
LOCAL: RUA JOAO DIAS DE VERGARA

SERVICOS:
INSTALACAO DE CAIXA DAGUA: 1 un

NUCLEO: BONANCA
EQUIPE: MARIO
LOCAL: RUA DA PAZ DIVINA

SERVICOS:
LIGACOES DE ESGOTO: 5 un
""".strip()
        parsed = self.parser.parse_text(mensagem, source_name="multi_nucleos.txt")
        self.assertIsNotNone(parsed)
        self.assertEqual(len(parsed["execucao"]), 3)

        exec_by_nucleo = {str(row.get("nucleo", "") or "").strip(): row for row in parsed["execucao"]}
        self.assertIn("VILA MENCK", exec_by_nucleo)
        self.assertIn("JARDIM OLINDA", exec_by_nucleo)
        self.assertIn("BONANCA", exec_by_nucleo)

        self.assertEqual(exec_by_nucleo["VILA MENCK"]["logradouro"], "LOURENCO BELOLI")
        self.assertEqual(exec_by_nucleo["JARDIM OLINDA"]["logradouro"], "RUA JOAO DIAS DE VERGARA")
        self.assertEqual(exec_by_nucleo["BONANCA"]["logradouro"], "RUA DA PAZ DIVINA")

        vila_menck = exec_by_nucleo["VILA MENCK"]
        self.assertEqual(vila_menck["quantidade"], 18.75)
        self.assertEqual(vila_menck["unidade"], "m2")

    def test_modelo_novo_multiplos_nucleos_com_titulos_marcados(self):
        mensagem = """
RDO - Oeste 1
DATA: 02/04/2026

NÚCLEO: VILA MENCK
EQUIPE: ROGÉRIO
LOCAL: LOURENÇO BELOLI

*SERVIÇOS:*
APLICAÇÃO DE PAVIMENTO CBUQ: 18.75 m²

*OCORRÊNCIAS:*
EQUIPE REDUZIDA

NÚCLEO: BONANÇA
EQUIPE: MARIO
LOCAL: RUA DA PAZ DIVINA

*SERVIÇOS:*
LIGAÇÕES DE ESGOTO: 5 un
""".strip()
        parsed = self.parser.parse_text(mensagem, source_name="multi_nucleos_marcado.txt")
        self.assertIsNotNone(parsed)
        self.assertEqual(len(parsed["execucao"]), 2)
        nucleos = [str(row.get("nucleo", "") or "").strip() for row in parsed["execucao"]]
        self.assertIn("VILA MENCK", nucleos)
        self.assertIn("BONANÇA", nucleos)

if __name__ == "__main__":
    unittest.main()

