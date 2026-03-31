from __future__ import annotations

import csv
import json
import re
import tempfile
import unittest
from io import BytesIO
from pathlib import Path

from docx import Document

from app.routes.web_app import create_app


class WebAppTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)

        self.outputs_root = self.tmp_path / "saidas"
        self.master_dir = self.tmp_path / "BASE_MESTRA"
        self.history_file = self.tmp_path / "data" / "runtime" / "processing_history.csv"
        self.draft_dir = self.tmp_path / "data" / "drafts" / "web"
        self.nucleo_reference_file = self.tmp_path / "config" / "nucleo_reference.json"
        self.nucleo_reference_file.parent.mkdir(parents=True, exist_ok=True)
        self.nucleo_reference_file.write_text(
            json.dumps(
                {
                    "version": "test",
                    "nucleos": [
                        {
                            "nucleo": "Mississipi",
                            "municipio": "Carapicuiba",
                            "aliases": [
                                "Viela Mississipi",
                                "Mississipi - Caixa UMA",
                                "Mississipi — Caixa UMA",
                                "Mississipi - Esgoto",
                                "Mississipi — Esgoto",
                                "Viela 1 / Viela 7",
                                "Viela 1",
                                "Viela 7"
                            ],
                            "logradouros_padrao": [],
                            "equipes_padrao": [],
                        },
                        {
                            "nucleo": "Vila Dirce",
                            "municipio": "Carapicuiba",
                            "status": "inativo",
                            "aliases": [],
                            "logradouros_padrao": [],
                            "equipes_padrao": [],
                        }
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        self.app = create_app(
            {
                "TESTING": True,
                "SECRET_KEY": "test-secret",
                "OUTPUTS_ROOT": str(self.outputs_root),
                "MASTER_DIR": str(self.master_dir),
                "HISTORY_FILE": str(self.history_file),
                "DRAFT_DIR": str(self.draft_dir),
                "NUCLEO_REFERENCE_FILE": str(self.nucleo_reference_file),
            }
        )
        self.client = self.app.test_client()

    def tearDown(self):
        self.tmp.cleanup()

    def test_index_renderiza_copy_da_etapa_de_analise(self):
        resp = self.client.get("/")
        self.assertEqual(resp.status_code, 200)
        html = resp.data.decode("utf-8")
        self.assertIn("Analisar mensagem", html)
        self.assertIn("Ir para revisao", html)
        self.assertIn("Nesta etapa voce apenas analisa a mensagem", html)

    def test_review_renderiza_stepper_com_revisao_ativa(self):
        mensagem = """
RDO - 11/03/2026

EXECUCAO:
- 1 un hidrometro
""".strip()

        preview_resp = self.client.post(
            "/preview",
            data={
                "data": "11/03/2026",
                "nucleo": "Mississipi",
                "logradouro": "Viela 04",
                "municipio": "Carapicuiba",
                "equipe": "Equipe 03",
                "mensagem": mensagem,
            },
        )
        self.assertEqual(preview_resp.status_code, 200)
        html = preview_resp.data.decode("utf-8")
        self.assertIn('class="entry-stepper"', html)
        self.assertIn('data-entry-step="2"', html)
        self.assertIn("Etapa ativa", html)

    def test_web_flow_process_preview_generate_and_history(self):
        mensagem = """
RDO - 11/03/2026

NUCLEO: Mississipi
LOGRADOURO: Viela 04 - acesso pela Rua Sao Jorge
MUNICIPIO: Carapicuiba
EQUIPE: Equipe 03

EXECUCAO:
- 12 un hidrometro
- 3 un servico ultradesconhecido xyz

FRENTES:
- assentamento de rede

OCORRENCIAS:
- interferencia com rede de esgoto

OBS:
- trecho com acesso estreito
""".strip()

        preview_resp = self.client.post(
            "/preview",
            data={
                "data": "",
                "nucleo": "",
                "logradouro": "",
                "municipio": "",
                "equipe": "",
                "mensagem": mensagem,
            },
        )
        self.assertEqual(preview_resp.status_code, 200)
        html = preview_resp.data.decode("utf-8")
        self.assertIn("Revisao / previa", html)
        self.assertIn("Ajuste rapido de campos", html)
        self.assertIn("Confirmacao final para geracao", html)

        m = re.search(r'name="draft_id" value="([^"]+)"', html)
        self.assertIsNotNone(m)
        draft_id = m.group(1)

        gen_resp = self.client.post(
            "/generate",
            data={
                "draft_id": draft_id,
                "action": "generate",
                "data": "11/03/2026",
                "nucleo": "Mississipi",
                "logradouro": "Viela 04 - acesso pela Rua Sao Jorge",
                "municipio": "Carapicuiba",
                "equipe": "Equipe 03",
            },
        )
        self.assertEqual(gen_resp.status_code, 200)
        self.assertIn("Processamento concluido", gen_resp.data.decode("utf-8"))

        output_dirs = [p for p in self.outputs_root.iterdir() if p.is_dir()]
        self.assertGreaterEqual(len(output_dirs), 1)
        latest = sorted(output_dirs)[-1]

        self.assertTrue((latest / "execucao.csv").exists())
        self.assertTrue((latest / "frentes.csv").exists())
        self.assertTrue((latest / "ocorrencias.csv").exists())
        self.assertTrue((latest / "servico_nao_mapeado.csv").exists())
        self.assertTrue((latest / "base_gerencial.xlsx").exists())

        self.assertTrue((self.master_dir / "base_mestra_execucao.csv").exists())
        self.assertTrue((self.master_dir / "base_mestra_ocorrencias.csv").exists())
        self.assertTrue((self.master_dir / "base_mestra_frentes.csv").exists())
        self.assertTrue((self.master_dir / "base_gerencial.xlsx").exists())

        self.assertTrue(self.history_file.exists())
        with self.history_file.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        self.assertGreaterEqual(len(rows), 1)
        self.assertEqual(rows[-1]["status"], "sucesso")

        history_resp = self.client.get("/history?nm_days=365&nm_runs=50")
        self.assertEqual(history_resp.status_code, 200)
        history_html = history_resp.data.decode("utf-8")
        self.assertIn("Gestao de servicos nao mapeados", history_html)
        self.assertIn("Mais recorrentes", history_html)
        self.assertIn("ultradesconhecido", history_html.lower())

    def test_multiplos_nucleos_nao_sobrescreve_por_padrao(self):
        mensagem = """
RDO - AL 33 - Oeste 1
27/02/2026

Nucleo: Patrocinio (Equipe Sidomar - Tv. Rancharia / Rua Rio Tocantins)
Ramais agua: 3

Nucleo: Mississipi - Esgoto (Equipe Weslyn - Viela Mississipi)
Ramais esgoto: 3

Nucleo: Cerejas - Jandira (Equipe Reggis - Tv. Haitiana)
Caixas UMA: 16
Embutida: 7
Mureta: 9
""".strip()

        preview_resp = self.client.post(
            "/preview",
            data={
                "data": "",
                "nucleo": "",
                "logradouro": "",
                "municipio": "",
                "equipe": "",
                "mensagem": mensagem,
            },
        )
        self.assertEqual(preview_resp.status_code, 200)
        html = preview_resp.data.decode("utf-8")
        self.assertIn("Contexto detectado", html)

        m = re.search(r'name="draft_id" value="([^"]+)"', html)
        self.assertIsNotNone(m)
        draft_id = m.group(1)

        gen_resp = self.client.post(
            "/generate",
            data={
                "draft_id": draft_id,
                "action": "generate",
                "data": "27/02/2026",
                "nucleo": "Patrocinio",
                "logradouro": "Tv. Rancharia / Rua Rio Tocantins",
                "municipio": "Carapicuiba",
                "equipe": "Sidomar",
                # sem aplicar_todos: deve preservar frentes multi-nucleo
            },
        )
        self.assertEqual(gen_resp.status_code, 200)

        output_dirs = [p for p in self.outputs_root.iterdir() if p.is_dir()]
        latest = sorted(output_dirs)[-1]
        with (latest / "execucao.csv").open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))

        nucleos = {str(r.get("nucleo", "") or "").strip() for r in rows if str(r.get("nucleo", "") or "").strip()}
        self.assertGreaterEqual(len(nucleos), 2)
        self.assertIn("Patrocinio", nucleos)
        self.assertIn("Cerejas", nucleos)
        self.assertTrue(any(n != "Patrocinio" for n in nucleos))

    def test_generate_aplica_ajuste_por_nucleo(self):
        mensagem = """
RDO - AL 33 - Oeste 1
27/02/2026

Nucleo: Patrocinio (Equipe Sidomar - Tv. Rancharia / Rua Rio Tocantins)
Ramais agua: 3

Nucleo: Mississipi - Esgoto (Equipe Weslyn - Viela Mississipi)
Ramais esgoto: 3

Nucleo: Cerejas - Jandira (Equipe Reggis - Tv. Haitiana)
Caixas UMA: 16
Embutida: 7
Mureta: 9
""".strip()

        preview_resp = self.client.post(
            "/preview",
            data={
                "data": "",
                "nucleo": "",
                "logradouro": "",
                "municipio": "",
                "equipe": "",
                "mensagem": mensagem,
            },
        )
        self.assertEqual(preview_resp.status_code, 200)
        html = preview_resp.data.decode("utf-8")
        self.assertIn("Ajuste por nucleo", html)

        m = re.search(r'name="draft_id" value="([^"]+)"', html)
        self.assertIsNotNone(m)
        draft_id = m.group(1)

        gen_resp = self.client.post(
            "/generate",
            data={
                "draft_id": draft_id,
                "action": "generate",
                "data": "27/02/2026",
                "nucleo": "",
                "logradouro": "",
                "municipio": "",
                "equipe": "",
                "nucleo_map_count": "2",
                "nucleo_map_0_name": "Patrocinio",
                "nucleo_map_0_municipio": "Municipio Teste Patrocinio",
                "nucleo_map_0_logradouro": "Logradouro Teste Patrocinio",
                "nucleo_map_0_equipe": "Equipe Teste Patrocinio",
                "nucleo_map_1_name": "Cerejas",
                "nucleo_map_1_municipio": "Municipio Teste Cerejas",
                "nucleo_map_1_logradouro": "Logradouro Teste Cerejas",
                "nucleo_map_1_equipe": "Equipe Teste Cerejas",
            },
        )
        self.assertEqual(gen_resp.status_code, 200)

        output_dirs = [p for p in self.outputs_root.iterdir() if p.is_dir()]
        latest = sorted(output_dirs)[-1]
        with (latest / "execucao.csv").open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))

        pat_rows = [r for r in rows if str(r.get("nucleo", "") or "").strip() == "Patrocinio"]
        self.assertTrue(pat_rows)
        self.assertTrue(all(str(r.get("municipio", "") or "").strip() == "Municipio Teste Patrocinio" for r in pat_rows))
        self.assertTrue(all(str(r.get("logradouro", "") or "").strip() == "Logradouro Teste Patrocinio" for r in pat_rows))
        self.assertTrue(all(str(r.get("equipe", "") or "").strip() == "Equipe Teste Patrocinio" for r in pat_rows))

        cer_rows = [r for r in rows if str(r.get("nucleo", "") or "").strip() == "Cerejas"]
        self.assertTrue(cer_rows)
        self.assertTrue(all(str(r.get("municipio", "") or "").strip() == "Municipio Teste Cerejas" for r in cer_rows))
        self.assertTrue(all(str(r.get("logradouro", "") or "").strip() == "Logradouro Teste Cerejas" for r in cer_rows))
        self.assertTrue(all(str(r.get("equipe", "") or "").strip() == "Equipe Teste Cerejas" for r in cer_rows))


    def test_preview_autopreenche_municipio_por_nucleo(self):
        mensagem = """
RDO - 11/03/2026

NUCLEO: Mississipi
LOGRADOURO: Tv. do Alemao
MUNICIPIO: Carapicuiba
EQUIPE: Equipe 99 / Equipe 88

EXECUCAO:
- 1 un hidrometro
FRENTES:
- frente unica
OCORRENCIAS:
- sem ocorrencias
""".strip()

        resp = self.client.post(
            "/preview",
            data={
                "data": "11/03/2026",
                "nucleo": "Mississipi",
                "logradouro": "",
                "municipio": "",
                "equipe": "",
                "mensagem": mensagem,
            },
        )
        self.assertEqual(resp.status_code, 200)
        html = resp.data.decode("utf-8")
        self.assertIn('name="municipio" value="Carapicuiba"', html)
        self.assertIn("Cadastro de nucleo aplicado", html)

    def test_preview_alerta_municipio_divergente_por_nucleo(self):
        mensagem = """
RDO - 11/03/2026

EXECUCAO:
- 1 un hidrometro
""".strip()

        resp = self.client.post(
            "/preview",
            data={
                "data": "11/03/2026",
                "nucleo": "Mississipi",
                "logradouro": "Viela 04",
                "municipio": "Barueri",
                "equipe": "Equipe 03",
                "mensagem": mensagem,
            },
        )
        self.assertEqual(resp.status_code, 200)
        html = resp.data.decode("utf-8")
        self.assertIn("Checklist operacional por prioridade", html)
        self.assertIn("Municipio divergente do cadastro do nucleo", html)
        self.assertIn("Atencao (", html)

    def test_generate_persiste_municipio_oficial_do_cadastro_mestre(self):
        mensagem = """
RDO - 11/03/2026

NUCLEO: Mississipi
LOGRADOURO: Viela 04
MUNICIPIO: Barueri
EQUIPE: Equipe 03

EXECUCAO:
- 1 un hidrometro
""".strip()

        preview_resp = self.client.post(
            "/preview",
            data={
                "data": "",
                "nucleo": "",
                "logradouro": "",
                "municipio": "",
                "equipe": "",
                "mensagem": mensagem,
            },
        )
        self.assertEqual(preview_resp.status_code, 200)
        html = preview_resp.data.decode("utf-8")
        self.assertIn("Municipio reconciliado pelo cadastro mestre", html)

        m = re.search(r'name="draft_id" value="([^"]+)"', html)
        self.assertIsNotNone(m)
        draft_id = m.group(1)

        gen_resp = self.client.post(
            "/generate",
            data={
                "draft_id": draft_id,
                "action": "generate",
                "data": "11/03/2026",
                "nucleo": "Mississipi",
                "logradouro": "Viela 04",
                "municipio": "Barueri",
                "equipe": "Equipe 03",
            },
        )
        self.assertEqual(gen_resp.status_code, 200)

        latest = sorted([p for p in self.outputs_root.iterdir() if p.is_dir()])[-1]
        with (latest / "execucao.csv").open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        self.assertTrue(rows)
        self.assertEqual("Mississipi", rows[0]["nucleo"])
        self.assertEqual("Carapicuiba", rows[0]["municipio"])
        self.assertEqual("Mississipi", rows[0]["nucleo_oficial"])
        self.assertEqual("Carapicuiba", rows[0]["municipio_oficial"])
        self.assertEqual("Mississipi", rows[0]["nucleo_detectado_texto"])
        self.assertEqual("Barueri", rows[0]["municipio_detectado_texto"])

        with self.history_file.open("r", encoding="utf-8-sig", newline="") as f:
            history_rows = list(csv.DictReader(f))
        self.assertTrue(history_rows)
        self.assertEqual("Carapicuiba", history_rows[-1]["municipio"])
        self.assertEqual("Carapicuiba", history_rows[-1]["municipio_oficial"])

    def test_build_nucleo_groups_mantem_um_municipio_oficial_por_nucleo(self):
        service = self.app.config["PIPELINE_SERVICE"]
        parsed = {
            "frentes": [
                {
                    "nucleo": "Mississipi",
                    "municipio": "Barueri",
                    "municipio_oficial": "Carapicuiba",
                    "logradouro": "Viela 1",
                    "equipe": "Carlos / Weslyn",
                },
                {
                    "nucleo": "Mississipi",
                    "municipio": "Carapicuiba",
                    "municipio_oficial": "Carapicuiba",
                    "logradouro": "Viela 7",
                    "equipe": "Carlos / Xavier",
                },
            ],
            "execucao": [],
            "ocorrencias": [],
            "observacoes": [],
        }

        groups = service.build_nucleo_groups(parsed)
        self.assertEqual(1, len(groups))
        self.assertEqual("Mississipi", groups[0]["nucleo"])
        self.assertEqual("Carapicuiba", groups[0]["municipio"])
        self.assertEqual(["Carlos"], groups[0]["equipes"])

    def test_cadastro_mestre_nucleos_permite_criar_e_editar(self):
        resp = self.client.post(
            "/nucleos",
            data={
                "original_nucleo": "",
                "nucleo": "Savoy",
                "municipio": "Jandira",
                "status": "ativo",
                "aliases": "Savoi\nSavoy 01",
                "observacoes": "Cadastro criado via teste",
                "logradouro_principal": "Rua 1",
                "logradouros_padrao": "Rua 1\nRua 2",
                "equipes_padrao": "Equipe 01 / Equipe 02",
            },
            follow_redirects=True,
        )
        self.assertEqual(resp.status_code, 200)
        html = resp.data.decode("utf-8")
        self.assertIn("Cadastro salvo com sucesso.", html)
        self.assertIn("Savoy", html)
        self.assertIn("Jandira", html)

        with self.nucleo_reference_file.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        saved = next(item for item in payload["nucleos"] if item["nucleo"] == "Savoy")
        self.assertEqual("Jandira", saved["municipio"])
        self.assertEqual("ativo", saved["status"])
        self.assertIn("Savoi", saved["aliases"])
        self.assertEqual(["Equipe 01", "Equipe 02"], saved["equipes_padrao"])

        resp_edit = self.client.post(
            "/nucleos",
            data={
                "original_nucleo": "Savoy",
                "nucleo": "Savoy",
                "municipio": "Barueri",
                "status": "inativo",
                "aliases": "Savoy",
                "observacoes": "Ajustado",
                "logradouro_principal": "Rua central",
                "logradouros_padrao": "Rua central",
                "equipes_padrao": "Equipe 03",
            },
            follow_redirects=True,
        )
        self.assertEqual(resp_edit.status_code, 200)
        html_edit = resp_edit.data.decode("utf-8")
        self.assertIn("Barueri", html_edit)
        self.assertIn("inativo", html_edit)

    def test_generate_com_correcao_manual_de_nao_mapeado(self):
        mensagem = """
RDO - 11/03/2026

NUCLEO: Mississipi
LOGRADOURO: Viela 04
MUNICIPIO: Carapicuiba
EQUIPE: Equipe 03

EXECUCAO:
- 2 un servico estranhissimo xyz
""".strip()

        preview_resp = self.client.post(
            "/preview",
            data={
                "data": "",
                "nucleo": "",
                "logradouro": "",
                "municipio": "",
                "equipe": "",
                "mensagem": mensagem,
            },
        )
        self.assertEqual(preview_resp.status_code, 200)
        html = preview_resp.data.decode("utf-8")

        draft_match = re.search(r'name="draft_id" value="([^"]+)"', html)
        self.assertIsNotNone(draft_match)
        draft_id = draft_match.group(1)

        key_match = re.search(r'name="unmapped_fix_0_key" value="([^"]+)"', html)
        self.assertIsNotNone(key_match)
        unmapped_key = key_match.group(1)

        gen_resp = self.client.post(
            "/generate",
            data={
                "draft_id": draft_id,
                "action": "generate",
                "data": "11/03/2026",
                "nucleo": "Mississipi",
                "logradouro": "Viela 04",
                "municipio": "Carapicuiba",
                "equipe": "Equipe 03",
                "unmapped_fix_count": "1",
                "unmapped_fix_0_key": unmapped_key,
                "unmapped_fix_0_term": "servico estranhissimo xyz",
                "unmapped_fix_0_servico": "hidrometro",
            },
        )
        self.assertEqual(gen_resp.status_code, 200)
        self.assertIn("Processamento concluido", gen_resp.data.decode("utf-8"))

        output_dirs = [p for p in self.outputs_root.iterdir() if p.is_dir()]
        latest = sorted(output_dirs)[-1]

        with (latest / "execucao.csv").open("r", encoding="utf-8-sig", newline="") as f:
            exec_rows = list(csv.DictReader(f))
        self.assertTrue(exec_rows)
        self.assertTrue(any(str(r.get("servico_oficial", "") or "").strip() == "hidrometro" for r in exec_rows))
        self.assertTrue(any(str(r.get("categoria", "") or "").strip() == "hidrometro" for r in exec_rows))
        self.assertTrue(any(str(r.get("regra_disparada", "") or "").strip() == "correcao_manual" for r in exec_rows))

        with (latest / "servico_nao_mapeado.csv").open("r", encoding="utf-8-sig", newline="") as f:
            unmapped_rows = list(csv.DictReader(f))
        self.assertTrue(unmapped_rows)
        self.assertTrue(any(str(r.get("corrigido_manual", "") or "").strip() == "sim" for r in unmapped_rows))

    def test_review_e_resultado_mantem_apenas_primeira_equipe(self):
        mensagem = """
RDO - 11/03/2026

EXECUCAO:
- 1 un hidrometro
""".strip()

        preview_resp = self.client.post(
            "/preview",
            data={
                "data": "11/03/2026",
                "nucleo": "Mississipi",
                "logradouro": "Tv. do Alemao / Tv. do Bala; Tv. da Bete",
                "municipio": "",
                "equipe": "Equipe 01 / Equipe 02",
                "mensagem": mensagem,
            },
        )
        self.assertEqual(preview_resp.status_code, 200)
        html = preview_resp.data.decode("utf-8")
        self.assertIn("Logradouros separados", html)
        self.assertIn("Tv. do Alemao", html)
        self.assertIn("Tv. do Bala", html)
        self.assertIn("Equipe 01", html)
        self.assertNotIn("Equipe 02", html)

        m = re.search(r'name="draft_id" value="([^"]+)"', html)
        self.assertIsNotNone(m)
        draft_id = m.group(1)

        gen_resp = self.client.post(
            "/generate",
            data={
                "draft_id": draft_id,
                "action": "generate",
                "data": "11/03/2026",
                "nucleo": "Mississipi",
                "logradouro": "Tv. do Alemao / Tv. do Bala; Tv. da Bete",
                "municipio": "",
                "equipe": "Equipe 01 / Equipe 02",
            },
        )
        self.assertEqual(gen_resp.status_code, 200)
        result_html = gen_resp.data.decode("utf-8")
        self.assertIn("Dados principais consolidados", result_html)
        self.assertIn("Logradouros separados", result_html)
        self.assertIn("Tv. do Alemao", result_html)
        self.assertIn("Tv. da Bete", result_html)
        self.assertNotIn("Equipe 02", result_html)

    def test_history_filters_por_data_nucleo_equipe_status(self):
        service = self.app.config["PIPELINE_SERVICE"]
        service._append_history(
            {
                "processed_at": "10/03/2026 09:00:00",
                "obra_data": "10/03/2026",
                "nucleo": "NUCLEO_ALVO_X",
                "logradouro": "LOGRADOURO_ALVO_X",
                "municipio": "MUNICIPIO_ALVO_X",
                "equipe": "EQUIPE_ALVO_X",
                "status": "sucesso",
                "output_dir": str((self.tmp_path / "saidas" / "saida_alvo").resolve()),
                "base_gerencial_path": str((self.tmp_path / "saidas" / "saida_alvo" / "base_gerencial.xlsx").resolve()),
                "master_dir": str(self.master_dir),
                "nao_mapeados": "2",
                "alertas": "alerta especifico alvo | alerta 2 alvo",
                "mensagem": "mensagem alvo",
            }
        )
        service._append_history(
            {
                "processed_at": "11/03/2026 10:00:00",
                "obra_data": "11/03/2026",
                "nucleo": "NUCLEO_OUTRO_Y",
                "logradouro": "LOGRADOURO_OUTRO_Y",
                "municipio": "MUNICIPIO_OUTRO_Y",
                "equipe": "EQUIPE_OUTRO_Y",
                "status": "erro: falha xyz",
                "output_dir": str((self.tmp_path / "saidas" / "saida_outro").resolve()),
                "base_gerencial_path": str((self.tmp_path / "saidas" / "saida_outro" / "base_gerencial.xlsx").resolve()),
                "master_dir": str(self.master_dir),
                "nao_mapeados": "0",
                "alertas": "",
                "mensagem": "mensagem outro",
            }
        )

        resp = self.client.get(
            "/history?obra_data=10/03/2026&nucleo=nucleo_alvo_x&municipio=municipio_alvo_x&equipe=equipe_alvo_x&status=sucesso&alertas=com_alerta&processed_from=2026-03-10&processed_to=2026-03-10"
        )
        self.assertEqual(resp.status_code, 200)
        html = resp.data.decode("utf-8")
        self.assertIn("NUCLEO_ALVO_X", html)
        self.assertIn("Municipio Alvo X", html)
        self.assertIn("EQUIPE_ALVO_X", html)
        self.assertNotIn("NUCLEO_OUTRO_Y", html)
        self.assertNotIn("Municipio Outro Y", html)
        self.assertIn("alerta especifico alvo", html)
        self.assertIn("Abrir pasta de saida", html)
        self.assertIn("Abrir base gerencial", html)

    def test_history_exibe_linhas_antigas_e_erros_sem_arquivo(self):
        service = self.app.config["PIPELINE_SERVICE"]
        service._append_history(
            {
                "processed_at": "10/03/2026 09:00:00",
                "obra_data": "10/03/2026",
                "nucleo": "NUCLEO_HIST_OLD_OK",
                "logradouro": "LOG_OLD_OK",
                "municipio": "MUNICIPIO_OLD_OK",
                "equipe": "EQUIPE_OLD_OK",
                "status": "sucesso",
                "output_dir": "",
                "base_gerencial_path": "",
                "master_dir": str(self.master_dir),
                "nao_mapeados": "0",
                "alertas": "",
                "mensagem": "registro antigo sem arquivo",
            }
        )
        service._append_history(
            {
                "processed_at": "11/03/2026 10:00:00",
                "obra_data": "11/03/2026",
                "nucleo": "NUCLEO_HIST_OLD_ERRO",
                "logradouro": "LOG_OLD_ERRO",
                "municipio": "MUNICIPIO_OLD_ERRO",
                "equipe": "EQUIPE_OLD_ERRO",
                "status": "erro: falha de validacao",
                "output_dir": "",
                "base_gerencial_path": "",
                "master_dir": str(self.master_dir),
                "nao_mapeados": "0",
                "alertas": "erro de parse",
                "mensagem": "registro antigo com erro sem arquivo",
            }
        )

        resp = self.client.get("/history")
        self.assertEqual(resp.status_code, 200)
        html = resp.data.decode("utf-8")
        self.assertIn("NUCLEO_HIST_OLD_OK", html)
        self.assertIn("NUCLEO_HIST_OLD_ERRO", html)
        self.assertIn("Sem caminhos dispon", html)

    def test_history_filtro_status_erro_funciona_com_base_completa(self):
        service = self.app.config["PIPELINE_SERVICE"]
        service._append_history(
            {
                "processed_at": "10/03/2026 09:00:00",
                "obra_data": "10/03/2026",
                "nucleo": "NUCLEO_OK_STATUS",
                "logradouro": "LOG_OK_STATUS",
                "municipio": "MUNICIPIO_OK_STATUS",
                "equipe": "EQUIPE_OK_STATUS",
                "status": "sucesso",
                "output_dir": "",
                "base_gerencial_path": "",
                "master_dir": str(self.master_dir),
                "nao_mapeados": "0",
                "alertas": "",
                "mensagem": "ok",
            }
        )
        service._append_history(
            {
                "processed_at": "11/03/2026 10:00:00",
                "obra_data": "11/03/2026",
                "nucleo": "NUCLEO_ERRO_STATUS",
                "logradouro": "LOG_ERRO_STATUS",
                "municipio": "MUNICIPIO_ERRO_STATUS",
                "equipe": "EQUIPE_ERRO_STATUS",
                "status": "erro: falha no parser",
                "output_dir": "",
                "base_gerencial_path": "",
                "master_dir": str(self.master_dir),
                "nao_mapeados": "1",
                "alertas": "falha",
                "mensagem": "erro",
            }
        )

        resp = self.client.get("/history?status=erro")
        self.assertEqual(resp.status_code, 200)
        html = resp.data.decode("utf-8")
        self.assertIn("NUCLEO_ERRO_STATUS", html)
        self.assertNotIn("NUCLEO_OK_STATUS", html)

    def test_history_persiste_apenas_primeira_equipe(self):
        service = self.app.config["PIPELINE_SERVICE"]
        service._append_history(
            {
                "processed_at": "10/03/2026 13:10:00",
                "obra_data": "10/03/2026",
                "nucleo": "MISSISSIPI",
                "logradouro": "Viela 6",
                "municipio": "CARAPICUIBA",
                "equipe": "Equipe 01 / Equipe 02",
                "status": "sucesso",
                "output_dir": str((self.tmp_path / "saidas" / "saida_hist").resolve()),
                "base_gerencial_path": "",
                "master_dir": str(self.master_dir),
                "nao_mapeados": "0",
                "alertas": "",
                "mensagem": "msg",
            }
        )

        with self.history_file.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        self.assertTrue(rows)
        self.assertEqual(rows[-1].get("equipe"), "Equipe 01")

    def test_history_gera_csv_candidatos_alias(self):
        output_dir = self.outputs_root / "saida_nm_001"
        output_dir.mkdir(parents=True, exist_ok=True)

        headers = [
            "data",
            "mensagem_original",
            "servico_bruto",
            "servico_normalizado",
            "quantidade",
            "unidade",
            "nucleo",
            "logradouro",
            "municipio",
            "equipe",
            "sugestao_categoria",
            "regra_disparada",
            "corrigido_manual",
            "servico_corrigido_manual",
        ]
        rows = [
            {
                "data": "10/03/2026",
                "mensagem_original": "- 2 un termo novo x",
                "servico_bruto": "Termo novo X",
                "servico_normalizado": "termo novo x",
                "quantidade": "2",
                "unidade": "un",
                "nucleo": "Mississipi",
                "logradouro": "Viela 1",
                "municipio": "Carapicuiba",
                "equipe": "Equipe 01",
                "sugestao_categoria": "hidrometro",
                "regra_disparada": "nao_mapeado",
                "corrigido_manual": "",
                "servico_corrigido_manual": "",
            },
            {
                "data": "10/03/2026",
                "mensagem_original": "- 1 un termo novo x",
                "servico_bruto": "Termo novo X",
                "servico_normalizado": "termo novo x",
                "quantidade": "1",
                "unidade": "un",
                "nucleo": "Mississipi",
                "logradouro": "Viela 2",
                "municipio": "Carapicuiba",
                "equipe": "Equipe 02",
                "sugestao_categoria": "hidrometro",
                "regra_disparada": "keyword_hidromet",
                "corrigido_manual": "sim",
                "servico_corrigido_manual": "hidrometro",
            },
        ]
        with (output_dir / "servico_nao_mapeado.csv").open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)

        service = self.app.config["PIPELINE_SERVICE"]
        service._append_history(
            {
                "processed_at": "12/03/2026 10:00:00",
                "obra_data": "10/03/2026",
                "nucleo": "Mississipi",
                "logradouro": "Viela 1",
                "municipio": "Carapicuiba",
                "equipe": "Equipe 01",
                "status": "sucesso",
                "output_dir": str(output_dir.resolve()),
                "base_gerencial_path": str((output_dir / "base_gerencial.xlsx").resolve()),
                "master_dir": str(self.master_dir),
                "nao_mapeados": "2",
                "alertas": "servico nao mapeado",
                "mensagem": "msg",
            }
        )

        resp = self.client.get("/history?nm_days=365&nm_runs=50")
        self.assertEqual(resp.status_code, 200)
        html = resp.data.decode("utf-8")
        self.assertIn("Candidatos a alias gerados", html)

        candidates_file = service.unmapped_candidates_file
        self.assertTrue(candidates_file.exists())
        with candidates_file.open("r", encoding="utf-8-sig", newline="") as f:
            candidates = list(csv.DictReader(f))
        self.assertTrue(any(str(r.get("termo_candidato", "") or "").strip() == "termo novo x" for r in candidates))


    def test_history_page_loads(self):
        resp = self.client.get("/history")
        self.assertEqual(resp.status_code, 200)
        html = resp.data.decode("utf-8")
        self.assertIn("Historico", html)
        self.assertIn("Gestao de servicos nao mapeados", html)

    def test_servicos_page_returns_controlled_message_when_db_is_unavailable(self):
        resp = self.client.get("/servicos")
        self.assertEqual(resp.status_code, 503)
        html = resp.data.decode("utf-8")
        self.assertIn("Cadastro de servicos", html)
        self.assertIn("indisponivel", html.lower())




    def test_institucional_page_loads(self):
        resp = self.client.get("/institucional")
        self.assertEqual(resp.status_code, 200)
        html = resp.data.decode("utf-8")
        self.assertIn("Relatório institucional", html)
        self.assertIn("Filtros de geração", html)
        self.assertIn("Resumo executivo", html)
        self.assertIn("Prévia técnica interna (revisão)", html)

    def test_gerencial_page_loads_com_blocos_executivos(self):
        resp = self.client.get("/gerencial")
        self.assertEqual(resp.status_code, 200)
        html = resp.data.decode("utf-8")
        self.assertIn("Painel gerencial", html)
        self.assertIn("Gráficos executivos", html)
        self.assertIn("Evolução de processamentos por período", html)
        self.assertIn("Mapeado x não mapeado", html)

    def test_gerencial_consolida_variacoes_de_mississipi_e_ignora_vila_dirce(self):
        output_dir = self.outputs_root / "saida_institucional_demo"
        output_dir.mkdir(parents=True, exist_ok=True)

        exec_headers = [
            "id_item", "id_frente", "data_referencia", "nucleo", "nucleo_oficial", "equipe", "municipio", "municipio_oficial", "servico_oficial", "item_normalizado", "categoria_item", "quantidade", "unidade"
        ]
        exec_rows = [
            {
                "id_item": "I001",
                "id_frente": "F001",
                "data_referencia": "10/03/2026",
                "nucleo": "Mississipi — Caixa UMA",
                "equipe": "Equipes Weslyn",
                "municipio": "Carapicuíba",
                "servico_oficial": "caixa_uma",
                "item_normalizado": "caixa_uma",
                "categoria_item": "caixa_uma",
                "quantidade": "10",
                "unidade": "un",
            },
            {
                "id_item": "I002",
                "id_frente": "F001",
                "data_referencia": "10/03/2026",
                "nucleo": "Missisipi",
                "equipe": "Weslyn",
                "municipio": "Carapicuiba",
                "servico_oficial": "ramal_agua",
                "item_normalizado": "ramal_agua",
                "categoria_item": "ramal",
                "quantidade": "6",
                "unidade": "un",
            },
            {
                "id_item": "I003",
                "id_frente": "F001",
                "data_referencia": "10/03/2026",
                "nucleo": "Viela 1 / Viela 7",
                "equipe": "Carlos / Wesley",
                "municipio": "Carapicuiba",
                "servico_oficial": "rede_agua",
                "item_normalizado": "rede_agua",
                "categoria_item": "rede",
                "quantidade": "15",
                "unidade": "m",
            },
            {
                "id_item": "I004",
                "id_frente": "F002",
                "data_referencia": "10/03/2026",
                "nucleo": "Vila Dirce",
                "equipe": "Equipe Ezequiel",
                "municipio": "Carapicuiba",
                "servico_oficial": "recomposicao_asfalto",
                "item_normalizado": "recomposicao_asfalto",
                "categoria_item": "recomposicao",
                "quantidade": "11",
                "unidade": "vala",
            },
        ]
        with (output_dir / "execucao.csv").open(
            "w", encoding="utf-8-sig", newline=""
        ) as f:
            writer = csv.DictWriter(f, fieldnames=exec_headers)
            writer.writeheader()
            writer.writerows(exec_rows)

        frentes_headers = [
            "id_frente",
            "data_referencia",
            "nucleo",
            "equipe",
            "status_frente",
        ]
        frentes_rows = [
            {
                "id_frente": "F001",
                "data_referencia": "10/03/2026",
                "nucleo": "Mississipi — Esgoto",
                "equipe": "Weslyn",
                "status_frente": "com_producao",
            }
        ]
        with (output_dir / "frentes.csv").open(
            "w", encoding="utf-8-sig", newline=""
        ) as f:
            writer = csv.DictWriter(f, fieldnames=frentes_headers)
            writer.writeheader()
            writer.writerows(frentes_rows)

        ocorr_headers = [
            "id_ocorrencia",
            "data_referencia",
            "nucleo",
            "equipe",
            "municipio",
            "tipo_ocorrencia",
        ]
        ocorr_rows = [
            {
                "id_ocorrencia": "O001",
                "data_referencia": "10/03/2026",
                "nucleo": "Mississipi — Esgoto",
                "equipe": "Weslyn",
                "municipio": "Carapicuiba",
                "tipo_ocorrencia": "interferencia",
            }
        ]
        with (output_dir / "ocorrencias.csv").open(
            "w", encoding="utf-8-sig", newline=""
        ) as f:
            writer = csv.DictWriter(f, fieldnames=ocorr_headers)
            writer.writeheader()
            writer.writerows(ocorr_rows)

        service = self.app.config["PIPELINE_SERVICE"]
        service._append_history(
            {
                "processed_at": "12/03/2026 10:00:00",
                "obra_data": "10/03/2026",
                "nucleo": "Viela 1 / Viela 7",
                "logradouro": "Viela 1 / Viela 7",
                "municipio": "Carapicuíba / Carapicuiba",
                "equipe": "Equipes Weslyn / Carlos",
                "status": "sucesso",
                "output_dir": str(output_dir.resolve()),
                "base_gerencial_path": str((output_dir / "base_gerencial.xlsx").resolve()),
                "master_dir": str(self.master_dir),
                "nao_mapeados": "0",
                "alertas": "",
                "mensagem": "msg consolidacao",
            }
        )

        dashboard = service.build_management_layer(
            {"obra_from": "2026-03-01", "obra_to": "2026-03-31", "top_n": 20}
        )

        ranking_nucleos = [
            str(r.get("nucleo", "") or "")
            for r in dashboard.get("ranking_nucleos_processamentos", [])
        ]
        self.assertIn("Mississipi", ranking_nucleos)
        self.assertNotIn("Mississipi — Caixa UMA", ranking_nucleos)
        self.assertNotIn("Mississipi — Esgoto", ranking_nucleos)
        self.assertNotIn("Viela 1 / Viela 7", ranking_nucleos)
        self.assertNotIn("Vila Dirce", ranking_nucleos)

        ranking_municipios = [
            str(r.get("municipio", "") or "")
            for r in dashboard.get("ranking_municipios_processamentos", [])
        ]
        self.assertIn("Carapicuiba", ranking_municipios)
        self.assertEqual(
            1,
            sum(1 for m in ranking_municipios if re.sub(r"[^a-z]", "", m.lower()) == "carapicuiba"),
        )

        ranking_equipes = [
            str(r.get("equipe", "") or "")
            for r in dashboard.get("ranking_equipes_processamentos", [])
        ]
        self.assertIn("Weslyn", ranking_equipes)
        self.assertNotIn("Equipes Weslyn", ranking_equipes)
        self.assertNotIn("Wesley", ranking_equipes)

    def test_institucional_gera_minuta_e_exporta_html_docx(self):
        output_dir = self.outputs_root / "saida_institucional_demo"
        output_dir.mkdir(parents=True, exist_ok=True)

        exec_headers = [
            "id_item",
            "id_frente",
            "data_referencia",
            "nucleo",
            "nucleo_oficial",
            "equipe",
            "municipio",
            "municipio_oficial",
            "servico_oficial",
            "item_normalizado",
            "categoria_item",
            "quantidade",
            "unidade",
        ]
        exec_rows = [
            {
                "id_item": "I001",
                "id_frente": "F001",
                "data_referencia": "10/03/2026",
                "nucleo": "Mississipi",
                "nucleo_oficial": "Mississipi",
                "equipe": "Weslyn / Carlos",
                "municipio": "Barueri",
                "municipio_oficial": "Carapicuiba",
                "servico_oficial": "ramal_agua",
                "item_normalizado": "ramal_agua",
                "categoria_item": "ramal",
                "quantidade": "6",
                "unidade": "un",
            },
            {
                "id_item": "I002",
                "id_frente": "F001",
                "data_referencia": "10/03/2026",
                "nucleo": "Mississipi",
                "nucleo_oficial": "Mississipi",
                "equipe": "Weslyn",
                "municipio": "Carapicuiba",
                "municipio_oficial": "Carapicuiba",
                "servico_oficial": "rede_agua",
                "item_normalizado": "rede_agua",
                "categoria_item": "rede",
                "quantidade": "15",
                "unidade": "m",
            },
        ]
        with (output_dir / "execucao.csv").open(
            "w", encoding="utf-8-sig", newline=""
        ) as f:
            writer = csv.DictWriter(f, fieldnames=exec_headers)
            writer.writeheader()
            writer.writerows(exec_rows)

        frentes_headers = [
            "id_frente",
            "data_referencia",
            "nucleo",
            "nucleo_oficial",
            "equipe",
            "status_frente",
        ]
        frentes_rows = [
            {
                "id_frente": "F001",
                "data_referencia": "10/03/2026",
                "nucleo": "Mississipi",
                "nucleo_oficial": "Mississipi",
                "equipe": "Weslyn",
                "status_frente": "com_producao",
            }
        ]
        with (output_dir / "frentes.csv").open(
            "w", encoding="utf-8-sig", newline=""
        ) as f:
            writer = csv.DictWriter(f, fieldnames=frentes_headers)
            writer.writeheader()
            writer.writerows(frentes_rows)

        ocorr_headers = [
            "id_ocorrencia",
            "data_referencia",
            "nucleo",
            "nucleo_oficial",
            "equipe",
            "municipio",
            "municipio_oficial",
            "tipo_ocorrencia",
        ]
        ocorr_rows = [
            {
                "id_ocorrencia": "O001",
                "data_referencia": "10/03/2026",
                "nucleo": "Mississipi",
                "nucleo_oficial": "Mississipi",
                "equipe": "Weslyn / Carlos",
                "municipio": "Barueri",
                "municipio_oficial": "Carapicuiba",
                "tipo_ocorrencia": "interferencia",
            }
        ]
        with (output_dir / "ocorrencias.csv").open(
            "w", encoding="utf-8-sig", newline=""
        ) as f:
            writer = csv.DictWriter(f, fieldnames=ocorr_headers)
            writer.writeheader()
            writer.writerows(ocorr_rows)

        service = self.app.config["PIPELINE_SERVICE"]
        service._append_history(
            {
                "processed_at": "12/03/2026 10:00:00",
                "obra_data": "10/03/2026",
                "nucleo": "Mississipi",
                "nucleo_oficial": "Mississipi",
                "logradouro": "Viela 1",
                "municipio": "Barueri",
                "municipio_oficial": "Carapicuiba",
                "equipe": "Weslyn / Carlos",
                "status": "sucesso",
                "output_dir": str(output_dir.resolve()),
                "base_gerencial_path": str((output_dir / "base_gerencial.xlsx").resolve()),
                "master_dir": str(self.master_dir),
                "nao_mapeados": "0",
                "alertas": "",
                "mensagem": "msg institucional",
            }
        )

        resp = self.client.get("/institucional?obra_from=2026-03-01&obra_to=2026-03-31&top_n=20")
        self.assertEqual(resp.status_code, 200)
        html = resp.data.decode("utf-8")
        self.assertIn("Relatório institucional", html)
        self.assertIn("Mississipi", html)
        self.assertRegex(html, r"Carapicu[ií]ba")
        self.assertNotIn("processamento(s)", html)
        self.assertNotIn("registro(s)", html)

        resp_html = self.client.get(
            "/institucional/export?obra_from=2026-03-01&obra_to=2026-03-31&top_n=20&formato=html"
        )
        self.assertEqual(resp_html.status_code, 200)
        export_html = resp_html.data.decode("utf-8")
        self.assertIn("Relatório Institucional de Acompanhamento Operacional", export_html)
        self.assertRegex(export_html, r"Carapicu[ií]ba")
        self.assertNotIn("processamento(s)", export_html)
        self.assertNotIn("registro(s)", export_html)
        self.assertNotIn("Prévia técnica interna", export_html)

        resp_docx = self.client.get(
            "/institucional/export?obra_from=2026-03-01&obra_to=2026-03-31&top_n=20&formato=docx"
        )
        self.assertEqual(resp_docx.status_code, 200)
        document = Document(BytesIO(resp_docx.data))
        docx_text = "\n".join(p.text for p in document.paragraphs)
        self.assertIn("Relatório Institucional de Acompanhamento Operacional", docx_text)
        self.assertRegex(docx_text, r"Carapicu[ií]ba")
        self.assertNotIn("processamento(s)", docx_text)
        self.assertNotIn("registro(s)", docx_text)
        report = self.app.config["PIPELINE_SERVICE"].build_institutional_report({"obra_from": "2026-03-01", "obra_to": "2026-03-31", "top_n": 20})
        analise = list(report.get("analise_por_nucleo", []) or [])
        self.assertTrue(analise)
        self.assertEqual("Carapicuiba", analise[0].get("municipio"))
        self.assertNotIn("municipios", analise[0])

    def test_gerencial_separa_multiplos_com_virgula_no_historico(self):
        service = self.app.config["PIPELINE_SERVICE"]
        service._append_history(
            {
                "processed_at": "13/03/2026 11:00:00",
                "obra_data": "12/03/2026",
                "nucleo": "Multiplos (2): Mississipi, Savoy",
                "logradouro": "Multiplos (2): Viela 6, Viela 7",
                "municipio": "Multiplos (2): Carapicuiba, Jandira",
                "equipe": "Multiplos (2): Carlos, Weslyn",
                "status": "erro: validacao",
                "output_dir": str((self.tmp_path / "nao_existe").resolve()),
                "base_gerencial_path": "",
                "master_dir": str(self.master_dir),
                "nao_mapeados": "0",
                "alertas": "",
                "mensagem": "msg multiplo",
            }
        )

        history_rows = service.read_history(limit=5)
        self.assertTrue(history_rows)
        self.assertEqual("Mississipi / Savoy", history_rows[0]["nucleo"])
        self.assertEqual("", history_rows[0]["municipio"])
        self.assertNotIn("Multiplos", history_rows[0]["nucleo"])
        self.assertNotIn("Multiplos", history_rows[0]["municipio"])

        dashboard = service.build_management_layer({"top_n": 20})

        ranking_nucleos = [
            str(r.get("nucleo", "") or "")
            for r in dashboard.get("ranking_nucleos_processamentos", [])
        ]
        self.assertIn("Mississipi", ranking_nucleos)
        self.assertIn("Savoy", ranking_nucleos)
        self.assertNotIn("Mississipi, Savoy", ranking_nucleos)

        ranking_municipios = [
            str(r.get("municipio", "") or "")
            for r in dashboard.get("ranking_municipios_processamentos", [])
        ]
        self.assertNotIn("Carapicuiba", ranking_municipios)
        self.assertNotIn("Jandira", ranking_municipios)
        self.assertNotIn("Carapicuiba, Jandira", ranking_municipios)

        ranking_equipes = [
            str(r.get("equipe", "") or "")
            for r in dashboard.get("ranking_equipes_processamentos", [])
        ]
        self.assertIn("Carlos", ranking_equipes)
        self.assertNotIn("Weslyn", ranking_equipes)
        self.assertNotIn("Carlos, Weslyn", ranking_equipes)

    def test_gerencial_separa_nucleo_com_virgula_no_execucao_csv(self):
        output_dir = self.outputs_root / "saida_nucleo_virgula_csv"
        output_dir.mkdir(parents=True, exist_ok=True)

        exec_headers = [
            "id_item",
            "id_frente",
            "data_referencia",
            "nucleo",
            "equipe",
            "municipio",
            "servico_oficial",
            "item_normalizado",
            "categoria_item",
            "quantidade",
            "unidade",
        ]
        exec_rows = [
            {
                "id_item": "I001",
                "id_frente": "F001",
                "data_referencia": "10/03/2026",
                "nucleo": "Mississipi, Savoy",
                "equipe": "Weslyn",
                "municipio": "Carapicuiba",
                "servico_oficial": "rede_agua",
                "item_normalizado": "rede_agua",
                "categoria_item": "rede",
                "quantidade": "20",
                "unidade": "m",
            }
        ]
        with (output_dir / "execucao.csv").open(
            "w", encoding="utf-8-sig", newline=""
        ) as f:
            writer = csv.DictWriter(f, fieldnames=exec_headers)
            writer.writeheader()
            writer.writerows(exec_rows)

        service = self.app.config["PIPELINE_SERVICE"]
        service._append_history(
            {
                "processed_at": "14/03/2026 08:00:00",
                "obra_data": "10/03/2026",
                "nucleo": "Mississipi, Savoy",
                "logradouro": "Viela 6",
                "municipio": "Carapicuiba",
                "equipe": "Weslyn",
                "status": "sucesso",
                "output_dir": str(output_dir.resolve()),
                "base_gerencial_path": "",
                "master_dir": str(self.master_dir),
                "nao_mapeados": "0",
                "alertas": "",
                "mensagem": "msg nucleo virgula",
            }
        )

        dashboard = service.build_management_layer({"top_n": 20})
        ranking_nucleos = [
            str(r.get("nucleo", "") or "")
            for r in dashboard.get("ranking_nucleos_processamentos", [])
        ]
        self.assertIn("Mississipi", ranking_nucleos)
        self.assertIn("Savoy", ranking_nucleos)
        self.assertNotIn("Mississipi, Savoy", ranking_nucleos)
if __name__ == "__main__":
    unittest.main()



























