from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from master_builder import update_master_from_output


class MasterBuilderTests(unittest.TestCase):
    def _write_csv(self, path: Path, rows: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    def _write_output_pack(self, output_dir: Path, date_ref: str, origem: str, quantidade: str) -> None:
        exec_rows = [
            {
                "data_referencia": date_ref,
                "contrato": "CTR-01",
                "programa": "OBRA",
                "nucleo": "Mississipi",
                "equipe": "Equipe 03",
                "logradouro": "Rua Sao Jorge",
                "item_normalizado": "rede_agua",
                "quantidade": quantidade,
                "unidade": "m",
                "material": "PVC",
                "especificacao": "50mm",
                "arquivo_origem": origem,
                "categoria_item": "rede",
                "servico_bruto": "rede de agua",
                "servico_normalizado": "rede de agua",
                "servico_oficial": "rede_agua",
                "categoria": "rede",
            }
        ]
        occ_rows = [
            {
                "data_referencia": date_ref,
                "contrato": "CTR-01",
                "programa": "OBRA",
                "nucleo": "Mississipi",
                "equipe": "Equipe 03",
                "tipo_ocorrencia": "interferencia",
                "descricao": "interferencia com rede de esgoto",
                "arquivo_origem": origem,
            }
        ]
        front_rows = [
            {
                "data_referencia": date_ref,
                "contrato": "CTR-01",
                "programa": "OBRA",
                "nucleo": "Mississipi",
                "equipe": "Equipe 03",
                "logradouro": "Rua Sao Jorge",
                "status_frente": "ativa",
                "arquivo_origem": origem,
            }
        ]

        self._write_csv(output_dir / "execucao.csv", exec_rows)
        self._write_csv(output_dir / "ocorrencias.csv", occ_rows)
        self._write_csv(output_dir / "frentes.csv", front_rows)

    def _count_rows(self, csv_path: Path) -> int:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            return len(list(csv.DictReader(f)))

    def test_update_master_acumula_datas_sem_duplicar(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            master_dir = tmp / "BASE_MESTRA"

            out_1 = tmp / "saida_1"
            self._write_output_pack(out_1, "01/03/2026", "msg_01.txt", "10")
            stats_1 = update_master_from_output(out_1, master_dir)
            self.assertEqual(stats_1, {"execucao": 1, "ocorrencias": 1, "frentes": 1})

            out_1_dup = tmp / "saida_1_dup"
            self._write_output_pack(out_1_dup, "01/03/2026", "msg_01.txt", "10")
            stats_dup = update_master_from_output(out_1_dup, master_dir)
            self.assertEqual(stats_dup, {"execucao": 0, "ocorrencias": 0, "frentes": 0})

            out_2 = tmp / "saida_2"
            self._write_output_pack(out_2, "02/03/2026", "msg_02.txt", "20")
            stats_2 = update_master_from_output(out_2, master_dir)
            self.assertEqual(stats_2, {"execucao": 1, "ocorrencias": 1, "frentes": 1})

            self.assertEqual(self._count_rows(master_dir / "base_mestra_execucao.csv"), 2)
            self.assertEqual(self._count_rows(master_dir / "base_mestra_ocorrencias.csv"), 2)
            self.assertEqual(self._count_rows(master_dir / "base_mestra_frentes.csv"), 2)

            self.assertTrue((master_dir / "execucao.csv").exists())
            self.assertTrue((master_dir / "ocorrencias.csv").exists())
            self.assertTrue((master_dir / "frentes.csv").exists())
            self.assertTrue((master_dir / "base_gerencial.xlsx").exists())

    def test_update_master_rejeita_pasta_invalida(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            with self.assertRaises(FileNotFoundError):
                update_master_from_output(tmp / "nao_existe", tmp / "BASE_MESTRA")

    def test_update_master_tolera_evolucao_de_colunas(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            master_dir = tmp / "BASE_MESTRA"
            out = tmp / "saida_web"

            # Base mestra antiga (sem campos novos como municipio/data)
            self._write_csv(
                master_dir / "base_mestra_frentes.csv",
                [
                    {
                        "data_referencia": "08/03/2026",
                        "contrato": "CTR-01",
                        "programa": "OBRA",
                        "nucleo": "Santa Terezinha",
                        "equipe": "Sidney",
                        "logradouro": "Av. Deputado Emilio Carlos",
                        "status_frente": "ativa",
                        "arquivo_origem": "antigo.txt",
                    }
                ],
            )

            # Saida nova com campos adicionais da camada atual (municipio/data)
            self._write_csv(
                out / "frentes.csv",
                [
                    {
                        "data_referencia": "09/03/2026",
                        "data": "09/03/2026",
                        "contrato": "CTR-01",
                        "programa": "OBRA",
                        "nucleo": "Santa Terezinha",
                        "equipe": "Sidney",
                        "logradouro": "Av. Deputado Emilio Carlos",
                        "municipio": "Carapicuiba",
                        "status_frente": "ativa",
                        "arquivo_origem": "novo.txt",
                    }
                ],
            )
            self._write_csv(
                out / "execucao.csv",
                [
                    {
                        "data_referencia": "09/03/2026",
                        "contrato": "CTR-01",
                        "programa": "OBRA",
                        "nucleo": "Santa Terezinha",
                        "equipe": "Sidney",
                        "logradouro": "Av. Deputado Emilio Carlos",
                        "item_normalizado": "rede_agua",
                        "quantidade": "5",
                        "unidade": "m",
                        "material": "PVC",
                        "especificacao": "50mm",
                        "arquivo_origem": "novo.txt",
                    }
                ],
            )
            self._write_csv(
                out / "ocorrencias.csv",
                [
                    {
                        "data_referencia": "09/03/2026",
                        "contrato": "CTR-01",
                        "programa": "OBRA",
                        "nucleo": "Santa Terezinha",
                        "equipe": "Sidney",
                        "tipo_ocorrencia": "interferencia",
                        "descricao": "interferencia com rede de esgoto",
                        "arquivo_origem": "novo.txt",
                    }
                ],
            )

            stats = update_master_from_output(out, master_dir)
            self.assertEqual(stats["frentes"], 1)

            with (master_dir / "base_mestra_frentes.csv").open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames or []
                rows = list(reader)

            self.assertIn("municipio", headers)
            self.assertIn("data", headers)
            self.assertEqual(len(rows), 2)

    def test_update_master_normaliza_equipe_existente_e_nova_para_primeira(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            master_dir = tmp / "BASE_MESTRA"
            out = tmp / "saida"

            self._write_csv(
                master_dir / "base_mestra_execucao.csv",
                [
                    {
                        "data_referencia": "10/03/2026",
                        "contrato": "CTR-01",
                        "programa": "OBRA",
                        "nucleo": "Mississipi",
                        "equipe": "Equipe 01 / Equipe 02",
                        "logradouro": "Viela 6",
                        "item_normalizado": "ramal_agua",
                        "quantidade": "2",
                        "unidade": "un",
                        "material": "",
                        "especificacao": "",
                        "arquivo_origem": "origem.txt",
                    }
                ],
            )
            self._write_csv(
                master_dir / "base_mestra_frentes.csv",
                [
                    {
                        "data_referencia": "10/03/2026",
                        "contrato": "CTR-01",
                        "programa": "OBRA",
                        "nucleo": "Mississipi",
                        "equipe": "Equipe 01 / Equipe 02",
                        "logradouro": "Viela 6",
                        "status_frente": "ativa",
                        "arquivo_origem": "origem.txt",
                    }
                ],
            )
            self._write_csv(
                master_dir / "base_mestra_ocorrencias.csv",
                [
                    {
                        "data_referencia": "10/03/2026",
                        "contrato": "CTR-01",
                        "programa": "OBRA",
                        "nucleo": "Mississipi",
                        "equipe": "Equipe 01 / Equipe 02",
                        "tipo_ocorrencia": "interferencia",
                        "descricao": "interferencia",
                        "arquivo_origem": "origem.txt",
                    }
                ],
            )

            self._write_csv(
                out / "execucao.csv",
                [
                    {
                        "data_referencia": "10/03/2026",
                        "contrato": "CTR-01",
                        "programa": "OBRA",
                        "nucleo": "Mississipi",
                        "equipe": "Equipe 01 / Equipe 99",
                        "logradouro": "Viela 6",
                        "item_normalizado": "ramal_agua",
                        "quantidade": "2",
                        "unidade": "un",
                        "material": "",
                        "especificacao": "",
                        "arquivo_origem": "origem.txt",
                    }
                ],
            )
            self._write_csv(
                out / "frentes.csv",
                [
                    {
                        "data_referencia": "10/03/2026",
                        "contrato": "CTR-01",
                        "programa": "OBRA",
                        "nucleo": "Mississipi",
                        "equipe": "Equipe 01 / Equipe 99",
                        "logradouro": "Viela 6",
                        "status_frente": "ativa",
                        "arquivo_origem": "origem.txt",
                    }
                ],
            )
            self._write_csv(
                out / "ocorrencias.csv",
                [
                    {
                        "data_referencia": "10/03/2026",
                        "contrato": "CTR-01",
                        "programa": "OBRA",
                        "nucleo": "Mississipi",
                        "equipe": "Equipe 01 / Equipe 99",
                        "tipo_ocorrencia": "interferencia",
                        "descricao": "interferencia",
                        "arquivo_origem": "origem.txt",
                    }
                ],
            )

            stats = update_master_from_output(out, master_dir)
            self.assertEqual(stats, {"execucao": 0, "ocorrencias": 0, "frentes": 0})

            with (master_dir / "base_mestra_execucao.csv").open("r", encoding="utf-8-sig", newline="") as f:
                rows_exec = list(csv.DictReader(f))
            with (master_dir / "base_mestra_frentes.csv").open("r", encoding="utf-8-sig", newline="") as f:
                rows_fr = list(csv.DictReader(f))
            with (master_dir / "base_mestra_ocorrencias.csv").open("r", encoding="utf-8-sig", newline="") as f:
                rows_oc = list(csv.DictReader(f))

            self.assertEqual(rows_exec[0].get("equipe"), "Equipe 01")
            self.assertEqual(rows_fr[0].get("equipe"), "Equipe 01")
            self.assertEqual(rows_oc[0].get("equipe"), "Equipe 01")

    def test_update_master_aplica_municipio_oficial_do_cadastro_mestre(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            master_dir = tmp / "BASE_MESTRA"
            out = tmp / "saida"

            self._write_csv(
                out / "execucao.csv",
                [
                    {
                        "data_referencia": "10/03/2026",
                        "contrato": "CTR-01",
                        "programa": "OBRA",
                        "nucleo": "Patrocinio",
                        "equipe": "Equipe 05",
                        "logradouro": "Rua A",
                        "municipio": "Barueri",
                        "item_normalizado": "hidrometro",
                        "quantidade": "3",
                        "unidade": "un",
                        "material": "",
                        "especificacao": "",
                        "arquivo_origem": "origem.txt",
                        "servico_bruto": "hidrometro",
                        "servico_normalizado": "hidrometro",
                        "servico_oficial": "hidrometro",
                        "categoria": "hidrometro",
                    }
                ],
            )
            self._write_csv(
                out / "frentes.csv",
                [
                    {
                        "data_referencia": "10/03/2026",
                        "contrato": "CTR-01",
                        "programa": "OBRA",
                        "nucleo": "Patrocinio",
                        "equipe": "Equipe 05",
                        "logradouro": "Rua A",
                        "municipio": "Barueri",
                        "status_frente": "ativa",
                        "arquivo_origem": "origem.txt",
                    }
                ],
            )
            self._write_csv(
                out / "ocorrencias.csv",
                [
                    {
                        "data_referencia": "10/03/2026",
                        "contrato": "CTR-01",
                        "programa": "OBRA",
                        "nucleo": "Patrocinio",
                        "equipe": "Equipe 05",
                        "municipio": "Barueri",
                        "tipo_ocorrencia": "vistoria",
                        "descricao": "vistoria",
                        "arquivo_origem": "origem.txt",
                    }
                ],
            )

            update_master_from_output(out, master_dir)

            with (master_dir / "base_mestra_execucao.csv").open("r", encoding="utf-8-sig", newline="") as f:
                rows = list(csv.DictReader(f))

            self.assertTrue(rows)
            self.assertEqual("Patrocínio", rows[0]["nucleo"])
            self.assertEqual("Carapicuíba", rows[0]["municipio"])
            self.assertEqual("Patrocinio", rows[0]["nucleo_detectado_texto"])
            self.assertEqual("Barueri", rows[0]["municipio_detectado_texto"])
            self.assertEqual("Patrocínio", rows[0]["nucleo_oficial"])
            self.assertEqual("Carapicuíba", rows[0]["municipio_oficial"])


if __name__ == "__main__":
    unittest.main()

