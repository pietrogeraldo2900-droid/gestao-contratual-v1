from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _friendly_import_error(exc: Exception) -> None:
    msg = str(exc)
    print("\nERRO DE DEPENDENCIA:")
    print(msg)
    print("\nInstale as dependencias com:")
    print("python -m pip install -r requirements.txt\n")
    sys.exit(1)


try:
    from config.settings import load_settings
    from app.core.input_layer import OfficialMessageParser, aplicar_regra_primeira_equipe, carregar_dicionario_servicos
    from app.core.nucleo_master import load_nucleo_registry, reconcile_parsed_with_registry
    from app.services.base_builder import build_management_workbook
    from app.services.master_builder import consolidate_outputs_folder, update_master_from_output
    from app.services.report_system import ReportGenerator, ServiceDictionary, WhatsAppReportParser, save_parsed_outputs
except ModuleNotFoundError as exc:
    _friendly_import_error(exc)


BASE_DIR = Path(__file__).resolve().parent
SETTINGS = load_settings(BASE_DIR)
SERVICE_DICTIONARY_CSV = SETTINGS.service_dictionary_csv
SERVICE_DICTIONARY_V2_JSON = SETTINGS.service_dictionary_v2_json
NUCLEO_REFERENCE_JSON = SETTINGS.nucleo_reference_file


def process_text_file(input_path: Path, output_dir: Path, master_dir: Path | None = None) -> Path:
    dictionary = ServiceDictionary(SERVICE_DICTIONARY_CSV)
    text = Path(input_path).read_text(encoding="utf-8")
    parsed = None
    nucleo_registry = load_nucleo_registry(NUCLEO_REFERENCE_JSON)

    official_dict_path = SERVICE_DICTIONARY_V2_JSON
    if official_dict_path.exists():
        official_parser = OfficialMessageParser(carregar_dicionario_servicos(official_dict_path))
        parsed = official_parser.parse_text(text, source_name=Path(input_path).name)

    if parsed is None:
        parser = WhatsAppReportParser(dictionary)
        parsed = parser.parse_text(text, source_name=Path(input_path).name)
    aplicar_regra_primeira_equipe(parsed)
    parsed = reconcile_parsed_with_registry(parsed, nucleo_registry)

    save_parsed_outputs(parsed, output_dir)
    generator = ReportGenerator(dictionary)
    generator.generate_nucleus_reports(parsed, output_dir / "relatorios_nucleos")
    build_management_workbook(
        output_dir,
        SERVICE_DICTIONARY_CSV,
        nucleo_reference_file=NUCLEO_REFERENCE_JSON,
    )
    if master_dir:
        update_master_from_output(
            output_dir,
            master_dir,
            SERVICE_DICTIONARY_CSV,
            nucleo_reference_file=NUCLEO_REFERENCE_JSON,
        )
    return output_dir


def cmd_parse(args):
    output_dir = process_text_file(
        Path(args.input),
        Path(args.output),
        Path(args.master_dir) if getattr(args, "master_dir", None) else None,
    )
    print(f"Saida gerada em: {output_dir}")


def cmd_batch(args):
    folder = Path(args.folder)
    txts = sorted(folder.glob("*.txt"))
    if not txts:
        print("Nenhum arquivo .txt encontrado na pasta.")
        return
    for txt in txts:
        output_dir = folder / f"saida_{txt.stem}"
        process_text_file(txt, output_dir, Path(args.master_dir) if getattr(args, "master_dir", None) else None)
        print(f"Processado: {txt.name} -> {output_dir.name}")


def cmd_update_master(args):
    stats = update_master_from_output(
        Path(args.output_dir),
        Path(args.master_dir),
        SERVICE_DICTIONARY_CSV,
        nucleo_reference_file=NUCLEO_REFERENCE_JSON,
    )
    print(f"Base mestra atualizada em: {args.master_dir}")
    print(
        "Novos registros adicionados -> "
        f"execucao: {stats['execucao']} | ocorrencias: {stats['ocorrencias']} | frentes: {stats['frentes']}"
    )


def cmd_consolidate_master(args):
    stats = consolidate_outputs_folder(
        Path(args.outputs_parent),
        Path(args.output),
        SERVICE_DICTIONARY_CSV,
        nucleo_reference_file=NUCLEO_REFERENCE_JSON,
    )
    print(f"Base consolidada gerada em: {args.output}")
    print(
        "Novos registros adicionados -> "
        f"execucao: {stats['execucao']} | ocorrencias: {stats['ocorrencias']} | frentes: {stats['frentes']}"
    )


def cmd_web(args):
    from run_web import run

    run(host=args.host, port=args.port, debug=False)


def build_cli():
    cli = argparse.ArgumentParser(
        description="CLI legada/local para processamento operacional. Para interface web oficial, use: python run_web.py"
    )
    sub = cli.add_subparsers(dest="command", required=True)

    p = sub.add_parser("parse", help="Processa um consolidado de WhatsApp")
    p.add_argument("input", help="Arquivo txt com a mensagem do WhatsApp")
    p.add_argument("--output", default="saida", help="Pasta de saida")
    p.add_argument("--master-dir", default="", help="Pasta da base mestra acumulada (opcional)")
    p.set_defaults(func=cmd_parse)

    b = sub.add_parser("batch", help="Processa todos os .txt de uma pasta")
    b.add_argument("folder", help="Pasta com arquivos .txt")
    b.add_argument("--master-dir", default="", help="Pasta da base mestra acumulada (opcional)")
    b.set_defaults(func=cmd_batch)

    u = sub.add_parser("update-master", help="Atualiza a base mestra a partir de uma pasta de saida existente")
    u.add_argument("output_dir", help="Pasta de saida que contem execucao.csv, frentes.csv e ocorrencias.csv")
    u.add_argument("--master-dir", default="BASE_MESTRA", help="Pasta da base mestra acumulada")
    u.set_defaults(func=cmd_update_master)

    c = sub.add_parser("consolidate-master", help="Consolida varias pastas de saida em uma base unica")
    c.add_argument("outputs_parent", help="Pasta que contem varias subpastas de saida")
    c.add_argument("--output", default="base_consolidada", help="Pasta final da base consolidada")
    c.set_defaults(func=cmd_consolidate_master)

    w = sub.add_parser("web", help="Inicia a interface web local")
    w.add_argument("--host", default=SETTINGS.web_host, help="Host local para subir o servidor")
    w.add_argument("--port", type=int, default=SETTINGS.web_port, help="Porta da interface web")
    w.set_defaults(func=cmd_web)

    return cli


if __name__ == "__main__":
    cli = build_cli()
    args = cli.parse_args()
    args.func(args)
