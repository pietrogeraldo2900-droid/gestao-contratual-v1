from __future__ import annotations

import csv
import shutil
from pathlib import Path
from typing import Dict, List, Tuple

from app.core.input_layer import extrair_primeira_equipe
from app.core.nucleo_master import load_nucleo_registry, reconcile_rows_with_registry
from app.services.base_builder import build_management_workbook

CSV_NAMES = ["execucao", "ocorrencias", "frentes"]
NUCLEO_REFERENCE_FILE = Path(__file__).resolve().parents[2] / "config" / "nucleo_reference.json"
KEYS = {
    "execucao": [
        "data_referencia",
        "contrato",
        "programa",
        "nucleo",
        "equipe",
        "logradouro",
        "item_normalizado",
        "quantidade",
        "unidade",
        "material",
        "especificacao",
        "arquivo_origem",
    ],
    "ocorrencias": [
        "data_referencia",
        "contrato",
        "programa",
        "nucleo",
        "equipe",
        "tipo_ocorrencia",
        "descricao",
        "arquivo_origem",
    ],
    "frentes": [
        "data_referencia",
        "contrato",
        "programa",
        "nucleo",
        "equipe",
        "logradouro",
        "status_frente",
        "arquivo_origem",
    ],
}


def _read_csv(path: Path) -> List[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _normalize_equipe_rows(rows: List[dict]) -> List[dict]:
    out: List[dict] = []
    for row in rows:
        clean = dict(row)
        clean["equipe"] = extrair_primeira_equipe(clean.get("equipe", ""))
        out.append(clean)
    return out


def _normalize_rows_for_master(rows: List[dict], nucleo_reference_file: Path | None = None) -> List[dict]:
    normalized = _normalize_equipe_rows(rows)
    registry = load_nucleo_registry(nucleo_reference_file or NUCLEO_REFERENCE_FILE)
    return reconcile_rows_with_registry(normalized, registry)


def _write_csv(path: Path, rows: List[dict], nucleo_reference_file: Path | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return

    # Permite evolucao de schema entre execucoes (ex.: base antiga sem 'municipio'/'data')
    # sem quebrar a atualizacao da base mestra.
    headers: List[str] = []
    seen = set()
    normalized_rows = _normalize_rows_for_master(rows, nucleo_reference_file)

    for row in normalized_rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                headers.append(key)

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in normalized_rows:
            writer.writerow({h: row.get(h, "") for h in headers})


def _key(row: dict, kind: str) -> Tuple[str, ...]:
    return tuple(str(row.get(k, "")).strip() for k in KEYS[kind])


def _sync_alias_csvs(base_dir: Path, source_prefix: str) -> None:
    """Mirror prefixed CSV names to aliases expected by workbook builder."""
    for kind in CSV_NAMES:
        src = base_dir / f"{source_prefix}{kind}.csv"
        dst = base_dir / f"{kind}.csv"
        if src.exists():
            shutil.copyfile(src, dst)


def merge_csv_into_master(
    source_csv: Path,
    master_csv: Path,
    kind: str,
    nucleo_reference_file: Path | None = None,
) -> int:
    source_rows = _normalize_rows_for_master(_read_csv(source_csv), nucleo_reference_file)
    if not source_rows:
        return 0
    master_rows = _normalize_rows_for_master(_read_csv(master_csv), nucleo_reference_file)
    existing = {_key(r, kind) for r in master_rows}
    added = 0
    for row in source_rows:
        k = _key(row, kind)
        if k not in existing:
            master_rows.append(row)
            existing.add(k)
            added += 1
    _write_csv(master_csv, master_rows, nucleo_reference_file)
    return added


def update_master_from_output(
    output_dir: Path,
    master_dir: Path,
    dictionary_csv: Path | None = None,
    nucleo_reference_file: Path | None = None,
) -> Dict[str, int]:
    output_dir = Path(output_dir)
    master_dir = Path(master_dir)

    if not output_dir.exists() or not output_dir.is_dir():
        raise FileNotFoundError(f"Pasta de saida invalida: {output_dir}")

    has_any_csv = any((output_dir / f"{kind}.csv").exists() for kind in CSV_NAMES)
    if not has_any_csv:
        raise FileNotFoundError(
            f"Nenhum CSV esperado encontrado em {output_dir}. Esperado: execucao.csv, ocorrencias.csv, frentes.csv"
        )

    master_dir.mkdir(parents=True, exist_ok=True)
    stats: Dict[str, int] = {}
    for kind in CSV_NAMES:
        stats[kind] = merge_csv_into_master(
            output_dir / f"{kind}.csv",
            master_dir / f"base_mestra_{kind}.csv",
            kind,
            nucleo_reference_file=nucleo_reference_file,
        )

    # Garantia: base_builder le os aliases sem prefixo.
    _sync_alias_csvs(master_dir, "base_mestra_")
    build_management_workbook(master_dir, dictionary_csv, nucleo_reference_file=nucleo_reference_file)
    return stats


def consolidate_outputs_folder(
    outputs_parent: Path,
    consolidated_dir: Path,
    dictionary_csv: Path | None = None,
    nucleo_reference_file: Path | None = None,
) -> Dict[str, int]:
    outputs_parent = Path(outputs_parent)
    consolidated_dir = Path(consolidated_dir)

    if not outputs_parent.exists() or not outputs_parent.is_dir():
        raise FileNotFoundError(f"Pasta de saidas invalida: {outputs_parent}")

    consolidated_dir.mkdir(parents=True, exist_ok=True)
    total = {k: 0 for k in CSV_NAMES}

    for sub in sorted(outputs_parent.iterdir()):
        if not sub.is_dir():
            continue
        for kind in CSV_NAMES:
            csv_path = sub / f"{kind}.csv"
            if csv_path.exists():
                total[kind] += merge_csv_into_master(
                    csv_path,
                    consolidated_dir / f"consolidado_{kind}.csv",
                    kind,
                    nucleo_reference_file=nucleo_reference_file,
                )

    _sync_alias_csvs(consolidated_dir, "consolidado_")
    build_management_workbook(consolidated_dir, dictionary_csv, nucleo_reference_file=nucleo_reference_file)
    return total

