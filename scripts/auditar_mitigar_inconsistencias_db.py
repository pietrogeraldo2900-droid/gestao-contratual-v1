from __future__ import annotations

import argparse
import json
import os
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.database.connection import build_database_manager
from app.repositories.service_mapping_repository import ServiceMappingRepository
from config.settings import load_settings


def _dict_row_factory():
    try:
        from psycopg.rows import dict_row

        return dict_row
    except Exception:
        return None


def _safe_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_lookup(value: object) -> str:
    text = _safe_text(value).lower()
    text = "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_contract_candidate(value: object) -> str:
    raw = _safe_text(value)
    if not raw:
        return ""
    text = raw.replace("\u2013", "-").replace("\u2014", "-")
    text = re.sub(r"\s*-\s*", " - ", text)
    text = re.sub(r"\s+", " ", text).strip(" -:\t")
    if not text:
        return ""

    match = re.match(r"^\s*al\s*[- ]*\d+\s*-\s*(.+?)\s*$", text, flags=re.IGNORECASE)
    if not match:
        match = re.match(r"^\s*al\s*[- ]*\d+\s+(.+?)\s*$", text, flags=re.IGNORECASE)
    if not match:
        match = re.match(r"^\s*\d{4,}\s*-\s*(.+?)\s*$", text, flags=re.IGNORECASE)
    if match:
        candidate = re.sub(r"\s+", " ", str(match.group(1) or "")).strip(" -:\t")
        if candidate:
            return candidate
    return text


def _build_contract_catalog(cur) -> dict[str, Any]:
    cur.execute(
        """
        SELECT
            id,
            COALESCE(NULLIF(numero_contrato, ''), NULLIF(contract_code, ''), NULLIF(title, '')) AS contract_key,
            COALESCE(
                NULLIF(nome_contrato, ''),
                NULLIF(title, ''),
                NULLIF(numero_contrato, ''),
                NULLIF(contract_code, '')
            ) AS contract_name,
            numero_contrato,
            contract_code,
            title,
            nome_contrato
        FROM contracts
        ORDER BY id
        """
    )
    rows = cur.fetchall() or []

    by_key: dict[str, dict[str, Any]] = {}
    by_id: dict[str, dict[str, Any]] = {}
    contracts: list[dict[str, Any]] = []

    for row in rows:
        row_dict = dict(row)
        contract_id = _safe_text(row_dict.get("id"))
        contract_name = _safe_text(row_dict.get("contract_name"))
        if not contract_id or not contract_name:
            continue
        entry = {
            "id": contract_id,
            "name": contract_name,
        }
        contracts.append(entry)
        by_id[contract_id] = entry

        for field in (
            row_dict.get("contract_key"),
            row_dict.get("numero_contrato"),
            row_dict.get("contract_code"),
            row_dict.get("title"),
            row_dict.get("nome_contrato"),
            contract_name,
            _normalize_contract_candidate(row_dict.get("contract_key")),
            _normalize_contract_candidate(row_dict.get("numero_contrato")),
            _normalize_contract_candidate(row_dict.get("contract_code")),
            _normalize_contract_candidate(contract_name),
        ):
            key = _normalize_lookup(field)
            if key:
                by_key[key] = entry

    return {"contracts": contracts, "by_id": by_id, "by_key": by_key}


def _resolve_contract(raw_value: object, catalog: dict[str, Any]) -> dict[str, str] | None:
    raw = _safe_text(raw_value)
    if not raw:
        return None

    by_key = catalog.get("by_key", {})
    by_id = catalog.get("by_id", {})

    if raw.isdigit():
        hit = by_id.get(raw)
        if hit:
            return {"id": hit["id"], "name": hit["name"]}

    key_raw = _normalize_lookup(raw)
    if key_raw and key_raw in by_key:
        hit = by_key[key_raw]
        return {"id": hit["id"], "name": hit["name"]}

    candidate = _normalize_contract_candidate(raw)
    key_candidate = _normalize_lookup(candidate)
    if key_candidate and key_candidate in by_key:
        hit = by_key[key_candidate]
        return {"id": hit["id"], "name": hit["name"]}

    return None


def _count_duplicate_execucao(cur) -> int:
    cur.execute(
        """
        WITH ranked AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        COALESCE(data_referencia::text, ''),
                        LOWER(TRIM(COALESCE(contrato, ''))),
                        LOWER(TRIM(COALESCE(id_frente, ''))),
                        LOWER(TRIM(COALESCE(id_item, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(nucleo_oficial, ''), nucleo, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(municipio_oficial, ''), municipio, ''))),
                        LOWER(TRIM(COALESCE(equipe, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(servico_oficial, ''), NULLIF(servico_normalizado, ''), NULLIF(servico_bruto, ''), NULLIF(item_original, ''), ''))),
                        LOWER(TRIM(COALESCE(NULLIF(categoria, ''), NULLIF(categoria_item, ''), ''))),
                        COALESCE(quantidade::text, ''),
                        LOWER(TRIM(COALESCE(unidade, ''))),
                        LOWER(TRIM(COALESCE(logradouro, '')))
                    ORDER BY id
                ) AS rn
            FROM management_execucao
        )
        SELECT COUNT(*)::BIGINT AS total
        FROM ranked
        WHERE rn > 1
        """
    )
    row = cur.fetchone() or {}
    return int(row.get("total", 0) or 0)


def _count_duplicate_frentes(cur) -> int:
    cur.execute(
        """
        WITH ranked AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        COALESCE(data_referencia::text, ''),
                        LOWER(TRIM(COALESCE(contrato, ''))),
                        LOWER(TRIM(COALESCE(id_frente, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(nucleo_oficial, ''), nucleo, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(municipio_oficial, ''), municipio, ''))),
                        LOWER(TRIM(COALESCE(equipe, ''))),
                        LOWER(TRIM(COALESCE(status_frente, ''))),
                        LOWER(TRIM(COALESCE(logradouro, '')))
                    ORDER BY id
                ) AS rn
            FROM management_frentes
        )
        SELECT COUNT(*)::BIGINT AS total
        FROM ranked
        WHERE rn > 1
        """
    )
    row = cur.fetchone() or {}
    return int(row.get("total", 0) or 0)


def _count_duplicate_ocorrencias(cur) -> int:
    cur.execute(
        """
        WITH ranked AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        COALESCE(data_referencia::text, ''),
                        LOWER(TRIM(COALESCE(contrato, ''))),
                        LOWER(TRIM(COALESCE(id_frente, ''))),
                        LOWER(TRIM(COALESCE(id_ocorrencia, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(nucleo_oficial, ''), nucleo, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(municipio_oficial, ''), municipio, ''))),
                        LOWER(TRIM(COALESCE(equipe, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(tipo_ocorrencia, ''), NULLIF(descricao, ''), ''))),
                        LOWER(TRIM(COALESCE(logradouro, '')))
                    ORDER BY id
                ) AS rn
            FROM management_ocorrencias
        )
        SELECT COUNT(*)::BIGINT AS total
        FROM ranked
        WHERE rn > 1
        """
    )
    row = cur.fetchone() or {}
    return int(row.get("total", 0) or 0)


def _delete_duplicate_execucao(cur) -> int:
    cur.execute(
        """
        WITH ranked AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        COALESCE(data_referencia::text, ''),
                        LOWER(TRIM(COALESCE(contrato, ''))),
                        LOWER(TRIM(COALESCE(id_frente, ''))),
                        LOWER(TRIM(COALESCE(id_item, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(nucleo_oficial, ''), nucleo, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(municipio_oficial, ''), municipio, ''))),
                        LOWER(TRIM(COALESCE(equipe, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(servico_oficial, ''), NULLIF(servico_normalizado, ''), NULLIF(servico_bruto, ''), NULLIF(item_original, ''), ''))),
                        LOWER(TRIM(COALESCE(NULLIF(categoria, ''), NULLIF(categoria_item, ''), ''))),
                        COALESCE(quantidade::text, ''),
                        LOWER(TRIM(COALESCE(unidade, ''))),
                        LOWER(TRIM(COALESCE(logradouro, '')))
                    ORDER BY id
                ) AS rn
            FROM management_execucao
        )
        DELETE FROM management_execucao e
        USING ranked r
        WHERE e.id = r.id
          AND r.rn > 1
        """
    )
    return int(cur.rowcount or 0)


def _delete_duplicate_frentes(cur) -> int:
    cur.execute(
        """
        WITH ranked AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        COALESCE(data_referencia::text, ''),
                        LOWER(TRIM(COALESCE(contrato, ''))),
                        LOWER(TRIM(COALESCE(id_frente, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(nucleo_oficial, ''), nucleo, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(municipio_oficial, ''), municipio, ''))),
                        LOWER(TRIM(COALESCE(equipe, ''))),
                        LOWER(TRIM(COALESCE(status_frente, ''))),
                        LOWER(TRIM(COALESCE(logradouro, '')))
                    ORDER BY id
                ) AS rn
            FROM management_frentes
        )
        DELETE FROM management_frentes f
        USING ranked r
        WHERE f.id = r.id
          AND r.rn > 1
        """
    )
    return int(cur.rowcount or 0)


def _delete_duplicate_ocorrencias(cur) -> int:
    cur.execute(
        """
        WITH ranked AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        COALESCE(data_referencia::text, ''),
                        LOWER(TRIM(COALESCE(contrato, ''))),
                        LOWER(TRIM(COALESCE(id_frente, ''))),
                        LOWER(TRIM(COALESCE(id_ocorrencia, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(nucleo_oficial, ''), nucleo, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(municipio_oficial, ''), municipio, ''))),
                        LOWER(TRIM(COALESCE(equipe, ''))),
                        LOWER(TRIM(COALESCE(NULLIF(tipo_ocorrencia, ''), NULLIF(descricao, ''), ''))),
                        LOWER(TRIM(COALESCE(logradouro, '')))
                    ORDER BY id
                ) AS rn
            FROM management_ocorrencias
        )
        DELETE FROM management_ocorrencias o
        USING ranked r
        WHERE o.id = r.id
          AND r.rn > 1
        """
    )
    return int(cur.rowcount or 0)


def _audit_snapshot(cur, catalog: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}

    cur.execute("SELECT COUNT(*)::BIGINT AS total FROM management_execucao")
    out["total_execucao"] = int((cur.fetchone() or {}).get("total", 0) or 0)
    cur.execute("SELECT COUNT(*)::BIGINT AS total FROM management_frentes")
    out["total_frentes"] = int((cur.fetchone() or {}).get("total", 0) or 0)
    cur.execute("SELECT COUNT(*)::BIGINT AS total FROM management_ocorrencias")
    out["total_ocorrencias"] = int((cur.fetchone() or {}).get("total", 0) or 0)

    out["duplicados_execucao"] = _count_duplicate_execucao(cur)
    out["duplicados_frentes"] = _count_duplicate_frentes(cur)
    out["duplicados_ocorrencias"] = _count_duplicate_ocorrencias(cur)

    cur.execute(
        """
        SELECT COUNT(*)::BIGINT AS total
        FROM management_execucao
        WHERE COALESCE(TRIM(servico_oficial), '') = ''
        """
    )
    out["servico_oficial_vazio"] = int((cur.fetchone() or {}).get("total", 0) or 0)

    cur.execute(
        """
        SELECT COUNT(*)::BIGINT AS total
        FROM management_execucao
        WHERE LOWER(TRIM(COALESCE(servico_oficial, ''))) IN ('servico_nao_mapeado', 'nao_mapeado', '-')
        """
    )
    out["servico_nao_mapeado"] = int((cur.fetchone() or {}).get("total", 0) or 0)

    cur.execute(
        """
        SELECT COUNT(*)::BIGINT AS total
        FROM management_execucao
        WHERE COALESCE(TRIM(categoria), '') = ''
           OR COALESCE(TRIM(categoria_item), '') = ''
        """
    )
    out["categoria_vazia_ou_incompleta"] = int((cur.fetchone() or {}).get("total", 0) or 0)

    cur.execute(
        """
        SELECT
            COALESCE(NULLIF(TRIM(servico_oficial), ''), '(vazio)') AS label,
            COUNT(*)::BIGINT AS qtd
        FROM management_execucao
        GROUP BY 1
        ORDER BY qtd DESC, label ASC
        LIMIT 2000
        """
    )
    service_rows = cur.fetchall() or []
    service_groups: dict[str, dict[str, Any]] = defaultdict(lambda: {"total": 0, "variantes": defaultdict(int)})
    for row in service_rows:
        label = _safe_text(row.get("label")) or "(vazio)"
        qtd = int(row.get("qtd", 0) or 0)
        key = _normalize_lookup(label) or "(vazio)"
        service_groups[key]["total"] += qtd
        service_groups[key]["variantes"][label] += qtd

    variant_conflicts: list[dict[str, Any]] = []
    for key, group in service_groups.items():
        variants = group["variantes"]
        if len(variants) > 1:
            variant_conflicts.append(
                {
                    "chave_normalizada": key,
                    "total": int(group["total"]),
                    "variantes": [
                        {"nome": name, "qtd": int(count)}
                        for name, count in sorted(variants.items(), key=lambda item: (-item[1], item[0]))
                    ],
                }
            )
    variant_conflicts.sort(key=lambda item: (-int(item.get("total", 0)), item.get("chave_normalizada", "")))
    out["variacoes_servico_mesma_chave"] = variant_conflicts[:30]

    cur.execute(
        """
        SELECT
            COALESCE(
                NULLIF(TRIM(servico_bruto), ''),
                NULLIF(TRIM(item_original), ''),
                NULLIF(TRIM(servico_normalizado), ''),
                '(vazio)'
            ) AS termo,
            COUNT(*)::BIGINT AS qtd
        FROM management_execucao
        WHERE LOWER(TRIM(COALESCE(servico_oficial, ''))) IN ('servico_nao_mapeado', 'nao_mapeado', '-')
           OR COALESCE(TRIM(servico_oficial), '') = ''
        GROUP BY 1
        ORDER BY qtd DESC, termo ASC
        LIMIT 50
        """
    )
    out["top_termos_nao_mapeados"] = [
        {"termo": _safe_text(row.get("termo")) or "(vazio)", "qtd": int(row.get("qtd", 0) or 0)}
        for row in (cur.fetchall() or [])
    ]

    catalog_by_key = catalog.get("by_key", {})
    cur.execute(
        """
        SELECT contrato, COUNT(*)::BIGINT AS qtd
        FROM (
            SELECT contrato FROM management_execucao
            UNION ALL
            SELECT contrato FROM management_frentes
            UNION ALL
            SELECT contrato FROM management_ocorrencias
        ) src
        WHERE COALESCE(TRIM(contrato), '') <> ''
        GROUP BY contrato
        ORDER BY qtd DESC, contrato ASC
        """
    )
    contract_rows = cur.fetchall() or []
    unknown_contracts = []
    for row in contract_rows:
        contrato = _safe_text(row.get("contrato"))
        qtd = int(row.get("qtd", 0) or 0)
        key_raw = _normalize_lookup(contrato)
        key_candidate = _normalize_lookup(_normalize_contract_candidate(contrato))
        if key_raw in catalog_by_key or key_candidate in catalog_by_key:
            continue
        unknown_contracts.append({"contrato": contrato, "qtd": qtd})
    out["contratos_sem_correspondencia"] = unknown_contracts[:50]

    return out


def _apply_contract_normalization(cur, catalog: dict[str, Any]) -> dict[str, int]:
    stats = {
        "management_execucao": 0,
        "management_frentes": 0,
        "management_ocorrencias": 0,
        "processing_history": 0,
    }

    for table in ("management_execucao", "management_frentes", "management_ocorrencias"):
        cur.execute(f"SELECT id, contrato FROM {table}")
        rows = cur.fetchall() or []
        updates = []
        for row in rows:
            row_id = int(row.get("id", 0) or 0)
            raw = _safe_text(row.get("contrato"))
            if row_id <= 0 or not raw:
                continue
            resolved = _resolve_contract(raw, catalog)
            if not resolved:
                continue
            target = _safe_text(resolved.get("name"))
            if target and target != raw:
                updates.append((target, row_id))
        if updates:
            cur.executemany(f"UPDATE {table} SET contrato = %s WHERE id = %s", updates)
        stats[table] = len(updates)

    cur.execute("SELECT id, contract_id, contract_label FROM processing_history")
    history_rows = cur.fetchall() or []
    history_updates = []
    for row in history_rows:
        row_id = int(row.get("id", 0) or 0)
        contract_id = _safe_text(row.get("contract_id"))
        contract_label = _safe_text(row.get("contract_label"))
        if row_id <= 0:
            continue

        resolved = None
        if contract_id:
            resolved = _resolve_contract(contract_id, catalog)
        if not resolved and contract_label:
            resolved = _resolve_contract(contract_label, catalog)

        if not resolved:
            continue
        target_id = _safe_text(resolved.get("id"))
        target_label = _safe_text(resolved.get("name"))
        if not target_label:
            continue
        if contract_id == target_id and contract_label == target_label:
            continue
        history_updates.append((target_id, target_label, row_id))

    if history_updates:
        cur.executemany(
            "UPDATE processing_history SET contract_id = %s, contract_label = %s WHERE id = %s",
            history_updates,
        )
    stats["processing_history"] = len(history_updates)
    return stats


def _apply_service_defaults(cur) -> dict[str, int]:
    stats = {
        "servico_vazio_para_nao_mapeado": 0,
        "categoria_vazia_preenchida": 0,
    }

    cur.execute(
        """
        UPDATE management_execucao
        SET servico_oficial = 'servico_nao_mapeado'
        WHERE COALESCE(TRIM(servico_oficial), '') = ''
        """
    )
    stats["servico_vazio_para_nao_mapeado"] = int(cur.rowcount or 0)

    cur.execute(
        """
        UPDATE management_execucao
        SET
            categoria = CASE
                WHEN COALESCE(TRIM(categoria), '') = '' THEN COALESCE(NULLIF(TRIM(categoria_item), ''), 'servico_nao_mapeado')
                ELSE categoria
            END,
            categoria_item = CASE
                WHEN COALESCE(TRIM(categoria_item), '') = '' THEN COALESCE(NULLIF(TRIM(categoria), ''), 'servico_nao_mapeado')
                ELSE categoria_item
            END
        WHERE COALESCE(TRIM(categoria), '') = ''
           OR COALESCE(TRIM(categoria_item), '') = ''
        """
    )
    stats["categoria_vazia_preenchida"] = int(cur.rowcount or 0)
    return stats


def _table_has_column(cur, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (str(table_name or "").strip(), str(column_name or "").strip()),
    )
    return cur.fetchone() is not None


def _run_audit_and_fix(database_url: str, apply_changes: bool) -> dict[str, Any]:
    settings = load_settings(Path(".").resolve())
    if database_url:
        os.environ["DATABASE_URL"] = database_url
        settings = load_settings(Path(".").resolve())

    db = build_database_manager(settings)
    if db is None:
        raise RuntimeError("Banco indisponivel. Defina DB_ENABLED=1 e DATABASE_URL.")

    cursor_kwargs = {}
    dict_factory = _dict_row_factory()
    if dict_factory is not None:
        cursor_kwargs["row_factory"] = dict_factory

    output: dict[str, Any] = {
        "mode": "apply" if apply_changes else "dry_run",
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "before": {},
        "after": {},
        "changes": {},
    }

    with db.connection() as conn:
        with conn.cursor(**cursor_kwargs) as cur:
            catalog = _build_contract_catalog(cur)
            output["contracts_catalog_size"] = len(catalog.get("contracts", []))
            output["before"] = _audit_snapshot(cur, catalog)

            if not apply_changes:
                conn.rollback()
                return output

            contract_stats = _apply_contract_normalization(cur, catalog)
            duplicate_stats = {
                "deleted_execucao": _delete_duplicate_execucao(cur),
                "deleted_frentes": _delete_duplicate_frentes(cur),
                "deleted_ocorrencias": _delete_duplicate_ocorrencias(cur),
            }
            service_defaults = _apply_service_defaults(cur)

            # Remapeia aliases registrados (comportamento existente do sistema).
            # Compatibilidade: alguns ambientes ainda nao possuem "diametro_mm".
            alias_remap_stats: dict[str, Any] = {
                "aliases_loaded": 0,
                "rows_scanned": 0,
                "rows_updated": 0,
                "skipped_reason": "",
            }
            if _table_has_column(cur, "management_execucao", "diametro_mm"):
                repo = ServiceMappingRepository(db)
                alias_remap_stats = repo.remap_management_execucao_by_aliases(
                    only_unmapped=False,
                    limit=2_000_000,
                )
            else:
                alias_remap_stats["skipped_reason"] = "coluna diametro_mm ausente em management_execucao"

            # Atualiza categorias a partir do service_registry quando houver divergência.
            cur.execute(
                """
                UPDATE management_execucao e
                SET
                    categoria = s.categoria,
                    categoria_item = s.categoria
                FROM service_registry s
                WHERE COALESCE(NULLIF(TRIM(e.servico_oficial), ''), '') <> ''
                  AND LOWER(TRIM(e.servico_oficial)) = LOWER(TRIM(s.servico_oficial))
                  AND (
                        COALESCE(NULLIF(TRIM(e.categoria), ''), '') <> COALESCE(NULLIF(TRIM(s.categoria), ''), '')
                     OR COALESCE(NULLIF(TRIM(e.categoria_item), ''), '') <> COALESCE(NULLIF(TRIM(s.categoria), ''), '')
                  )
                """
            )
            category_sync_rows = int(cur.rowcount or 0)

            conn.commit()

        with conn.cursor(**cursor_kwargs) as cur_after:
            output["after"] = _audit_snapshot(cur_after, catalog)

    output["changes"] = {
        "contracts_normalized": contract_stats,
        "duplicates_deleted": duplicate_stats,
        "service_defaults": service_defaults,
        "alias_remap": {
            "aliases_loaded": int(alias_remap_stats.get("aliases_loaded", 0) or 0),
            "rows_scanned": int(alias_remap_stats.get("rows_scanned", 0) or 0),
            "rows_updated": int(alias_remap_stats.get("rows_updated", 0) or 0),
            "skipped_reason": _safe_text(alias_remap_stats.get("skipped_reason")),
        },
        "category_sync_rows": int(category_sync_rows or 0),
    }
    return output


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Auditoria e saneamento de inconsistencias em tabelas management_* e processing_history."
    )
    parser.add_argument("--database-url", default="", help="DSN do Postgres. Se omitido, usa variaveis de ambiente.")
    parser.add_argument("--apply", action="store_true", help="Aplica correcoes no banco. Sem esta flag roda em dry-run.")
    parser.add_argument(
        "--output-json",
        default="",
        help="Arquivo JSON de saida. Default: SISG_AUDITORIA/db_auditoria_relatorio_<timestamp>.json",
    )
    args = parser.parse_args()

    report = _run_audit_and_fix(
        database_url=str(args.database_url or "").strip(),
        apply_changes=bool(args.apply),
    )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_json = str(args.output_json or "").strip()
    if not output_json:
        output_json = str(Path("SISG_AUDITORIA") / f"db_auditoria_relatorio_{stamp}.json")
    out_path = Path(output_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    mode = report.get("mode", "dry_run")
    print(f"[ok] Auditoria concluida ({mode}).")
    print(f"[ok] Relatorio: {out_path.resolve()}")
    print(
        "[resumo] duplicados(exec/frentes/ocorr): "
        f"{report.get('before', {}).get('duplicados_execucao', 0)} / "
        f"{report.get('before', {}).get('duplicados_frentes', 0)} / "
        f"{report.get('before', {}).get('duplicados_ocorrencias', 0)}"
    )
    print(
        "[resumo] nao mapeados: "
        f"{report.get('before', {}).get('servico_nao_mapeado', 0)}"
    )
    if mode == "apply":
        print(f"[changes] {json.dumps(report.get('changes', {}), ensure_ascii=False)}")


if __name__ == "__main__":
    main()
