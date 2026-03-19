from __future__ import annotations

import json
import re
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from input_layer import extrair_primeira_equipe, normalizar_texto


def split_registry_text(value: object) -> List[str]:
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return []

    parts = re.split(r"\s*(?:/|;|\n)+\s*", text)
    out: List[str] = []
    seen = set()
    for part in parts:
        clean = re.sub(r"^[\u2022\-\*]+\s*", "", str(part or "")).strip(" -")
        clean = " ".join(clean.split()).strip()
        if not clean:
            continue
        key = normalizar_texto(clean)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(clean)
    return out


def _normalize_nucleo_key(value: object) -> str:
    return normalizar_texto(str(value or "")).strip()


def _compact_normalized_key(value: object) -> str:
    key = _normalize_nucleo_key(value)
    if not key:
        return ""
    return re.sub(r"(.)\1+", r"\1", key)


def _canonical_status(value: object) -> str:
    raw = normalizar_texto(str(value or "")).strip()
    if raw in {"inativo", "inactive"}:
        return "inativo"
    return "ativo"


def _looks_like_logradouro(value: object) -> bool:
    text = " ".join(str(value or "").split()).strip()
    if not text:
        return False
    prefixes = (
        "viela",
        "rua",
        "travessa",
        "tv",
        "avenida",
        "av",
        "estrada",
        "rodovia",
        "acesso",
        "alameda",
        "beco",
        "praca",
        "largo",
    )
    parts = re.split(r"\s*(?:/|;|\n)+\s*", text)
    if not parts:
        return False
    for part in parts:
        norm = normalizar_texto(part).strip().replace(".", "")
        if not norm:
            continue
        if not any(norm == prefix or norm.startswith(f"{prefix} ") for prefix in prefixes):
            return False
    return True


def _coerce_list(value: object, *, team_mode: bool = False) -> List[str]:
    out: List[str] = []
    if isinstance(value, list):
        for item in value:
            out.extend(split_registry_text(item))
    else:
        out.extend(split_registry_text(value))

    deduped: List[str] = []
    seen = set()
    for item in out:
        clean = extrair_primeira_equipe(item) if team_mode else str(item or "").strip()
        if not clean:
            continue
        key = normalizar_texto(clean)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(clean)
    return deduped


def _coerce_entry(raw: dict, now: str | None = None) -> dict | None:
    if not isinstance(raw, dict):
        return None

    nucleo = " ".join(str(raw.get("nucleo", "") or "").split()).strip()
    if not nucleo:
        return None

    municipio = " ".join(str(raw.get("municipio", "") or "").split()).strip()
    logradouro_principal = " ".join(
        str(raw.get("logradouro_principal", "") or "").split()
    ).strip()
    observacoes = " ".join(str(raw.get("observacoes", "") or "").split()).strip()
    status = _canonical_status(raw.get("status", "ativo"))

    logradouros_padrao = _coerce_list(raw.get("logradouros_padrao", []))
    if logradouro_principal:
        log_key = normalizar_texto(logradouro_principal)
        if log_key and all(normalizar_texto(x) != log_key for x in logradouros_padrao):
            logradouros_padrao.insert(0, logradouro_principal)
    elif logradouros_padrao:
        logradouro_principal = logradouros_padrao[0]

    equipes_padrao = _coerce_list(raw.get("equipes_padrao", []), team_mode=True)
    aliases = _coerce_list(raw.get("aliases", []))

    created_at = str(raw.get("created_at", "") or "").strip()
    updated_at = str(raw.get("updated_at", "") or "").strip()
    stamp = now or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not created_at:
        created_at = stamp
    if not updated_at:
        updated_at = created_at

    return {
        "nucleo": nucleo,
        "municipio": municipio,
        "status": status,
        "aliases": aliases,
        "observacoes": observacoes,
        "logradouro_principal": logradouro_principal,
        "logradouros_padrao": logradouros_padrao,
        "equipes_padrao": equipes_padrao,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def load_nucleo_registry(path: Path) -> dict:
    registry_path = Path(path)
    if not registry_path.exists():
        return {
            "version": "2.0",
            "entries": [],
            "active_entries": [],
            "by_key": {},
            "by_compact_key": {},
            "municipio_by_key": {},
            "ambiguous_keys": [],
            "ambiguous_compact_keys": [],
        }

    try:
        with registry_path.open("r", encoding="utf-8-sig") as f:
            payload = json.load(f)
    except Exception:
        return {
            "version": "2.0",
            "entries": [],
            "active_entries": [],
            "by_key": {},
            "by_compact_key": {},
            "municipio_by_key": {},
            "ambiguous_keys": [],
            "ambiguous_compact_keys": [],
        }

    raw_entries = payload.get("nucleos", []) if isinstance(payload, dict) else []
    version = str(payload.get("version", "2.0") or "2.0") if isinstance(payload, dict) else "2.0"

    entries: List[dict] = []
    active_entries: List[dict] = []
    by_key: Dict[str, dict | None] = {}
    by_compact_key: Dict[str, dict | None] = {}
    municipio_by_key: Dict[str, str] = {}
    ambiguous_keys = set()
    ambiguous_compact_keys = set()

    for raw in raw_entries:
        entry = _coerce_entry(raw)
        if not entry:
            continue
        entries.append(entry)
        if entry.get("status") != "inativo":
            active_entries.append(entry)

        for alias in [entry["nucleo"], *entry.get("aliases", [])]:
            key = _normalize_nucleo_key(alias)
            if not key:
                continue
            existing = by_key.get(key)
            if existing and existing.get("nucleo") != entry.get("nucleo"):
                ambiguous_keys.add(key)
                by_key[key] = None
            elif key not in ambiguous_keys:
                by_key[key] = entry

            compact_key = _compact_normalized_key(alias)
            if compact_key:
                existing_compact = by_compact_key.get(compact_key)
                if existing_compact and existing_compact.get("nucleo") != entry.get("nucleo"):
                    ambiguous_compact_keys.add(compact_key)
                    by_compact_key[compact_key] = None
                elif compact_key not in ambiguous_compact_keys:
                    by_compact_key[compact_key] = entry

        municipio = str(entry.get("municipio", "") or "").strip()
        municipio_key = normalizar_texto(municipio)
        if municipio_key and municipio_key not in municipio_by_key:
            municipio_by_key[municipio_key] = municipio

    return {
        "version": version,
        "entries": entries,
        "active_entries": active_entries,
        "by_key": {k: v for k, v in by_key.items() if isinstance(v, dict)},
        "by_compact_key": {
            k: v for k, v in by_compact_key.items() if isinstance(v, dict)
        },
        "municipio_by_key": municipio_by_key,
        "ambiguous_keys": sorted(ambiguous_keys),
        "ambiguous_compact_keys": sorted(ambiguous_compact_keys),
    }


def get_nucleo_profile(registry: dict, nucleo: object) -> dict | None:
    key = _normalize_nucleo_key(nucleo)
    if key:
        profile = dict(registry.get("by_key", {}) or {}).get(key)
        if profile:
            return profile

    compact_key = _compact_normalized_key(nucleo)
    if not compact_key:
        return None
    return dict(registry.get("by_compact_key", {}) or {}).get(compact_key)


def save_nucleo_registry(path: Path, entries: List[dict], version: str = "2.0") -> dict:
    registry_path = Path(path)
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    clean_entries: List[dict] = []
    for raw in entries:
        entry = _coerce_entry(raw, now=now)
        if entry:
            clean_entries.append(entry)

    clean_entries.sort(key=lambda item: normalizar_texto(item.get("nucleo", "")))
    payload = {
        "version": version,
        "nucleos": clean_entries,
    }
    with registry_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return load_nucleo_registry(registry_path)


def _resolve_row(
    row: dict,
    registry: dict,
    *,
    fallback_nucleo: object = "",
) -> tuple[dict, dict]:
    resolved = dict(row or {})
    resolved["equipe"] = extrair_primeira_equipe(resolved.get("equipe", ""))

    detected_nucleo = str(
        resolved.get("nucleo_detectado_texto", "") or resolved.get("nucleo", "")
    ).strip()
    detected_municipio = str(
        resolved.get("municipio_detectado_texto", "") or resolved.get("municipio", "")
    ).strip()

    working_nucleo = str(resolved.get("nucleo", "") or "").strip() or detected_nucleo
    working_municipio = str(resolved.get("municipio", "") or "").strip() or detected_municipio

    if not working_nucleo:
        working_nucleo = str(fallback_nucleo or "").strip()

    resolved["nucleo_detectado_texto"] = detected_nucleo
    resolved["municipio_detectado_texto"] = detected_municipio

    lookup_nucleo = working_nucleo
    if _looks_like_logradouro(working_nucleo) and fallback_nucleo:
        lookup_nucleo = str(fallback_nucleo or "").strip() or working_nucleo

    profile = get_nucleo_profile(registry, lookup_nucleo)
    mismatch = False

    if profile:
        official_nucleo = str(profile.get("nucleo", "") or "").strip()
        official_municipio = str(profile.get("municipio", "") or "").strip()
        resolved["nucleo_oficial"] = official_nucleo
        resolved["municipio_oficial"] = official_municipio
        resolved["nucleo_status_cadastro"] = "cadastrado"
        if official_nucleo:
            resolved["nucleo"] = official_nucleo
        else:
            resolved["nucleo"] = working_nucleo
        if official_municipio:
            resolved["municipio"] = official_municipio
            compare_municipio = detected_municipio or working_municipio
            if compare_municipio and normalizar_texto(compare_municipio) != normalizar_texto(official_municipio):
                mismatch = True
        else:
            resolved["municipio"] = working_municipio
    else:
        resolved["nucleo_oficial"] = ""
        resolved["municipio_oficial"] = ""
        resolved["nucleo_status_cadastro"] = "nao_cadastrado"
        resolved["nucleo"] = working_nucleo
        resolved["municipio"] = working_municipio

    meta = {
        "nucleo_detectado_texto": detected_nucleo,
        "municipio_detectado_texto": detected_municipio,
        "nucleo_final": str(resolved.get("nucleo", "") or "").strip(),
        "municipio_final": str(resolved.get("municipio", "") or "").strip(),
        "nucleo_oficial": str(resolved.get("nucleo_oficial", "") or "").strip(),
        "municipio_oficial": str(resolved.get("municipio_oficial", "") or "").strip(),
        "status_cadastro": str(resolved.get("nucleo_status_cadastro", "") or "").strip(),
        "municipio_divergente": mismatch,
    }
    return resolved, meta


def reconcile_rows_with_registry(rows: List[dict], registry: dict) -> List[dict]:
    out: List[dict] = []
    metas: List[dict] = []
    fallback_nucleo = ""
    unresolved_municipios: Dict[str, dict] = {}

    for idx, raw in enumerate(rows or []):
        resolved, meta = _resolve_row(raw, registry, fallback_nucleo=fallback_nucleo)
        if meta.get("nucleo_final"):
            fallback_nucleo = meta["nucleo_final"]
        out.append(resolved)
        metas.append(meta)

        if str(meta.get("status_cadastro", "") or "").strip() != "nao_cadastrado":
            continue

        final_nucleo = str(meta.get("nucleo_final", "") or "").strip()
        municipio_final = str(meta.get("municipio_final", "") or "").strip()
        key = _normalize_nucleo_key(final_nucleo)
        if not key:
            continue

        item = unresolved_municipios.setdefault(
            key,
            {
                "municipios": {},
                "row_indexes": [],
            },
        )
        municipio_key = _normalize_nucleo_key(municipio_final)
        if municipio_key:
            item["municipios"][municipio_key] = municipio_final
        item["row_indexes"].append(idx)

    for item in unresolved_municipios.values():
        if len(item.get("municipios", {})) <= 1:
            continue
        for idx in item.get("row_indexes", []):
            try:
                out[idx]["municipio"] = ""
                out[idx]["municipio_oficial"] = ""
                out[idx]["municipio_conflitante_sem_cadastro"] = "sim"
            except Exception:
                continue

    return out


def reconcile_parsed_with_registry(parsed: dict, registry: dict) -> dict:
    data = deepcopy(parsed or {})

    summary = {
        "version": str(registry.get("version", "2.0") or "2.0"),
        "nucleos_cadastrados": [],
        "nucleos_nao_cadastrados": [],
        "divergencias_municipio": [],
        "conflitos_nucleo_sem_cadastro": [],
    }

    root_row = {
        "nucleo": data.get("nucleo", ""),
        "municipio": data.get("municipio", ""),
        "logradouro": data.get("logradouro", ""),
        "equipe": data.get("equipe", ""),
        "nucleo_detectado_texto": data.get("nucleo_detectado_texto", ""),
        "municipio_detectado_texto": data.get("municipio_detectado_texto", ""),
    }
    root_resolved, root_meta = _resolve_row(root_row, registry)
    for field in (
        "nucleo",
        "municipio",
        "logradouro",
        "equipe",
        "nucleo_detectado_texto",
        "municipio_detectado_texto",
        "nucleo_oficial",
        "municipio_oficial",
        "nucleo_status_cadastro",
    ):
        if field in root_resolved:
            data[field] = root_resolved.get(field, "")

    unresolved_municipios: Dict[str, dict] = {}

    for bucket in ("frentes", "execucao", "ocorrencias", "observacoes", "servicos_nao_mapeados"):
        rows = list(data.get(bucket, []) or [])
        resolved_rows: List[dict] = []
        fallback_nucleo = str(data.get("nucleo", "") or "").strip()

        for idx, row in enumerate(rows):
            if not isinstance(row, dict):
                resolved_rows.append(row)
                continue

            resolved, meta = _resolve_row(row, registry, fallback_nucleo=fallback_nucleo)
            if meta.get("nucleo_final"):
                fallback_nucleo = meta["nucleo_final"]

            final_nucleo = str(meta.get("nucleo_final", "") or "").strip()
            status_cadastro = str(meta.get("status_cadastro", "") or "").strip()
            municipio_final = str(meta.get("municipio_final", "") or "").strip()

            if status_cadastro == "cadastrado" and final_nucleo:
                if all(normalizar_texto(x) != normalizar_texto(final_nucleo) for x in summary["nucleos_cadastrados"]):
                    summary["nucleos_cadastrados"].append(final_nucleo)
            elif final_nucleo:
                if all(normalizar_texto(x) != normalizar_texto(final_nucleo) for x in summary["nucleos_nao_cadastrados"]):
                    summary["nucleos_nao_cadastrados"].append(final_nucleo)

            if meta.get("municipio_divergente") and meta.get("municipio_oficial"):
                summary["divergencias_municipio"].append(
                    {
                        "bucket": bucket,
                        "indice": idx,
                        "nucleo_detectado_texto": meta.get("nucleo_detectado_texto", ""),
                        "municipio_detectado_texto": meta.get("municipio_detectado_texto", ""),
                        "nucleo_oficial": meta.get("nucleo_oficial", ""),
                        "municipio_oficial": meta.get("municipio_oficial", ""),
                    }
                )

            if status_cadastro == "nao_cadastrado" and final_nucleo:
                key = _normalize_nucleo_key(final_nucleo)
                if key:
                    item = unresolved_municipios.setdefault(
                        key,
                        {
                            "nucleo": final_nucleo,
                            "municipios": {},
                            "row_refs": [],
                        },
                    )
                    municipio_key = _normalize_nucleo_key(municipio_final)
                    if municipio_key:
                        item["municipios"][municipio_key] = municipio_final
                    item["row_refs"].append((bucket, idx))

            resolved_rows.append(resolved)

        data[bucket] = resolved_rows

    conflict_keys = []
    for item in unresolved_municipios.values():
        municipios = list(item.get("municipios", {}).values())
        if len(municipios) <= 1:
            continue
        conflict_keys.append(_normalize_nucleo_key(item.get("nucleo", "")))
        summary["conflitos_nucleo_sem_cadastro"].append(
            {
                "nucleo": item.get("nucleo", ""),
                "municipios_detectados": municipios,
            }
        )
        for bucket, idx in item.get("row_refs", []):
            try:
                data[bucket][idx]["municipio"] = ""
                data[bucket][idx]["municipio_oficial"] = ""
                data[bucket][idx]["municipio_conflitante_sem_cadastro"] = "sim"
            except Exception:
                continue

    if root_meta.get("status_cadastro") == "nao_cadastrado":
        root_key = _normalize_nucleo_key(root_meta.get("nucleo_final", ""))
        if root_key and root_key in conflict_keys:
            data["municipio"] = ""
            data["municipio_oficial"] = ""
            data["municipio_conflitante_sem_cadastro"] = "sim"

    data["nucleo_master_summary"] = summary
    return data



