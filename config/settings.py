from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "on", "yes", "sim", "s"}


@dataclass(frozen=True)
class AppSettings:
    base_dir: Path
    secret_key: str
    config_dir: Path
    data_dir: Path
    outputs_root: Path
    master_dir: Path
    history_file: Path
    draft_dir: Path
    nucleo_reference_file: Path
    service_dictionary_csv: Path
    service_dictionary_v2_json: Path
    base_gerencial_template: Path
    web_host: str
    web_port: int
    web_debug: bool


def load_settings(base_dir: Path | None = None) -> AppSettings:
    base = Path(base_dir) if base_dir else Path(__file__).resolve().parents[1]
    config_dir = base / "config"
    data_dir = base / "data"
    outputs_root = Path(os.environ.get("OUTPUTS_ROOT", str(base / "saidas")))
    master_dir = Path(os.environ.get("MASTER_DIR", str(base / "BASE_MESTRA")))
    history_file = Path(os.environ.get("HISTORY_FILE", str(data_dir / "runtime" / "processing_history.csv")))
    draft_dir = Path(os.environ.get("DRAFT_DIR", str(data_dir / "drafts" / "web")))
    nucleo_reference_file = Path(
        os.environ.get("NUCLEO_REFERENCE_FILE", str(config_dir / "nucleo_reference.json"))
    )
    service_dictionary_csv = Path(
        os.environ.get("SERVICE_DICTIONARY_CSV", str(config_dir / "service_dictionary.csv"))
    )
    service_dictionary_v2_json = Path(
        os.environ.get("SERVICE_DICTIONARY_V2_JSON", str(config_dir / "service_dictionary_v2.json"))
    )
    base_gerencial_template = Path(
        os.environ.get("BASE_GERENCIAL_TEMPLATE", str(config_dir / "base_gerencial_template.xlsx"))
    )

    web_port_raw = str(os.environ.get("WEB_PORT", "5000") or "5000").strip()
    try:
        web_port = int(web_port_raw)
    except Exception:
        web_port = 5000

    return AppSettings(
        base_dir=base,
        secret_key=str(os.environ.get("WEB_APP_SECRET", "sabesp-relatorios-local") or "sabesp-relatorios-local"),
        config_dir=config_dir,
        data_dir=data_dir,
        outputs_root=outputs_root,
        master_dir=master_dir,
        history_file=history_file,
        draft_dir=draft_dir,
        nucleo_reference_file=nucleo_reference_file,
        service_dictionary_csv=service_dictionary_csv,
        service_dictionary_v2_json=service_dictionary_v2_json,
        base_gerencial_template=base_gerencial_template,
        web_host=str(os.environ.get("WEB_HOST", "127.0.0.1") or "127.0.0.1"),
        web_port=web_port,
        web_debug=_env_bool("WEB_DEBUG", default=False),
    )
