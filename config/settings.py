from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

DEFAULT_LOCAL_SECRET = "sisg-local-dev-secret-key-2026-32-bytes"


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "on", "yes", "sim", "s"}


def _env_int(name: str, default: int, min_value: int | None = None, max_value: int | None = None) -> int:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        value = default
    else:
        try:
            value = int(raw)
        except Exception:
            value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def _build_postgres_dsn() -> str:
    db_host = str(os.environ.get("DB_HOST", "localhost") or "localhost").strip() or "localhost"
    db_port = _env_int("DB_PORT", 5432, min_value=1, max_value=65535)
    db_name = str(os.environ.get("DB_NAME", "sisg") or "sisg").strip() or "sisg"
    db_user = str(os.environ.get("DB_USER", "sisg") or "sisg").strip() or "sisg"
    db_password = str(os.environ.get("DB_PASSWORD", "sisg_local") or "sisg_local")
    db_sslmode = str(os.environ.get("DB_SSLMODE", "prefer") or "prefer").strip() or "prefer"
    return (
        f"postgresql://{quote_plus(db_user)}:{quote_plus(db_password)}"
        f"@{db_host}:{db_port}/{quote_plus(db_name)}?sslmode={quote_plus(db_sslmode)}"
    )


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
    db_enabled: bool
    database_url: str
    db_connect_timeout: int
    db_strict_startup: bool
    contracts_auto_init_schema: bool
    auth_jwt_secret: str
    auth_jwt_exp_minutes: int


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

    web_port = _env_int("WEB_PORT", 5000, min_value=1, max_value=65535)
    database_url = str(os.environ.get("DATABASE_URL", "") or "").strip()
    if not database_url:
        database_url = _build_postgres_dsn()

    return AppSettings(
        base_dir=base,
        secret_key=str(os.environ.get("WEB_APP_SECRET", DEFAULT_LOCAL_SECRET) or DEFAULT_LOCAL_SECRET),
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
        db_enabled=_env_bool("DB_ENABLED", default=False),
        database_url=database_url,
        db_connect_timeout=_env_int("DB_CONNECT_TIMEOUT", 5, min_value=1, max_value=60),
        db_strict_startup=_env_bool("DATABASE_STRICT_STARTUP", default=False),
        contracts_auto_init_schema=_env_bool("CONTRACTS_AUTO_INIT_SCHEMA", default=True),
        auth_jwt_secret=str(
            os.environ.get("AUTH_JWT_SECRET", os.environ.get("WEB_APP_SECRET", DEFAULT_LOCAL_SECRET))
            or DEFAULT_LOCAL_SECRET
        ),
        auth_jwt_exp_minutes=_env_int("AUTH_JWT_EXP_MINUTES", 60, min_value=5, max_value=1440),
    )
