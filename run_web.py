from __future__ import annotations

import argparse
import os

from config.settings import load_settings
from app.routes.web_app import create_app


def _port_from_env(default_port: int) -> int:
    for env_name in ("PORT", "WEB_PORT"):
        raw_value = str(os.environ.get(env_name, "") or "").strip()
        if not raw_value:
            continue
        try:
            return int(raw_value)
        except ValueError:
            continue
    return default_port


def run(host: str | None = None, port: int | None = None, debug: bool | None = None) -> None:
    settings = load_settings()
    app = create_app(settings=settings)
    resolved_port = port if port is not None else _port_from_env(settings.web_port)
    app.run(
        host=host or settings.web_host,
        port=resolved_port,
        debug=settings.web_debug if debug is None else bool(debug),
    )


def main() -> None:
    settings = load_settings()
    default_port = _port_from_env(settings.web_port)
    parser = argparse.ArgumentParser(description="Entrypoint oficial da interface web local")
    parser.add_argument("--host", default=settings.web_host, help="Host para subir o servidor web")
    parser.add_argument("--port", type=int, default=default_port, help="Porta para subir o servidor web")
    parser.add_argument(
        "--debug",
        action="store_true",
        default=settings.web_debug,
        help="Ativa debug do Flask (desligado por padrao)",
    )
    args = parser.parse_args()
    run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
