from __future__ import annotations

from functools import wraps
from typing import Any, Callable

from flask import g, jsonify, request

from app.services.user_service import UserService
from app.utils.jwt_utils import JwtValidationError, validate_jwt_token


def _extract_bearer_token(auth_header: str) -> str:
    text = str(auth_header or "").strip()
    if not text:
        return ""
    parts = text.split(" ", 1)
    if len(parts) != 2:
        return ""
    if parts[0].lower() != "bearer":
        return ""
    return str(parts[1] or "").strip()


def require_auth(user_service: UserService | None, jwt_secret: str) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any):
            if user_service is None:
                return (
                    jsonify(
                        {
                            "success": False,
                            "data": None,
                            "error": "Autenticacao indisponivel. Habilite DB_ENABLED para usar auth.",
                        }
                    ),
                    503,
                )

            token = _extract_bearer_token(request.headers.get("Authorization", ""))
            if not token:
                return jsonify({"success": False, "data": None, "error": "Token ausente."}), 401

            try:
                claims = validate_jwt_token(token, jwt_secret)
                user_id = int(str(claims.get("sub", "") or "0"))
            except (JwtValidationError, ValueError):
                return jsonify({"success": False, "data": None, "error": "Token invalido."}), 401

            user = user_service.get_user_by_id(user_id)
            if not user:
                return jsonify({"success": False, "data": None, "error": "Usuario nao encontrado."}), 401

            g.current_user = user
            g.jwt_claims = claims
            return func(*args, **kwargs)

        return wrapper

    return decorator
