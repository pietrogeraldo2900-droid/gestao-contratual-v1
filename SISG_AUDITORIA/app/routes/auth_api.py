from __future__ import annotations

from flask import Flask, g, jsonify, request

from app.repositories.user_repository import UserAlreadyExistsError
from app.routes.auth_middleware import require_auth
from app.services.user_service import UserAuthError, UserService, UserValidationError
from app.utils.jwt_utils import generate_jwt_token


def register_auth_routes(
    app: Flask,
    user_service: UserService | None,
    jwt_secret: str,
    jwt_exp_minutes: int,
) -> None:
    @app.post("/auth/register")
    def auth_register():
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

        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            payload = request.form.to_dict()

        email = str(payload.get("email", "") or "")
        password = str(payload.get("password", "") or "")

        try:
            user = user_service.register_user(email=email, password=password)
        except UserValidationError as exc:
            return jsonify({"success": False, "data": None, "error": str(exc)}), 400
        except UserAlreadyExistsError as exc:
            return jsonify({"success": False, "data": None, "error": str(exc)}), 409
        except Exception as exc:
            app.logger.exception("Erro ao registrar usuario")
            return jsonify({"success": False, "data": None, "error": "Falha ao registrar usuario.", "detail": str(exc)}), 500

        return jsonify({"success": True, "data": user}), 201

    @app.post("/auth/login")
    def auth_login():
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

        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            payload = request.form.to_dict()

        email = str(payload.get("email", "") or "")
        password = str(payload.get("password", "") or "")

        try:
            user = user_service.authenticate_user(email=email, password=password)
        except (UserValidationError, UserAuthError) as exc:
            return jsonify({"success": False, "data": None, "error": str(exc)}), 401
        except Exception as exc:
            app.logger.exception("Erro ao autenticar usuario")
            return jsonify({"success": False, "data": None, "error": "Falha no login.", "detail": str(exc)}), 500

        token = generate_jwt_token(
            user_id=int(user.get("id", 0) or 0),
            email=str(user.get("email", "") or ""),
            secret=jwt_secret,
            expires_minutes=jwt_exp_minutes,
        )
        return jsonify(
            {
                "success": True,
                "data": {
                    "token": token,
                    "token_type": "Bearer",
                    "expires_in_minutes": jwt_exp_minutes,
                    "user": user,
                },
            }
        ), 200

    @app.get("/auth/me")
    @require_auth(user_service, jwt_secret)
    def auth_me():
        return jsonify({"success": True, "data": g.current_user}), 200
