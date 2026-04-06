from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

try:
    import jwt
except Exception:  # pragma: no cover - fallback para ambientes sem dependencia instalada
    jwt = None


class JwtValidationError(ValueError):
    pass


def generate_jwt_token(
    user_id: int,
    email: str,
    secret: str,
    expires_minutes: int = 60,
) -> str:
    if jwt is None:
        raise RuntimeError("Dependencia PyJWT nao instalada no ambiente.")
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(int(user_id)),
        "email": str(email or "").strip().lower(),
        "iat": now,
        "exp": now + timedelta(minutes=max(1, int(expires_minutes))),
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def validate_jwt_token(token: str, secret: str) -> Dict[str, Any]:
    if jwt is None:
        raise JwtValidationError("Dependencia PyJWT nao instalada no ambiente.")
    try:
        decoded = jwt.decode(token, secret, algorithms=["HS256"])
    except jwt.ExpiredSignatureError as exc:
        raise JwtValidationError("Token expirado.") from exc
    except jwt.InvalidTokenError as exc:
        raise JwtValidationError("Token invalido.") from exc

    if not isinstance(decoded, dict):
        raise JwtValidationError("Token invalido.")
    if not str(decoded.get("sub", "") or "").strip():
        raise JwtValidationError("Token invalido: claim 'sub' ausente.")
    return decoded
