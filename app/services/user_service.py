from __future__ import annotations

import re
from datetime import datetime
from typing import Any

try:
    import bcrypt
except Exception:  # pragma: no cover - fallback para ambientes sem dependencia instalada
    bcrypt = None

from app.repositories.user_repository import UserAlreadyExistsError, UserRepository


class UserValidationError(ValueError):
    pass


class UserAuthError(ValueError):
    pass


class UserService:
    def __init__(self, repository: UserRepository):
        self._repository = repository

    def _normalize_email(self, email: str) -> str:
        return str(email or "").strip().lower()

    def _validate_email(self, email: str) -> None:
        if not email:
            raise UserValidationError("Email obrigatorio.")
        if len(email) > 255:
            raise UserValidationError("Email excede 255 caracteres.")
        if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
            raise UserValidationError("Email invalido.")

    def _validate_password(self, password: str) -> None:
        if not password:
            raise UserValidationError("Senha obrigatoria.")
        if len(password) < 8:
            raise UserValidationError("Senha deve ter no minimo 8 caracteres.")
        if len(password) > 128:
            raise UserValidationError("Senha excede 128 caracteres.")

    def _public_user(self, user_row: dict[str, Any]) -> dict[str, Any]:
        created_at = user_row.get("created_at")
        approved_at = user_row.get("approved_at")
        last_login_at = user_row.get("last_login_at")
        authorized_contract_ids: list[int] = []
        raw_contract_ids = user_row.get("authorized_contract_ids", user_row.get("contract_ids", []))
        if isinstance(raw_contract_ids, (list, tuple, set)):
            for item in raw_contract_ids:
                try:
                    contract_id = int(item)
                except Exception:
                    continue
                if contract_id > 0:
                    authorized_contract_ids.append(contract_id)
        return {
            "id": int(user_row.get("id", 0) or 0),
            "email": str(user_row.get("email", "") or ""),
            "role": str(user_row.get("role", "") or ""),
            "status": str(user_row.get("status", "") or ""),
            "contractor_name": str(user_row.get("contractor_name", "") or "").strip(),
            "authorized_contract_ids": authorized_contract_ids,
            "contract_ids": authorized_contract_ids,
            "approved_by": user_row.get("approved_by"),
            "approved_at": approved_at.isoformat() if isinstance(approved_at, datetime) else str(approved_at or ""),
            "last_login_at": last_login_at.isoformat()
            if isinstance(last_login_at, datetime)
            else str(last_login_at or ""),
            "created_at": created_at.isoformat() if isinstance(created_at, datetime) else str(created_at or ""),
        }

    def register_user(self, email: str, password: str) -> dict[str, Any]:
        clean_email = self._normalize_email(email)
        self._validate_email(clean_email)
        self._validate_password(password)
        if bcrypt is None:
            raise RuntimeError("Dependencia bcrypt nao instalada no ambiente.")

        password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        created = self._repository.create_user(clean_email, password_hash, status="pending", role=None)
        return self._public_user(created)

    def authenticate_user(self, email: str, password: str) -> dict[str, Any]:
        clean_email = self._normalize_email(email)
        self._validate_email(clean_email)
        if not str(password or ""):
            raise UserAuthError("Credenciais invalidas.")

        user = self._repository.get_user_by_email(clean_email)
        if not user:
            raise UserAuthError("Credenciais invalidas.")

        status = str(user.get("status", "") or "").strip().lower()
        if status != "active":
            if status == "pending":
                raise UserAuthError("Seu cadastro foi recebido e esta aguardando aprovacao.")
            if status == "rejected":
                raise UserAuthError("Seu cadastro nao foi aprovado. Entre em contato com o administrador.")
            if status == "disabled":
                raise UserAuthError("Sua conta esta desativada no momento. Entre em contato com o administrador.")
            raise UserAuthError("Sua conta nao esta ativa. Entre em contato com o administrador.")

        password_hash = str(user.get("password", "") or "")
        if not password_hash:
            raise UserAuthError("Credenciais invalidas.")
        if bcrypt is None:
            raise RuntimeError("Dependencia bcrypt nao instalada no ambiente.")

        valid = False
        try:
            valid = bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
        except Exception:
            valid = False
        if not valid:
            raise UserAuthError("Credenciais invalidas.")
        try:
            self._repository.update_last_login(int(user.get("id", 0) or 0))
        except Exception:
            pass
        return self._public_user(user)

    def get_user_by_id(self, user_id: int) -> dict[str, Any] | None:
        try:
            uid = int(user_id)
        except Exception:
            return None
        if uid <= 0:
            return None
        user = self._repository.get_user_by_id(uid)
        if not user:
            return None
        status = str(user.get("status", "") or "").strip().lower()
        if status != "active":
            return None
        return self._public_user(user)


__all__ = ["UserAlreadyExistsError", "UserAuthError", "UserService", "UserValidationError"]
