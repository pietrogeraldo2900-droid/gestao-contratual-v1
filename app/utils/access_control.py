from __future__ import annotations

from typing import Iterable

ROLE_SUPERADMIN = "superadmin"
ROLE_ADMIN = "admin_operacional"
ROLE_OPERADOR = "operador"
ROLE_LEITOR = "leitor"

ALL_ROLES = {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_OPERADOR, ROLE_LEITOR}

PERMISSIONS = {
    "dashboard": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_OPERADOR, ROLE_LEITOR},
    "gerencial": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_LEITOR},
    "institucional": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_LEITOR},
    "entradas": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_OPERADOR},
    "historico": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_OPERADOR},
    "resultados": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_OPERADOR, ROLE_LEITOR},
    "contratos": {ROLE_SUPERADMIN, ROLE_ADMIN},
    "nucleos": {ROLE_SUPERADMIN, ROLE_ADMIN},
    "servicos": {ROLE_SUPERADMIN, ROLE_ADMIN},
    "usuarios_admin": {ROLE_SUPERADMIN},
    "configuracoes": {ROLE_SUPERADMIN},
}


def normalize_role(value: object) -> str:
    raw = str(value or "").strip().lower()
    if raw in ALL_ROLES:
        return raw
    return ROLE_LEITOR


def can_access(role: object, permission: str) -> bool:
    normalized = normalize_role(role)
    allowed = PERMISSIONS.get(str(permission or "").strip().lower())
    if not allowed:
        return True
    return normalized in allowed


def can_access_any(role: object, permissions: Iterable[str]) -> bool:
    for perm in permissions:
        if can_access(role, perm):
            return True
    return False
