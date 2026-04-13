from __future__ import annotations

from typing import Iterable

ROLE_SUPERADMIN = "superadmin"
ROLE_ADMIN = "admin_operacional"
ROLE_OPERADOR = "operador"
ROLE_LEITOR = "leitor"
ROLE_CONTRATADA = "contratada"
ROLE_FISCAL = "fiscal"

ALL_ROLES = {
    ROLE_SUPERADMIN,
    ROLE_ADMIN,
    ROLE_OPERADOR,
    ROLE_LEITOR,
    ROLE_CONTRATADA,
    ROLE_FISCAL,
}

PERMISSIONS = {
    "dashboard": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_OPERADOR, ROLE_LEITOR},
    "gerencial": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_LEITOR},
    "institucional": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_LEITOR},
    "entradas": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_OPERADOR},
    "historico": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_OPERADOR},
    "resultados": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_OPERADOR, ROLE_LEITOR},
    "vistorias": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_OPERADOR, ROLE_LEITOR, ROLE_FISCAL},
    "vistorias_edicao": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_OPERADOR},
    "conferencia_operacional": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_OPERADOR, ROLE_FISCAL},
    "conferencia_operacional_execucao": {ROLE_FISCAL},
    "conferencia_contratada": {ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_CONTRATADA},
    "contratos": {ROLE_SUPERADMIN, ROLE_ADMIN},
    "nucleos": {ROLE_SUPERADMIN, ROLE_ADMIN},
    "servicos": {ROLE_SUPERADMIN, ROLE_ADMIN},
    "usuarios_admin": {ROLE_SUPERADMIN, ROLE_ADMIN},
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
        return False
    return normalized in allowed


def can_access_any(role: object, permissions: Iterable[str]) -> bool:
    for perm in permissions:
        if can_access(role, perm):
            return True
    return False
