import pytest

from app.utils.access_control import can_access


def test_access_matrix_basics():
    assert can_access("superadmin", "gerencial")
    assert can_access("admin_operacional", "gerencial")
    assert can_access("leitor", "gerencial")
    assert not can_access("operador", "gerencial")


def test_access_entries_and_results():
    assert can_access("operador", "entradas")
    assert not can_access("leitor", "entradas")
    assert can_access("leitor", "resultados")


def test_access_scoped_profiles():
    assert can_access("contratada", "conferencia_contratada")
    assert not can_access("contratada", "entradas")
    assert not can_access("contratada", "vistorias")
    assert can_access("fiscal", "vistorias")
    assert can_access("fiscal", "conferencia_operacional")
    assert can_access("fiscal", "conferencia_operacional_execucao")
    assert not can_access("admin_operacional", "conferencia_operacional_execucao")
    assert not can_access("leitor", "conferencia_operacional")
    assert not can_access("fiscal", "entradas")


def test_unknown_permission_defaults_to_deny():
    assert not can_access("superadmin", "permissao_inexistente")
