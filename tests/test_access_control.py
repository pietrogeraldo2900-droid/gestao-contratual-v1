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

