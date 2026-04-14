from app.repositories.management_repository import _parse_number


def test_parse_number_keeps_dot_decimal() -> None:
    assert _parse_number("18.75") == 18.75


def test_parse_number_accepts_comma_decimal() -> None:
    assert _parse_number("18,75") == 18.75


def test_parse_number_accepts_thousand_ptbr() -> None:
    assert _parse_number("1.234,56") == 1234.56
