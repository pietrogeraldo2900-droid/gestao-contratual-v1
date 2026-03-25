from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, List, Optional

from app.database.connection import DatabaseManager


def _dict_row_factory():
    try:
        from psycopg.rows import dict_row

        return dict_row
    except Exception:
        return None


def _safe_text(value: object) -> str:
    return str(value or "").strip()


def _parse_date(value: object) -> date | None:
    raw = _safe_text(value)
    if not raw:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            continue
    return None


def _parse_number(value: object) -> float:
    raw = _safe_text(value).replace(".", "").replace(",", ".")
    if not raw:
        return 0.0
    try:
        return float(raw)
    except Exception:
        return 0.0


def _display_number(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _normalize(value: object) -> str:
    return _safe_text(value).lower()


def _pick_first_text(*values: object) -> str:
    for value in values:
        text = _safe_text(value)
        if text:
            return text
    return ""


def _match(value: object, probe: str) -> bool:
    if not probe:
        return True
    return probe in _normalize(value)


def _chart_items(rows: Iterable[dict[str, Any]], *, unit_field: str = "") -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    max_value = 0.0
    for idx, row in enumerate(rows, start=1):
        label = _safe_text(row.get("label", "")) or "-"
        value = float(row.get("value", 0) or 0)
        unit = _safe_text(row.get(unit_field, "")) if unit_field else ""
        max_value = max(max_value, value)
        value_fmt = _display_number(value)
        value_display = f"{value_fmt} {unit}".strip()
        parsed.append(
            {
                "rank": idx,
                "label": label,
                "value": value,
                "value_fmt": value_fmt,
                "value_display": value_display,
                "unit": unit,
                "meta": "",
            }
        )
    if max_value <= 0:
        max_value = 1.0
    for item in parsed:
        val = float(item["value"] or 0)
        item["width_pct"] = max(10.0, round((val / max_value) * 100.0, 1)) if val > 0 else 0
    return parsed


@dataclass(frozen=True)
class _Filters:
    obra_from: date | None
    obra_to: date | None
    processed_from: date | None
    processed_to: date | None
    nucleo: str
    municipio: str
    equipe: str
    status: str
    alertas: str
    top_n: int


class ManagementRepository:
    def __init__(self, db: Optional[DatabaseManager], master_dir: Path):
        self._db = db
        self._master_dir = Path(master_dir)

    def _pick_csv(self, preferred: str, fallback: str) -> Path | None:
        p1 = self._master_dir / preferred
        if p1.exists() and p1.is_file():
            return p1
        p2 = self._master_dir / fallback
        if p2.exists() and p2.is_file():
            return p2
        return None

    def _read_csv(self, path: Path | None) -> list[dict[str, str]]:
        if path is None or not path.exists():
            return []
        for encoding in ("utf-8-sig", "latin-1"):
            try:
                with path.open("r", encoding=encoding, newline="") as fp:
                    return [dict(row) for row in csv.DictReader(fp)]
            except Exception:
                continue
        return []

    def _load_rows_from_master_csv(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        exec_csv = self._pick_csv("base_mestra_execucao.csv", "execucao.csv")
        frentes_csv = self._pick_csv("base_mestra_frentes.csv", "frentes.csv")
        ocorr_csv = self._pick_csv("base_mestra_ocorrencias.csv", "ocorrencias.csv")

        exec_rows: list[dict[str, Any]] = []
        for row in self._read_csv(exec_csv):
            exec_rows.append(
                {
                    "id_item": _safe_text(row.get("id_item")),
                    "id_frente": _safe_text(row.get("id_frente")),
                    "data_referencia": _parse_date(row.get("data_referencia") or row.get("data")),
                    "nucleo": _safe_text(row.get("nucleo")),
                    "nucleo_oficial": _safe_text(row.get("nucleo_oficial")),
                    "municipio": _safe_text(row.get("municipio")),
                    "municipio_oficial": _safe_text(row.get("municipio_oficial")),
                    "equipe": _safe_text(row.get("equipe")),
                    "servico_oficial": _safe_text(row.get("servico_oficial")),
                    "servico_normalizado": _safe_text(row.get("servico_normalizado")),
                    "servico_bruto": _safe_text(row.get("servico_bruto")),
                    "item_normalizado": _safe_text(row.get("item_normalizado")),
                    "item_original": _safe_text(row.get("item_original")),
                    "categoria": _safe_text(row.get("categoria")),
                    "categoria_item": _safe_text(row.get("categoria_item")),
                    "quantidade": _parse_number(row.get("quantidade")),
                    "unidade": _safe_text(row.get("unidade")),
                }
            )

        frentes_rows: list[dict[str, Any]] = []
        for row in self._read_csv(frentes_csv):
            frentes_rows.append(
                {
                    "id_frente": _safe_text(row.get("id_frente")),
                    "data_referencia": _parse_date(row.get("data_referencia") or row.get("data")),
                    "nucleo": _safe_text(row.get("nucleo")),
                    "nucleo_oficial": _safe_text(row.get("nucleo_oficial")),
                    "municipio": _safe_text(row.get("municipio")),
                    "municipio_oficial": _safe_text(row.get("municipio_oficial")),
                    "equipe": _safe_text(row.get("equipe")),
                    "status_frente": _safe_text(row.get("status_frente")),
                }
            )

        ocorr_rows: list[dict[str, Any]] = []
        for row in self._read_csv(ocorr_csv):
            ocorr_rows.append(
                {
                    "id_ocorrencia": _safe_text(row.get("id_ocorrencia")),
                    "id_frente": _safe_text(row.get("id_frente")),
                    "data_referencia": _parse_date(row.get("data_referencia") or row.get("data")),
                    "nucleo": _safe_text(row.get("nucleo")),
                    "nucleo_oficial": _safe_text(row.get("nucleo_oficial")),
                    "municipio": _safe_text(row.get("municipio")),
                    "municipio_oficial": _safe_text(row.get("municipio_oficial")),
                    "equipe": _safe_text(row.get("equipe")),
                    "tipo_ocorrencia": _safe_text(row.get("tipo_ocorrencia")),
                    "descricao": _safe_text(row.get("descricao")),
                }
            )

        return exec_rows, frentes_rows, ocorr_rows

    def _sync_master_tables(self) -> dict[str, int]:
        if self._db is None:
            raise RuntimeError("database_manager_unavailable")

        exec_csv = self._pick_csv("base_mestra_execucao.csv", "execucao.csv")
        frentes_csv = self._pick_csv("base_mestra_frentes.csv", "frentes.csv")
        ocorr_csv = self._pick_csv("base_mestra_ocorrencias.csv", "ocorrencias.csv")

        exec_rows = self._read_csv(exec_csv)
        frentes_rows = self._read_csv(frentes_csv)
        ocorr_rows = self._read_csv(ocorr_csv)

        exec_values: list[tuple[Any, ...]] = []
        for row in exec_rows:
            source_uid = hashlib.sha1(
                "|".join(
                    [
                        _safe_text(row.get("id_item")),
                        _safe_text(row.get("id_frente")),
                        _safe_text(row.get("arquivo_origem")),
                        _safe_text(row.get("data_referencia")),
                        _safe_text(row.get("nucleo")),
                        _safe_text(row.get("equipe")),
                        _safe_text(row.get("item_original")),
                    ]
                ).encode("utf-8", errors="ignore")
            ).hexdigest()
            exec_values.append(
                (
                    source_uid,
                    _safe_text(row.get("id_item")),
                    _safe_text(row.get("id_frente")),
                    _parse_date(row.get("data_referencia") or row.get("data")),
                    _safe_text(row.get("contrato")),
                    _safe_text(row.get("programa")),
                    _safe_text(row.get("nucleo")),
                    _safe_text(row.get("logradouro")),
                    _safe_text(row.get("municipio")),
                    _safe_text(row.get("equipe")),
                    _safe_text(row.get("servico_oficial")),
                    _safe_text(row.get("servico_normalizado")),
                    _safe_text(row.get("servico_bruto")),
                    _safe_text(row.get("item_normalizado")),
                    _safe_text(row.get("item_original")),
                    _safe_text(row.get("categoria")),
                    _safe_text(row.get("categoria_item")),
                    _parse_number(row.get("quantidade")),
                    _safe_text(row.get("unidade")),
                    _safe_text(row.get("arquivo_origem")),
                    _safe_text(row.get("nucleo_oficial")),
                    _safe_text(row.get("municipio_oficial")),
                    _safe_text(row.get("nucleo_status_cadastro")),
                )
            )

        frentes_values: list[tuple[Any, ...]] = []
        for row in frentes_rows:
            source_uid = hashlib.sha1(
                "|".join(
                    [
                        _safe_text(row.get("id_frente")),
                        _safe_text(row.get("arquivo_origem")),
                        _safe_text(row.get("data_referencia")),
                        _safe_text(row.get("nucleo")),
                        _safe_text(row.get("equipe")),
                        _safe_text(row.get("frente")),
                    ]
                ).encode("utf-8", errors="ignore")
            ).hexdigest()
            frentes_values.append(
                (
                    source_uid,
                    _safe_text(row.get("id_frente")),
                    _parse_date(row.get("data_referencia") or row.get("data")),
                    _safe_text(row.get("contrato")),
                    _safe_text(row.get("programa")),
                    _safe_text(row.get("nucleo")),
                    _safe_text(row.get("equipe")),
                    _safe_text(row.get("logradouro")),
                    _safe_text(row.get("municipio")),
                    _safe_text(row.get("status_frente")),
                    _safe_text(row.get("frente")),
                    _safe_text(row.get("arquivo_origem")),
                    _safe_text(row.get("nucleo_oficial")),
                    _safe_text(row.get("municipio_oficial")),
                    _safe_text(row.get("nucleo_status_cadastro")),
                )
            )

        ocorr_values: list[tuple[Any, ...]] = []
        for row in ocorr_rows:
            source_uid = hashlib.sha1(
                "|".join(
                    [
                        _safe_text(row.get("id_ocorrencia")),
                        _safe_text(row.get("id_frente")),
                        _safe_text(row.get("arquivo_origem")),
                        _safe_text(row.get("data_referencia")),
                        _safe_text(row.get("nucleo")),
                        _safe_text(row.get("equipe")),
                        _safe_text(row.get("tipo_ocorrencia")),
                    ]
                ).encode("utf-8", errors="ignore")
            ).hexdigest()
            ocorr_values.append(
                (
                    source_uid,
                    _safe_text(row.get("id_ocorrencia")),
                    _safe_text(row.get("id_frente")),
                    _parse_date(row.get("data_referencia") or row.get("data")),
                    _safe_text(row.get("contrato")),
                    _safe_text(row.get("programa")),
                    _safe_text(row.get("nucleo")),
                    _safe_text(row.get("equipe")),
                    _safe_text(row.get("logradouro")),
                    _safe_text(row.get("municipio")),
                    _safe_text(row.get("tipo_ocorrencia")),
                    _safe_text(row.get("descricao")),
                    _safe_text(row.get("impacto_producao")),
                    _safe_text(row.get("arquivo_origem")),
                    _safe_text(row.get("nucleo_oficial")),
                    _safe_text(row.get("municipio_oficial")),
                    _safe_text(row.get("nucleo_status_cadastro")),
                )
            )

        with self._db.connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE management_execucao, management_frentes, management_ocorrencias")
                    if exec_values:
                        cur.executemany(
                            """
                            INSERT INTO management_execucao (
                                source_uid, id_item, id_frente, data_referencia, contrato, programa,
                                nucleo, logradouro, municipio, equipe, servico_oficial,
                                servico_normalizado, servico_bruto, item_normalizado, item_original,
                                categoria, categoria_item, quantidade, unidade, arquivo_origem,
                                nucleo_oficial, municipio_oficial, nucleo_status_cadastro
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s
                            )
                            """,
                            exec_values,
                        )
                    if frentes_values:
                        cur.executemany(
                            """
                            INSERT INTO management_frentes (
                                source_uid, id_frente, data_referencia, contrato, programa, nucleo,
                                equipe, logradouro, municipio, status_frente, frente, arquivo_origem,
                                nucleo_oficial, municipio_oficial, nucleo_status_cadastro
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            """,
                            frentes_values,
                        )
                    if ocorr_values:
                        cur.executemany(
                            """
                            INSERT INTO management_ocorrencias (
                                source_uid, id_ocorrencia, id_frente, data_referencia, contrato, programa,
                                nucleo, equipe, logradouro, municipio, tipo_ocorrencia, descricao,
                                impacto_producao, arquivo_origem, nucleo_oficial, municipio_oficial,
                                nucleo_status_cadastro
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s
                            )
                            """,
                            ocorr_values,
                        )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return {
            "execucao": len(exec_values),
            "frentes": len(frentes_values),
            "ocorrencias": len(ocorr_values),
        }

    def _load_rows_from_database(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        if self._db is None:
            raise RuntimeError("database_manager_unavailable")

        exec_rows: list[dict[str, Any]] = []
        frentes_rows: list[dict[str, Any]] = []
        ocorr_rows: list[dict[str, Any]] = []

        cursor_kwargs = {}
        dict_factory = _dict_row_factory()
        if dict_factory is not None:
            cursor_kwargs["row_factory"] = dict_factory

        with self._db.connection() as conn:
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(
                    """
                    SELECT
                        id_item,
                        id_frente,
                        data_referencia,
                        nucleo,
                        nucleo_oficial,
                        municipio,
                        municipio_oficial,
                        equipe,
                        servico_oficial,
                        servico_normalizado,
                        servico_bruto,
                        item_normalizado,
                        item_original,
                        categoria,
                        categoria_item,
                        quantidade,
                        unidade
                    FROM management_execucao
                    """
                )
                exec_rows = cur.fetchall() or []
                cur.execute(
                    """
                    SELECT
                        id_frente,
                        data_referencia,
                        nucleo,
                        nucleo_oficial,
                        municipio,
                        municipio_oficial,
                        equipe,
                        status_frente
                    FROM management_frentes
                    """
                )
                frentes_rows = cur.fetchall() or []
                cur.execute(
                    """
                    SELECT
                        id_ocorrencia,
                        id_frente,
                        data_referencia,
                        nucleo,
                        nucleo_oficial,
                        municipio,
                        municipio_oficial,
                        equipe,
                        tipo_ocorrencia,
                        descricao
                    FROM management_ocorrencias
                    """
                )
                ocorr_rows = cur.fetchall() or []

        return exec_rows, frentes_rows, ocorr_rows

    def _parse_filters(self, raw_filters: dict[str, object] | None) -> _Filters:
        raw = raw_filters or {}
        top_n_raw = _safe_text(raw.get("top_n")) or "10"
        try:
            top_n = int(top_n_raw)
        except Exception:
            top_n = 10
        top_n = max(3, min(top_n, 50))
        return _Filters(
            obra_from=_parse_date(raw.get("obra_from")),
            obra_to=_parse_date(raw.get("obra_to")),
            processed_from=_parse_date(raw.get("processed_from")),
            processed_to=_parse_date(raw.get("processed_to")),
            nucleo=_normalize(raw.get("nucleo")),
            municipio=_normalize(raw.get("municipio")),
            equipe=_normalize(raw.get("equipe")),
            status=_normalize(raw.get("status")),
            alertas=_normalize(raw.get("alertas")),
            top_n=top_n,
        )

    def _filter_by_date(self, dt: date | None, filters: _Filters) -> bool:
        if filters.obra_from and (not dt or dt < filters.obra_from):
            return False
        if filters.obra_to and (not dt or dt > filters.obra_to):
            return False
        if filters.processed_from and (not dt or dt < filters.processed_from):
            return False
        if filters.processed_to and (not dt or dt > filters.processed_to):
            return False
        return True

    def _filter_row(self, row: dict[str, Any], filters: _Filters) -> bool:
        dt = row.get("data_referencia")
        if not self._filter_by_date(dt, filters):
            return False
        nucleo = _safe_text(row.get("nucleo_oficial")) or _safe_text(row.get("nucleo"))
        municipio = _safe_text(row.get("municipio_oficial")) or _safe_text(row.get("municipio"))
        equipe = _safe_text(row.get("equipe"))
        if not _match(nucleo, filters.nucleo):
            return False
        if not _match(municipio, filters.municipio):
            return False
        if not _match(equipe, filters.equipe):
            return False
        return True

    def _format_range(self, start: date | None, end: date | None) -> str:
        if not start and not end:
            return "-"
        if start and end:
            if start == end:
                return start.strftime("%d/%m/%Y")
            return f"{start:%d/%m/%Y} a {end:%d/%m/%Y}"
        if start:
            return f"A partir de {start:%d/%m/%Y}"
        return f"Ate {end:%d/%m/%Y}"

    def build_gerencial_dashboard(self, raw_filters: dict[str, object] | None = None) -> dict[str, Any]:
        filters = self._parse_filters(raw_filters)

        exec_rows: list[dict[str, Any]] = []
        frentes_rows: list[dict[str, Any]] = []
        ocorr_rows: list[dict[str, Any]] = []
        use_csv_fallback = False

        try:
            exec_rows, frentes_rows, ocorr_rows = self._load_rows_from_database()
        except Exception:
            use_csv_fallback = True
            exec_rows, frentes_rows, ocorr_rows = self._load_rows_from_master_csv()

        exec_filtered = [row for row in exec_rows if self._filter_row(row, filters)]
        frentes_filtered = [row for row in frentes_rows if self._filter_row(row, filters)]
        ocorr_filtered = [row for row in ocorr_rows if self._filter_row(row, filters)]

        if filters.status == "erro":
            exec_filtered = []
            frentes_filtered = []
            ocorr_filtered = []

        frentes_com_ocorrencia = {
            _safe_text(row.get("id_frente"))
            for row in ocorr_filtered
            if _safe_text(row.get("id_frente"))
        }
        if filters.alertas == "com_alerta":
            exec_filtered = [
                row
                for row in exec_filtered
                if _safe_text(row.get("id_frente")) and _safe_text(row.get("id_frente")) in frentes_com_ocorrencia
            ]
            frentes_filtered = [
                row
                for row in frentes_filtered
                if _safe_text(row.get("id_frente")) and _safe_text(row.get("id_frente")) in frentes_com_ocorrencia
            ]
        elif filters.alertas == "sem_alerta":
            exec_filtered = [
                row
                for row in exec_filtered
                if not _safe_text(row.get("id_frente")) or _safe_text(row.get("id_frente")) not in frentes_com_ocorrencia
            ]
            frentes_filtered = [
                row
                for row in frentes_filtered
                if not _safe_text(row.get("id_frente")) or _safe_text(row.get("id_frente")) not in frentes_com_ocorrencia
            ]

        total_execucao = len(exec_filtered)
        total_frentes = len(frentes_filtered)
        total_ocorrencias = len(ocorr_filtered)
        volume_total = sum(float(row.get("quantidade", 0) or 0) for row in exec_filtered)

        nucleo_values: dict[str, float] = {}
        equipe_values: dict[str, float] = {}
        categoria_values: dict[str, float] = {}
        servico_values: dict[str, float] = {}
        servico_units: dict[str, set[str]] = {}
        municipio_values: dict[str, float] = {}
        ocorrencia_tipo_values: dict[str, float] = {}
        servicos_set: set[str] = set()
        municipios_set: set[str] = set()

        for row in exec_filtered:
            nucleo = _pick_first_text(row.get("nucleo_oficial"), row.get("nucleo")) or "-"
            equipe = _safe_text(row.get("equipe")) or "-"
            municipio = _pick_first_text(row.get("municipio_oficial"), row.get("municipio")) or "-"
            categoria = _pick_first_text(row.get("categoria"), row.get("categoria_item")) or "-"
            servico = _pick_first_text(
                row.get("servico_oficial"),
                row.get("servico_normalizado"),
                row.get("servico_bruto"),
                row.get("item_normalizado"),
                row.get("item_original"),
            ) or "-"
            quantidade = float(row.get("quantidade", 0) or 0)
            unidade = _safe_text(row.get("unidade"))
            peso = quantidade if quantidade > 0 else 1.0

            nucleo_values[nucleo] = nucleo_values.get(nucleo, 0) + peso
            equipe_values[equipe] = equipe_values.get(equipe, 0) + peso
            categoria_values[categoria] = categoria_values.get(categoria, 0) + peso
            servico_values[servico] = servico_values.get(servico, 0) + peso
            municipio_values[municipio] = municipio_values.get(municipio, 0) + peso
            if unidade:
                units = servico_units.get(servico)
                if units is None:
                    units = set()
                    servico_units[servico] = units
                units.add(unidade)
            servicos_set.add(servico)
            municipios_set.add(municipio)

        for row in ocorr_filtered:
            tipo = _pick_first_text(row.get("tipo_ocorrencia"), row.get("descricao")) or "-"
            ocorrencia_tipo_values[tipo] = ocorrencia_tipo_values.get(tipo, 0) + 1

        top_n = filters.top_n
        nucleo_rows = [
            {"label": label, "value": value}
            for label, value in sorted(nucleo_values.items(), key=lambda item: item[1], reverse=True)[:top_n]
        ]
        equipe_rows = [
            {"label": label, "value": value}
            for label, value in sorted(equipe_values.items(), key=lambda item: item[1], reverse=True)[:top_n]
        ]
        categoria_rows = [
            {"label": label, "value": value}
            for label, value in sorted(categoria_values.items(), key=lambda item: item[1], reverse=True)[:top_n]
        ]
        servico_rows = [
            {
                "label": label,
                "value": value,
                "unit": next(iter(servico_units.get(label, set())), "") if len(servico_units.get(label, set())) == 1 else "",
            }
            for label, value in sorted(servico_values.items(), key=lambda item: item[1], reverse=True)[:top_n]
        ]
        municipio_rows = [
            {"label": label, "value": value}
            for label, value in sorted(municipio_values.items(), key=lambda item: item[1], reverse=True)[:top_n]
        ]
        ocorrencia_tipo_rows = [
            {"label": label, "value": value}
            for label, value in sorted(ocorrencia_tipo_values.items(), key=lambda item: item[1], reverse=True)[:top_n]
        ]

        data_refs = [row.get("data_referencia") for row in exec_filtered if row.get("data_referencia")]
        data_refs = [dt for dt in data_refs if isinstance(dt, date)]
        obra_start = min(data_refs) if data_refs else None
        obra_end = max(data_refs) if data_refs else None

        sem_producao = sum(
            1
            for row in frentes_filtered
            if "sem_producao" in _normalize(row.get("status_frente"))
            or "sem producao" in _normalize(row.get("status_frente"))
        )
        com_producao = max(total_frentes - sem_producao, 0)

        total_mapeados = sum(1 for row in exec_filtered if _safe_text(row.get("servico_oficial")))
        total_nao_mapeados = max(total_execucao - total_mapeados, 0)
        percentual_mapeado = (100.0 * total_mapeados / total_execucao) if total_execucao else 0.0
        percentual_nao_mapeado = 100.0 - percentual_mapeado if total_execucao else 0.0

        has_data = bool(total_execucao or total_frentes or total_ocorrencias)
        total_nucleos = len(nucleo_values)
        total_equipes = len(equipe_values)
        processamentos_com_alerta = min(total_execucao, total_ocorrencias)
        processamentos_sem_alerta = max(total_execucao - processamentos_com_alerta, 0)

        return {
            "has_data": has_data,
            "source": "master_csv" if use_csv_fallback else "database",
            "kpis_principais": {
                "total_processamentos": total_execucao,
                "total_execucoes": total_execucao,
                "total_frentes": total_frentes,
                "total_ocorrencias": total_ocorrencias,
                "total_nucleos": total_nucleos,
                "total_equipes": total_equipes,
                "total_municipios": len(municipios_set),
                "total_mapeados": total_mapeados,
                "total_nao_mapeados": total_nao_mapeados,
                "percentual_mapeado": percentual_mapeado,
                "percentual_mapeado_fmt": _display_number(percentual_mapeado),
                "percentual_nao_mapeado": percentual_nao_mapeado,
                "percentual_nao_mapeado_fmt": _display_number(percentual_nao_mapeado),
                "processamentos_com_alerta": processamentos_com_alerta,
                "processamentos_sem_alerta": processamentos_sem_alerta,
                "processamentos_sucesso": total_execucao,
                "processamentos_erro": 0,
            },
            "consolidado_periodo": {
                "runs_consideradas": 1 if has_data else 0,
                "runs_com_dados": 1 if has_data else 0,
                "periodo_obra": self._format_range(obra_start, obra_end),
                "periodo_processamento": self._format_range(obra_start, obra_end),
                "execucao_registros": total_execucao,
                "frentes": total_frentes,
                "frentes_com_producao": com_producao,
                "frentes_sem_producao": sem_producao,
                "ocorrencias": total_ocorrencias,
                "volume_total": volume_total,
                "volume_total_fmt": _display_number(volume_total),
                "nucleos_ativos": total_nucleos,
                "equipes_ativas": total_equipes,
                "servicos_distintos": len(servicos_set),
            },
            "graficos_executivos": {
                "nucleos": {
                    "items": _chart_items(nucleo_rows),
                    "has_data": bool(nucleo_rows),
                    "max_value_fmt": _display_number(max((row["value"] for row in nucleo_rows), default=0)),
                },
                "equipes": {
                    "items": _chart_items(equipe_rows),
                    "has_data": bool(equipe_rows),
                    "max_value_fmt": _display_number(max((row["value"] for row in equipe_rows), default=0)),
                },
                "categorias": {
                    "items": _chart_items(categoria_rows),
                    "has_data": bool(categoria_rows),
                    "max_value_fmt": _display_number(max((row["value"] for row in categoria_rows), default=0)),
                },
                "servicos": {
                    "items": _chart_items(servico_rows, unit_field="unit"),
                    "has_data": bool(servico_rows),
                    "max_value_fmt": _display_number(max((row["value"] for row in servico_rows), default=0)),
                },
                "municipios": {
                    "items": _chart_items(municipio_rows),
                    "has_data": bool(municipio_rows),
                    "max_value_fmt": _display_number(max((row["value"] for row in municipio_rows), default=0)),
                },
                "tipos_ocorrencia": {
                    "items": _chart_items(ocorrencia_tipo_rows),
                    "has_data": bool(ocorrencia_tipo_rows),
                    "max_value_fmt": _display_number(max((row["value"] for row in ocorrencia_tipo_rows), default=0)),
                },
            },
        }
