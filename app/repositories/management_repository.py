from __future__ import annotations

import csv
import hashlib
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
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


def _strip_prefix_symbols(value: object) -> str:
    text = _safe_text(value)
    if not text:
        return ""
    text = re.sub(r"^[^0-9A-Za-zÀ-ÿ]+", "", text).strip()
    return re.sub(r"\s+", " ", text).strip()


def _normalize_lookup(value: object) -> str:
    text = _strip_prefix_symbols(value).lower()
    text = "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


_SERVICE_EQUIVALENCE_MAP: dict[str, str] = {
    "prolongamento rede agua": "Prolongamento de rede de água",
    "prolongamento de rede agua": "Prolongamento de rede de água",
    "prolongamento de rede de agua": "Prolongamento de rede de água",
    "prolongamento rede": "Prolongamento de rede de água",
    "prolongamento de rede": "Prolongamento de rede de água",
    "ramais agua": "Execução de ramais de água",
    "ramais de agua": "Execução de ramais de água",
    "ramais esgoto": "Execução de ramais de esgoto",
    "ramais de esgoto": "Execução de ramais de esgoto",
    "intradomiciliares": "Ligações intradomiciliares",
    "ligacoes intradomiciliares": "Ligações intradomiciliares",
    "hidrometro": "Instalação de hidrômetros",
    "hidrometros": "Instalação de hidrômetros",
    "hidrometros instalados": "Instalação de hidrômetros",
    "caixas uma": "Instalação de caixas UMA",
    "instalacao de caixas uma": "Instalação de caixas UMA",
    "instalacao de caixa uma": "Instalação de caixas UMA",
    "caixas uma instaladas": "Instalação de caixas UMA",
    "embutida": "Instalação de caixas UMA",
    "mureta": "Instalação de caixas UMA",
    "interligacao": "Execução de interligação de rede",
    "interligacoes de rede": "Execução de interligação de rede",
    "interligacao de rede": "Execução de interligação de rede",
    "interligacoes executadas": "Execução de interligação de rede",
    "interligacao 63 x 63": "Execução de interligação de rede",
    "concretagem de vala realizada": "Concretagem de vala",
    "servico complementar": "Instalação de caixa de inspeção",
}


def _canonical_service_label(value: object) -> str:
    text = _strip_prefix_symbols(value)
    if not text:
        return "-"
    lookup = _normalize_lookup(text)
    mapped = _SERVICE_EQUIVALENCE_MAP.get(lookup)
    if mapped:
        return mapped
    if lookup.startswith("instalacao de caixas uma") or lookup.startswith("instalacao de caixa uma"):
        return "Instalação de caixas UMA"
    if lookup.startswith("corte de pavimento com serra clip"):
        return "Corte de pavimento com serra clip"
    if lookup.startswith("prolongamento rede") or lookup.startswith("prolongamento de rede"):
        return "Prolongamento de rede de água"
    if lookup.startswith("interligacao"):
        return "Execução de interligação de rede"
    return text


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
                    "contrato": _safe_text(row.get("contrato")),
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
                    "contrato": _safe_text(row.get("contrato")),
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
                    "contrato": _safe_text(row.get("contrato")),
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

        # Evita truncar as tabelas quando ainda nao ha base mestre consolidada.
        if not exec_rows and not frentes_rows and not ocorr_rows:
            return {
                "execucao": 0,
                "frentes": 0,
                "ocorrencias": 0,
                "skipped": 1,
            }

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
                            ON CONFLICT (source_uid) DO NOTHING
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
                            ON CONFLICT (source_uid) DO NOTHING
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
                            ON CONFLICT (source_uid) DO NOTHING
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

    def sync_master_tables(self) -> dict[str, int]:
        return self._sync_master_tables()

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
                        contrato,
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
                        contrato,
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
                        contrato,
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

    def _load_bi_mvp_snapshot(self, filters: _Filters) -> dict[str, Any]:
        snapshot: dict[str, Any] = {
            "enabled": False,
            "has_data": False,
            "error": "",
            "kpis": {
                "registros": 0,
                "registros_fmt": "0",
                "volume_total": 0.0,
                "volume_total_fmt": "0",
                "dias": 0,
                "percentual_mapeado": 0.0,
                "percentual_mapeado_fmt": "0",
                "nao_mapeados": 0,
            },
            "top_servico": {"servico": "-", "volume_total_fmt": "0", "unidade": "", "registros": 0},
            "top_ocorrencia": {"tipo_ocorrencia": "-", "ocorrencias": 0},
            "top_servicos": [],
            "timeline": [],
        }
        if self._db is None:
            return snapshot

        exec_where_parts: list[str] = []
        exec_params: list[Any] = []
        if filters.obra_from:
            exec_where_parts.append("data_referencia >= %s")
            exec_params.append(filters.obra_from)
        if filters.obra_to:
            exec_where_parts.append("data_referencia <= %s")
            exec_params.append(filters.obra_to)
        if filters.nucleo:
            exec_where_parts.append("COALESCE(nucleo, '') ILIKE %s")
            exec_params.append(f"%{filters.nucleo}%")
        if filters.municipio:
            exec_where_parts.append("COALESCE(municipio, '') ILIKE %s")
            exec_params.append(f"%{filters.municipio}%")
        if filters.equipe:
            exec_where_parts.append("COALESCE(equipe, '') ILIKE %s")
            exec_params.append(f"%{filters.equipe}%")
        exec_where_sql = f" WHERE {' AND '.join(exec_where_parts)}" if exec_where_parts else ""

        date_where_parts: list[str] = []
        date_params: list[Any] = []
        if filters.obra_from:
            date_where_parts.append("data_referencia >= %s")
            date_params.append(filters.obra_from)
        if filters.obra_to:
            date_where_parts.append("data_referencia <= %s")
            date_params.append(filters.obra_to)
        date_where_sql = f" WHERE {' AND '.join(date_where_parts)}" if date_where_parts else ""

        cursor_kwargs = {}
        dict_factory = _dict_row_factory()
        if dict_factory is not None:
            cursor_kwargs["row_factory"] = dict_factory

        try:
            with self._db.connection() as conn:
                with conn.cursor(**cursor_kwargs) as cur:
                    cur.execute(
                        f"""
                        SELECT
                            COUNT(*)::BIGINT AS registros,
                            COALESCE(SUM(quantidade), 0)::NUMERIC(18,3) AS volume_total,
                            COUNT(DISTINCT data_referencia)::BIGINT AS dias
                        FROM vw_bi_execucao_fato
                        {exec_where_sql}
                        """,
                        exec_params,
                    )
                    recorte = cur.fetchone() or {}

                    cur.execute(
                        f"""
                        SELECT
                            COALESCE(SUM(CASE WHEN mapeado THEN 1 ELSE 0 END), 0)::BIGINT AS mapeados,
                            COALESCE(SUM(CASE WHEN mapeado THEN 0 ELSE 1 END), 0)::BIGINT AS nao_mapeados
                        FROM vw_bi_execucao_fato
                        {exec_where_sql}
                        """,
                        exec_params,
                    )
                    mapa = cur.fetchone() or {}

                    cur.execute(
                        f"""
                        SELECT
                            servico,
                            COALESCE(NULLIF(unidade, ''), 'un') AS unidade,
                            COUNT(*)::BIGINT AS registros,
                            COALESCE(SUM(quantidade), 0)::NUMERIC(18,3) AS volume_total
                        FROM vw_bi_execucao_fato
                        {exec_where_sql}
                        GROUP BY servico, COALESCE(NULLIF(unidade, ''), 'un')
                        ORDER BY volume_total DESC, registros DESC
                        LIMIT 1
                        """,
                        exec_params,
                    )
                    top_servico = cur.fetchone() or {}

                    cur.execute(
                        f"""
                        SELECT
                            servico,
                            COALESCE(NULLIF(unidade, ''), 'un') AS unidade,
                            COUNT(*)::BIGINT AS registros,
                            COALESCE(SUM(quantidade), 0)::NUMERIC(18,3) AS volume_total
                        FROM vw_bi_execucao_fato
                        {exec_where_sql}
                        GROUP BY servico, COALESCE(NULLIF(unidade, ''), 'un')
                        ORDER BY volume_total DESC, registros DESC
                        LIMIT 5
                        """,
                        exec_params,
                    )
                    top_servicos = cur.fetchall() or []

                    cur.execute(
                        f"""
                        SELECT
                            tipo_ocorrencia,
                            COALESCE(SUM(ocorrencias), 0)::BIGINT AS ocorrencias_total
                        FROM vw_bi_ocorrencias_tipo
                        {date_where_sql}
                        GROUP BY tipo_ocorrencia
                        ORDER BY ocorrencias_total DESC
                        LIMIT 1
                        """,
                        date_params,
                    )
                    top_ocorrencia = cur.fetchone() or {}

                    cur.execute(
                        f"""
                        SELECT
                            data_referencia,
                            registros_execucao,
                            volume_total
                        FROM vw_bi_kpi_diario
                        {date_where_sql}
                        ORDER BY data_referencia DESC
                        LIMIT 7
                        """,
                        date_params,
                    )
                    timeline_rows = cur.fetchall() or []
        except Exception as exc:
            snapshot["error"] = str(exc)
            return snapshot

        registros = int(recorte.get("registros", 0) or 0)
        volume_total = float(recorte.get("volume_total", 0) or 0)
        dias = int(recorte.get("dias", 0) or 0)
        mapeados = int(mapa.get("mapeados", 0) or 0)
        nao_mapeados = int(mapa.get("nao_mapeados", 0) or 0)
        percentual_mapeado = (100.0 * mapeados / registros) if registros else 0.0

        timeline: list[dict[str, Any]] = []
        for row in reversed(timeline_rows):
            dt = row.get("data_referencia")
            data_label = f"{dt:%d/%m}" if isinstance(dt, date) else _safe_text(dt)
            timeline.append(
                {
                    "data": data_label,
                    "registros": int(row.get("registros_execucao", 0) or 0),
                    "volume_total": float(row.get("volume_total", 0) or 0.0),
                    "volume_total_fmt": _display_number(float(row.get("volume_total", 0) or 0.0)),
                }
            )

        normalized_top_servicos = []
        for row in top_servicos:
            volume = float(row.get("volume_total", 0) or 0.0)
            unidade = _safe_text(row.get("unidade"))
            normalized_top_servicos.append(
                {
                    "servico": _safe_text(row.get("servico")) or "-",
                    "unidade": unidade,
                    "registros": int(row.get("registros", 0) or 0),
                    "volume_total": volume,
                    "volume_total_fmt": _display_number(volume),
                    "value_display": f"{_display_number(volume)} {unidade}".strip(),
                }
            )
        max_top_servico_volume = max((row.get("volume_total", 0.0) for row in normalized_top_servicos), default=0.0)
        if max_top_servico_volume > 0:
            for row in normalized_top_servicos:
                row["width_pct"] = round((float(row.get("volume_total", 0.0) or 0.0) / max_top_servico_volume) * 100.0, 1)
        else:
            for row in normalized_top_servicos:
                row["width_pct"] = 0.0

        top_servico_volume = float(top_servico.get("volume_total", 0) or 0.0)
        top_servico_unidade = _safe_text(top_servico.get("unidade"))
        snapshot["enabled"] = True
        snapshot["has_data"] = registros > 0
        snapshot["kpis"] = {
            "registros": registros,
            "registros_fmt": _display_number(float(registros)),
            "volume_total": volume_total,
            "volume_total_fmt": _display_number(volume_total),
            "dias": dias,
            "percentual_mapeado": percentual_mapeado,
            "percentual_mapeado_fmt": _display_number(percentual_mapeado),
            "nao_mapeados": nao_mapeados,
        }
        snapshot["top_servico"] = {
            "servico": _safe_text(top_servico.get("servico")) or "-",
            "volume_total": top_servico_volume,
            "volume_total_fmt": _display_number(top_servico_volume),
            "unidade": top_servico_unidade,
            "registros": int(top_servico.get("registros", 0) or 0),
        }
        snapshot["top_ocorrencia"] = {
            "tipo_ocorrencia": _safe_text(top_ocorrencia.get("tipo_ocorrencia")) or "-",
            "ocorrencias": int(top_ocorrencia.get("ocorrencias_total", 0) or 0),
        }
        snapshot["top_servicos"] = normalized_top_servicos
        snapshot["timeline"] = timeline
        return snapshot

    def build_gerencial_dashboard(self, raw_filters: dict[str, object] | None = None) -> dict[str, Any]:
        filters = self._parse_filters(raw_filters)

        exec_rows: list[dict[str, Any]] = []
        frentes_rows: list[dict[str, Any]] = []
        ocorr_rows: list[dict[str, Any]] = []
        source_kind = "database"

        # Fonte canônica no ambiente com banco: tabelas management_* acumuladas.
        if self._db is not None:
            exec_rows, frentes_rows, ocorr_rows = self._load_rows_from_database()
            # Fallback de segurança para CSV quando banco estiver vazio.
            if not exec_rows and not frentes_rows and not ocorr_rows:
                csv_exec, csv_frentes, csv_ocorr = self._load_rows_from_master_csv()
                if csv_exec or csv_frentes or csv_ocorr:
                    source_kind = "master_csv"
                    exec_rows, frentes_rows, ocorr_rows = csv_exec, csv_frentes, csv_ocorr
        else:
            source_kind = "master_csv"
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
        ligacoes_agua_total = 0.0
        ligacoes_esgoto_total = 0.0
        prolongamento_rede_agua_total = 0.0
        prolongamento_rede_esgoto_total = 0.0

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
            )
            servico = _canonical_service_label(servico)
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

            servico_lookup = _normalize_lookup(servico)
            if "prolongamento" in servico_lookup and "esgoto" in servico_lookup:
                prolongamento_rede_esgoto_total += peso
            elif "prolongamento" in servico_lookup and (
                "agua" in servico_lookup or "rede" in servico_lookup
            ):
                prolongamento_rede_agua_total += peso

            is_ligacao_like = (
                "ligacao" in servico_lookup
                or "ramal" in servico_lookup
                or "intradomiciliar" in servico_lookup
            )
            if is_ligacao_like and "esgoto" in servico_lookup:
                ligacoes_esgoto_total += peso
            elif is_ligacao_like and (
                "agua" in servico_lookup
                or "hidrometro" in servico_lookup
                or "intradomiciliar" in servico_lookup
            ):
                ligacoes_agua_total += peso

        for row in ocorr_filtered:
            tipo = _pick_first_text(row.get("tipo_ocorrencia"), row.get("descricao")) or "-"
            ocorrencia_tipo_values[tipo] = ocorrencia_tipo_values.get(tipo, 0) + 1

        top_n = filters.top_n
        contract_stats: dict[str, dict[str, Any]] = {}

        def _contract_bucket(value: object) -> str:
            label = _safe_text(value)
            return label if label else "Sem contrato"

        def _ensure_contract_stat(contract_label: str) -> dict[str, Any]:
            item = contract_stats.get(contract_label)
            if item is None:
                item = {
                    "contract": contract_label,
                    "execucoes": 0,
                    "volume": 0.0,
                    "nao_mapeados": 0,
                    "ocorrencias": 0,
                    "frentes": 0,
                    "frentes_sem_producao": 0,
                    "volume_by_date": {},
                }
                contract_stats[contract_label] = item
            return item

        for row in exec_filtered:
            contract_label = _contract_bucket(row.get("contrato"))
            stat = _ensure_contract_stat(contract_label)
            quantidade = float(row.get("quantidade", 0) or 0)
            peso = quantidade if quantidade > 0 else 1.0
            stat["execucoes"] += 1
            stat["volume"] += peso
            mapped_service = _safe_text(row.get("servico_oficial"))
            if not mapped_service or _normalize(mapped_service) in {"servico_nao_mapeado", "-", "nao_mapeado"}:
                stat["nao_mapeados"] += 1
            dt = row.get("data_referencia")
            if isinstance(dt, date):
                by_date = stat["volume_by_date"]
                by_date[dt] = float(by_date.get(dt, 0.0) or 0.0) + peso

        for row in ocorr_filtered:
            contract_label = _contract_bucket(row.get("contrato"))
            stat = _ensure_contract_stat(contract_label)
            stat["ocorrencias"] += 1

        for row in frentes_filtered:
            contract_label = _contract_bucket(row.get("contrato"))
            stat = _ensure_contract_stat(contract_label)
            stat["frentes"] += 1
            status_frente = _normalize(row.get("status_frente"))
            if "sem_producao" in status_frente or "sem producao" in status_frente:
                stat["frentes_sem_producao"] += 1

        all_refs = [r.get("data_referencia") for r in exec_filtered if isinstance(r.get("data_referencia"), date)]
        latest_ref = max(all_refs) if all_refs else None

        max_volume_contract = max((float(item.get("volume", 0) or 0) for item in contract_stats.values()), default=0.0)
        occ_rates = []
        unmapped_rates = []
        for item in contract_stats.values():
            exec_count = max(int(item.get("execucoes", 0) or 0), 1)
            occ_rates.append((float(item.get("ocorrencias", 0) or 0) / exec_count) * 100.0)
            unmapped_rates.append((float(item.get("nao_mapeados", 0) or 0) / exec_count) * 100.0)
        max_occ_rate = max(occ_rates) if occ_rates else 0.0
        max_unmapped_rate = max(unmapped_rates) if unmapped_rates else 0.0

        risk_items: list[dict[str, Any]] = []
        for item in contract_stats.values():
            exec_count = int(item.get("execucoes", 0) or 0)
            occ_count = int(item.get("ocorrencias", 0) or 0)
            unmapped_count = int(item.get("nao_mapeados", 0) or 0)
            volume_total_contract = float(item.get("volume", 0) or 0.0)
            frentes_sem_producao_contract = int(item.get("frentes_sem_producao", 0) or 0)

            occ_rate = (occ_count / max(exec_count, 1)) * 100.0
            unmapped_rate = (unmapped_count / max(exec_count, 1)) * 100.0

            trend_drop_pct = 0.0
            if latest_ref is not None:
                recent_start = latest_ref - timedelta(days=6)
                previous_end = recent_start - timedelta(days=1)
                previous_start = previous_end - timedelta(days=6)
                by_date = item.get("volume_by_date", {}) or {}
                recent_volume = sum(
                    float(v or 0.0)
                    for dt, v in by_date.items()
                    if isinstance(dt, date) and recent_start <= dt <= latest_ref
                )
                previous_volume = sum(
                    float(v or 0.0)
                    for dt, v in by_date.items()
                    if isinstance(dt, date) and previous_start <= dt <= previous_end
                )
                if previous_volume > 0 and recent_volume < previous_volume:
                    trend_drop_pct = ((previous_volume - recent_volume) / previous_volume) * 100.0

            occ_component = (occ_rate / max_occ_rate) if max_occ_rate > 0 else 0.0
            unmapped_component = (unmapped_rate / max_unmapped_rate) if max_unmapped_rate > 0 else 0.0
            trend_component = min(max(trend_drop_pct / 100.0, 0.0), 1.0)
            volume_component = 0.0
            if max_volume_contract > 0:
                volume_component = max(0.0, min(1.0, 1.0 - (volume_total_contract / max_volume_contract)))

            score = int(
                round(
                    (
                        (0.35 * occ_component)
                        + (0.25 * unmapped_component)
                        + (0.20 * trend_component)
                        + (0.20 * volume_component)
                    )
                    * 100.0
                )
            )
            if score < 0:
                score = 0
            if score > 100:
                score = 100

            if score >= 75:
                level_key = "critico"
                level_label = "Critico"
            elif score >= 55:
                level_key = "alto"
                level_label = "Alto"
            elif score >= 35:
                level_key = "moderado"
                level_label = "Moderado"
            else:
                level_key = "baixo"
                level_label = "Baixo"

            reasons: list[str] = []
            if occ_count > 0 and occ_component >= 0.45:
                reasons.append(f"{occ_count} ocorrencia(s) no recorte")
            if unmapped_count > 0 and unmapped_component >= 0.35:
                reasons.append(f"{unmapped_count} item(ns) nao mapeado(s)")
            if trend_drop_pct >= 15:
                reasons.append(f"queda de {round(trend_drop_pct, 1)}% no volume (7d)")
            if frentes_sem_producao_contract > 0:
                reasons.append(f"{frentes_sem_producao_contract} frente(s) sem producao")
            if not reasons:
                reasons.append("operacao estavel no recorte atual")

            risk_items.append(
                {
                    "contract_label": str(item.get("contract", "") or "Sem contrato"),
                    "score": score,
                    "level_key": level_key,
                    "level_label": level_label,
                    "execucoes": exec_count,
                    "ocorrencias": occ_count,
                    "nao_mapeados": unmapped_count,
                    "volume_total": volume_total_contract,
                    "volume_total_fmt": _display_number(volume_total_contract),
                    "occ_rate_fmt": _display_number(occ_rate),
                    "unmapped_rate_fmt": _display_number(unmapped_rate),
                    "trend_drop_pct_fmt": _display_number(trend_drop_pct),
                    "bar_width_pct": max(8, score),
                    "summary": (
                        f"Volume { _display_number(volume_total_contract) } | "
                        f"Execucoes {exec_count} | Ocorrencias {occ_count}"
                    ),
                    "reasons": reasons,
                }
            )

        risk_items.sort(
            key=lambda item: (
                -int(item.get("score", 0) or 0),
                -int(item.get("ocorrencias", 0) or 0),
                -float(item.get("volume_total", 0) or 0.0),
                str(item.get("contract_label", "") or ""),
            )
        )
        radar_limit = max(5, min(top_n, 12))
        risk_items_limited = risk_items[:radar_limit]
        risk_monitorados = len(risk_items)
        risk_critico = sum(1 for item in risk_items if item.get("level_key") == "critico")
        risk_alto = sum(1 for item in risk_items if item.get("level_key") == "alto")
        risk_moderado = sum(1 for item in risk_items if item.get("level_key") == "moderado")
        risk_baixo = sum(1 for item in risk_items if item.get("level_key") == "baixo")

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
        bi_mvp = self._load_bi_mvp_snapshot(filters)
        if nucleo_values:
            nucleo_maior_volume, nucleo_maior_volume_valor = max(
                nucleo_values.items(),
                key=lambda item: item[1],
            )
        else:
            nucleo_maior_volume = "-"
            nucleo_maior_volume_valor = 0.0

        return {
            "has_data": has_data,
            "source": source_kind,
            "bi_mvp": bi_mvp,
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
                "ligacoes_agua_total": ligacoes_agua_total,
                "ligacoes_agua_total_fmt": _display_number(ligacoes_agua_total),
                "ligacoes_esgoto_total": ligacoes_esgoto_total,
                "ligacoes_esgoto_total_fmt": _display_number(ligacoes_esgoto_total),
                "prolongamento_rede_agua_total": prolongamento_rede_agua_total,
                "prolongamento_rede_agua_total_fmt": _display_number(prolongamento_rede_agua_total),
                "prolongamento_rede_esgoto_total": prolongamento_rede_esgoto_total,
                "prolongamento_rede_esgoto_total_fmt": _display_number(prolongamento_rede_esgoto_total),
                "nucleo_maior_volume": str(nucleo_maior_volume or "-"),
                "nucleo_maior_volume_valor": nucleo_maior_volume_valor,
                "nucleo_maior_volume_valor_fmt": _display_number(float(nucleo_maior_volume_valor or 0.0)),
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
            "radar_risco_contratos": {
                "items": risk_items_limited,
                "has_data": bool(risk_items_limited),
                "monitorados": risk_monitorados,
                "critico": risk_critico,
                "alto": risk_alto,
                "moderado": risk_moderado,
                "baixo": risk_baixo,
                "window": "ultimos 7 dias x 7 dias anteriores",
            },
        }

    def list_master_execucao_rows(
        self,
        raw_filters: dict[str, object] | None = None,
        limit: int = 5000,
    ) -> list[dict[str, Any]]:
        if self._db is None:
            return []

        filters = raw_filters or {}
        where_parts: list[str] = []
        params: list[Any] = []

        contrato = _safe_text(filters.get("contrato"))
        if contrato:
            where_parts.append("contrato ILIKE %s")
            params.append(f"%{contrato}%")

        nucleo = _safe_text(filters.get("nucleo"))
        if nucleo:
            where_parts.append("COALESCE(NULLIF(nucleo_oficial, ''), nucleo, '') ILIKE %s")
            params.append(f"%{nucleo}%")

        municipio = _safe_text(filters.get("municipio"))
        if municipio:
            where_parts.append("COALESCE(NULLIF(municipio_oficial, ''), municipio, '') ILIKE %s")
            params.append(f"%{municipio}%")

        equipe = _safe_text(filters.get("equipe"))
        if equipe:
            where_parts.append("equipe ILIKE %s")
            params.append(f"%{equipe}%")

        servico = _safe_text(filters.get("servico"))
        if servico:
            where_parts.append(
                "("
                "servico_oficial ILIKE %s OR "
                "servico_normalizado ILIKE %s OR "
                "servico_bruto ILIKE %s OR "
                "item_original ILIKE %s"
                ")"
            )
            probe = f"%{servico}%"
            params.extend([probe, probe, probe, probe])

        categoria = _safe_text(filters.get("categoria"))
        if categoria:
            where_parts.append("COALESCE(NULLIF(categoria, ''), NULLIF(categoria_item, ''), '') ILIKE %s")
            params.append(f"%{categoria}%")

        data_from = _parse_date(filters.get("data_from"))
        if data_from is not None:
            where_parts.append("data_referencia >= %s")
            params.append(data_from)

        data_to = _parse_date(filters.get("data_to"))
        if data_to is not None:
            where_parts.append("data_referencia <= %s")
            params.append(data_to)

        base_sql = """
            SELECT
                data_referencia,
                contrato,
                COALESCE(NULLIF(nucleo_oficial, ''), nucleo, '-') AS nucleo_view,
                COALESCE(NULLIF(municipio_oficial, ''), municipio, '-') AS municipio_view,
                equipe,
                servico_oficial,
                servico_normalizado,
                servico_bruto,
                item_original,
                categoria,
                categoria_item,
                quantidade,
                unidade
            FROM management_execucao
        """
        if where_parts:
            base_sql += " WHERE " + " AND ".join(where_parts)
        base_sql += " ORDER BY data_referencia DESC NULLS LAST, id DESC LIMIT %s"
        params.append(max(100, min(int(limit or 5000), 50000)))

        cursor_kwargs = {}
        dict_factory = _dict_row_factory()
        if dict_factory is not None:
            cursor_kwargs["row_factory"] = dict_factory

        with self._db.connection() as conn:
            with conn.cursor(**cursor_kwargs) as cur:
                cur.execute(base_sql, params)
                rows = cur.fetchall() or []

        parsed_rows: list[dict[str, Any]] = []
        for row in rows:
            dt = row.get("data_referencia")
            if isinstance(dt, date):
                data_label = dt.strftime("%d/%m/%Y")
                data_input = dt.strftime("%Y-%m-%d")
            else:
                raw_dt = _safe_text(dt)
                parsed_dt = _parse_date(raw_dt)
                data_label = parsed_dt.strftime("%d/%m/%Y") if parsed_dt else raw_dt
                data_input = parsed_dt.strftime("%Y-%m-%d") if parsed_dt else ""

            quantidade = float(row.get("quantidade", 0) or 0.0)
            quantidade_fmt = _display_number(quantidade)

            parsed_rows.append(
                {
                    "data_referencia": data_label,
                    "data_referencia_iso": data_input,
                    "contrato": _safe_text(row.get("contrato")) or "-",
                    "nucleo": _safe_text(row.get("nucleo_view")) or "-",
                    "municipio": _safe_text(row.get("municipio_view")) or "-",
                    "equipe": _safe_text(row.get("equipe")) or "-",
                    "servico_oficial": _safe_text(row.get("servico_oficial")) or "-",
                    "servico_normalizado": _safe_text(row.get("servico_normalizado")) or "-",
                    "servico_bruto": _safe_text(row.get("servico_bruto")) or "-",
                    "item_original": _safe_text(row.get("item_original")) or "-",
                    "categoria": _pick_first_text(row.get("categoria"), row.get("categoria_item")) or "-",
                    "quantidade": quantidade,
                    "quantidade_fmt": quantidade_fmt,
                    "unidade": _safe_text(row.get("unidade")) or "",
                }
            )

        return parsed_rows

    def list_master_execucao_filter_options(self) -> dict[str, list[str]]:
        options = {
            "contratos": [],
            "nucleos": [],
            "municipios": [],
            "equipes": [],
            "servicos": [],
        }
        if self._db is None:
            return options

        cursor_kwargs = {}
        dict_factory = _dict_row_factory()
        if dict_factory is not None:
            cursor_kwargs["row_factory"] = dict_factory

        queries = {
            "contratos": "SELECT DISTINCT contrato AS value FROM management_execucao WHERE contrato IS NOT NULL AND contrato <> '' ORDER BY contrato LIMIT 500",
            "nucleos": "SELECT DISTINCT COALESCE(NULLIF(nucleo_oficial, ''), nucleo) AS value FROM management_execucao WHERE COALESCE(NULLIF(nucleo_oficial, ''), nucleo) IS NOT NULL AND COALESCE(NULLIF(nucleo_oficial, ''), nucleo) <> '' ORDER BY value LIMIT 500",
            "municipios": "SELECT DISTINCT COALESCE(NULLIF(municipio_oficial, ''), municipio) AS value FROM management_execucao WHERE COALESCE(NULLIF(municipio_oficial, ''), municipio) IS NOT NULL AND COALESCE(NULLIF(municipio_oficial, ''), municipio) <> '' ORDER BY value LIMIT 500",
            "equipes": "SELECT DISTINCT equipe AS value FROM management_execucao WHERE equipe IS NOT NULL AND equipe <> '' ORDER BY equipe LIMIT 500",
            "servicos": "SELECT DISTINCT COALESCE(NULLIF(servico_oficial, ''), NULLIF(servico_normalizado, ''), NULLIF(servico_bruto, ''), NULLIF(item_original, '')) AS value FROM management_execucao WHERE COALESCE(NULLIF(servico_oficial, ''), NULLIF(servico_normalizado, ''), NULLIF(servico_bruto, ''), NULLIF(item_original, '')) IS NOT NULL ORDER BY value LIMIT 800",
        }

        with self._db.connection() as conn:
            with conn.cursor(**cursor_kwargs) as cur:
                for key, sql in queries.items():
                    cur.execute(sql)
                    rows = cur.fetchall() or []
                    clean_values = []
                    for row in rows:
                        value = _safe_text(row.get("value"))
                        if value:
                            clean_values.append(value)
                    options[key] = clean_values

        return options
