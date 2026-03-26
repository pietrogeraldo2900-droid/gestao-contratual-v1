from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

from flask import Flask, abort, flash, g, make_response, redirect, render_template, request, send_file, session, url_for

from config.settings import AppSettings, load_settings
from app.database import build_database_manager, init_db
from app.repositories import ContractRepository, ManagementRepository, ReportRepository, UserRepository
from app.repositories.contract_repository import ContractConflictError
from app.repositories.user_repository import UserAlreadyExistsError
from app.routes.auth_api import register_auth_routes
from app.routes.contracts_api import register_contract_routes
from app.services.contract_service import ContractService, ContractValidationError
from app.services.report_service import ReportService, create_report_service
from app.services.user_service import UserAuthError, UserService, UserValidationError
from app.utils.jwt_utils import generate_jwt_token
from app.services.web_service import WebPipelineService


SESSION_USER_ID_KEY = "web_user_id"
SESSION_USER_EMAIL_KEY = "web_user_email"
SESSION_AUTH_TOKEN_KEY = "web_auth_token"


def _collect_form_fields(form) -> Dict[str, object]:
    apply_all = str(form.get("aplicar_todos", "") or "").strip().lower() in {"1", "true", "on", "yes", "sim", "s"}

    try:
        nucleo_map_count = int(str(form.get("nucleo_map_count", "0") or "0").strip())
    except Exception:
        nucleo_map_count = 0
    nucleo_map_count = max(0, min(nucleo_map_count, 50))

    nucleo_overrides = []
    for idx in range(nucleo_map_count):
        nucleo = str(form.get(f"nucleo_map_{idx}_name", "") or "").strip()
        municipio = str(form.get(f"nucleo_map_{idx}_municipio", "") or "").strip()
        logradouro = str(form.get(f"nucleo_map_{idx}_logradouro", "") or "").strip()
        equipe = str(form.get(f"nucleo_map_{idx}_equipe", "") or "").strip()
        if not nucleo:
            continue
        if not (municipio or logradouro or equipe):
            continue
        nucleo_overrides.append(
            {
                "nucleo": nucleo,
                "municipio": municipio,
                "logradouro": logradouro,
                "equipe": equipe,
            }
        )

    try:
        unmapped_fix_count = int(str(form.get("unmapped_fix_count", "0") or "0").strip())
    except Exception:
        unmapped_fix_count = 0
    unmapped_fix_count = max(0, min(unmapped_fix_count, 200))

    manual_service_corrections = []
    for idx in range(unmapped_fix_count):
        key = str(form.get(f"unmapped_fix_{idx}_key", "") or "").strip()
        servico = str(form.get(f"unmapped_fix_{idx}_servico", "") or "").strip()
        term = str(form.get(f"unmapped_fix_{idx}_term", "") or "").strip()
        if not key or not servico:
            continue
        manual_service_corrections.append(
            {
                "unmapped_key": key,
                "servico": servico,
                "servico_term": term,
            }
        )

    return {
        "data": str(form.get("data", "") or "").strip(),
        "nucleo": str(form.get("nucleo", "") or "").strip(),
        "logradouro": str(form.get("logradouro", "") or "").strip(),
        "municipio": str(form.get("municipio", "") or "").strip(),
        "equipe": str(form.get("equipe", "") or "").strip(),
        "contract_id": str(form.get("contract_id", "") or "").strip(),
        "aplicar_todos": "1" if apply_all else "",
        "nucleo_overrides": nucleo_overrides,
        "manual_service_corrections": manual_service_corrections,
    }



def _build_main_field_audit(
    service: WebPipelineService,
    baseline_main: Dict[str, object],
    final_main: Dict[str, object],
    autofill_info: Dict[str, object] | None = None,
) -> Dict[str, object]:
    fields = ["data", "nucleo", "logradouro", "municipio", "equipe"]
    baseline = dict(baseline_main or {})
    final = dict(final_main or {})
    applied = dict((autofill_info or {}).get("applied", {}) or {})

    autopreenchidos = []
    for field in fields:
        if field in applied:
            autopreenchidos.append({"campo": field, "valor": str(applied.get(field, "") or "")})

    alterados = []
    mantidos = []
    for field in fields:
        before = str(baseline.get(field, "") or "").strip()
        after = str(final.get(field, "") or "").strip()
        changed = service._normalize_nucleo_key(before) != service._normalize_nucleo_key(after)
        item = {
            "campo": field,
            "antes": before,
            "depois": after,
        }
        if changed:
            alterados.append(item)
        else:
            mantidos.append(item)

    return {
        "autopreenchidos": autopreenchidos,
        "alterados": alterados,
        "mantidos": mantidos,
        "autopreenchidos_count": len(autopreenchidos),
        "alterados_count": len(alterados),
        "mantidos_count": len(mantidos),
    }


def _collect_management_filters(args) -> Dict[str, object]:
    obra_from = str(args.get("obra_from", "") or "").strip()
    obra_to = str(args.get("obra_to", "") or "").strip()
    processed_from = str(args.get("processed_from", "") or "").strip()
    processed_to = str(args.get("processed_to", "") or "").strip()
    nucleo = str(args.get("nucleo", "") or "").strip()
    municipio = str(args.get("municipio", "") or "").strip()
    equipe = str(args.get("equipe", "") or "").strip()
    status = str(args.get("status", "") or "").strip().lower()
    alertas = str(args.get("alertas", "") or "").strip().lower()
    top_n_raw = str(args.get("top_n", "10") or "10").strip()

    try:
        top_n = int(top_n_raw)
    except Exception:
        top_n = 10
    top_n = max(3, min(top_n, 50))

    return {
        "obra_from": obra_from,
        "obra_to": obra_to,
        "processed_from": processed_from,
        "processed_to": processed_to,
        "nucleo": nucleo,
        "municipio": municipio,
        "equipe": equipe,
        "status": status,
        "alertas": alertas,
        "top_n": top_n,
    }


def _is_safe_internal_next(next_url: str) -> bool:
    text = str(next_url or "").strip()
    if not text:
        return False
    if not text.startswith("/"):
        return False
    parsed = urlparse(text)
    return not parsed.scheme and not parsed.netloc


def create_app(test_config: dict | None = None, settings: AppSettings | None = None) -> Flask:
    settings = settings or load_settings(Path(__file__).resolve().parents[2])
    base_dir = settings.base_dir

    app = Flask(
        __name__,
        template_folder=str(base_dir / "templates"),
        static_folder=str(base_dir / "static"),
        static_url_path="/static",
    )
    app.config.from_mapping(
        SECRET_KEY=settings.secret_key,
        OUTPUTS_ROOT=str(settings.outputs_root),
        MASTER_DIR=str(settings.master_dir),
        HISTORY_FILE=str(settings.history_file),
        DRAFT_DIR=str(settings.draft_dir),
        NUCLEO_REFERENCE_FILE=str(settings.nucleo_reference_file),
    )

    if test_config:
        app.config.update(test_config)

    service = WebPipelineService(
        base_dir=base_dir,
        outputs_root=Path(app.config["OUTPUTS_ROOT"]),
        master_dir=Path(app.config["MASTER_DIR"]),
        history_file=Path(app.config["HISTORY_FILE"]),
        draft_dir=Path(app.config["DRAFT_DIR"]),
        nucleo_reference_file=Path(app.config["NUCLEO_REFERENCE_FILE"]),
    )
    app.config["PIPELINE_SERVICE"] = service

    contract_service: ContractService | None = None
    report_service: ReportService | None = None
    user_service: UserService | None = None
    management_repository: ManagementRepository | None = None
    app.config["CONTRACTS_DB_ENABLED"] = settings.db_enabled
    db_manager = build_database_manager(settings)
    service.set_database_manager(db_manager)
    management_repository = ManagementRepository(db_manager, settings.master_dir)
    if db_manager is not None:
        user_repository = UserRepository(db_manager)
        contract_repository = ContractRepository(db_manager)
        report_repository = ReportRepository(db_manager)
        user_service = UserService(user_repository)
        contract_service = ContractService(contract_repository)
        report_service = ReportService(report_repository, contract_repository)
        if settings.contracts_auto_init_schema:
            try:
                init_db(db_manager)
            except Exception as exc:
                app.logger.warning("Falha ao inicializar schema de contratos: %s", exc)
                if settings.db_strict_startup:
                    raise
    else:
        app.logger.info(
            "Banco/auth web local desabilitados (DB_ENABLED=0). "
            "As telas autenticadas exibirao modo publico ate habilitar DB_ENABLED=1."
        )
    app.config["CONTRACTS_SERVICE"] = contract_service
    app.config["REPORT_SERVICE"] = report_service
    app.config["USER_SERVICE"] = user_service
    app.config["MANAGEMENT_REPOSITORY"] = management_repository

    protected_web_endpoints = {
        "dashboard",
        "web_profile",
        "web_settings",
        "results_list",
        "result_detail",
        "result_file",
        "web_contracts_list",
        "web_contracts_new",
        "web_contracts_create",
        "index",
        "nucleos",
        "nucleos_save",
        "preview",
        "generate",
        "history",
        "gerencial",
        "institucional",
        "institucional_export",
        "nova_entrada_redirect",
    }
    public_web_endpoints = {"web_login", "web_register", "web_logout", "web_register_redirect"}

    def _active_user_service() -> UserService | None:
        candidate = app.config.get("USER_SERVICE")
        required = ("authenticate_user", "register_user", "get_user_by_id")
        if candidate is not None and all(callable(getattr(candidate, name, None)) for name in required):
            return candidate  # type: ignore[return-value]
        return None

    def _active_contract_service() -> ContractService | None:
        candidate = app.config.get("CONTRACTS_SERVICE")
        required = ("count_contracts", "list_contracts")
        if candidate is not None and all(callable(getattr(candidate, name, None)) for name in required):
            return candidate  # type: ignore[return-value]
        return None

    def _active_contract_write_service() -> ContractService | None:
        candidate = app.config.get("CONTRACTS_SERVICE")
        required = ("create_contract", "list_contracts")
        if candidate is not None and all(callable(getattr(candidate, name, None)) for name in required):
            return candidate  # type: ignore[return-value]
        return None

    def _active_report_service() -> ReportService | None:
        candidate = app.config.get("REPORT_SERVICE")
        required = ("count_reports", "count_recent_reports", "list_recent_reports")
        if candidate is not None and all(callable(getattr(candidate, name, None)) for name in required):
            return candidate  # type: ignore[return-value]
        return None

    def _build_institutional_report_from_dashboard_fallback(
        dashboard: dict,
        filters: Dict[str, object],
    ) -> dict:
        top_n = max(3, min(int(filters.get("top_n", 5) or 5), 20))
        kpis = dict(dashboard.get("kpis_principais", {}) or {})
        consolidado = dict(dashboard.get("consolidado_periodo", {}) or {})
        charts = dict(dashboard.get("graficos_executivos", {}) or {})

        total_processamentos = int(kpis.get("total_processamentos", 0) or 0)
        total_execucoes = int(kpis.get("total_execucoes", 0) or 0)
        total_frentes = int(kpis.get("total_frentes", 0) or 0)
        total_ocorrencias = int(kpis.get("total_ocorrencias", 0) or 0)
        total_volume = float(kpis.get("total_volume", consolidado.get("volume_total", 0.0)) or 0.0)
        total_volume_fmt = str(kpis.get("total_volume_fmt", consolidado.get("volume_total_fmt", "0")) or "0")
        processamentos_alerta = int(kpis.get("processamentos_com_alerta", 0) or 0)
        processamentos_erro = int(kpis.get("processamentos_erro", 0) or 0)
        total_mapeados = int(kpis.get("total_mapeados", 0) or 0)
        total_nao_mapeados = int(kpis.get("total_nao_mapeados", 0) or 0)

        def _render_filter_value(value: object, fallback: str = "Todos") -> str:
            text = str(value or "").strip()
            return text if text else fallback

        def _chart_items(key: str) -> list[dict]:
            return list(charts.get(key, {}).get("items", []) or [])[:top_n]

        def _plural(valor: int, singular: str, plural: str) -> str:
            return singular if int(valor) == 1 else plural

        ranking_servicos = []
        for row in _chart_items("servicos"):
            servico = service._institutional_label(row.get("label", ""), domain="servico")
            value_fmt = str(row.get("value_display", "") or row.get("value_fmt", "0") or "0")
            registros = int(float(row.get("value", 0) or 0))
            desc = f"{servico}, com quantitativo consolidado de {value_fmt}"
            if registros > 0:
                desc += f" ({registros} {_plural(registros, 'registro', 'registros')})."
            else:
                desc += "."
            ranking_servicos.append(
                {
                    "servico": servico,
                    "categoria": "-",
                    "registros": registros,
                    "volume_total": float(row.get("value", 0) or 0.0),
                    "volume_total_fmt": str(row.get("value_fmt", "0") or "0"),
                    "unidades": str(row.get("unit", "") or "").strip() or "-",
                    "descricao_institucional": desc,
                }
            )

        ranking_categorias = []
        for row in _chart_items("categorias"):
            categoria = service._institutional_label(row.get("label", ""), domain="categoria")
            ranking_categorias.append(
                {
                    "categoria": categoria,
                    "registros": int(float(row.get("value", 0) or 0)),
                    "volume_total": float(row.get("value", 0) or 0.0),
                    "volume_total_fmt": str(row.get("value_fmt", "0") or "0"),
                    "percentual_volume": 0.0,
                    "percentual_volume_fmt": "0",
                    "descricao_institucional": f"{categoria}, com volume consolidado de {row.get('value_fmt', '0')}.",
                }
            )

        ranking_ocorrencias = []
        for row in _chart_items("tipos_ocorrencia"):
            tipo = service._institutional_label(row.get("label", ""), domain="ocorrencia")
            qtd = int(float(row.get("value", 0) or 0))
            ranking_ocorrencias.append(
                {
                    "tipo_ocorrencia": tipo,
                    "ocorrencias": qtd,
                    "descricao_institucional": f"{tipo}, com {qtd} {_plural(qtd, 'ocorrência', 'ocorrências')}.",
                }
            )

        analise_por_nucleo = []
        for row in _chart_items("nucleos"):
            nucleo = service._institutional_label(row.get("label", ""), domain="nucleo")
            qtd = int(float(row.get("value", 0) or 0))
            analise_por_nucleo.append(
                {
                    "nucleo": nucleo,
                    "processamentos": qtd,
                    "volume_total": float(row.get("value", 0) or 0.0),
                    "volume_total_fmt": str(row.get("value_fmt", "0") or "0"),
                    "sucesso": qtd,
                    "erro": 0,
                    "processamentos_alerta": 0,
                    "municipio": "-",
                    "equipes": [],
                    "logradouro": "-",
                    "logradouros": [],
                    "principais_servicos": ranking_servicos[:3],
                    "principais_ocorrencias": ranking_ocorrencias[:3],
                    "observacoes_relevantes": [],
                    "observacao_analitica": (
                        f"{nucleo} consolidou quantitativo de {row.get('value_fmt', '0')} no recorte aplicado."
                    ),
                }
            )

        filtros_aplicados = [
            {"label": "Período da obra (de)", "valor": _render_filter_value(filters.get("obra_from", ""), "-")},
            {"label": "Período da obra (até)", "valor": _render_filter_value(filters.get("obra_to", ""), "-")},
            {"label": "Processado em (de)", "valor": _render_filter_value(filters.get("processed_from", ""), "-")},
            {"label": "Processado em (até)", "valor": _render_filter_value(filters.get("processed_to", ""), "-")},
            {"label": "Núcleo", "valor": _render_filter_value(filters.get("nucleo", ""))},
            {"label": "Município", "valor": _render_filter_value(filters.get("municipio", ""))},
            {"label": "Equipe", "valor": _render_filter_value(filters.get("equipe", ""))},
            {"label": "Status", "valor": _render_filter_value(filters.get("status", ""), "Todos")},
            {"label": "Alertas", "valor": _render_filter_value(filters.get("alertas", ""), "Todos")},
        ]

        top_nucleo = analise_por_nucleo[0]["nucleo"] if analise_por_nucleo else "-"
        top_servico = ranking_servicos[0]["servico"] if ranking_servicos else "-"
        top_ocorrencia = ranking_ocorrencias[0]["tipo_ocorrencia"] if ranking_ocorrencias else "-"

        resumo_linhas = [
            (
                f"O quantitativo real consolidado no período foi de {total_volume_fmt}, com "
                f"{total_execucoes} {_plural(total_execucoes, 'execução registrada', 'execuções registradas')} "
                f"e {total_processamentos} {_plural(total_processamentos, 'mensagem processada', 'mensagens processadas')}."
            ),
            f"A maior concentração operacional ocorreu em {top_nucleo}.",
            f"Em serviços executados, houve predominância de {top_servico}.",
            f"No eixo de risco operacional, a ocorrência mais recorrente foi {top_ocorrencia}.",
        ]

        conclusao = (
            f"No recorte analisado, o quantitativo consolidado foi de {total_volume_fmt}. "
            f"Como próximos focos gerenciais, recomenda-se manter o acompanhamento por núcleo, serviço e ocorrência."
        )

        final_payload = {
            "has_data": bool(total_processamentos or total_execucoes or total_volume > 0),
            "header": {
                "titulo": "Relatório Institucional de Acompanhamento Operacional",
                "periodo": str(consolidado.get("periodo_obra", "-") or "-"),
                "emissao": datetime.now().strftime("%d/%m/%Y %H:%M"),
                "filtros_aplicados": filtros_aplicados,
            },
            "resumo_executivo": {"linhas": resumo_linhas},
            "indicadores_principais": {
                "total_volume": total_volume,
                "total_volume_fmt": total_volume_fmt,
                "total_processamentos": total_processamentos,
                "total_execucoes": total_execucoes,
                "total_frentes": total_frentes,
                "total_ocorrencias": total_ocorrencias,
                "total_nucleos": int(kpis.get("total_nucleos", 0) or 0),
                "total_municipios": int(kpis.get("total_municipios", 0) or 0),
                "total_equipes": int(kpis.get("total_equipes", 0) or 0),
                "total_mapeados": total_mapeados,
                "total_nao_mapeados": total_nao_mapeados,
                "percentual_mapeado_fmt": str(kpis.get("percentual_mapeado_fmt", "0") or "0"),
                "percentual_nao_mapeado_fmt": str(kpis.get("percentual_nao_mapeado_fmt", "0") or "0"),
                "processamentos_alerta": processamentos_alerta,
                "processamentos_erro": processamentos_erro,
            },
            "panorama_operacional": {
                "texto_analitico": (
                    f"Panorama consolidado com quantitativo total de {total_volume_fmt}, "
                    f"distribuído entre serviços, categorias e ocorrências do recorte."
                ),
                "servicos_recorrentes": ranking_servicos,
                "categorias_recorrentes": ranking_categorias,
                "ocorrencias_recorrentes": ranking_ocorrencias,
                "resumo_periodo": [],
            },
            "analise_por_nucleo": analise_por_nucleo,
            "alertas_pendencias": {
                "processamentos_alerta": processamentos_alerta,
                "processamentos_erro": processamentos_erro,
                "municipio_ausente": 0,
                "nao_mapeados_recorrentes": [],
                "inconsistencias": ["Relatório gerado a partir do consolidado gerencial do banco de dados."],
            },
            "conclusao": conclusao,
            "top_n": top_n,
            "filters_applied": {
                "obra_from": str(filters.get("obra_from", "") or ""),
                "obra_to": str(filters.get("obra_to", "") or ""),
                "processed_from": str(filters.get("processed_from", "") or ""),
                "processed_to": str(filters.get("processed_to", "") or ""),
                "nucleo": str(filters.get("nucleo", "") or ""),
                "municipio": str(filters.get("municipio", "") or ""),
                "equipe": str(filters.get("equipe", "") or ""),
                "status": str(filters.get("status", "") or ""),
                "alertas": str(filters.get("alertas", "") or ""),
            },
        }

        return {
            **final_payload,
            "relatorio_final": dict(final_payload),
            "previa_tecnica": {
                "runs_analisadas": int(kpis.get("total_processamentos", 0) or 0),
                "analise_por_nucleo_tecnica": [],
                "alertas_internos": ["Fallback aplicado com base no consolidado gerencial."],
                "totais": {
                    "multiplos_nucleos": 0,
                    "multiplos_logradouros": 0,
                    "multiplas_equipes": 0,
                    "nao_mapeados": total_nao_mapeados,
                    "processamentos_erro": processamentos_erro,
                },
            },
        }

    def _build_institutional_report_with_fallback(filters: Dict[str, object]) -> dict:
        report = service.build_institutional_report(
            {
                **filters,
                "history_limit": 3000,
            }
        )
        if bool((report or {}).get("has_data")):
            return report

        management_repo = app.config.get("MANAGEMENT_REPOSITORY")
        build_dashboard = getattr(management_repo, "build_gerencial_dashboard", None)
        if not callable(build_dashboard):
            return report

        try:
            dashboard = build_dashboard(filters)
        except Exception as exc:
            app.logger.warning("Falha no fallback do institucional via gerencial: %s", exc)
            return report

        if not bool((dashboard or {}).get("has_data")):
            return report

        app.logger.info("Institucional: aplicando fallback com consolidado gerencial (management_*).")
        return _build_institutional_report_from_dashboard_fallback(dashboard, filters)

    def _resolve_contract_context(contract_id_raw: object) -> tuple[str, dict[str, object] | None]:
        raw = str(contract_id_raw or "").strip()
        if not raw:
            return "", None
        candidate = app.config.get("CONTRACTS_SERVICE")
        getter = getattr(candidate, "get_contract_report_context", None)
        if not callable(getter):
            return raw, None
        try:
            context = getter(int(raw))
        except Exception:
            return raw, None
        if not isinstance(context, dict):
            return raw, None

        numero = str(context.get("numero_contrato", context.get("contract_code", "")) or "").strip()
        nome = str(context.get("nome_contrato", context.get("contract_title", "")) or "").strip()
        if numero and nome:
            return f"{numero} - {nome}", context
        if nome:
            return nome, context
        if numero:
            return numero, context
        return raw, context

    def _build_contract_options(limit: int = 300) -> list[dict[str, str]]:
        contracts_service = _active_contract_service()
        if contracts_service is None:
            return []
        try:
            contracts = contracts_service.list_contracts(limit=max(1, int(limit)))
        except Exception as exc:
            app.logger.warning("Falha ao carregar opcoes de contrato para o fluxo web: %s", exc)
            return []

        options: list[dict[str, str]] = []
        for contract in contracts:
            to_dict = getattr(contract, "to_dict", None)
            if not callable(to_dict):
                continue
            payload = dict(to_dict() or {})
            contract_id = str(payload.get("id", "") or "").strip()
            if not contract_id:
                continue
            numero = str(payload.get("numero_contrato", "") or "").strip()
            nome = str(payload.get("nome_contrato", "") or "").strip()
            if numero and nome:
                label = f"{numero} - {nome}"
            elif nome:
                label = nome
            elif numero:
                label = numero
            else:
                label = f"Contrato {contract_id}"
            options.append({"id": contract_id, "label": label})

        return options

    def _clear_web_auth_session() -> None:
        session.pop(SESSION_USER_ID_KEY, None)
        session.pop(SESSION_USER_EMAIL_KEY, None)
        session.pop(SESSION_AUTH_TOKEN_KEY, None)

    def _set_web_auth_session(user: Dict[str, object]) -> None:
        session[SESSION_USER_ID_KEY] = int(user.get("id", 0) or 0)
        session[SESSION_USER_EMAIL_KEY] = str(user.get("email", "") or "").strip().lower()
        try:
            session[SESSION_AUTH_TOKEN_KEY] = generate_jwt_token(
                user_id=int(user.get("id", 0) or 0),
                email=str(user.get("email", "") or ""),
                secret=settings.auth_jwt_secret,
                expires_minutes=settings.auth_jwt_exp_minutes,
            )
        except Exception:
            session.pop(SESSION_AUTH_TOKEN_KEY, None)

    def _resolve_next(default_endpoint: str = "index") -> str:
        raw_next = str(request.values.get("next", "") or "").strip()
        if _is_safe_internal_next(raw_next):
            return raw_next
        return url_for(default_endpoint)

    def _auth_unavailable_message(area: str = "Autenticacao") -> str:
        if not settings.db_enabled:
            return (
                f"{area} indisponivel no momento. Ambiente local sem banco/auth habilitados (DB_ENABLED=0). "
                "Para usar login/cadastro local, habilite DB_ENABLED=1 e configure DB_HOST, DB_PORT, DB_NAME, DB_USER e DB_PASSWORD."
            )
        return f"{area} indisponivel no momento. Verifique a configuracao do banco."

    def _render_entry_page(
        form_data: Dict[str, object] | None = None,
        mensagem: str = "",
        error_message: str = "",
        autofill_info: Dict[str, object] | None = None,
    ):
        defaults = {
            "data": "",
            "nucleo": "",
            "logradouro": "",
            "municipio": "",
            "equipe": "",
            "contract_id": "",
            "aplicar_todos": "",
        }
        payload = dict(defaults)
        payload.update(dict(form_data or {}))
        return render_template(
            "index.html",
            form_data=payload,
            mensagem=str(mensagem or ""),
            error_message=str(error_message or ""),
            nucleo_reference=service.get_nucleo_reference_ui(),
            autofill_info=autofill_info or {"matched": False, "applied": {}, "profile": {}},
            contract_options=_build_contract_options(),
        )

    def _read_workspace_snapshot(report_limit: int = 5) -> Dict[str, object]:
        snapshot: Dict[str, object] = {
            "total_contracts": 0,
            "total_reports": 0,
            "recent_reports_count": 0,
            "recent_reports": [],
        }

        contract_service = _active_contract_service()
        if contract_service is not None:
            try:
                snapshot["total_contracts"] = max(0, int(contract_service.count_contracts()))
            except Exception as exc:
                app.logger.warning("Falha ao contar contratos na camada web: %s", exc)

        report_service = _active_report_service()
        if report_service is not None:
            try:
                snapshot["total_reports"] = max(0, int(report_service.count_reports()))
                snapshot["recent_reports_count"] = max(0, int(report_service.count_recent_reports(days=7)))
                snapshot["recent_reports"] = list(report_service.list_recent_reports(limit=max(1, int(report_limit))))
            except Exception as exc:
                app.logger.warning("Falha ao carregar snapshot de relatorios: %s", exc)

        return snapshot

    @app.context_processor
    def _inject_web_auth_context():
        user_email = str(session.get(SESSION_USER_EMAIL_KEY, "") or "").strip()
        return {
            "web_user_authenticated": bool(session.get(SESSION_USER_ID_KEY)),
            "web_user_email": user_email,
        }

    @app.before_request
    def _enforce_web_auth():
        endpoint = request.endpoint or ""
        if not endpoint or endpoint == "static":
            return None

        auth_service = _active_user_service()
        if auth_service is None:
            g.current_web_user = None
            return None

        current_user = None
        raw_user_id = session.get(SESSION_USER_ID_KEY)
        if raw_user_id is not None:
            try:
                current_user = auth_service.get_user_by_id(int(raw_user_id))
            except Exception:
                current_user = None
        if current_user:
            g.current_web_user = current_user
            return None
        if raw_user_id is not None:
            _clear_web_auth_session()
        g.current_web_user = None

        if endpoint in public_web_endpoints:
            return None
        if endpoint not in protected_web_endpoints:
            return None

        next_value = request.path
        if request.query_string:
            next_value = request.full_path.rstrip("?")
        return redirect(url_for("web_login", next=next_value))

    @app.route("/login", methods=["GET", "POST"])
    def web_login():
        auth_service = _active_user_service()
        next_url = _resolve_next("dashboard")

        if auth_service is None:
            return (
                render_template(
                    "login.html",
                    title="Login",
                    next_url=next_url,
                    form_data={"email": ""},
                    error_message=_auth_unavailable_message("Autenticacao"),
                ),
                503,
            )

        if request.method == "GET":
            if getattr(g, "current_web_user", None):
                return redirect(next_url)
            return render_template(
                "login.html",
                title="Login",
                next_url=next_url,
                form_data={"email": ""},
                error_message="",
            )

        email = str(request.form.get("email", "") or "").strip()
        password = str(request.form.get("password", "") or "")
        if not email or not password:
            return (
                render_template(
                    "login.html",
                    title="Login",
                    next_url=next_url,
                    form_data={"email": email},
                    error_message="Preencha email e senha para continuar.",
                ),
                400,
            )

        try:
            user = auth_service.authenticate_user(email=email, password=password)
        except (UserValidationError, UserAuthError):
            return (
                render_template(
                    "login.html",
                    title="Login",
                    next_url=next_url,
                    form_data={"email": email},
                    error_message="Login invalido. Confira email e senha.",
                ),
                401,
            )
        except Exception as exc:
            app.logger.exception("Falha no login web")
            return (
                render_template(
                    "login.html",
                    title="Login",
                    next_url=next_url,
                    form_data={"email": email},
                    error_message=f"Nao foi possivel autenticar agora. Tente novamente. Detalhe: {exc}",
                ),
                500,
            )

        _set_web_auth_session(user)
        flash("Login realizado com sucesso.", "success")
        return redirect(next_url)

    @app.route("/cadastro", methods=["GET", "POST"])
    def web_register():
        auth_service = _active_user_service()
        next_url = _resolve_next("dashboard")

        if auth_service is None:
            return (
                render_template(
                    "register.html",
                    title="Cadastro",
                    next_url=next_url,
                    form_data={"email": ""},
                    error_message=_auth_unavailable_message("Cadastro"),
                ),
                503,
            )

        if request.method == "GET":
            if getattr(g, "current_web_user", None):
                return redirect(next_url)
            return render_template(
                "register.html",
                title="Cadastro",
                next_url=next_url,
                form_data={"email": ""},
                error_message="",
            )

        email = str(request.form.get("email", "") or "").strip()
        password = str(request.form.get("password", "") or "")
        confirm_password = str(request.form.get("confirm_password", "") or "")

        if not email or not password or not confirm_password:
            return (
                render_template(
                    "register.html",
                    title="Cadastro",
                    next_url=next_url,
                    form_data={"email": email},
                    error_message="Preencha todos os campos obrigatorios.",
                ),
                400,
            )
        if password != confirm_password:
            return (
                render_template(
                    "register.html",
                    title="Cadastro",
                    next_url=next_url,
                    form_data={"email": email},
                    error_message="As senhas informadas nao conferem.",
                ),
                400,
            )

        try:
            user = auth_service.register_user(email=email, password=password)
        except UserAlreadyExistsError:
            return (
                render_template(
                    "register.html",
                    title="Cadastro",
                    next_url=next_url,
                    form_data={"email": email},
                    error_message="Usuario ja existe para este email.",
                ),
                409,
            )
        except UserValidationError as exc:
            return (
                render_template(
                    "register.html",
                    title="Cadastro",
                    next_url=next_url,
                    form_data={"email": email},
                    error_message=str(exc),
                ),
                400,
            )
        except Exception as exc:
            app.logger.exception("Falha no cadastro web")
            return (
                render_template(
                    "register.html",
                    title="Cadastro",
                    next_url=next_url,
                    form_data={"email": email},
                    error_message=f"Nao foi possivel concluir o cadastro agora. Detalhe: {exc}",
                ),
                500,
            )

        _set_web_auth_session(user)
        flash("Cadastro realizado com sucesso.", "success")
        return redirect(next_url)

    @app.get("/register")
    def web_register_redirect():
        next_url = str(request.args.get("next", "") or "").strip()
        if _is_safe_internal_next(next_url):
            return redirect(url_for("web_register", next=next_url))
        return redirect(url_for("web_register"))

    @app.route("/logout", methods=["GET", "POST"])
    def web_logout():
        _clear_web_auth_session()
        flash("Sessao encerrada.", "info")
        return redirect(url_for("web_login"))

    @app.get("/dashboard")
    def dashboard():
        contracts_data: list[dict[str, object]] = []
        recent_reports: list[dict[str, object]] = []
        total_contracts = 0
        total_reports = 0
        recent_reports_count = 0
        recent_window_days = 7
        warning_message = ""
        empty_state_message = ""
        load_errors: list[str] = []

        dashboard_contract_service = _active_contract_service()
        dashboard_report_service = _active_report_service()
        if dashboard_contract_service is None or dashboard_report_service is None:
            empty_state_message = (
                "Ainda nao ha dados iniciais para exibir no dashboard. "
                "Assim que contratos e relatorios forem cadastrados, os indicadores aparecem aqui."
            )
        else:
            try:
                total_contracts = max(0, int(dashboard_contract_service.count_contracts()))
                contracts = dashboard_contract_service.list_contracts(limit=20)
                contracts_data = [c.to_dict() for c in contracts]
            except Exception as exc:
                app.logger.warning("Falha ao carregar contratos do dashboard: %s", exc)
                load_errors.append("contracts")

            try:
                total_reports = max(0, int(dashboard_report_service.count_reports()))
                recent_reports_count = max(
                    0,
                    int(dashboard_report_service.count_recent_reports(days=recent_window_days)),
                )
                recent_reports = list(dashboard_report_service.list_recent_reports(limit=20))
            except Exception as exc:
                app.logger.warning("Falha ao carregar relatorios do dashboard: %s", exc)
                load_errors.append("reports")

            if load_errors:
                warning_message = (
                    "Os dados do dashboard nao puderam ser atualizados completamente agora. "
                    "Exibindo o que foi possivel carregar."
                )
            elif (
                total_contracts == 0
                and total_reports == 0
                and recent_reports_count == 0
                and not contracts_data
                and not recent_reports
            ):
                empty_state_message = (
                    "Nenhum contrato ou relatorio encontrado ainda. "
                    "Cadastre os primeiros registros para popular o dashboard."
                )

        return render_template(
            "dashboard.html",
            title="Dashboard",
            warning_message=warning_message,
            empty_state_message=empty_state_message,
            total_contracts=total_contracts,
            total_reports=total_reports,
            recent_reports_count=recent_reports_count,
            recent_window_days=recent_window_days,
            contracts=contracts_data,
            recent_reports=recent_reports,
        )

    @app.get("/perfil")
    def web_profile():
        user = dict(getattr(g, "current_web_user", None) or {})
        snapshot = _read_workspace_snapshot(report_limit=3)
        created_at = str(user.get("created_at", "") or "").strip()
        account_status = "Sessao web ativa" if session.get(SESSION_USER_ID_KEY) else "Sessao indisponivel"
        return render_template(
            "profile.html",
            title="Perfil",
            user=user,
            created_at=created_at,
            account_status=account_status,
            token_active=bool(session.get(SESSION_AUTH_TOKEN_KEY)),
            database_enabled=bool(app.config.get("CONTRACTS_DB_ENABLED")),
            total_contracts=int(snapshot.get("total_contracts", 0) or 0),
            total_reports=int(snapshot.get("total_reports", 0) or 0),
            recent_reports_count=int(snapshot.get("recent_reports_count", 0) or 0),
            recent_reports=list(snapshot.get("recent_reports", []) or []),
        )

    @app.get("/configuracoes")
    def web_settings():
        user = dict(getattr(g, "current_web_user", None) or {})
        snapshot = _read_workspace_snapshot(report_limit=3)
        return render_template(
            "settings.html",
            title="Configuracoes",
            user=user,
            token_active=bool(session.get(SESSION_AUTH_TOKEN_KEY)),
            database_enabled=bool(app.config.get("CONTRACTS_DB_ENABLED")),
            total_contracts=int(snapshot.get("total_contracts", 0) or 0),
            total_reports=int(snapshot.get("total_reports", 0) or 0),
            recent_reports_count=int(snapshot.get("recent_reports_count", 0) or 0),
        )

    @app.get("/resultados")
    def results_list():
        rows = [row for row in service.list_processing_results(limit=200) if bool(row.get("has_files"))]
        for row in rows:
            if row.get("contract_label"):
                continue
            resolved_label, _ = _resolve_contract_context(row.get("contract_id", ""))
            if resolved_label and resolved_label != str(row.get("contract_id", "") or "").strip():
                row["contract_label"] = resolved_label

        nucleo_registry_rows = service.list_nucleo_registry(search="", status="")
        nucleo_options = sorted(
            {
                str(item.get("nucleo", "") or "").strip()
                for item in nucleo_registry_rows
                if str(item.get("nucleo", "") or "").strip()
            },
            key=lambda value: service._normalize_nucleo_key(value),
        )

        summary = {
            "total": len(rows),
            "success": sum(1 for row in rows if row.get("status_level") == "sucesso"),
            "error": sum(1 for row in rows if row.get("status_level") == "erro"),
            "with_files": sum(1 for row in rows if row.get("has_files")),
        }
        return render_template(
            "results.html",
            title="Resultados gerados",
            rows=rows,
            summary=summary,
            nucleo_options=nucleo_options,
        )

    @app.get("/resultados/<output_name>")
    def result_detail(output_name: str):
        row = service.get_processing_result(output_name)
        if row is None:
            abort(404)

        if not row.get("contract_label"):
            resolved_label, _ = _resolve_contract_context(row.get("contract_id", ""))
            if resolved_label and resolved_label != str(row.get("contract_id", "") or "").strip():
                row["contract_label"] = resolved_label

        return render_template(
            "result_detail.html",
            title="Resultado do processamento",
            row=row,
        )

    @app.get("/resultados/arquivo/<path:relative_path>")
    def result_file(relative_path: str):
        file_path = service.resolve_result_file(relative_path)
        if file_path is None:
            abort(404)

        download = str(request.args.get("download", "") or "").strip().lower() in {"1", "true", "on", "sim"}
        return send_file(
            file_path,
            as_attachment=download,
            download_name=file_path.name,
        )

    def _render_contract_form(
        form_data: Dict[str, str] | None = None,
        error_message: str = "",
    ):
        defaults = {
            "nome_contrato": "",
            "numero_contrato": "",
            "objeto_contrato": "",
            "data_assinatura": "",
            "vigencia_inicio": "",
            "vigencia_fim": "",
            "prazo_dias": "",
            "valor_contrato": "",
            "contratante_nome": "",
            "contratante_cnpj": "",
            "contratada_nome": "",
            "contratada_cnpj": "",
            "regional": "",
            "diretoria": "",
            "municipios_atendidos": "",
            "status_contrato": "em_implantacao",
            "reajuste_indice": "",
            "prazo_pagamento_dias": "",
            "possui_ordem_servico": "",
            "observacoes": "",
        }
        payload = dict(defaults)
        payload.update(dict(form_data or {}))
        return render_template(
            "contract_form.html",
            title="Novo contrato",
            form_data=payload,
            error_message=str(error_message or ""),
            status_options=["em_implantacao", "ativo", "suspenso", "encerrado"],
        )

    @app.get("/contratos")
    def web_contracts_list():
        contracts_service = _active_contract_service()
        if contracts_service is None:
            return (
                render_template(
                    "contracts_list.html",
                    title="Contratos",
                    contracts=[],
                    error_message=(
                        "O modulo de contratos ainda nao esta disponivel neste ambiente. "
                        "Tente novamente depois que a conexao com banco estiver ativa."
                    ),
                ),
                503,
            )

        try:
            contracts = contracts_service.list_contracts(limit=200)
            rows = [item.to_dict() for item in contracts]
        except Exception as exc:
            app.logger.warning("Falha ao listar contratos na interface web: %s", exc)
            return (
                render_template(
                    "contracts_list.html",
                    title="Contratos",
                    contracts=[],
                    error_message="Nao foi possivel carregar os contratos agora. Tente novamente em instantes.",
                ),
                500,
            )

        return render_template(
            "contracts_list.html",
            title="Contratos",
            contracts=rows,
            error_message="",
        )

    @app.get("/contratos/novo")
    def web_contracts_new():
        contracts_service = _active_contract_write_service()
        if contracts_service is None:
            return (
                _render_contract_form(
                    error_message=(
                        "O modulo de contratos ainda nao esta disponivel neste ambiente. "
                        "Ative a conexao com banco para criar novos contratos."
                    )
                ),
                503,
            )
        return _render_contract_form()

    @app.post("/contratos")
    def web_contracts_create():
        contracts_service = _active_contract_write_service()
        if contracts_service is None:
            return (
                _render_contract_form(
                    form_data={
                        "nome_contrato": str(request.form.get("nome_contrato", "") or "").strip(),
                        "numero_contrato": str(request.form.get("numero_contrato", "") or "").strip(),
                        "objeto_contrato": str(request.form.get("objeto_contrato", "") or "").strip(),
                        "data_assinatura": str(request.form.get("data_assinatura", "") or "").strip(),
                        "vigencia_inicio": str(request.form.get("vigencia_inicio", "") or "").strip(),
                        "vigencia_fim": str(request.form.get("vigencia_fim", "") or "").strip(),
                        "prazo_dias": str(request.form.get("prazo_dias", "") or "").strip(),
                        "valor_contrato": str(request.form.get("valor_contrato", "") or "").strip(),
                        "contratante_nome": str(request.form.get("contratante_nome", "") or "").strip(),
                        "contratante_cnpj": str(request.form.get("contratante_cnpj", "") or "").strip(),
                        "contratada_nome": str(request.form.get("contratada_nome", "") or "").strip(),
                        "contratada_cnpj": str(request.form.get("contratada_cnpj", "") or "").strip(),
                        "regional": str(request.form.get("regional", "") or "").strip(),
                        "diretoria": str(request.form.get("diretoria", "") or "").strip(),
                        "municipios_atendidos": str(request.form.get("municipios_atendidos", "") or "").strip(),
                        "status_contrato": str(request.form.get("status_contrato", "em_implantacao") or "em_implantacao").strip().lower(),
                        "reajuste_indice": str(request.form.get("reajuste_indice", "") or "").strip(),
                        "prazo_pagamento_dias": str(request.form.get("prazo_pagamento_dias", "") or "").strip(),
                        "possui_ordem_servico": "1" if request.form.get("possui_ordem_servico") else "",
                        "observacoes": str(request.form.get("observacoes", "") or "").strip(),
                    },
                    error_message=(
                        "O modulo de contratos ainda nao esta disponivel neste ambiente. "
                        "Ative a conexao com banco para criar novos contratos."
                    ),
                ),
                503,
            )

        payload = {
            "nome_contrato": str(request.form.get("nome_contrato", "") or "").strip(),
            "numero_contrato": str(request.form.get("numero_contrato", "") or "").strip(),
            "objeto_contrato": str(request.form.get("objeto_contrato", "") or "").strip(),
            "data_assinatura": str(request.form.get("data_assinatura", "") or "").strip(),
            "vigencia_inicio": str(request.form.get("vigencia_inicio", "") or "").strip(),
            "vigencia_fim": str(request.form.get("vigencia_fim", "") or "").strip(),
            "prazo_dias": str(request.form.get("prazo_dias", "") or "").strip(),
            "valor_contrato": str(request.form.get("valor_contrato", "") or "").strip(),
            "contratante_nome": str(request.form.get("contratante_nome", "") or "").strip(),
            "contratante_cnpj": str(request.form.get("contratante_cnpj", "") or "").strip(),
            "contratada_nome": str(request.form.get("contratada_nome", "") or "").strip(),
            "contratada_cnpj": str(request.form.get("contratada_cnpj", "") or "").strip(),
            "regional": str(request.form.get("regional", "") or "").strip(),
            "diretoria": str(request.form.get("diretoria", "") or "").strip(),
            "municipios_atendidos": str(request.form.get("municipios_atendidos", "") or "").strip(),
            "status_contrato": str(request.form.get("status_contrato", "em_implantacao") or "em_implantacao").strip().lower(),
            "reajuste_indice": str(request.form.get("reajuste_indice", "") or "").strip(),
            "prazo_pagamento_dias": str(request.form.get("prazo_pagamento_dias", "") or "").strip(),
            "possui_ordem_servico": "1" if request.form.get("possui_ordem_servico") else "",
            "observacoes": str(request.form.get("observacoes", "") or "").strip(),
        }

        try:
            created = contracts_service.create_contract(payload)
        except ContractValidationError as exc:
            return _render_contract_form(form_data=payload, error_message=str(exc)), 400
        except ContractConflictError as exc:
            return _render_contract_form(form_data=payload, error_message=str(exc)), 409
        except Exception as exc:
            app.logger.warning("Falha ao criar contrato via interface web: %s", exc)
            return (
                _render_contract_form(
                    form_data=payload,
                    error_message="Nao foi possivel salvar o contrato agora. Tente novamente em instantes.",
                ),
                500,
            )

        flash(f"Contrato {created.numero_contrato} criado com sucesso.", "success")
        return redirect(url_for("web_contracts_list"))

    @app.get("/")
    def index():
        if _active_user_service() is not None:
            return redirect(url_for("dashboard"))
        return _render_entry_page()

    @app.get("/nucleos")
    def nucleos():
        search = str(request.args.get("q", "") or "").strip()
        status = str(request.args.get("status", "") or "").strip().lower()
        edit_name = str(request.args.get("edit", "") or "").strip()
        prefill = {
            "nucleo": str(request.args.get("prefill_nucleo", "") or "").strip(),
            "municipio": str(request.args.get("prefill_municipio", "") or "").strip(),
            "logradouro_principal": str(request.args.get("prefill_logradouro", "") or "").strip(),
        }
        form_data = service.get_nucleo_registry_form(edit_name, prefill=prefill)
        rows = service.list_nucleo_registry(search=search, status=status)

        return render_template(
            "nucleos.html",
            rows=rows,
            q=search,
            status_filter=status,
            form_data=form_data,
            saved_message=str(request.args.get("saved", "") or "").strip(),
            error_message="",
        )

    @app.post("/nucleos")
    def nucleos_save():
        form_data = {
            "original_nucleo": str(request.form.get("original_nucleo", "") or "").strip(),
            "nucleo": str(request.form.get("nucleo", "") or "").strip(),
            "municipio": str(request.form.get("municipio", "") or "").strip(),
            "status": str(request.form.get("status", "ativo") or "ativo").strip(),
            "aliases": str(request.form.get("aliases", "") or ""),
            "observacoes": str(request.form.get("observacoes", "") or "").strip(),
            "logradouro_principal": str(request.form.get("logradouro_principal", "") or "").strip(),
            "logradouros_padrao": str(request.form.get("logradouros_padrao", "") or ""),
            "equipes_padrao": str(request.form.get("equipes_padrao", "") or ""),
        }
        saved_entry, error_message = service.upsert_nucleo_registry_entry(form_data)
        search = str(request.form.get("q", "") or "").strip()
        status = str(request.form.get("status_filter", "") or "").strip().lower()
        rows = service.list_nucleo_registry(search=search, status=status)

        if error_message:
            return render_template(
                "nucleos.html",
                rows=rows,
                q=search,
                status_filter=status,
                form_data=form_data,
                saved_message="",
                error_message=error_message,
            )

        saved_name = str((saved_entry or {}).get("nucleo", "") or form_data.get("nucleo", "")).strip()
        return redirect(url_for("nucleos", q=search, status=status, edit=saved_name, saved="Cadastro salvo com sucesso."))

    @app.post("/preview")
    def preview():
        mensagem = str(request.form.get("mensagem", "") or "").strip()
        form_data = _collect_form_fields(request.form)
        form_data, autofill_info = service.apply_nucleo_defaults(form_data)

        if not mensagem:
            return _render_entry_page(
                form_data=form_data,
                mensagem=mensagem,
                error_message="Cole a mensagem completa do WhatsApp para iniciar a revisao.",
                autofill_info=autofill_info,
            )

        preview_data = service.build_preview(mensagem, form_data)
        draft_id = service.save_draft(
            {
                "raw_message": preview_data["raw_message"],
                "parsed": preview_data["parsed"],
                "parser_mode": preview_data["parser_mode"],
            }
        )

        return render_template(
            "review.html",
            draft_id=draft_id,
            parser_mode=preview_data["parser_mode"],
            main_fields=preview_data["main_fields"],
            main_lists=preview_data["main_lists"],
            nucleo_groups=preview_data.get("nucleo_groups", []),
            missing_main_fields=preview_data["missing_main_fields"],
            summary=preview_data["summary"],
            parsed=preview_data["parsed"],
            alerts=preview_data["alerts"],
            alert_items=preview_data["alert_items"],
            alerts_by_priority=preview_data.get("alerts_by_priority", {}),
            municipio_profile_status=preview_data.get("municipio_profile_status", {}),
            service_catalog=preview_data.get("service_catalog", {}),
            unmapped_rows=preview_data["unmapped_rows"],
            unmapped_term_stats=preview_data["unmapped_term_stats"],
            incomplete_rows=preview_data["incomplete_rows"],
            context_overview=preview_data["context_overview"],
            nucleo_reference=preview_data["nucleo_reference"],
            autofill_info=autofill_info,
            apply_all=form_data.get("aplicar_todos") == "1",
            contract_id=str(form_data.get("contract_id", "") or "").strip(),
            contract_options=_build_contract_options(),
            raw_message=preview_data["raw_message"],
            error_message="",
        )

    @app.post("/generate")
    def generate():
        draft_id = str(request.form.get("draft_id", "") or "").strip()
        action = str(request.form.get("action", "generate") or "generate").strip()
        form_data = _collect_form_fields(request.form)
        form_data, autofill_info = service.apply_nucleo_defaults(form_data)
        contract_id_raw = str(form_data.get("contract_id", "") or "").strip()
        contract_label, contract_context = _resolve_contract_context(contract_id_raw)

        if not draft_id:
            return _render_entry_page(
                form_data=form_data,
                mensagem=str(request.form.get("mensagem", "") or ""),
                error_message="Previa nao localizada. Volte para Nova entrada e processe novamente.",
                autofill_info=autofill_info,
            )

        try:
            draft = service.load_draft(draft_id)
        except Exception as exc:
            return _render_entry_page(
                form_data=form_data,
                mensagem=str(request.form.get("mensagem", "") or ""),
                error_message=f"Nao foi possivel abrir a previa salva. Refaca o processamento da mensagem. Detalhe: {exc}",
                autofill_info=autofill_info,
            )

        if action == "back":
            return _render_entry_page(
                form_data=form_data,
                mensagem=str(draft.get("raw_message", "") or ""),
                error_message="",
                autofill_info=autofill_info,
            )

        try:
            result = service.generate_from_draft(
                draft_id,
                form_data,
                contract_id=contract_id_raw,
                contract_label=contract_label,
            )
        except Exception as exc:
            service.register_error_history(
                draft_id,
                form_data,
                str(exc),
                contract_id=contract_id_raw,
                contract_label=contract_label,
            )
            parsed = service.apply_overrides(draft.get("parsed", {}), form_data)
            main_fields = service.extract_main_fields(parsed)
            context_overview = service.collect_context_overview(parsed)
            main_lists = service.build_main_lists(main_fields, context_overview)
            nucleo_groups = service.build_nucleo_groups(parsed)
            unmapped_rows = service.enrich_unmapped_for_review(service.collect_unmapped(parsed))
            incomplete_rows = service.collect_incomplete_execution(parsed)
            municipio_profile_status = service._municipio_profile_status(main_fields)
            alert_items = service.build_alert_items(
                parsed,
                main_fields=main_fields,
                main_lists=main_lists,
                context_overview=context_overview,
                unmapped_rows=unmapped_rows,
                incomplete_rows=incomplete_rows,
            )
            return render_template(
                "review.html",
                draft_id=draft_id,
                parser_mode=str(draft.get("parser_mode", "desconhecido") or "desconhecido"),
                main_fields=main_fields,
                main_lists=main_lists,
                nucleo_groups=nucleo_groups,
                missing_main_fields=service._missing_main_fields(main_fields),
                summary=service.summarize_preview(parsed),
                parsed=parsed,
                alerts=[a.get("message", "") for a in alert_items],
                alert_items=alert_items,
                alerts_by_priority=service.split_alerts_by_priority(alert_items),
                municipio_profile_status=municipio_profile_status,
                service_catalog=service.get_service_catalog_ui(),
                unmapped_rows=unmapped_rows,
                unmapped_term_stats=service.collect_unmapped_term_stats(unmapped_rows),
                incomplete_rows=incomplete_rows,
                context_overview=context_overview,
                nucleo_reference=service.get_nucleo_reference_ui(),
                autofill_info=autofill_info,
                apply_all=form_data.get("aplicar_todos") == "1",
                contract_id=str(form_data.get("contract_id", "") or "").strip(),
                contract_options=_build_contract_options(),
                raw_message=str(draft.get("raw_message", "") or ""),
                error_message=f"Nao foi possivel gerar os arquivos agora. Revise os alertas abaixo e tente novamente. Detalhe: {exc}",
            )

        baseline_main = service.extract_main_fields(draft.get("parsed", {}))
        result["autofill_info"] = autofill_info
        result["main_field_audit"] = _build_main_field_audit(
            service,
            baseline_main,
            result.get("main_fields", {}),
            autofill_info,
        )
        result["contract_label"] = contract_label
        result["contract_context"] = contract_context or {}
        result["output_dir_uri"] = service._to_file_uri(result.get("output_dir", ""))
        result["base_gerencial_uri"] = service._to_file_uri(result.get("base_gerencial_path", ""))

        report_sync = {"success": True, "data": {"registered": 0, "items": []}}
        if contract_id_raw:
            if report_service is None:
                report_sync = {
                    "success": False,
                    "data": None,
                    "error": "Registro de relatorios desativado: banco indisponivel.",
                }
            else:
                try:
                    contract_id = int(contract_id_raw)
                    registered = []
                    files_map = dict(result.get("files", {}) or {})
                    for file_path in files_map.values():
                        filename = Path(str(file_path or "")).name
                        if not filename:
                            continue
                        created = create_report_service(report_service, contract_id, filename)
                        registered.append(created)
                    report_sync = {
                        "success": True,
                        "data": {"registered": len(registered), "items": registered},
                    }
                except Exception as exc:
                    app.logger.warning("Falha ao registrar relatorios no banco: %s", exc)
                    report_sync = {
                        "success": False,
                        "data": None,
                        "error": str(exc),
                    }
        result["report_sync"] = report_sync

        return render_template("result.html", result=result)

    @app.get("/history")
    def history():
        q_raw = str(request.args.get("q", "") or "").strip()
        status_filter = str(request.args.get("status", "") or "").strip().lower()
        obra_data_filter_raw = str(request.args.get("obra_data", "") or "").strip()
        nucleo_filter_raw = str(request.args.get("nucleo", "") or "").strip()
        municipio_filter_raw = str(request.args.get("municipio", "") or "").strip()
        equipe_filter_raw = str(request.args.get("equipe", "") or "").strip()
        alertas_filter = str(request.args.get("alertas", "") or "").strip().lower()
        processed_from_raw = str(request.args.get("processed_from", "") or "").strip()
        processed_to_raw = str(request.args.get("processed_to", "") or "").strip()
        nm_days_raw = str(request.args.get("nm_days", "1") or "1").strip()
        nm_runs_raw = str(request.args.get("nm_runs", "120") or "120").strip()

        q_norm = service._normalize_nucleo_key(q_raw)
        obra_data_filter = service._normalize_nucleo_key(obra_data_filter_raw)
        nucleo_filter = service._normalize_nucleo_key(nucleo_filter_raw)
        municipio_filter = service._normalize_nucleo_key(municipio_filter_raw)
        equipe_filter = service._normalize_nucleo_key(equipe_filter_raw)

        def _safe_int(raw: str, default: int, min_value: int, max_value: int) -> int:
            try:
                parsed = int(raw)
            except Exception:
                parsed = default
            return max(min_value, min(parsed, max_value))

        def _parse_date(raw: str):
            text = str(raw or "").strip()
            if not text:
                return None
            for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
                try:
                    return datetime.strptime(text, fmt).date()
                except Exception:
                    continue
            return None

        nm_days = _safe_int(nm_days_raw, default=30, min_value=1, max_value=3650)
        nm_runs = _safe_int(nm_runs_raw, default=120, min_value=1, max_value=5000)
        processed_from = _parse_date(processed_from_raw)
        processed_to = _parse_date(processed_to_raw)

        all_rows = list(service.read_history(limit=1000))
        rows = list(all_rows)

        if status_filter in {"sucesso", "erro"}:
            rows = [r for r in rows if r.get("status_level") == status_filter]

        if alertas_filter == "com_alerta":
            rows = [r for r in rows if str(r.get("has_alerts", "") or "").strip().lower() == "sim"]
        elif alertas_filter == "sem_alerta":
            rows = [r for r in rows if str(r.get("has_alerts", "") or "").strip().lower() == "nao"]

        if obra_data_filter:
            rows = [r for r in rows if service._match_filter_text(r.get("obra_data", ""), obra_data_filter)]

        if nucleo_filter:
            rows = [r for r in rows if service._match_filter_text(r.get("nucleo", ""), nucleo_filter)]

        if municipio_filter:
            rows = [r for r in rows if service._match_filter_text(r.get("municipio", ""), municipio_filter)]

        if equipe_filter:
            rows = [r for r in rows if service._match_filter_text(r.get("equipe", ""), equipe_filter)]

        if processed_from or processed_to:
            filtered_rows = []
            for row in rows:
                dt = service._parse_history_datetime(row.get("processed_at", ""))
                if not dt:
                    continue
                dt_date = dt.date()
                if processed_from and dt_date < processed_from:
                    continue
                if processed_to and dt_date > processed_to:
                    continue
                filtered_rows.append(row)
            rows = filtered_rows

        if q_norm:
            def _row_text(r: dict) -> str:
                parts = [
                    r.get("processed_at", ""),
                    r.get("obra_data", ""),
                    r.get("nucleo", ""),
                    r.get("municipio", ""),
                    r.get("logradouro", ""),
                    r.get("equipe", ""),
                    r.get("status", ""),
                    r.get("alertas", ""),
                    r.get("output_dir", ""),
                ]
                return " ".join(str(p) for p in parts)

            rows = [r for r in rows if service._match_filter_text(_row_text(r), q_norm)]

        summary = {
            "total": len(all_rows),
            "sucesso": sum(1 for r in all_rows if r.get("status_level") == "sucesso"),
            "erro": sum(1 for r in all_rows if r.get("status_level") == "erro"),
            "filtrado": len(rows),
        }
        unmapped_dashboard = service.build_unmapped_dashboard(window_days=nm_days, recent_runs=nm_runs)

        return render_template(
            "history.html",
            rows=rows,
            q=q_raw,
            obra_data_filter=obra_data_filter_raw,
            nucleo_filter=nucleo_filter_raw,
            municipio_filter=municipio_filter_raw,
            equipe_filter=equipe_filter_raw,
            alertas_filter=alertas_filter,
            processed_from=processed_from_raw,
            processed_to=processed_to_raw,
            status_filter=status_filter,
            summary=summary,
            nm_days=nm_days,
            nm_runs=nm_runs,
            unmapped_dashboard=unmapped_dashboard,
        )
    @app.get("/gerencial")
    def gerencial():
        filters = _collect_management_filters(request.args)
        dashboard = None
        management_repo = app.config.get("MANAGEMENT_REPOSITORY")
        build_dashboard = getattr(management_repo, "build_gerencial_dashboard", None)
        if callable(build_dashboard):
            try:
                dashboard = build_dashboard(filters)
            except Exception as exc:
                app.logger.warning("Falha ao carregar painel gerencial pelo banco: %s", exc)

        # Fallback legado apenas quando o repositório/banco não está disponível.
        if dashboard is None:
            dashboard = service.build_management_layer(
                {
                    **filters,
                    "history_limit": 3000,
                }
            )

        return render_template(
            "gerencial.html",
            dashboard=dashboard,
            obra_from=filters["obra_from"],
            obra_to=filters["obra_to"],
            processed_from=filters["processed_from"],
            processed_to=filters["processed_to"],
            nucleo=filters["nucleo"],
            municipio=filters["municipio"],
            equipe=filters["equipe"],
            status_filter=filters["status"],
            alertas_filter=filters["alertas"],
            top_n=filters["top_n"],
        )

    @app.get("/institucional")
    def institucional():
        filters = _collect_management_filters(request.args)
        report = _build_institutional_report_with_fallback(filters)

        return render_template(
            "institucional.html",
            report=report,
            obra_from=filters["obra_from"],
            obra_to=filters["obra_to"],
            processed_from=filters["processed_from"],
            processed_to=filters["processed_to"],
            nucleo=filters["nucleo"],
            municipio=filters["municipio"],
            equipe=filters["equipe"],
            status_filter=filters["status"],
            alertas_filter=filters["alertas"],
            top_n=filters["top_n"],
        )

    @app.get("/institucional/export")
    def institucional_export():
        filters = _collect_management_filters(request.args)
        formato = str(request.args.get("formato", "html") or "html").strip().lower()
        report = _build_institutional_report_with_fallback(filters)

        if formato == "docx":
            content, filename = service.export_institutional_docx(report)
            response = make_response(content)
            response.headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            response.headers["Content-Disposition"] = f"attachment; filename={filename}"
            return response

        html_doc = render_template("institucional_export.html", report=report)
        filename = f"relatorio_institucional_{datetime.now():%Y%m%d_%H%M%S}.html"
        response = make_response(html_doc)
        response.headers["Content-Type"] = "text/html; charset=utf-8"
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response

    @app.get("/nova-entrada")
    def nova_entrada_redirect():
        return _render_entry_page()

    register_auth_routes(app, user_service, settings.auth_jwt_secret, settings.auth_jwt_exp_minutes)
    register_contract_routes(app, contract_service, report_service)

    return app


def main() -> None:
    settings = load_settings(Path(__file__).resolve().parents[2])
    app = create_app(settings=settings)
    app.run(host=settings.web_host, port=settings.web_port, debug=settings.web_debug)


if __name__ == "__main__":
    main()












