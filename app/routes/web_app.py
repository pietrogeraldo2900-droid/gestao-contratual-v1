from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict

from flask import Flask, make_response, redirect, render_template, request, url_for

from config.settings import AppSettings, load_settings
from app.services.web_service import WebPipelineService


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


def create_app(test_config: dict | None = None, settings: AppSettings | None = None) -> Flask:
    settings = settings or load_settings(Path(__file__).resolve().parents[2])
    base_dir = settings.base_dir

    app = Flask(__name__, template_folder=str(base_dir / "templates"))
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

    @app.get("/")
    def index():
        return render_template(
            "index.html",
            form_data={"data": "", "nucleo": "", "logradouro": "", "municipio": "", "equipe": "", "aplicar_todos": ""},
            mensagem="",
            error_message="",
            nucleo_reference=service.get_nucleo_reference_ui(),
            autofill_info={"matched": False, "applied": {}, "profile": {}},
        )

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
            return render_template(
                "index.html",
                form_data=form_data,
                mensagem=mensagem,
                error_message="Cole a mensagem completa do WhatsApp para iniciar a revisao.",
                nucleo_reference=service.get_nucleo_reference_ui(),
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
            raw_message=preview_data["raw_message"],
            error_message="",
        )

    @app.post("/generate")
    def generate():
        draft_id = str(request.form.get("draft_id", "") or "").strip()
        action = str(request.form.get("action", "generate") or "generate").strip()
        form_data = _collect_form_fields(request.form)
        form_data, autofill_info = service.apply_nucleo_defaults(form_data)

        if not draft_id:
            return render_template(
                "index.html",
                form_data=form_data,
                mensagem=str(request.form.get("mensagem", "") or ""),
                error_message="Previa nao localizada. Volte para Nova entrada e processe novamente.",
                nucleo_reference=service.get_nucleo_reference_ui(),
                autofill_info=autofill_info,
            )

        try:
            draft = service.load_draft(draft_id)
        except Exception as exc:
            return render_template(
                "index.html",
                form_data=form_data,
                mensagem=str(request.form.get("mensagem", "") or ""),
                error_message=f"Nao foi possivel abrir a previa salva. Refaca o processamento da mensagem. Detalhe: {exc}",
                nucleo_reference=service.get_nucleo_reference_ui(),
                autofill_info=autofill_info,
            )

        if action == "back":
            return render_template(
                "index.html",
                form_data=form_data,
                mensagem=str(draft.get("raw_message", "") or ""),
                error_message="",
                nucleo_reference=service.get_nucleo_reference_ui(),
                autofill_info=autofill_info,
            )

        try:
            result = service.generate_from_draft(draft_id, form_data)
        except Exception as exc:
            service.register_error_history(draft_id, form_data, str(exc))
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
        result["output_dir_uri"] = service._to_file_uri(result.get("output_dir", ""))
        result["base_gerencial_uri"] = service._to_file_uri(result.get("base_gerencial_path", ""))
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
        nm_days_raw = str(request.args.get("nm_days", "30") or "30").strip()
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

        all_rows = service.read_history(limit=1000)
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
        report = service.build_institutional_report(
            {
                **filters,
                "history_limit": 3000,
            }
        )

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
        report = service.build_institutional_report(
            {
                **filters,
                "history_limit": 3000,
            }
        )

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
        return redirect(url_for("index"))

    return app


def main() -> None:
    settings = load_settings(Path(__file__).resolve().parents[2])
    app = create_app(settings=settings)
    app.run(host=settings.web_host, port=settings.web_port, debug=settings.web_debug)


if __name__ == "__main__":
    main()












