from __future__ import annotations

from app.database.connection import DatabaseManager


def init_db(db: DatabaseManager) -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS users (
            id BIGSERIAL PRIMARY KEY,
            email VARCHAR(255) NOT NULL UNIQUE,
            password VARCHAR(255) NOT NULL,
            role VARCHAR(40),
            status VARCHAR(30) NOT NULL DEFAULT 'pending',
            approved_by BIGINT,
            approved_at TIMESTAMPTZ,
            last_login_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS role VARCHAR(40)",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS status VARCHAR(30) NOT NULL DEFAULT 'pending'",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS approved_by BIGINT",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMPTZ",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS contractor_name VARCHAR(255)",
        "UPDATE users SET status = 'active' WHERE status IS NULL OR status = ''",
        "UPDATE users SET role = 'admin_operacional' WHERE role IS NULL OR role = ''",
        """
        UPDATE users
        SET role = 'superadmin'
        WHERE id = (
            SELECT id FROM users ORDER BY created_at ASC LIMIT 1
        )
        AND NOT EXISTS (SELECT 1 FROM users WHERE role = 'superadmin')
        """,
        """
        CREATE TABLE IF NOT EXISTS contracts (
            id BIGSERIAL PRIMARY KEY,
            contract_code VARCHAR(80) NOT NULL UNIQUE,
            title VARCHAR(255) NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            status VARCHAR(30) NOT NULL DEFAULT 'rascunho',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS nome_contrato VARCHAR(255)",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS numero_contrato VARCHAR(80)",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS objeto_contrato TEXT",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS data_assinatura DATE",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS vigencia_inicio DATE",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS vigencia_fim DATE",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS prazo_dias INTEGER",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS valor_contrato NUMERIC(15,2)",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS contratante_nome VARCHAR(255)",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS contratante_cnpj VARCHAR(20)",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS contratada_nome VARCHAR(255)",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS contratada_cnpj VARCHAR(20)",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS regional VARCHAR(120)",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS diretoria VARCHAR(120)",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS municipios_atendidos TEXT",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS status_contrato VARCHAR(30)",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS reajuste_indice VARCHAR(120)",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS prazo_pagamento_dias INTEGER",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS possui_ordem_servico BOOLEAN",
        "ALTER TABLE contracts ADD COLUMN IF NOT EXISTS observacoes TEXT",
        """
        UPDATE contracts
        SET
            numero_contrato = COALESCE(NULLIF(numero_contrato, ''), contract_code),
            nome_contrato = COALESCE(NULLIF(nome_contrato, ''), title),
            objeto_contrato = COALESCE(NULLIF(objeto_contrato, ''), description),
            status_contrato = COALESCE(
                NULLIF(status_contrato, ''),
                CASE
                    WHEN status = 'rascunho' THEN 'em_implantacao'
                    WHEN status = 'cancelado' THEN 'suspenso'
                    WHEN status IN ('ativo', 'encerrado') THEN status
                    ELSE 'em_implantacao'
                END
            )
        """,
        """
        CREATE TABLE IF NOT EXISTS user_contract_permissions (
            user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            contract_id BIGINT NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (user_id, contract_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS reports (
            id BIGSERIAL PRIMARY KEY,
            contract_id BIGINT NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
            file_name VARCHAR(255) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS inspections (
            id BIGSERIAL PRIMARY KEY,
            contract_id BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
            titulo VARCHAR(255) NOT NULL,
            data_vistoria DATE NOT NULL,
            periodo VARCHAR(40),
            nucleo VARCHAR(180),
            municipio VARCHAR(180),
            local_vistoria TEXT,
            equipe VARCHAR(180),
            fiscal_nome VARCHAR(180),
            fiscal_contato VARCHAR(120),
            responsavel_nome VARCHAR(180),
            responsavel_contato VARCHAR(120),
            status VARCHAR(40) NOT NULL DEFAULT 'aberta',
            prioridade VARCHAR(20) NOT NULL DEFAULT 'media',
            resultado VARCHAR(20) NOT NULL DEFAULT 'pendente',
            score_geral NUMERIC(5,2) NOT NULL DEFAULT 0,
            observacoes TEXT,
            created_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS inspection_items (
            id BIGSERIAL PRIMARY KEY,
            inspection_id BIGINT NOT NULL REFERENCES inspections(id) ON DELETE CASCADE,
            ordem INTEGER NOT NULL DEFAULT 0,
            area VARCHAR(120),
            item_titulo VARCHAR(255) NOT NULL,
            descricao TEXT,
            status VARCHAR(30) NOT NULL DEFAULT 'pendente',
            severidade VARCHAR(20) NOT NULL DEFAULT 'baixa',
            prazo_ajuste DATE,
            responsavel_ajuste VARCHAR(180),
            valor_multa NUMERIC(14,2),
            evidencia_ref TEXT,
            quantidade_declarada NUMERIC(18,3) NOT NULL DEFAULT 0,
            quantidade_verificada NUMERIC(18,3) NOT NULL DEFAULT 0,
            quantidade_oficial NUMERIC(18,3) NOT NULL DEFAULT 0,
            verificado_informado BOOLEAN NOT NULL DEFAULT FALSE,
            divergencia_absoluta NUMERIC(18,3) NOT NULL DEFAULT 0,
            divergencia_percentual NUMERIC(9,2) NOT NULL DEFAULT 0,
            divergencia_status VARCHAR(40) NOT NULL DEFAULT 'sem_divergencia',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT chk_inspection_items_base_oficial_from_fiscal
                CHECK (quantidade_oficial = quantidade_verificada)
        )
        """,
        "ALTER TABLE inspection_items ADD COLUMN IF NOT EXISTS quantidade_declarada NUMERIC(18,3) NOT NULL DEFAULT 0",
        "ALTER TABLE inspection_items ADD COLUMN IF NOT EXISTS quantidade_verificada NUMERIC(18,3) NOT NULL DEFAULT 0",
        "ALTER TABLE inspection_items ADD COLUMN IF NOT EXISTS quantidade_oficial NUMERIC(18,3) NOT NULL DEFAULT 0",
        "ALTER TABLE inspection_items ADD COLUMN IF NOT EXISTS verificado_informado BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE inspection_items ADD COLUMN IF NOT EXISTS divergencia_absoluta NUMERIC(18,3) NOT NULL DEFAULT 0",
        "ALTER TABLE inspection_items ADD COLUMN IF NOT EXISTS divergencia_percentual NUMERIC(9,2) NOT NULL DEFAULT 0",
        "ALTER TABLE inspection_items ADD COLUMN IF NOT EXISTS divergencia_status VARCHAR(40) NOT NULL DEFAULT 'sem_divergencia'",
        """
        UPDATE inspection_items
        SET
            quantidade_declarada = COALESCE(quantidade_declarada, 0),
            quantidade_verificada = COALESCE(quantidade_verificada, 0),
            quantidade_oficial = COALESCE(quantidade_verificada, 0),
            verificado_informado = COALESCE(verificado_informado, FALSE),
            divergencia_absoluta = COALESCE(divergencia_absoluta, ABS(COALESCE(quantidade_verificada, 0) - COALESCE(quantidade_declarada, 0))),
            divergencia_percentual = COALESCE(
                divergencia_percentual,
                CASE
                    WHEN COALESCE(quantidade_declarada, 0) = 0 AND COALESCE(quantidade_verificada, 0) = 0 THEN 0
                    WHEN COALESCE(quantidade_declarada, 0) = 0 THEN 100
                    ELSE ROUND(
                        ABS(COALESCE(quantidade_verificada, 0) - COALESCE(quantidade_declarada, 0))
                        / NULLIF(ABS(COALESCE(quantidade_declarada, 0)), 0) * 100,
                        2
                    )
                END
            ),
            divergencia_status = COALESCE(NULLIF(divergencia_status, ''), 'sem_divergencia')
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'chk_inspection_items_base_oficial_from_fiscal'
                  AND conrelid = 'inspection_items'::regclass
            ) THEN
                ALTER TABLE inspection_items
                    ADD CONSTRAINT chk_inspection_items_base_oficial_from_fiscal
                    CHECK (quantidade_oficial = quantidade_verificada);
            END IF;
        END $$;
        """,
        """
        CREATE OR REPLACE FUNCTION trg_inspection_items_sync_oficial()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            NEW.quantidade_verificada := COALESCE(NEW.quantidade_verificada, 0);
            NEW.quantidade_declarada := COALESCE(NEW.quantidade_declarada, 0);
            NEW.quantidade_oficial := NEW.quantidade_verificada;
            NEW.verificado_informado := COALESCE(NEW.verificado_informado, FALSE);
            NEW.divergencia_absoluta := ABS(NEW.quantidade_verificada - NEW.quantidade_declarada);
            IF NEW.quantidade_declarada = 0 THEN
                IF NEW.quantidade_verificada = 0 THEN
                    NEW.divergencia_percentual := 0;
                ELSE
                    NEW.divergencia_percentual := 100;
                END IF;
            ELSE
                NEW.divergencia_percentual := ROUND(
                    ABS(NEW.quantidade_verificada - NEW.quantidade_declarada) / ABS(NEW.quantidade_declarada) * 100,
                    2
                );
            END IF;
            IF NOT NEW.verificado_informado THEN
                NEW.divergencia_status := 'nao_verificado';
            ELSIF NEW.quantidade_verificada = NEW.quantidade_declarada THEN
                NEW.divergencia_status := 'sem_divergencia';
            ELSIF NEW.quantidade_declarada = 0 AND NEW.quantidade_verificada > 0 THEN
                NEW.divergencia_status := 'novo_nao_declarado';
            ELSIF NEW.quantidade_declarada > 0 AND NEW.quantidade_verificada = 0 THEN
                NEW.divergencia_status := 'nao_executado';
            ELSIF NEW.quantidade_verificada > NEW.quantidade_declarada THEN
                NEW.divergencia_status := 'a_maior';
            ELSE
                NEW.divergencia_status := 'a_menor';
            END IF;
            RETURN NEW;
        END;
        $$;
        """,
        "DROP TRIGGER IF EXISTS trg_inspection_items_sync_oficial ON inspection_items",
        """
        CREATE TRIGGER trg_inspection_items_sync_oficial
        BEFORE INSERT OR UPDATE
        ON inspection_items
        FOR EACH ROW
        EXECUTE FUNCTION trg_inspection_items_sync_oficial()
        """,
        """
        CREATE TABLE IF NOT EXISTS daily_execution_declarations (
            id BIGSERIAL PRIMARY KEY,
            contract_id BIGINT NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
            declaration_date DATE NOT NULL,
            periodo VARCHAR(40),
            nucleo VARCHAR(180),
            municipio VARCHAR(180),
            logradouro TEXT,
            equipe VARCHAR(180),
            responsavel_nome VARCHAR(180),
            responsavel_contato VARCHAR(120),
            observacoes TEXT,
            is_official_base BOOLEAN NOT NULL DEFAULT FALSE,
            generated_inspection_id BIGINT REFERENCES inspections(id) ON DELETE SET NULL,
            created_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT chk_daily_execution_declarations_not_official CHECK (is_official_base = FALSE)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS daily_execution_declaration_items (
            id BIGSERIAL PRIMARY KEY,
            declaration_id BIGINT NOT NULL REFERENCES daily_execution_declarations(id) ON DELETE CASCADE,
            ordem INTEGER NOT NULL DEFAULT 0,
            servico_oficial VARCHAR(255) NOT NULL,
            servico_label VARCHAR(255),
            categoria VARCHAR(180),
            quantidade NUMERIC(18,3) NOT NULL DEFAULT 0,
            unidade VARCHAR(80),
            local_execucao TEXT,
            descricao TEXT,
            item_status VARCHAR(30) NOT NULL DEFAULT 'declarado',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS inspection_conference_sessions (
            id BIGSERIAL PRIMARY KEY,
            inspection_id BIGINT NOT NULL REFERENCES inspections(id) ON DELETE CASCADE,
            contract_id BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
            fiscal_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            status VARCHAR(20) NOT NULL DEFAULT 'rascunho',
            technical_notes TEXT,
            location_verified TEXT,
            summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_inspection_conference_session UNIQUE (inspection_id, fiscal_user_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS inspection_conference_item_results (
            id BIGSERIAL PRIMARY KEY,
            session_id BIGINT NOT NULL REFERENCES inspection_conference_sessions(id) ON DELETE CASCADE,
            inspection_item_id BIGINT REFERENCES inspection_items(id) ON DELETE SET NULL,
            ordem INTEGER NOT NULL DEFAULT 0,
            area VARCHAR(120),
            item_titulo VARCHAR(255) NOT NULL,
            descricao TEXT,
            quantidade_verificada NUMERIC(18,3) NOT NULL DEFAULT 0,
            verificado_informado BOOLEAN NOT NULL DEFAULT FALSE,
            status VARCHAR(30) NOT NULL DEFAULT 'pendente',
            observacao_tecnica TEXT,
            local_verificado TEXT,
            evidencia_ref TEXT,
            from_field BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_conference_item_result UNIQUE (session_id, inspection_item_id)
        )
        """,
        "ALTER TABLE inspection_conference_item_results ADD COLUMN IF NOT EXISTS verificado_informado BOOLEAN NOT NULL DEFAULT FALSE",
        """
        UPDATE inspection_conference_item_results
        SET verificado_informado = TRUE
        WHERE status <> 'pendente'
           OR quantidade_verificada <> 0
           OR COALESCE(NULLIF(TRIM(COALESCE(observacao_tecnica, '')), ''), '') <> ''
           OR COALESCE(NULLIF(TRIM(COALESCE(local_verificado, '')), ''), '') <> ''
           OR COALESCE(NULLIF(TRIM(COALESCE(evidencia_ref, '')), ''), '') <> ''
           OR from_field = TRUE
        """,
        """
        CREATE TABLE IF NOT EXISTS official_conference_results (
            id BIGSERIAL PRIMARY KEY,
            conference_session_id BIGINT NOT NULL REFERENCES inspection_conference_sessions(id) ON DELETE CASCADE,
            inspection_id BIGINT NOT NULL REFERENCES inspections(id) ON DELETE CASCADE,
            contract_id BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
            fiscal_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            source VARCHAR(40) NOT NULL DEFAULT 'fiscal_conferencia' CHECK (source = 'fiscal_conferencia'),
            item_origem VARCHAR(20) NOT NULL DEFAULT 'planejado',
            inspection_item_id BIGINT REFERENCES inspection_items(id) ON DELETE SET NULL,
            ordem INTEGER NOT NULL DEFAULT 0,
            area VARCHAR(120),
            item_titulo VARCHAR(255) NOT NULL,
            descricao TEXT,
            quantidade_verificada NUMERIC(18,3) NOT NULL DEFAULT 0,
            verificado_informado BOOLEAN NOT NULL DEFAULT FALSE,
            status VARCHAR(30) NOT NULL DEFAULT 'pendente',
            observacao_tecnica TEXT,
            local_verificado TEXT,
            evidencia_ref TEXT,
            consolidated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "ALTER TABLE official_conference_results ADD COLUMN IF NOT EXISTS verificado_informado BOOLEAN NOT NULL DEFAULT FALSE",
        """
        UPDATE official_conference_results
        SET verificado_informado = TRUE
        WHERE status <> 'pendente'
           OR quantidade_verificada <> 0
           OR COALESCE(NULLIF(TRIM(COALESCE(observacao_tecnica, '')), ''), '') <> ''
           OR COALESCE(NULLIF(TRIM(COALESCE(local_verificado, '')), ''), '') <> ''
           OR COALESCE(NULLIF(TRIM(COALESCE(evidencia_ref, '')), ''), '') <> ''
        """,
        """
        CREATE TABLE IF NOT EXISTS processing_history (
            id BIGSERIAL PRIMARY KEY,
            processed_at VARCHAR(40) NOT NULL,
            obra_data VARCHAR(32),
            nucleo TEXT,
            nucleo_detectado_texto TEXT,
            nucleo_oficial TEXT,
            logradouro TEXT,
            municipio TEXT,
            municipio_detectado_texto TEXT,
            municipio_oficial TEXT,
            nucleo_status_cadastro VARCHAR(80),
            equipe TEXT,
            status TEXT,
            contract_id VARCHAR(64),
            contract_label TEXT,
            output_dir TEXT,
            base_gerencial_path TEXT,
            master_dir TEXT,
            nao_mapeados VARCHAR(32),
            alertas TEXT,
            mensagem TEXT,
            generated_files_json JSONB NOT NULL DEFAULT '[]'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS admin_audit_log (
            id BIGSERIAL PRIMARY KEY,
            actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            target_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
            action VARCHAR(120) NOT NULL,
            metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS nucleo_registry_state (
            id SMALLINT PRIMARY KEY CHECK (id = 1),
            version VARCHAR(20) NOT NULL DEFAULT '2.0',
            payload_json JSONB NOT NULL DEFAULT '{"version":"2.0","nucleos":[]}'::jsonb,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS management_execucao (
            id BIGSERIAL PRIMARY KEY,
            source_uid VARCHAR(180) NOT NULL UNIQUE,
            id_item VARCHAR(80),
            id_frente VARCHAR(80),
            data_referencia DATE,
            contrato VARCHAR(160),
            programa VARCHAR(160),
            nucleo VARCHAR(180),
            logradouro TEXT,
            municipio VARCHAR(180),
            equipe VARCHAR(180),
            servico_oficial VARCHAR(255),
            servico_normalizado VARCHAR(255),
            servico_bruto TEXT,
            diametro_mm INTEGER,
            item_normalizado TEXT,
            item_original TEXT,
            categoria VARCHAR(180),
            categoria_item VARCHAR(180),
            quantidade NUMERIC(18,3) NOT NULL DEFAULT 0,
            unidade VARCHAR(80),
            arquivo_origem VARCHAR(255),
            nucleo_oficial VARCHAR(180),
            municipio_oficial VARCHAR(180),
            nucleo_status_cadastro VARCHAR(80)
        )
        """,
        "ALTER TABLE management_execucao ADD COLUMN IF NOT EXISTS servico_normalizado VARCHAR(255)",
        "ALTER TABLE management_execucao ADD COLUMN IF NOT EXISTS servico_bruto TEXT",
        "ALTER TABLE management_execucao ADD COLUMN IF NOT EXISTS diametro_mm INTEGER",
        "ALTER TABLE management_execucao ADD COLUMN IF NOT EXISTS item_normalizado TEXT",
        "ALTER TABLE management_execucao ADD COLUMN IF NOT EXISTS item_original TEXT",
        """
        CREATE TABLE IF NOT EXISTS management_frentes (
            id BIGSERIAL PRIMARY KEY,
            source_uid VARCHAR(180) NOT NULL UNIQUE,
            id_frente VARCHAR(80),
            data_referencia DATE,
            contrato VARCHAR(160),
            programa VARCHAR(160),
            nucleo VARCHAR(180),
            equipe VARCHAR(180),
            logradouro TEXT,
            municipio VARCHAR(180),
            status_frente VARCHAR(120),
            frente VARCHAR(180),
            arquivo_origem VARCHAR(255),
            nucleo_oficial VARCHAR(180),
            municipio_oficial VARCHAR(180),
            nucleo_status_cadastro VARCHAR(80)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS management_ocorrencias (
            id BIGSERIAL PRIMARY KEY,
            source_uid VARCHAR(180) NOT NULL UNIQUE,
            id_ocorrencia VARCHAR(80),
            id_frente VARCHAR(80),
            data_referencia DATE,
            contrato VARCHAR(160),
            programa VARCHAR(160),
            nucleo VARCHAR(180),
            equipe VARCHAR(180),
            logradouro TEXT,
            municipio VARCHAR(180),
            tipo_ocorrencia VARCHAR(180),
            descricao TEXT,
            impacto_producao VARCHAR(120),
            arquivo_origem VARCHAR(255),
            nucleo_oficial VARCHAR(180),
            municipio_oficial VARCHAR(180),
            nucleo_status_cadastro VARCHAR(80)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS service_registry (
            id BIGSERIAL PRIMARY KEY,
            servico_oficial VARCHAR(255) NOT NULL UNIQUE,
            categoria VARCHAR(180) NOT NULL DEFAULT 'servico_nao_mapeado',
            unidade_padrao VARCHAR(80),
            ativo BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS service_aliases (
            id BIGSERIAL PRIMARY KEY,
            alias_text VARCHAR(255) NOT NULL,
            alias_norm VARCHAR(255) NOT NULL UNIQUE,
            service_id BIGINT NOT NULL REFERENCES service_registry(id) ON DELETE CASCADE,
            source VARCHAR(40) NOT NULL DEFAULT 'manual',
            ativo BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_contracts_status ON contracts(status)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_contracts_numero_contrato ON contracts(numero_contrato)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_status_contrato ON contracts(status_contrato)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_created_at ON contracts(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_reports_contract_id ON reports(contract_id)",
        "CREATE INDEX IF NOT EXISTS idx_reports_created_at ON reports(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_inspections_contract_id ON inspections(contract_id)",
        "CREATE INDEX IF NOT EXISTS idx_inspections_status ON inspections(status)",
        "CREATE INDEX IF NOT EXISTS idx_inspections_data ON inspections(data_vistoria DESC)",
        "CREATE INDEX IF NOT EXISTS idx_inspection_items_inspection_id ON inspection_items(inspection_id)",
        "CREATE INDEX IF NOT EXISTS idx_inspection_items_status ON inspection_items(status)",
        "CREATE INDEX IF NOT EXISTS idx_inspection_items_severidade ON inspection_items(severidade)",
        "CREATE INDEX IF NOT EXISTS idx_inspection_items_divergencia_status ON inspection_items(divergencia_status)",
        "CREATE INDEX IF NOT EXISTS idx_inspection_items_qtd_oficial ON inspection_items(quantidade_oficial)",
        "CREATE INDEX IF NOT EXISTS idx_daily_declarations_contract ON daily_execution_declarations(contract_id)",
        "CREATE INDEX IF NOT EXISTS idx_daily_declarations_date ON daily_execution_declarations(declaration_date DESC)",
        "CREATE INDEX IF NOT EXISTS idx_daily_declarations_creator ON daily_execution_declarations(created_by)",
        "CREATE INDEX IF NOT EXISTS idx_daily_declarations_generated_inspection ON daily_execution_declarations(generated_inspection_id)",
        "CREATE INDEX IF NOT EXISTS idx_daily_declaration_items_declaration_id ON daily_execution_declaration_items(declaration_id)",
        "CREATE INDEX IF NOT EXISTS idx_daily_declaration_items_servico ON daily_execution_declaration_items(servico_oficial)",
        "CREATE INDEX IF NOT EXISTS idx_conference_sessions_inspection ON inspection_conference_sessions(inspection_id)",
        "CREATE INDEX IF NOT EXISTS idx_conference_sessions_fiscal ON inspection_conference_sessions(fiscal_user_id)",
        "CREATE INDEX IF NOT EXISTS idx_conference_sessions_status ON inspection_conference_sessions(status)",
        "CREATE INDEX IF NOT EXISTS idx_conference_item_results_session ON inspection_conference_item_results(session_id)",
        "CREATE INDEX IF NOT EXISTS idx_conference_item_results_status ON inspection_conference_item_results(status)",
        "CREATE INDEX IF NOT EXISTS idx_official_conference_inspection ON official_conference_results(inspection_id)",
        "CREATE INDEX IF NOT EXISTS idx_official_conference_contract ON official_conference_results(contract_id)",
        "CREATE INDEX IF NOT EXISTS idx_official_conference_fiscal ON official_conference_results(fiscal_user_id)",
        "CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)",
        "CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)",
        "CREATE INDEX IF NOT EXISTS idx_users_contractor_name ON users(contractor_name)",
        "CREATE INDEX IF NOT EXISTS idx_user_contract_permissions_contract_id ON user_contract_permissions(contract_id)",
        "CREATE INDEX IF NOT EXISTS idx_admin_audit_created_at ON admin_audit_log(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_admin_audit_actor ON admin_audit_log(actor_user_id)",
        "CREATE INDEX IF NOT EXISTS idx_admin_audit_target ON admin_audit_log(target_user_id)",
        "CREATE INDEX IF NOT EXISTS idx_processing_history_created_at ON processing_history(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_processing_history_processed_at ON processing_history(processed_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_mgmt_exec_data ON management_execucao(data_referencia DESC)",
        "CREATE INDEX IF NOT EXISTS idx_mgmt_exec_nucleo ON management_execucao(nucleo_oficial, nucleo)",
        "CREATE INDEX IF NOT EXISTS idx_mgmt_exec_equipe ON management_execucao(equipe)",
        "CREATE INDEX IF NOT EXISTS idx_mgmt_exec_categoria ON management_execucao(categoria, categoria_item)",
        "CREATE INDEX IF NOT EXISTS idx_mgmt_frentes_data ON management_frentes(data_referencia DESC)",
        "CREATE INDEX IF NOT EXISTS idx_mgmt_frentes_nucleo ON management_frentes(nucleo_oficial, nucleo)",
        "CREATE INDEX IF NOT EXISTS idx_mgmt_frentes_equipe ON management_frentes(equipe)",
        "CREATE INDEX IF NOT EXISTS idx_mgmt_ocorr_data ON management_ocorrencias(data_referencia DESC)",
        "CREATE INDEX IF NOT EXISTS idx_mgmt_ocorr_nucleo ON management_ocorrencias(nucleo_oficial, nucleo)",
        "CREATE INDEX IF NOT EXISTS idx_mgmt_ocorr_equipe ON management_ocorrencias(equipe)",
        "CREATE INDEX IF NOT EXISTS idx_service_registry_ativo ON service_registry(ativo)",
        "CREATE INDEX IF NOT EXISTS idx_service_registry_categoria ON service_registry(categoria)",
        "CREATE INDEX IF NOT EXISTS idx_service_aliases_service_id ON service_aliases(service_id)",
        "CREATE INDEX IF NOT EXISTS idx_service_aliases_ativo ON service_aliases(ativo)",
        """
        CREATE OR REPLACE VIEW vw_bi_execucao_fato AS
        SELECT
            id,
            data_referencia,
            COALESCE(NULLIF(contrato, ''), 'Sem contrato') AS contrato,
            COALESCE(NULLIF(nucleo_oficial, ''), NULLIF(nucleo, ''), 'Nao informado') AS nucleo,
            COALESCE(NULLIF(municipio_oficial, ''), NULLIF(municipio, ''), 'Nao informado') AS municipio,
            COALESCE(NULLIF(equipe, ''), 'Nao informado') AS equipe,
            CASE
                WHEN UPPER(COALESCE(NULLIF(servico_oficial, ''), '')) IN ('PRA', 'PRE')
                    AND diametro_mm IS NOT NULL
                    THEN UPPER(COALESCE(NULLIF(servico_oficial, ''), '')) || ' Ø' || diametro_mm::TEXT
                ELSE COALESCE(
                    NULLIF(servico_oficial, ''),
                    NULLIF(servico_normalizado, ''),
                    NULLIF(servico_bruto, ''),
                    NULLIF(item_original, ''),
                    'servico_nao_mapeado'
                )
            END AS servico,
            COALESCE(NULLIF(categoria, ''), NULLIF(categoria_item, ''), 'servico_nao_mapeado') AS categoria,
            quantidade,
            COALESCE(NULLIF(unidade, ''), 'un') AS unidade,
            diametro_mm,
            CASE
                WHEN COALESCE(NULLIF(servico_oficial, ''), '') IN ('', '-', 'servico_nao_mapeado', 'nao_mapeado')
                    THEN FALSE
                ELSE TRUE
            END AS mapeado
        FROM management_execucao
        """,
        """
        CREATE OR REPLACE VIEW vw_bi_kpi_diario AS
        SELECT
            data_referencia,
            COUNT(*)::BIGINT AS registros_execucao,
            COALESCE(SUM(quantidade), 0)::NUMERIC(18,3) AS volume_total,
            COUNT(DISTINCT contrato)::BIGINT AS contratos_distintos,
            COUNT(DISTINCT COALESCE(NULLIF(nucleo_oficial, ''), nucleo))::BIGINT AS nucleos_distintos,
            COUNT(DISTINCT COALESCE(NULLIF(equipe, ''), 'Nao informado'))::BIGINT AS equipes_distintas
        FROM management_execucao
        GROUP BY data_referencia
        """,
        """
        CREATE OR REPLACE VIEW vw_bi_kpi_contrato AS
        SELECT
            COALESCE(NULLIF(contrato, ''), 'Sem contrato') AS contrato,
            COUNT(*)::BIGINT AS registros_execucao,
            COALESCE(SUM(quantidade), 0)::NUMERIC(18,3) AS volume_total,
            COUNT(DISTINCT COALESCE(NULLIF(nucleo_oficial, ''), nucleo))::BIGINT AS nucleos_distintos,
            COUNT(DISTINCT COALESCE(NULLIF(equipe, ''), 'Nao informado'))::BIGINT AS equipes_distintas
        FROM management_execucao
        GROUP BY COALESCE(NULLIF(contrato, ''), 'Sem contrato')
        """,
        """
        CREATE OR REPLACE VIEW vw_bi_ranking_servico AS
        SELECT
            CASE
                WHEN UPPER(COALESCE(NULLIF(servico_oficial, ''), '')) IN ('PRA', 'PRE')
                    AND diametro_mm IS NOT NULL
                    THEN UPPER(COALESCE(NULLIF(servico_oficial, ''), '')) || ' Ø' || diametro_mm::TEXT
                ELSE COALESCE(
                    NULLIF(servico_oficial, ''),
                    NULLIF(servico_normalizado, ''),
                    NULLIF(servico_bruto, ''),
                    NULLIF(item_original, ''),
                    'servico_nao_mapeado'
                )
            END AS servico,
            COALESCE(NULLIF(unidade, ''), 'un') AS unidade,
            COUNT(*)::BIGINT AS registros_execucao,
            COALESCE(SUM(quantidade), 0)::NUMERIC(18,3) AS volume_total
        FROM management_execucao
        GROUP BY
            CASE
                WHEN UPPER(COALESCE(NULLIF(servico_oficial, ''), '')) IN ('PRA', 'PRE')
                    AND diametro_mm IS NOT NULL
                    THEN UPPER(COALESCE(NULLIF(servico_oficial, ''), '')) || ' Ø' || diametro_mm::TEXT
                ELSE COALESCE(
                    NULLIF(servico_oficial, ''),
                    NULLIF(servico_normalizado, ''),
                    NULLIF(servico_bruto, ''),
                    NULLIF(item_original, ''),
                    'servico_nao_mapeado'
                )
            END,
            COALESCE(NULLIF(unidade, ''), 'un')
        """,
        """
        CREATE OR REPLACE VIEW vw_bi_qualidade_mapeamento AS
        SELECT
            data_referencia,
            COUNT(*)::BIGINT AS registros_total,
            SUM(
                CASE
                    WHEN COALESCE(NULLIF(servico_oficial, ''), '') IN ('', '-', 'servico_nao_mapeado', 'nao_mapeado')
                        THEN 0
                    ELSE 1
                END
            )::BIGINT AS registros_mapeados,
            SUM(
                CASE
                    WHEN COALESCE(NULLIF(servico_oficial, ''), '') IN ('', '-', 'servico_nao_mapeado', 'nao_mapeado')
                        THEN 1
                    ELSE 0
                END
            )::BIGINT AS registros_nao_mapeados,
            ROUND(
                100.0 * SUM(
                    CASE
                        WHEN COALESCE(NULLIF(servico_oficial, ''), '') IN ('', '-', 'servico_nao_mapeado', 'nao_mapeado')
                            THEN 0
                        ELSE 1
                    END
                ) / NULLIF(COUNT(*), 0),
                2
            ) AS percentual_mapeado
        FROM management_execucao
        GROUP BY data_referencia
        """,
        """
        CREATE OR REPLACE VIEW vw_bi_ocorrencias_tipo AS
        SELECT
            data_referencia,
            COALESCE(NULLIF(tipo_ocorrencia, ''), 'nao_informado') AS tipo_ocorrencia,
            COUNT(*)::BIGINT AS ocorrencias
        FROM management_ocorrencias
        GROUP BY data_referencia, COALESCE(NULLIF(tipo_ocorrencia, ''), 'nao_informado')
        """,
        """
        CREATE OR REPLACE VIEW vw_conferencia_base_oficial AS
        SELECT
            r.conference_session_id,
            r.inspection_id,
            r.inspection_item_id,
            COALESCE(s.completed_at::DATE, i.data_vistoria) AS data_referencia,
            r.contract_id,
            COALESCE(NULLIF(c.numero_contrato, ''), NULLIF(c.contract_code, ''), 'Sem contrato') AS contrato,
            COALESCE(NULLIF(i.nucleo, ''), 'Nao informado') AS nucleo,
            COALESCE(NULLIF(i.municipio, ''), 'Nao informado') AS municipio,
            COALESCE(NULLIF(i.equipe, ''), 'Nao informado') AS equipe,
            COALESCE(NULLIF(i.fiscal_nome, ''), NULLIF(u.email, ''), 'Nao informado') AS fiscal_nome,
            COALESCE(NULLIF(r.area, ''), NULLIF(it.area, ''), 'Nao informada') AS area,
            COALESCE(NULLIF(r.item_titulo, ''), NULLIF(it.item_titulo, ''), 'Item sem titulo') AS item_titulo,
            COALESCE(NULLIF(r.status, ''), 'pendente') AS status_item,
            COALESCE(NULLIF(it.severidade, ''), 'baixa') AS severidade_item,
            COALESCE(r.quantidade_verificada, 0)::NUMERIC(18,3) AS quantidade_oficial,
            COALESCE(NULLIF(r.item_origem, ''), 'planejado') AS item_origem
        FROM official_conference_results r
        INNER JOIN inspections i ON i.id = r.inspection_id
        LEFT JOIN inspection_items it ON it.id = r.inspection_item_id
        LEFT JOIN inspection_conference_sessions s ON s.id = r.conference_session_id
        LEFT JOIN contracts c ON c.id = i.contract_id
        LEFT JOIN users u ON u.id = r.fiscal_user_id
        """,
        """
        CREATE OR REPLACE VIEW vw_conferencia_comparativo AS
        SELECT
            r.conference_session_id,
            r.inspection_id,
            r.inspection_item_id,
            COALESCE(s.completed_at::DATE, i.data_vistoria) AS data_referencia,
            r.contract_id,
            COALESCE(NULLIF(c.numero_contrato, ''), NULLIF(c.contract_code, ''), 'Sem contrato') AS contrato,
            COALESCE(NULLIF(i.nucleo, ''), 'Nao informado') AS nucleo,
            COALESCE(NULLIF(i.municipio, ''), 'Nao informado') AS municipio,
            COALESCE(NULLIF(i.equipe, ''), 'Nao informado') AS equipe,
            COALESCE(NULLIF(i.fiscal_nome, ''), NULLIF(u.email, ''), 'Nao informado') AS fiscal_nome,
            COALESCE(NULLIF(r.area, ''), NULLIF(it.area, ''), 'Nao informada') AS area,
            COALESCE(NULLIF(r.item_titulo, ''), NULLIF(it.item_titulo, ''), 'Item sem titulo') AS item_titulo,
            COALESCE(it.quantidade_declarada, 0)::NUMERIC(18,3) AS quantidade_declarada,
            COALESCE(r.quantidade_verificada, 0)::NUMERIC(18,3) AS quantidade_verificada,
            COALESCE(r.quantidade_verificada, 0)::NUMERIC(18,3) AS quantidade_oficial,
            COALESCE(r.verificado_informado, FALSE) AS verificado_informado,
            ABS(COALESCE(r.quantidade_verificada, 0) - COALESCE(it.quantidade_declarada, 0))::NUMERIC(18,3) AS divergencia_absoluta,
            CASE
                WHEN COALESCE(it.quantidade_declarada, 0) = 0 AND COALESCE(r.quantidade_verificada, 0) = 0 THEN 0::NUMERIC(9,2)
                WHEN COALESCE(it.quantidade_declarada, 0) = 0 THEN 100::NUMERIC(9,2)
                ELSE ROUND(
                    ABS(COALESCE(r.quantidade_verificada, 0) - COALESCE(it.quantidade_declarada, 0))
                    / NULLIF(ABS(COALESCE(it.quantidade_declarada, 0)), 0) * 100,
                    2
                )::NUMERIC(9,2)
            END AS divergencia_percentual,
            CASE
                WHEN COALESCE(r.quantidade_verificada, 0) = COALESCE(it.quantidade_declarada, 0) THEN 'sem_divergencia'
                WHEN COALESCE(it.quantidade_declarada, 0) = 0 AND COALESCE(r.quantidade_verificada, 0) > 0 THEN 'novo_nao_declarado'
                WHEN COALESCE(it.quantidade_declarada, 0) > 0 AND COALESCE(r.quantidade_verificada, 0) = 0 THEN 'nao_executado'
                WHEN COALESCE(r.quantidade_verificada, 0) > COALESCE(it.quantidade_declarada, 0) THEN 'a_maior'
                ELSE 'a_menor'
            END AS divergencia_status
        FROM official_conference_results r
        INNER JOIN inspections i ON i.id = r.inspection_id
        LEFT JOIN inspection_items it ON it.id = r.inspection_item_id
        LEFT JOIN inspection_conference_sessions s ON s.id = r.conference_session_id
        LEFT JOIN contracts c ON c.id = i.contract_id
        LEFT JOIN users u ON u.id = r.fiscal_user_id
        """,
        """
        CREATE OR REPLACE VIEW vw_conferencia_divergencias AS
        SELECT
            data_referencia,
            contrato,
            nucleo,
            municipio,
            equipe,
            fiscal_nome,
            COUNT(*)::BIGINT AS itens_total,
            SUM(CASE WHEN verificado_informado THEN 1 ELSE 0 END)::BIGINT AS itens_verificados,
            SUM(
                CASE
                    WHEN divergencia_status IN ('sem_divergencia', 'nao_verificado') THEN 0
                    ELSE 1
                END
            )::BIGINT AS itens_com_divergencia,
            COALESCE(SUM(quantidade_declarada), 0)::NUMERIC(18,3) AS volume_declarado,
            COALESCE(SUM(quantidade_verificada), 0)::NUMERIC(18,3) AS volume_verificado,
            COALESCE(SUM(quantidade_oficial), 0)::NUMERIC(18,3) AS volume_oficial,
            COALESCE(SUM(divergencia_absoluta), 0)::NUMERIC(18,3) AS divergencia_absoluta_total,
            ROUND(
                COALESCE(
                    AVG(
                        CASE
                            WHEN verificado_informado THEN divergencia_percentual
                            ELSE NULL
                        END
                    ),
                    0
                ),
                2
            )::NUMERIC(9,2) AS divergencia_percentual_media,
            ROUND(
                100.0 * SUM(
                    CASE
                        WHEN divergencia_status IN ('sem_divergencia', 'nao_verificado') THEN 0
                        ELSE 1
                    END
                ) / NULLIF(COUNT(*), 0),
                2
            )::NUMERIC(9,2) AS percentual_itens_divergentes
        FROM vw_conferencia_comparativo
        GROUP BY data_referencia, contrato, nucleo, municipio, equipe, fiscal_nome
        """,
    ]

    with db.connection() as conn:
        try:
            with conn.cursor() as cur:
                for stmt in statements:
                    cur.execute(stmt)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
