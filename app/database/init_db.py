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
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
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
        "CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)",
        "CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)",
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
            COALESCE(
                NULLIF(servico_oficial, ''),
                NULLIF(servico_normalizado, ''),
                NULLIF(servico_bruto, ''),
                NULLIF(item_original, ''),
                'servico_nao_mapeado'
            ) AS servico,
            COALESCE(NULLIF(categoria, ''), NULLIF(categoria_item, ''), 'servico_nao_mapeado') AS categoria,
            quantidade,
            COALESCE(NULLIF(unidade, ''), 'un') AS unidade,
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
            COALESCE(
                NULLIF(servico_oficial, ''),
                NULLIF(servico_normalizado, ''),
                NULLIF(servico_bruto, ''),
                NULLIF(item_original, ''),
                'servico_nao_mapeado'
            ) AS servico,
            COALESCE(NULLIF(unidade, ''), 'un') AS unidade,
            COUNT(*)::BIGINT AS registros_execucao,
            COALESCE(SUM(quantidade), 0)::NUMERIC(18,3) AS volume_total
        FROM management_execucao
        GROUP BY
            COALESCE(
                NULLIF(servico_oficial, ''),
                NULLIF(servico_normalizado, ''),
                NULLIF(servico_bruto, ''),
                NULLIF(item_original, ''),
                'servico_nao_mapeado'
            ),
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
