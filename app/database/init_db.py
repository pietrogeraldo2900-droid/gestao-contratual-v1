from __future__ import annotations

from app.database.connection import DatabaseManager


def init_db(db: DatabaseManager) -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS users (
            id BIGSERIAL PRIMARY KEY,
            email VARCHAR(255) NOT NULL UNIQUE,
            password VARCHAR(255) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
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
        "CREATE INDEX IF NOT EXISTS idx_contracts_status ON contracts(status)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_contracts_numero_contrato ON contracts(numero_contrato)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_status_contrato ON contracts(status_contrato)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_created_at ON contracts(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_reports_contract_id ON reports(contract_id)",
        "CREATE INDEX IF NOT EXISTS idx_reports_created_at ON reports(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at DESC)",
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
