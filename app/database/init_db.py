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
        "CREATE INDEX IF NOT EXISTS idx_contracts_status ON contracts(status)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_contracts_numero_contrato ON contracts(numero_contrato)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_status_contrato ON contracts(status_contrato)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_created_at ON contracts(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_reports_contract_id ON reports(contract_id)",
        "CREATE INDEX IF NOT EXISTS idx_reports_created_at ON reports(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at DESC)",
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
