# Sistema de Gestao de Relatorios de Obra

Sistema Python/Flask para processar mensagens operacionais, gerar saidas CSV/Excel, atualizar base mestra e operar via interface web local.

## Orientacao do projeto

Antes de alterar qualquer parte do sistema, consulte o [AGENTS.md](AGENTS.md). Ele descreve as regras de trabalho, prioridades, fluxo de validacao e cuidados de organizacao do projeto.

## Entrypoints oficiais

- Aplicacao web oficial: `python run_web.py`
- CLI legado/local: `python main.py ...`

`run_web.py` e o entrypoint oficial da interface web.
`main.py` permanece para processamento local/legado e compatibilidade operacional.

## Raiz consolidada (arquivos oficiais)

```text
.
|-- AGENTS.md
|-- DESIGN_SYSTEM_PREMIUM.md
|-- run_web.py
|-- main.py
|-- README.md
|-- requirements.txt
|-- .env.example
|-- .gitignore
|-- Dockerfile
|-- .dockerignore
|-- app/
|-- config/
|-- data/
|-- docs/
|   `-- changelog/
|-- static/
|   |-- css/
|   |   |-- app.css
|   |   |-- app-shell.css
|   |   |-- auth.css
|   |   |-- design-system/
|   |   `-- pages/
|   `-- ...
|-- templates/
|   |-- base.html
|   |-- design_system/
|   |-- auth/
|   |-- account/
|   |-- admin/
|   |-- dashboard/
|   |-- entries/
|   |-- contracts/
|   |-- contractor/
|   |-- conference/
|   |-- inspection/
|   |-- results/
|   |-- catalog/
|   `-- institutional/
|-- tests/
|   |-- unit/
|   |-- integration/
|   `-- web/
```

## Estrutura oficial por area

- `app/core`: parsing, normalizacao e reconciliacao
- `app/routes`: rotas Flask
- `app/services`: pipeline, relatorios e consolidadores
- `app/utils`: utilitarios compartilhados
- `config`: settings, dicionarios e template homologado
- `DESIGN_SYSTEM_PREMIUM.md`: guia da base visual compartilhada
- `data`: arquivos de apoio e, quando aplicavel, entradas ou artefatos temporarios do fluxo local
- `docs/changelog`: historico de versoes
- `static/css/design-system`: tokens e foundations compartilhados
- `static/css/pages`: estilos especificos por tela
- `static/css/app-shell.css`: shell autenticado do sistema
- `static/css/auth.css`: estilos auxiliares de autenticacao e flash messages
- `templates/base.html`: shell comum do Flask
- `templates/design_system`: componentes compartilhados de interface
- `templates/<dominio>`: telas organizadas por area funcional
- `tests`: validacoes automatizadas do projeto

## Consolidacao final da raiz

- `interface.py` (wrapper) removido da raiz.
- `processar_pasta_txt.py` (wrapper) removido da raiz.

## Configuracao centralizada

Toda configuracao de paths e ambiente fica em `config/settings.py`.

Variaveis suportadas:

- `WEB_APP_SECRET`
- `WEB_HOST`, `WEB_PORT`, `WEB_DEBUG`
- `OUTPUTS_ROOT`
- `MASTER_DIR`
- `HISTORY_FILE`
- `DRAFT_DIR`
- `NUCLEO_REFERENCE_FILE`
- `SERVICE_DICTIONARY_CSV`
- `SERVICE_DICTIONARY_V2_JSON`
- `BASE_GERENCIAL_TEMPLATE`

Arquivo exemplo:

```powershell
Copy-Item .env.example .env
```

Notas de ambiente:
- O `run_web.py` carrega o arquivo `.env` automaticamente (quando existir), sem sobrescrever variaveis ja definidas no sistema.
- Para rodar local fora de container (`python run_web.py`), mantenha `DB_HOST=localhost` no `.env`.
- Para rodar o servico `web` dentro do `docker-compose`, ajuste `DB_HOST=db`.

## Instalacao local (Windows)

```powershell
cd "C:\Users\Pietro\OneDrive\Documentos\Pietro\Sistema de Gestao"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Rodar aplicacao web (oficial)

Se for rodar local com autenticacao, suba antes o Postgres:

```powershell
docker compose up -d db
```

```powershell
python run_web.py
```

Com host e porta customizados:

```powershell
python run_web.py --host 127.0.0.1 --port 5000
```

Acesso local: [http://127.0.0.1:5000](http://127.0.0.1:5000)

## Docker (preparacao para hospedagem)

Build da imagem:

```powershell
docker build -t sisg-web:latest .
```

Execucao:

```powershell
docker run --rm -p 5000:5000 sisg-web:latest
```

Com persistencia de runtime no host:

```powershell
docker run --rm -p 5000:5000 `
  -v "${PWD}\saidas:/app/saidas" `
  -v "${PWD}\BASE_MESTRA:/app/BASE_MESTRA" `
  sisg-web:latest
```

## Runtime e versionamento

Pastas tratadas como runtime/local (nao codigo-fonte):

- `BASE_MESTRA/`
- `saidas/`

`.gitignore` impede versionamento de caches e dados gerados, preservando `.gitkeep` quando aplicavel.

## Conferencia Operacional (base oficial)

Regra central implementada:

- A declaracao da contratada **nao** compoe a base oficial.
- A base oficial e composta **apenas** pela quantidade verificada pelo fiscal.

Modelagem aplicada em `inspection_items`:

- `quantidade_declarada`
- `quantidade_verificada`
- `quantidade_oficial` (sincronizada automaticamente com `quantidade_verificada`)
- `divergencia_absoluta`, `divergencia_percentual`, `divergencia_status`

Protecao estrutural:

- `CHECK (quantidade_oficial = quantidade_verificada)`
- Trigger de sincronizacao `trg_inspection_items_sync_oficial`

## Camada BI (MVP)

O sistema agora cria automaticamente views de BI (somente leitura) no startup do schema:

- `vw_bi_execucao_fato`
- `vw_bi_kpi_diario`
- `vw_bi_kpi_contrato`
- `vw_bi_ranking_servico`
- `vw_bi_qualidade_mapeamento`
- `vw_bi_ocorrencias_tipo`
- `vw_conferencia_base_oficial`
- `vw_conferencia_comparativo`
- `vw_conferencia_divergencias`

Essas views usam as tabelas `management_*` e `inspections/inspection_items` e servem como base para Metabase/Power BI sem alterar o pipeline.

### Consultas rapidas (SQL)

Top 10 servicos por volume:

```sql
SELECT servico, unidade, volume_total
FROM vw_bi_ranking_servico
ORDER BY volume_total DESC
LIMIT 10;
```

KPI diario:

```sql
SELECT data_referencia, registros_execucao, volume_total, nucleos_distintos
FROM vw_bi_kpi_diario
ORDER BY data_referencia DESC;
```

Qualidade de mapeamento:

```sql
SELECT data_referencia, registros_total, registros_nao_mapeados, percentual_mapeado
FROM vw_bi_qualidade_mapeamento
ORDER BY data_referencia DESC;
```

### Metabase (passo a passo enxuto)

1. Conectar no mesmo Postgres do sistema (Railway).
2. Em "Models/Questions", usar as views `vw_bi_*`.
3. Montar dashboards iniciais:
   - Executivo: `vw_bi_kpi_diario` + `vw_bi_kpi_contrato`
   - Operacional: `vw_bi_execucao_fato` + `vw_bi_ranking_servico`
   - Qualidade: `vw_bi_qualidade_mapeamento` + `vw_bi_ocorrencias_tipo`

## Testes

```powershell
python -m pytest -q
```

## Compatibilidade

- Nao houve mudanca de regra de negocio nesta consolidacao.
- `run_web.py` permanece o entrypoint oficial web.
- `main.py` permanece o entrypoint legado/local.
