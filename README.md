# Sistema de Gestao de Relatorios de Obra

Sistema Python/Flask para processar mensagens operacionais, gerar saidas CSV/Excel, atualizar base mestra e operar via interface web local.

## Entrypoints oficiais

- Aplicacao web oficial: `python run_web.py`
- CLI legado/local: `python main.py ...`

`run_web.py` e o entrypoint oficial da interface web.
`main.py` permanece para processamento local/legado e compatibilidade operacional.

## Raiz consolidada (arquivos oficiais)

```text
.
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
|-- samples/
|-- scripts/
|-- templates/
`-- tests/
```

## Estrutura oficial por area

- `app/core`: parsing, normalizacao e reconciliacao
- `app/routes`: rotas Flask
- `app/services`: pipeline, relatorios e consolidadores
- `app/utils`: utilitarios compartilhados
- `config`: settings, dicionarios e template homologado
- `data/seed`: dados base versionaveis
- `data/runtime`: dados gerados em execucao
- `data/drafts`: rascunhos temporarios da web
- `scripts`: utilitarios operacionais e atalhos `.bat`
- `samples`: entradas de exemplo

## Consolidacao final da raiz

- `interface.py` (wrapper) removido da raiz.
- `processar_pasta_txt.py` (wrapper) removido da raiz.
- `entrada_interface.txt` movido para `samples/entrada_interface.txt`.
- `entrada_interface.txt` na raiz agora e ignorado no `.gitignore` (arquivo temporario de runtime).

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

Atalho:

```powershell
.\scripts\instalar_dependencias.bat
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

## Rodar fluxo legado/local (CLI)

Processar um TXT:

```powershell
python main.py parse "C:\caminho\entrada.txt" --output "C:\caminho\saida"
```

Processar pasta de TXTs:

```powershell
python main.py batch "C:\caminho\pasta_txt"
```

Atualizar base mestra:

```powershell
python main.py update-master "C:\caminho\saida" --master-dir "C:\caminho\BASE_MESTRA"
```

Consolidar varias saidas:

```powershell
python main.py consolidate-master "C:\caminho\saidas" --output "C:\caminho\BASE_CONSOLIDADA"
```

## Scripts operacionais (`scripts/`)

- `abrir_interface_web.bat` (web oficial)
- `abrir_interface.bat` (desktop legado Tkinter)
- `processar_arquivo_txt.bat`
- `processar_arquivo_e_atualizar_base_mestra.bat`
- `processar_pasta_txt.bat`
- `atualizar_base_mestra.bat`
- `consolidar_saidas_em_base_mestra.bat`
- `processar_exemplo.bat`
- `interface_legacy.py`
- `processar_pasta_txt.py`

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
  -v "${PWD}\data\runtime:/app/data/runtime" `
  -v "${PWD}\data\drafts\web:/app/data/drafts/web" `
  sisg-web:latest
```

## Runtime e versionamento

Pastas tratadas como runtime/local (nao codigo-fonte):

- `BASE_MESTRA/`
- `saidas/`
- `data/runtime/`
- `data/drafts/web/`

`.gitignore` impede versionamento de caches e dados gerados, preservando `.gitkeep` quando aplicavel.

## Camada BI (MVP)

O sistema agora cria automaticamente views de BI (somente leitura) no startup do schema:

- `vw_bi_execucao_fato`
- `vw_bi_kpi_diario`
- `vw_bi_kpi_contrato`
- `vw_bi_ranking_servico`
- `vw_bi_qualidade_mapeamento`
- `vw_bi_ocorrencias_tipo`

Essas views usam as tabelas `management_execucao` e `management_ocorrencias` e servem como base para Metabase/Power BI sem alterar o pipeline.

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
python -m unittest tests.test_input_layer tests.test_legacy_parser_mapping tests.test_master_builder tests.test_integration_workflow tests.test_web_app -v
```

## Compatibilidade

- Nao houve mudanca de regra de negocio nesta consolidacao.
- `run_web.py` permanece o entrypoint oficial web.
- `main.py` permanece o entrypoint legado/local.
