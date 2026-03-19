# Sistema de Gestao de Relatorios de Obra

Sistema Python/Flask para processar mensagens operacionais, gerar saidas CSV/Excel, atualizar base mestra e operar via interface web local.

## Entrypoints oficiais

- Aplicacao web (oficial): `python run_web.py`
- CLI legado/local: `python main.py ...`

`main.py` permanece ativo para processamento local e compatibilidade operacional. O ponto principal da aplicacao web e `run_web.py`.

## Estrutura oficial

```text
.
├── app/
│   ├── core/       # parsing, normalizacao e reconciliacao
│   ├── routes/     # rotas Flask
│   ├── services/   # pipeline, relatorios, consolidacao e workbook
│   └── utils/      # utilitarios compartilhados
├── config/
│   ├── settings.py # configuracao centralizada de ambiente e paths
│   └── ...         # dicionarios, template homologado e referencias
├── data/
│   ├── seed/       # dados base versionaveis
│   ├── runtime/    # dados gerados em execucao
│   └── drafts/     # rascunhos temporarios da interface web
├── docs/
├── scripts/
├── templates/
├── tests/
├── main.py         # fluxo legado/local
├── run_web.py      # entrypoint oficial web
└── README.md
```

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

Arquivo exemplo: `.env.example`

```powershell
Copy-Item .env.example .env
```

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

```powershell
python run_web.py
```

Customizando host e porta:

```powershell
python run_web.py --host 127.0.0.1 --port 5000
```

Acesso local: [http://127.0.0.1:5000](http://127.0.0.1:5000)

## Rodar fluxo legado/local (CLI)

Processar um TXT:

```powershell
python main.py parse "C:\caminho\entrada.txt" --output "C:\caminho\saida"
```

Processar e atualizar base mestra:

```powershell
python main.py parse "C:\caminho\entrada.txt" --output "C:\caminho\saida" --master-dir "C:\caminho\BASE_MESTRA"
```

Processar pasta de TXTs:

```powershell
python main.py batch "C:\caminho\pasta_txt"
```

Atualizar base mestra com uma saida existente:

```powershell
python main.py update-master "C:\caminho\saida" --master-dir "C:\caminho\BASE_MESTRA"
```

Consolidar varias saidas:

```powershell
python main.py consolidate-master "C:\caminho\saidas" --output "C:\caminho\BASE_CONSOLIDADA"
```

## Scripts operacionais (`scripts/`)

- `abrir_interface_web.bat`
- `abrir_interface.bat`
- `instalar_dependencias.bat`
- `processar_arquivo_txt.bat`
- `processar_arquivo_e_atualizar_base_mestra.bat`
- `processar_pasta_txt.bat`
- `atualizar_base_mestra.bat`
- `consolidar_saidas_em_base_mestra.bat`
- `processar_exemplo.bat`

## Docker (preparacao para hospedagem)

Build da imagem:

```powershell
docker build -t sisg-web:latest .
```

Execucao do container:

```powershell
docker run --rm -p 5000:5000 sisg-web:latest
```

Com bind para persistir saidas e base mestra no host:

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

`.gitignore` ja esta configurado para evitar versionamento dessas saidas, preservando `.gitkeep` quando necessario.

## Testes

```powershell
python -m unittest tests.test_input_layer tests.test_legacy_parser_mapping tests.test_master_builder tests.test_integration_workflow tests.test_web_app -v
```

## Compatibilidade

- Nao houve mudanca de regra de negocio nesta etapa.
- `run_web.py` e o entrypoint oficial da web.
- `main.py` continua disponivel para fluxo legado/local.
