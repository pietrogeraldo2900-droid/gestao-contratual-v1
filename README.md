# Sistema de Gestao de Relatorios de Obra

Sistema para processar mensagens operacionais (WhatsApp/consolidados), gerar saídas de obra, atualizar a base mestra e manter painel gerencial em Excel com layout homologado.

## O que o sistema faz

- Processa mensagem no formato oficial e no formato legado.
- Normaliza texto/unidade/quantidade.
- Mapeia servicos via dicionario (com fallback controlado).
- Registra servicos nao mapeados para melhoria continua.
- Gera CSVs operacionais, relatorios por nucleo e `base_gerencial.xlsx`.
- Atualiza a base mestra acumulada sem quebrar o fluxo atual.
- Disponibiliza interface web local com historico operacional.

## Saidas geradas por processamento

Na pasta de saída do processamento:

- `execucao.csv`
- `frentes.csv`
- `ocorrencias.csv`
- `observacoes.csv`
- `servico_nao_mapeado.csv`
- `resumo_nucleos.csv`
- `indicadores_dashboard.csv`
- `indicadores_ocorrencias.csv`
- `painel_geral.csv`
- `base_gerencial.xlsx`
- `relatorio_consolidado.json`
- `relatorios_nucleos/` (arquivos `.md`, `.docx`, `.pdf`)

## Requisitos

- Windows 10 ou 11
- Python 3.10+ (recomendado 3.12+)
- Permissao para instalar pacotes Python

## Dependencias

Arquivo: `requirements.txt`

- `Flask>=3.0.0`
- `openpyxl>=3.1.0`
- `python-docx>=1.1.0`
- `reportlab>=4.0.0`

## Instalacao e execucao do zero (Windows)

### 1) Abrir PowerShell na pasta do projeto

```powershell
cd "C:\Users\Pietro\OneDrive\Documentos\Pietro\Sistema de Gestão"
```

### 2) (Opcional, recomendado) Criar ambiente virtual

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Se `python` nao for reconhecido, use o executavel completo instalado na maquina.

### 3) Instalar dependencias

Opcao A (atalho `.bat`):

```powershell
.\instalar_dependencias.bat
```

Opcao B (comando direto):

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### 4) Subir a interface web

Opcao A (atalho `.bat`):

```powershell
.\abrir_interface_web.bat
```

Opcao B (CLI):

```powershell
python main.py web --host 127.0.0.1 --port 5000
```

Abrir no navegador:

- `http://127.0.0.1:5000`

### 5) Fluxo web operacional

1. Abrir `Nova entrada`
2. Colar a mensagem
3. Clicar `Processar mensagem`
4. Revisar dados (incluindo ajuste por nucleo quando houver multi-nucleo)
5. Clicar `Confirmar e gerar arquivos`
6. Conferir resultado e caminhos dos arquivos
7. Consultar `Historico`

## Uso via CLI

### Processar um arquivo TXT

```powershell
python main.py parse "C:\caminho\entrada.txt" --output "C:\caminho\saida"
```

Com atualizacao de base mestra no mesmo comando:

```powershell
python main.py parse "C:\caminho\entrada.txt" --output "C:\caminho\saida" --master-dir "C:\caminho\BASE_MESTRA"
```

### Processar todos os TXT de uma pasta

```powershell
python main.py batch "C:\caminho\pasta_txt"
```

Com atualizacao da base mestra:

```powershell
python main.py batch "C:\caminho\pasta_txt" --master-dir "C:\caminho\BASE_MESTRA"
```

### Atualizar base mestra a partir de uma saída existente

```powershell
python main.py update-master "C:\caminho\saida" --master-dir "C:\caminho\BASE_MESTRA"
```

### Consolidar varias subpastas de saída em uma base unica

```powershell
python main.py consolidate-master "C:\caminho\saidas" --output "C:\caminho\BASE_CONSOLIDADA"
```

## Base mestra (como funciona)

Pasta padrao:

- `BASE_MESTRA/`

Arquivos principais:

- `base_mestra_execucao.csv`
- `base_mestra_frentes.csv`
- `base_mestra_ocorrencias.csv`
- `execucao.csv` (alias operacional)
- `frentes.csv` (alias operacional)
- `ocorrencias.csv` (alias operacional)
- `base_gerencial.xlsx` (gerado a partir do acumulado)

Regras operacionais:

- O sistema acumula historico e evita duplicidade por chave de consolidacao.
- Sempre que a base mestra e atualizada, o `base_gerencial.xlsx` da `BASE_MESTRA` e regenerado.

## Onde ficam os dados operacionais

- Saidas da web: `saidas/saida_web_YYYYMMDD_HHMMSS_ffffff/`
- Saidas de scripts/CLI: caminho informado no comando (ou script)
- Historico web: `data/processing_history.csv`
- Drafts da revisao web: `data/web_drafts/*.json`
- Config de nucleo (autopreenchimento): `config/nucleo_reference.json`
- Dicionario legado: `config/service_dictionary.csv`
- Dicionario oficial: `config/service_dictionary_v2.json`

## Scripts .bat (Windows)

### Operacao principal

- `instalar_dependencias.bat`: instala dependencias Python
- `abrir_interface_web.bat`: sobe web local (`main.py web`)
- `processar_arquivo_txt.bat`: processa 1 TXT e abre pasta de saída
- `processar_arquivo_e_atualizar_base_mestra.bat`: processa 1 TXT e atualiza `BASE_MESTRA`
- `processar_pasta_txt.bat`: processa todos os `.txt` de uma pasta

### Consolidacao

- `atualizar_base_mestra.bat`: atualiza base mestra a partir de uma pasta de saída ja gerada
- `consolidar_saidas_em_base_mestra.bat`: consolida varias saídas em `BASE_CONSOLIDADA`

### Apoio

- `processar_exemplo.bat`: roda exemplo em `samples/`
- `abrir_interface.bat`: interface desktop legada (`interface.py`)

## Historico web (consulta e rastreabilidade)

Na tela `Historico`, voce pode filtrar por:

- busca livre
- status
- data da obra
- nucleo
- equipe
- periodo de processamento (de/ate)

A tela tambem mostra:

- resumo de alertas por processamento (com detalhes)
- links `file://` para pasta de saída e base gerencial
- caminhos completos dos arquivos para rastreabilidade

## Template homologado e layout Excel

- Template homologado: `config/base_gerencial_template.xlsx`
- Geracao do painel: `base_builder.py`

Observacao importante:

- Nao alterar layout homologado fora do fluxo previsto.

## Testes

Executar suite completa:

```powershell
python -m unittest tests.test_input_layer tests.test_legacy_parser_mapping tests.test_master_builder tests.test_integration_workflow tests.test_web_app -v
```

## Problemas comuns (Windows)

### `FileNotFoundError` para caminho de entrada

Causa: caminho de exemplo ou arquivo inexistente.

Acao:

```powershell
Test-Path "C:\caminho\entrada.txt"
```

### `PermissionError` ao gerar `base_gerencial.xlsx`

Causa: arquivo aberto no Excel.

Acao: fechar o Excel e executar novamente.

### `python` nao reconhecido

Causa: Python fora do PATH.

Acao:

- usar caminho completo do executavel Python
- ou ajustar PATH do sistema

## Checklist rapido de uso real

1. Instalar dependencias
2. Subir web
3. Processar mensagem
4. Revisar/gerar
5. Validar saída em `saidas/...`
6. Consultar `Historico`
7. Garantir base mestra atualizada
