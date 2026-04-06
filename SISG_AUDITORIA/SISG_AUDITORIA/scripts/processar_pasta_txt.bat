@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%SCRIPT_DIR%.."
cd /d "%ROOT_DIR%"

if "%~1"=="" (
  echo [ERRO] Informe uma pasta com arquivos TXT.
  echo Uso: processar_pasta_txt.bat "C:\caminho\pasta_txt"
  pause
  exit /b 1
)

python --version >nul 2>&1
if errorlevel 1 (
  echo [ERRO] Python nao encontrado no PATH.
  pause
  exit /b 1
)

echo [INFO] Processando todos os .txt da pasta "%~1"...
python "%ROOT_DIR%\main.py" batch "%~1"
if errorlevel 1 (
  echo [ERRO] Falha no processamento em lote.
  pause
  exit /b 1
)

echo [OK] Processamento em lote concluido.
pause
