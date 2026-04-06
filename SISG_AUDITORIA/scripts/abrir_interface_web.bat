@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%SCRIPT_DIR%.."
cd /d "%ROOT_DIR%"

python --version >nul 2>&1
if errorlevel 1 (
  echo [ERRO] Python nao encontrado no PATH.
  pause
  exit /b 1
)

echo [INFO] Iniciando interface web (entrypoint oficial: run_web.py)
python "%ROOT_DIR%\run_web.py"

if errorlevel 1 (
  echo [ERRO] A interface web encerrou com falha.
  pause
  exit /b 1
)
