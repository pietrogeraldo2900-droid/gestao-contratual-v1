@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%SCRIPT_DIR%.."
cd /d "%ROOT_DIR%"

echo [INFO] Instalando dependencias Python...
python --version >nul 2>&1
if errorlevel 1 (
  echo [ERRO] Python nao encontrado no PATH.
  echo [DICA] Instale o Python 3.10+ e tente novamente.
  pause
  exit /b 1
)

python -m pip install --upgrade pip
if errorlevel 1 (
  echo [ERRO] Falha ao atualizar pip.
  pause
  exit /b 1
)

python -m pip install -r "%ROOT_DIR%\requirements.txt"
if errorlevel 1 (
  echo [ERRO] Falha ao instalar requirements.txt.
  pause
  exit /b 1
)

echo [OK] Dependencias instaladas com sucesso.
pause
