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

set "SAMPLE=%ROOT_DIR%\samples\consolidado_2026_03_09.txt"
set "OUTDIR=%ROOT_DIR%\saida_exemplo"

echo [INFO] Processando exemplo: "%SAMPLE%"
python "%ROOT_DIR%\main.py" parse "%SAMPLE%" --output "%OUTDIR%"
if errorlevel 1 (
  echo [ERRO] Falha ao processar o exemplo.
  pause
  exit /b 1
)

if exist "%OUTDIR%" explorer "%OUTDIR%"
echo [OK] Exemplo processado.
pause
