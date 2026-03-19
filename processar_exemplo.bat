@echo off
setlocal
cd /d "%~dp0"

python --version >nul 2>&1
if errorlevel 1 (
  echo [ERRO] Python nao encontrado no PATH.
  pause
  exit /b 1
)

set "SAMPLE=samples\consolidado_2026_03_09.txt"
set "OUTDIR=saida_exemplo"

echo [INFO] Processando exemplo: "%SAMPLE%"
python main.py parse "%SAMPLE%" --output "%OUTDIR%"
if errorlevel 1 (
  echo [ERRO] Falha ao processar o exemplo.
  pause
  exit /b 1
)

if exist "%OUTDIR%" explorer "%OUTDIR%"
echo [OK] Exemplo processado.
pause
