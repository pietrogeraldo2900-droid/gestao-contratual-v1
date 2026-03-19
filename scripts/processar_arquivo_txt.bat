@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%SCRIPT_DIR%.."
cd /d "%ROOT_DIR%"

if "%~1"=="" (
  echo [ERRO] Informe um arquivo TXT.
  echo Uso: processar_arquivo_txt.bat "C:\caminho\entrada.txt"
  pause
  exit /b 1
)

python --version >nul 2>&1
if errorlevel 1 (
  echo [ERRO] Python nao encontrado no PATH.
  pause
  exit /b 1
)

set "INPUT=%~1"
set "STEM=%~n1"
set "OUTDIR=%ROOT_DIR%\saidas\saida_%STEM%"

echo [INFO] Processando: "%INPUT%"
echo [INFO] Saida: "%OUTDIR%"
python "%ROOT_DIR%\main.py" parse "%INPUT%" --output "%OUTDIR%"
if errorlevel 1 (
  echo [ERRO] Falha no processamento do arquivo TXT.
  pause
  exit /b 1
)

if exist "%OUTDIR%" explorer "%OUTDIR%"
echo [OK] Processamento concluido.
pause
