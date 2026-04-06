@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%SCRIPT_DIR%.."
cd /d "%ROOT_DIR%"

if "%~1"=="" (
  echo [ERRO] Informe um arquivo TXT.
  echo Uso: processar_arquivo_e_atualizar_base_mestra.bat "C:\caminho\entrada.txt"
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
set "MASTER_DIR=%ROOT_DIR%\BASE_MESTRA"

echo [INFO] Processando e atualizando base mestra...
echo [INFO] Entrada: "%INPUT%"
echo [INFO] Saida: "%OUTDIR%"
echo [INFO] Base mestra: "%MASTER_DIR%"
python "%ROOT_DIR%\main.py" parse "%INPUT%" --output "%OUTDIR%" --master-dir "%MASTER_DIR%"
if errorlevel 1 (
  echo [ERRO] Falha no processamento/atualizacao da base mestra.
  pause
  exit /b 1
)

if exist "%OUTDIR%" explorer "%OUTDIR%"
echo [OK] Processamento e atualizacao concluida.
pause
