@echo off
setlocal
cd /d "%~dp0"

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
set "OUTDIR=%~dp0saidas\saida_%STEM%"

echo [INFO] Processando: "%INPUT%"
echo [INFO] Saida: "%OUTDIR%"
python main.py parse "%INPUT%" --output "%OUTDIR%"
if errorlevel 1 (
  echo [ERRO] Falha no processamento do arquivo TXT.
  pause
  exit /b 1
)

if exist "%OUTDIR%" explorer "%OUTDIR%"
echo [OK] Processamento concluido.
pause
