@echo off
setlocal
cd /d "%~dp0"

if "%~1"=="" (
  echo [ERRO] Informe a pasta que contem varias subpastas de saida.
  echo Uso: consolidar_saidas_em_base_mestra.bat "C:\caminho\saidas"
  pause
  exit /b 1
)

python --version >nul 2>&1
if errorlevel 1 (
  echo [ERRO] Python nao encontrado no PATH.
  pause
  exit /b 1
)

set "OUTPUT_PARENT=%~1"
set "CONSOLIDADA_DIR=%~dp0BASE_CONSOLIDADA"

echo [INFO] Consolidando saidas em base unica...
echo [INFO] Origem: "%OUTPUT_PARENT%"
echo [INFO] Destino: "%CONSOLIDADA_DIR%"
python "%~dp0main.py" consolidate-master "%OUTPUT_PARENT%" --output "%CONSOLIDADA_DIR%"
if errorlevel 1 (
  echo [ERRO] Falha na consolidacao.
  pause
  exit /b 1
)

echo [OK] Consolidacao concluida.
pause
