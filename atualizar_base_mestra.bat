@echo off
setlocal
cd /d "%~dp0"

if "%~1"=="" (
  echo [ERRO] Informe uma pasta de SAIDA para atualizar a base mestra.
  echo Uso: atualizar_base_mestra.bat "C:\caminho\saida_web_..."
  pause
  exit /b 1
)

python --version >nul 2>&1
if errorlevel 1 (
  echo [ERRO] Python nao encontrado no PATH.
  pause
  exit /b 1
)

set "OUTPUT_DIR=%~1"
set "MASTER_DIR=%~dp0BASE_MESTRA"

echo [INFO] Atualizando base mestra...
echo [INFO] Pasta de saida: "%OUTPUT_DIR%"
echo [INFO] Base mestra: "%MASTER_DIR%"
python "%~dp0main.py" update-master "%OUTPUT_DIR%" --master-dir "%MASTER_DIR%"
if errorlevel 1 (
  echo [ERRO] Falha ao atualizar a base mestra.
  pause
  exit /b 1
)

echo [OK] Base mestra atualizada.
pause
