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

echo [INFO] Abrindo interface desktop legada (Tkinter)...
python "%ROOT_DIR%\scripts\interface_legacy.py"

if errorlevel 1 (
  echo [ERRO] A interface desktop encerrou com falha.
)

pause
