@echo off
setlocal
cd /d "%~dp0"

python --version >nul 2>&1
if errorlevel 1 (
  echo [ERRO] Python nao encontrado no PATH.
  pause
  exit /b 1
)

echo [INFO] Abrindo interface desktop legada (Tkinter)...
python interface.py

if errorlevel 1 (
  echo [ERRO] A interface desktop encerrou com falha.
)

pause
