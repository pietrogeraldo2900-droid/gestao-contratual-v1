@echo off
setlocal
cd /d "%~dp0"

python --version >nul 2>&1
if errorlevel 1 (
  echo [ERRO] Python nao encontrado no PATH.
  pause
  exit /b 1
)

echo [INFO] Iniciando interface web em http://127.0.0.1:5000
python main.py web --host 127.0.0.1 --port 5000

if errorlevel 1 (
  echo [ERRO] A interface web encerrou com falha.
  pause
  exit /b 1
)
