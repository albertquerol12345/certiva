@echo off
setlocal
cd /d %~dp0

if not exist .venv\Scripts\python.exe (
    echo [*] Creando entorno virtual (.venv)...
    python -m venv .venv
)

echo [*] Activando entorno virtual...
call .venv\Scripts\activate

echo [*] Instalando dependencias...
python -m pip install --upgrade pip >nul
pip install -r requirements.txt >nul

echo [*] Lanzando CERTIVA en http://localhost:8000 ...
uvicorn src.webapp:app --host 0.0.0.0 --port 8000
endlocal
