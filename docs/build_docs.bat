@echo off
where sphinx-build >nul 2>nul
IF NOT "%ERRORLEVEL%"=="0" (
    echo "Instaling sphinx-build in a new virtual environment"
    python -m venv .venv
    .venv\Scripts\activate.bat
    python -m pip install --upgrade --no-cache-dir pip setuptools
    python -m pip install --upgrade --no-cache-dir sphinx readthedocs-sphinx-ext
    python -m pip install --exists-action=w --no-cache-dir -r docs\requirements.txt
) ELSE (
    echo "Using sphinx-build from the environment"
)
sphinx-build docs\source docs\build