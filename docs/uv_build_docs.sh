# !/bin/bash
# if uv is not installed, install it
if ! command -v uv &> /dev/null
then
    echo "Instaling uv"
    curl -LsSf https://astral.sh/uv/install.sh | sh
else
    echo "Using uv from the environment"
fi

uv venv .venv
source .venv/bin/activate

# setup sphinx-build if not installed
if ! command -v sphinx-build &> /dev/null
then
    echo "Instaling sphinx-build in a new virtual environment"
    uv pip install --upgrade --no-cache-dir pip setuptools
    uv pip install --upgrade --no-cache-dir .
else 
    echo "Using sphinx-build from the environment"
fi
    
sphinx-build docs/source docs/build