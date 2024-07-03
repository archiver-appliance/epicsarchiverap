#! /bin/bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade --no-cache-dir pip setuptools
python -m pip install --upgrade --no-cache-dir sphinx readthedocs-sphinx-ext
python -m pip install --exists-action=w --no-cache-dir -r docs/requirements.txt
sphinx-build docs/source docs/build