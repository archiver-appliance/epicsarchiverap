# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

from pathlib import Path

project = "Archiver Appliance"
copyright = "2026-%Y, Archiver Appliance contributors"
author = "Archiver Appliance contributors"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinxcontrib.openapi",
    "sphinxext.opengraph",
    "sphinxext.rediraffe",
]

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", ".venv", ".pixi"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"

# conf.py is at docs/source/conf.py; parents[0]=source, parents[1]=docs, parents[2]=repo root
_javadoc_dir = Path(__file__).resolve().parents[2] / "build" / "docs"
html_static_path = ["_static"]
if _javadoc_dir.exists():
    html_static_path.append(str(_javadoc_dir))


# -- Options for MyST's markdown -----------------------------------------------
# https://myst-parser.readthedocs.io/en/latest/configuration.html

myst_enable_extensions = [
    "amsmath",
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_image",
    "replacements",
    "smartquotes",
    "strikethrough",
    "tasklist",
]

# -- Options for Sphinx Rediraffe --------------------------------------------
# https://github.com/sphinx-doc/sphinxext-rediraffe

rediraffe_redirects = "redirects.txt"
