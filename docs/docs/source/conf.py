# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

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
html_static_path = ['_static', '../../../build/docs', "js"]

templates_path = ['templates']

html_js_files = [
    'keybindings.js',
]

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
