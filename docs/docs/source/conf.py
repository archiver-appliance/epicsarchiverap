# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'archiverdocs'
copyright = '2024, Sky Brewer, Murali Shankar'
authors = ["Murali Shankar", "Sky Brewer"]

release = '0.1'
version = '0.1.0'
# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static', '../../../build/docs']

# Enable special syntax for admonitions (:::{directive})
myst_admonition_enable = True

# Enable definition lists (Term\n: Definition)
myst_deflist_enable = True

# Allow colon fencing of directives
myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "attrs_inline"
]

# -- Options for EPUB output
epub_show_urls = 'footnote'

