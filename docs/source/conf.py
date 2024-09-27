# Configuration file for the Sphinx documentation builder.

import os
import sys

src_root = os.path.abspath('../..')
sys.path.insert(0, src_root)

# noinspection PyUnresolvedReferences
import sphinx_rtd_theme

from esa_climate_toolbox.version import __version__

# -- Project information

project = 'ESA CCI Toolbox'
copyright = '2024, Brockmann Consult GmbH'
author = 'Brockmann Consult GmbH'

release = __version__

# -- General configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'nbsphinx'
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

# -- Options for HTML output

html_theme = 'sphinx_rtd_theme'

# -- Options for EPUB output
epub_show_urls = 'footnote'