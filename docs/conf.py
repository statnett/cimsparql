# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

import os

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import cimsparql

# -- Project information -----------------------------------------------------

project = "cimsparql"
copyright = "2020, Statnett"
author = "Statnett DataScience <Datascience.Drift@Statnett.no>"

# The full version, including alpha/beta/rc tags
release = cimsparql.__version__

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.inheritance_diagram",
    "sphinx.ext.intersphinx",
    "sphinx.ext.graphviz",
    "sphinx.ext.githubpages",
    "recommonmark",
    "sphinx.ext.imgmath",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "numpy": ("https://docs.scipy.org/doc/numpy", None),
}

autosectionlabel_prefix_document = True
napoleon_include_private_with_doc = True

inheritance_node_attrs = dict(
    shape="folder",
    fontsize=14,
    height=0.75,
    fillcolor="SteelBlue1",
    style="filled",
    arrowType="open",
    arrowSize=1.2,
)

graphviz_output_format = "svg"

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

autodoc_default_options = {"special-members": "__call__"}

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
# html_theme = "alabaster"
html_theme = "sphinx_rtd_theme"

autoclass_content = "both"
# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
