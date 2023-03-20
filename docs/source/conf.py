import pathlib
import sys

sys.path.insert(
    0, str(pathlib.Path(__file__).parent.parent.parent / "src" / "libraries" / "python")
)

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent / "src"))

print("\n\n\nPATH:")
print(sys.path)
print("\n\n\n")


# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Pharos API"
copyright = "2023, CGHSS Data Lab"
author = "CGHSS Data Lab"
release = "0.0.1"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinxcontrib.autodoc_pydantic",
]

templates_path = ["_templates"]
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# html_theme = "alabaster"
html_theme = "pydata_sphinx_theme"
# html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

autodoc_pydantic_model_show_json = True
autodoc_pydantic_model_show_config = False
