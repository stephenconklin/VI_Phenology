# Configuration file for the Sphinx documentation builder.
# https://www.sphinx-doc.org/en/master/usage/configuration.html

project = "VI Phenology"
copyright = "2026, Stephen Conklin"
author = "Stephen Conklin"

extensions = [
    "myst_parser",
]

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "sphinx_rtd_theme"

# MyST-Parser extensions
# https://myst-parser.readthedocs.io/en/stable/syntax/optional.html
myst_enable_extensions = [
    "colon_fence",  # ::: fenced directives (alternative to ``` fences)
    "deflist",      # definition lists
    "tasklist",     # - [ ] / - [x] task list items
]
