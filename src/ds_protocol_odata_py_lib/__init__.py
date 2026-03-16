"""
**File:** ``__init__.py``
**Region:** ``{{GITHUB_REPO}}``

Description
-----------
A Python package from the {{PROJECT_NAME}} library.

Example
-------
.. code-block:: python

    from {{PYTHON_MODULE_NAME}} import __version__

    print(f"Package version: {__version__}")
"""

from importlib.metadata import version

PACKAGE_NAME = "ds-protocol-odata-py-lib"

__version__ = version(PACKAGE_NAME)
__all__ = ["__version__"]
