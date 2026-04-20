"""
**File:** ``__init__.py``
**Region:** ``ds_protocol_odata_py_lib``

Description
-----------
A Python package from the ds-protocol-odata-py-lib library.

Example
-------
.. code-block:: python

    from ds_protocol_odata_py_lib import __version__

    print(f"Package version: {__version__}")
"""

from importlib.metadata import version

PACKAGE_NAME = "ds-protocol-odata-py-lib"

__version__ = version(PACKAGE_NAME)
__all__ = ["__version__"]
