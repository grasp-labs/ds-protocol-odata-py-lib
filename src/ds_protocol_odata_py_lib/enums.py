"""
**File:** ``enums.py``
**Region:** ``ds_protocol_odata_py_lib/enums``

Constants for OData-related enums, including HTTP methods and OData resource types.

Example:
    >>> HttpMethod.GET
    'GET'
    >>> ResourceType.DATASET_ODATA
    'DS.RESOURCE.DATASET.ODATA'
"""

from enum import StrEnum


class HttpMethod(StrEnum):
    """
    Constants for HTTP methods.
    """

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"


class ResourceType(StrEnum):
    """
    Constants for HTTP protocol.
    """

    DATASET_ODATA = "DS.RESOURCE.DATASET.ODATA"
