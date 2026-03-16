"""
**File:** ``enums.py``
**Region:** ``ds_protocol_http_py_lib/enums``

Constants for HTTP protocol.

Example:
    >>> ResourceType.LINKED_SERVICE
    'DS.RESOURCE.LINKED_SERVICE.HTTP'
    >>> ResourceType.DATASET
    'DS.RESOURCE.DATASET.HTTP'
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
