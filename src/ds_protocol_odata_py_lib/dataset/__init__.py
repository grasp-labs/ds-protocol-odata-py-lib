"""
**File:** ``__init__.py``
**Region:** ``ds_protocol_odata_py_lib/dataset``

Dataset module for OData protocol.

This module provides the OdataDataset and OdataDatasetSettings classes for
OData-compliant data sources. To use the dataset, import from the odata submodule.

Example:
    >>> from ds_protocol_odata_py_lib.dataset.odata import OdataDataset, OdataDatasetSettings
    >>> dataset = OdataDataset(
    ...     linked_service=linked_service,
    ...     settings=OdataDatasetSettings(
    ...         url="https://example.com/api/people",
    ...     ),
    ...     id=uuid.uuid4(),
    ...     name="people",
    ...     version="1.0.0",
    ... )
"""
