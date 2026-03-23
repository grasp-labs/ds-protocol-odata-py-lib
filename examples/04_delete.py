"""
**File:** ``04_delete.py``
**Region:** ``examples``

Delete an entity from an OData service.

This example demonstrates how to:
1. Create an HTTP linked service
2. Connect to the backend
3. Create an OData dataset with primary keys configured
4. Prepare input data for deletion
5. Delete rows matched by primary key via delete()
6. Inspect the output (backend response or input copy)
7. Close the linked service
"""

import uuid

import pandas as pd
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType

from ds_protocol_odata_py_lib.dataset.odata import OdataDataset, OdataDatasetSettings


def main() -> None:
    """Delete a person from TripPin OData service."""
    linked_service = HttpLinkedService(
        settings=HttpLinkedServiceSettings(
            host="services.odata.org",
            auth_type=AuthType.NO_AUTH,
            headers={"Accept": "application/json"},
        ),
        id=uuid.uuid4(),
        name="sample::linked_service",
        version="1.0.0",
    )

    dataset = OdataDataset(
        linked_service=linked_service,
        settings=OdataDatasetSettings(
            url="https://services.odata.org/TripPinRESTierService/People",
            primary_keys=["UserName"],
        ),
        id=uuid.uuid4(),
        name="sample::dataset",
        version="1.0.0",
    )

    dataset.input = pd.DataFrame([{"UserName": "russellwhyte"}])

    linked_service.connect()

    try:
        dataset.delete()
        print(f"Deleted rows: {len(dataset.output)}")
        print(dataset.output)
    finally:
        linked_service.close()


if __name__ == "__main__":
    main()
