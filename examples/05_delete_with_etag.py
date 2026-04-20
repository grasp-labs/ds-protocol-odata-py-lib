"""
**File:** ``05_delete_with_etag.py``
**Region:** ``examples``

Delete an entity from an OData service with ETag concurrency control.

This example demonstrates how to:
1. Create an HTTP linked service with ETag headers
2. Connect to the backend
3. Create an OData dataset with primary keys configured
4. Prepare input data for deletion
5. Delete rows matched by primary key via delete()
6. Use If-Match header for concurrency control
7. Inspect the output (backend response or input copy)
8. Close the linked service

Note:
    Change any character in the ETag to get a 412 Precondition Failed response.
"""

import uuid

import pandas as pd
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType

from ds_protocol_odata_py_lib.dataset.odata import OdataDataset, OdataDatasetSettings, DeleteSettings


def main() -> None:
    """Delete an airline from TripPin OData service with ETag concurrency control."""
    etag = 'W/"J0FtZXJpY2FuIEFpcmxpbmVzJw=="'

    linked_service = HttpLinkedService(
        settings=HttpLinkedServiceSettings(
            host="services.odata.org",
            auth_type=AuthType.NO_AUTH,
            headers={
                "Accept": "application/json",
                "If-Match": etag,
            },
        ),
        id=uuid.uuid4(),
        name="sample::linked_service",
        version="1.0.0",
    )

    dataset = OdataDataset(
        linked_service=linked_service,
        settings=OdataDatasetSettings(
            url="https://services.odata.org/TripPinRESTierService/Airlines",
            delete=DeleteSettings(
            primary_keys=["AirlineCode"]
            )
        ),
        id=uuid.uuid4(),
        name="sample::dataset",
        version="1.0.0",
    )

    dataset.input = pd.DataFrame([{"AirlineCode": "AA"}])

    linked_service.connect()

    try:
        dataset.delete()
        print(f"Deleted rows: {len(dataset.output)}")
        print(dataset.output)
    finally:
        linked_service.close()


if __name__ == "__main__":
    main()
