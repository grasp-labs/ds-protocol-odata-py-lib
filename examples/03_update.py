"""
**File:** ``03_update.py``
**Region:** ``examples``

Update an existing entity in an OData service.

This example demonstrates how to:
1. Create an HTTP linked service
2. Connect to the backend
3. Create an OData dataset with primary keys configured
4. Prepare input data for update
5. Update rows matched by primary key via update()
6. Inspect the output (backend response or input copy)
7. Close the linked service
"""

import uuid

import pandas as pd
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType

from ds_protocol_odata_py_lib.dataset.odata import OdataDataset, OdataDatasetSettings


def main() -> None:
    """Update a person in TripPin OData service."""
    linked_service = HttpLinkedService(
        settings=HttpLinkedServiceSettings(
            host="services.odata.org",
            auth_type=AuthType.NO_AUTH,
            headers={
                "Content-Type": "application/json",
                "OData-Version": "4.0",
                "OData-MaxVersion": "4.02",
            },
        ),
        id=uuid.uuid4(),
        name="sample::linked_service",
        version="1.0.0",
    )

    dataset = OdataDataset(
        linked_service=linked_service,
        settings=OdataDatasetSettings(
            url="https://services.odata.org/TripPinRESTierService/People",
            # primary_keys=["UserName"],
        ),
        id=uuid.uuid4(),
        description="Update a person in TripPin",
        name="sample::dataset",
        version="1.0.0",
    )

    dataset.input = pd.DataFrame(
        [
            {
                "UserName": "russellwhyte",
                "FirstName": "Mirs",
                "LastName": "King",
            }
        ]
    )

    linked_service.connect()

    try:
        dataset.update()
        print("Update completed. Output:")
        print(dataset.output)
    finally:
        linked_service.close()


if __name__ == "__main__":
    main()
