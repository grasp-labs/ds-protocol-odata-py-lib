"""
**File:** ``01_read.py``
**Region:** ``examples``

Read data from an OData service.

This example demonstrates how to:
1. Create an HTTP linked service
2. Connect to the backend
3. Create an OData dataset
4. Read data with optional filtering and pagination
5. Close the linked service
"""

import uuid

from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType

from ds_protocol_odata_py_lib.dataset.odata import OdataDataset, OdataDatasetSettings, ReadSettings


def main() -> None:
    """Read data from OData service."""
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
            read=ReadSettings(
                select="FirstName",
                top=15,
                # Uncomment to customize:
                # select='UserName, FirstName, LastName',
                # count=True,
                # skip=2,
                # filter="FirstName eq 'Scott'",
                # expand="Trips($filter=Name eq 'Trip in US')",
            ),
            # primary_keys = ["UserName"]
        ),
        id=uuid.uuid4(),
        description="A sample dataset",
        name="sample::dataset",
        version="1.0.0",
    )

    linked_service.connect()

    try:
        dataset.read()
        print(dataset.output)
    finally:
        linked_service.close()


if __name__ == "__main__":
    main()
