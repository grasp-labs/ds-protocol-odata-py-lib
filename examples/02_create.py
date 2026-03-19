import uuid

import pandas as pd
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType

from ds_protocol_odata_py_lib.dataset.odata import OdataDataset, OdataDatasetSettings

dataset = OdataDataset(
    linked_service=HttpLinkedService(
        settings=HttpLinkedServiceSettings(
            host="services.odata.org",
            auth_type=AuthType.NO_AUTH,
            headers={"Content-Type": "application/json"},
        ),
        id=uuid.uuid4(),
        name="sample::linked_service",
        version="1.0.0",
    ),
    settings=OdataDatasetSettings(
        url="https://services.odata.org/TripPinRESTierService/People",
    ),
    id=uuid.uuid4(),
    description="Create a person in TripPin",
    name="sample::dataset",
    version="1.0.0",
)

dataset.input = pd.DataFrame(
    [
        {
            "UserName": "lewisblack",
            "FirstName": "Lewis",
            "LastName": "Black",
            "Emails": ["lewisblack@example.com"],
            "AddressInfo": [
                {
                    "Address": "187 Suffolk Ln.",
                    "City": {
                        "Name": "Boise",
                        "CountryRegion": "United States",
                        "Region": "ID",
                    },
                }
            ],
        }
    ]
)

dataset.create()
print("Dataset created successfully. Output:")
print(dataset.output)
