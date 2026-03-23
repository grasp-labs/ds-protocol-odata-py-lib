# examples/04_delete.py
import uuid

import pandas as pd
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType

from ds_protocol_odata_py_lib.dataset.odata import OdataDataset, OdataDatasetSettings


def main() -> None:
    """change any character in ETag to get a 412 Precondition Failed response"""
    etag =  'W/"J0FtZXJpY2FuIEFpcmxpbmVzJw=="'
    linked_service = HttpLinkedService(
        settings=HttpLinkedServiceSettings(
            host="services.odata.org",
            auth_type=AuthType.NO_AUTH,
            headers={"Accept": "application/json",
                     "If-Match": etag
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
            primary_keys=["AirlineCode"],
        ),
        id=uuid.uuid4(),
        name="sample::dataset",
        version="1.0.0",
    )

    dataset.input = pd.DataFrame([{"AirlineCode": "AA"}])
    dataset.delete()

    print(f"Deleted rows: {len(dataset.output)}")
    print(dataset.output)


if __name__ == "__main__":
    main()
