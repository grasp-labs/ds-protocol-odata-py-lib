# examples/04_delete.py
import uuid

import pandas as pd
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType

from ds_protocol_odata_py_lib.dataset.odata import OdataDataset, OdataDatasetSettings


def main() -> None:
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
    dataset.delete()

    print(f"Deleted rows: {len(dataset.output)}")
    print(dataset.output)


if __name__ == "__main__":
    main()
