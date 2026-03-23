import uuid

from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType

from ds_protocol_odata_py_lib.dataset.odata import OdataDataset, OdataDatasetSettings

dataset = OdataDataset(
    linked_service=HttpLinkedService(
        settings=HttpLinkedServiceSettings(
            host="services.odata.org",
            auth_type=AuthType.NO_AUTH,
            headers={"Content-Type": "application/json",
                     "OData-Version": "4.0",
                     "OData-MaxVersion": "4.02"
                     },
        ),
        id=uuid.uuid4(),
        name="sample::linked_service",
        version="1.0.0",
    ),
    settings=OdataDatasetSettings(
        url="https://services.odata.org/TripPinRESTierService/People",  # https://www.odata.org/odata-services
        # select='UserName, FirstName, LastName',
        select='FirstName',
        # count=True,
        top=15,
        # skip=2,
        # paginate=True,
        # filter="FirstName eq 'Scott'",
        # expand="Trips($filter=Name eq 'Trip in US')"
        # primary_keys=["UserName"]

    ),
    id=uuid.uuid4(),
    description="A sample dataset",
    name="sample::dataset",
    version="1.0.0",
)
dataset.read()
print(dataset.output)
