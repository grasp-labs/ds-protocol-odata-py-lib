import json
from dataclasses import dataclass, field
from http.client import METHOD_NOT_ALLOWED
from typing import Any, Generic, NoReturn, TypeVar

import pandas as pd
import requests
from ds_common_logger_py_lib import Logger
from ds_protocol_http_py_lib import HttpLinkedService
from ds_protocol_http_py_lib.dataset.http import HttpLinkedServiceType
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetSettings, DatasetStorageFormatType, TabularDataset
from ds_resource_plugin_py_lib.common.resource.dataset.errors import CreateError, MismatchedLinkedServiceError, ReadError
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ..enums import ResourceType

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class OdataDatasetSettings(DatasetSettings):
    url: str
    primary_keys: list[str] | None = None

    # Pagination properties
    paginate: bool | None = False
    pagination_url: str | None = field(init=False)

    # Odata specific properties
    params: dict[str, Any] = field(init=False)
    top: int | None = 100
    filter: str | None = None
    skip: int | None = None
    select: str | None = None
    orderby: str | None = None
    search: str | None = None
    expand: str | None = None
    count: bool | None = None

    def __post_init__(self) -> None:
        raw_params = {
            "$top": self.top,
            "$filter": self.filter,
            "$skip": self.skip,
            "$select": self.select,
            "$orderby": self.orderby,
            "$search": self.search,
            "$expand": self.expand,
            "$count": str(self.count).lower() if isinstance(self.count, bool) else None,
        }
        self.params = {k: v for k, v in raw_params.items() if v is not None}


OdataDatasetSettingsType = TypeVar(
    "OdataDatasetSettingsType",
    bound=OdataDatasetSettings,
)


@dataclass(kw_only=True)
class OdataDataset(
    TabularDataset[
        HttpLinkedServiceType,
        OdataDatasetSettingsType,
        PandasSerializer,
        PandasDeserializer,
    ],
    Generic[HttpLinkedServiceType, OdataDatasetSettingsType],
):
    """
    Represent Odata dataset.
    """

    settings: OdataDatasetSettingsType
    linked_service: HttpLinkedServiceType

    serializer: PandasSerializer | None = field(
        default_factory=lambda: PandasSerializer(format=DatasetStorageFormatType.JSON),
    )
    deserializer: PandasDeserializer | None = field(
        default_factory=lambda: PandasDeserializer(format=DatasetStorageFormatType.JSON),
    )

    def __post_init__(self) -> None:
        self.linked_service.connect()

        if not isinstance(self.linked_service, HttpLinkedService):
            raise MismatchedLinkedServiceError("Linked service must be of type HTTP")

    @property
    def type(self) -> ResourceType:
        return ResourceType.DATASET_ODATA

    def _build_resource_url(self, entity: str) -> str:
        """
        Build the full resource URL for a specific entity based on its primary keys.

        This method constructs a full OData resource URL by dynamically appending
        the primary key values to the base URL in the format required by OData.
        It handles both single and composite keys and formats them appropriately
        for strings and numeric values.

        Examples:
        ---------
        **Single Key**:
        - Primary Keys: ['id']
        - Row Data: {'id': 123, 'name': 'John Doe'}
        - Result: "https://example.com/resource(id=123)"

        **Composite Keys**:
        - Primary Keys: ['userPrincipalName', 'tenantId']
        - Row Data: {'userPrincipalName': 'john.doe@example.com', 'tenantId': 456}
        - Result: "https://example.com/resource(userPrincipalName='john.doe@example.com',tenantId=456)"

        :param row: pd.Series: The row data containing the primary key values.
        :return: str: The full resource URL with primary key values appended.
        """
        if not self.settings.primary_keys:
            raise Exception("Primary keys must be provided to segment the URL.")

        payload: dict[str, str] = json.loads(entity)["value"]["0"]
        try:
            segment = ",".join(
                (f"{key}='{payload[key]}'" if isinstance(payload[key], str) else f"{key}={payload[key]}")
                for key in self.settings.primary_keys
            )
        except KeyError as exc:
            raise Exception(f"Primary key {exc} not found in the payload.") from exc

        return f"{self.settings.url}({segment})"

    @staticmethod
    def _response_info(response: requests.Response) -> dict[str, Any]:
        return {
            "status_code": response.status_code,
            "url": response.url,
            "headers": response.headers,
            "reason": response.reason,
            "content": response.content[:500] if response.content else None,
        }

    def read(self) -> None:
        """
        Read Odata dataset.

        Returns:
            None
        """

        logger.info(f"Sending GET request to {self.settings.url}")

        self.settings.pagination_url = self.settings.url
        result = []
        _next = False
        while self.settings.pagination_url:
            try:
                print(self.settings.pagination_url)
                print(self.settings.params)
                req = requests.Request(
                    method="GET",
                    url=self.settings.pagination_url,
                    params=self.settings.params if not _next else None,
                )
                prepared = self.linked_service.connection.session.prepare_request(req)
                response = self.linked_service.connection.session.send(prepared)

                logger.debug(f"HTTP Response Info: {self._response_info(response)}")
                response.raise_for_status()

                # Inspect the response.
                data: dict[str, str] = response.json()

                if data and self.deserializer:
                    deserialized_data = self.deserializer(data["value"])
                    result.append(deserialized_data)
                    logger.info(
                        "Fetched %s records from %s",
                        len(deserialized_data),
                        response.url,
                    )

                if not self.settings.paginate:
                    break

                if not data.get("@odata.nextLink"):
                    logger.debug("No more pages to fetch. Terminating data fetch loop.")

                # Update the pagination URL
                self.settings.pagination_url = data.get("@odata.nextLink")
                _next = True

            except requests.exceptions.HTTPError as exc:
                raise ReadError(
                    f"Failed to send GET request to {self.settings.url}: {exc}",
                ) from exc

        self.output = pd.concat(result, ignore_index=True)
        logger.info(
            "Successfully read a total of (%s) records for %s.",
            len(self.output),
            self.settings.url,
        )

    def create(self) -> None:
        """
        Create entity using Odata.

        Returns:
            None
        """
        logger.info(f"Sending POST request to {self.settings.url}")

        if not isinstance(self.input, pd.DataFrame) or self.input.empty:
            raise Exception("Content data must be provided for creation.")

        logger.debug("Serializing data for write")
        if not self.serializer:
            raise CreateError("Data serializer not provided")
        payload = self.serializer(self.input)

        req = requests.Request(
            method="POST",
            url=self.settings.url,
            data=payload,
        )
        prepared = self.linked_service.connection.session.prepare_request(req)
        response = self.linked_service.connection.session.send(prepared)

        logger.debug(f"HTTP Response Info: {self._response_info(response)}")
        response.raise_for_status()

        if self.deserializer and response.content:
            logger.debug("Deserializing response content.")
            self.output = self.deserializer(response.content)

        logger.info(
            "Successfully created (%s) records for %s.",
            len(self.input),
            self.settings.url,
        )

    def update(self) -> None:
        """
        Update entity using Odata.

        Returns:
            None
        """
        logger.info(f"Sending PUT request to {self.settings.url}")

        if not self.settings.primary_keys:
            raise Exception("Primary keys must be provided for update.")

        if not isinstance(self.input, pd.DataFrame) or self.input.empty:
            logger.warning("No content data provided for update.")
            return

        logger.debug("Serializing data for update")
        if not self.serializer:
            raise CreateError("Data serializer not provided")
        payload = self.serializer(self.input)

        url = self._build_resource_url(payload)
        req = requests.Request(
            method="PATCH",
            url=url,
            data=payload,
        )
        prepared = self.linked_service.connection.session.prepare_request(req)
        response = self.linked_service.connection.session.send(prepared)
        if response.status_code == METHOD_NOT_ALLOWED:
            req.method = "PUT"
            prepared = self.linked_service.connection.session.prepare_request(req)
            response = self.linked_service.connection.session.send(prepared)

        logger.debug(f"HTTP Response Info: {self._response_info(response)}")
        response.raise_for_status()

        if self.deserializer and response.content:
            logger.debug("Deserializing response content.")
            self.input = self.deserializer(response.content)

        logger.info(
            "Successfully updated (%s) records for %s.",
            len(self.input),
            self.settings.url,
        )

    def delete(self) -> NoReturn:
        """
        List entity using odata.
        """
        raise NotSupportedError("Delete operation is not supported for Odata dataset")

    def rename(self) -> NoReturn:
        """
        List entity using odata.
        """
        raise NotSupportedError("List operation is not supported for Odata datasets")

    def close(self) -> None:
        """ """
        pass

    def list(self) -> NoReturn:
        """
        List entity using odata.
        """
        raise NotSupportedError("List operation is not supported for Odata datasets")

    def purge(self) -> NoReturn:
        """
        Purge entity using odata.
        """
        raise NotSupportedError("Purge operation is not supported for Odata")

    def upsert(self) -> NoReturn:
        """
        Upsert entity using odata.
        """
        raise NotSupportedError("Upsert operation is not supported for Odata datasets")
