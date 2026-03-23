"""
**File:** ``odata.py``
**Region:** ``ds_protocol_odata_py_lib/dataset``

OData dataset implementation for reading and writing data via OData protocol.

This module provides the OdataDataset class which implements the Dataset
interface for OData-compliant APIs. It supports CRUD operations (create,
read, update, delete) with optional pagination and filtering.

Example:
    >>> from ds_protocol_odata_py_lib.dataset.odata import OdataDataset
    >>> linked_service = HttpLinkedService(settings=...)
    >>> linked_service.connect()
    >>> dataset = OdataDataset(linked_service=linked_service, settings=...)
    >>> dataset.read()

"""

from dataclasses import dataclass, field
from http.client import METHOD_NOT_ALLOWED
from typing import Any, Generic, NoReturn, TypeVar

import pandas as pd
import requests
from ds_common_logger_py_lib import Logger
from ds_protocol_http_py_lib import HttpLinkedService
from ds_protocol_http_py_lib.dataset.http import HttpLinkedServiceType
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetSettings, DatasetStorageFormatType, TabularDataset
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    DeleteError,
    MismatchedLinkedServiceError,
    ReadError,
    UpdateError,
)
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer
from requests import Response

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
        if not isinstance(self.linked_service, HttpLinkedService):
            raise MismatchedLinkedServiceError("Linked service must be of type HTTP")

    @property
    def type(self) -> ResourceType:
        return ResourceType.DATASET_ODATA

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
                response_content: dict[str, str] = response.json()

                if response_content and self.deserializer and "value" in response_content:
                    deserialized_data = self.deserializer(response_content["value"])
                    result.append(deserialized_data)
                    logger.info(
                        "Fetched %s records from %s",
                        len(deserialized_data),
                        response.url,
                    )

                if not self.settings.paginate:
                    break

                if not response_content.get("@odata.nextLink"):
                    logger.debug("No more pages to fetch. Terminating data fetch loop.")

                # Update the pagination URL
                self.settings.pagination_url = response_content.get("@odata.nextLink")
                _next = True

            except requests.exceptions.HTTPError as exc:
                raise ReadError(
                    f"Failed to send GET request to {self.settings.url}: {exc}",
                ) from exc

        self.output = pd.concat(result, ignore_index=True) if result else pd.DataFrame()
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
            logger.warning("No content data provided for create.")
            return

        logger.debug("Serializing data for write")
        if not self.serializer:
            raise CreateError("Data serializer not provided")
        payload = self.serializer(self.input)

        try:
            req = requests.Request(
                method="POST",
                url=self.settings.url,
                data=payload,
                headers=self.linked_service.settings.headers,
            )
            prepared = self.linked_service.connection.session.prepare_request(req)
            response = self.linked_service.connection.session.send(prepared)

            logger.debug(f"HTTP Response Info: {self._response_info(response)}")
            response.raise_for_status()
            self._set_output_from_response(response, fallback_to_input=True)
        except requests.exceptions.RequestException as exc:
            raise CreateError(
                f"Failed to send POST request to {self.settings.url}: {exc}",
            ) from exc

    def update(self) -> None:
        """
        Update entity using Odata.

        Returns:
            None
        """
        logger.info(f"Sending PUT request to {self.settings.url}")

        if not isinstance(self.input, pd.DataFrame) or self.input.empty:
            logger.warning("No content data provided for update.")
            return

        if not self.settings.primary_keys:
            raise UpdateError("Primary keys must be provided for update.")

        logger.debug("Serializing data for update")
        if not self.serializer:
            raise UpdateError("Data serializer not provided")
        payload = self.serializer(self.input)

        try:
            req = requests.Request(
                method="PATCH",
                url=self.settings.url,
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
            self._set_output_from_response(response, fallback_to_input=True)
        except requests.exceptions.RequestException as exc:
            raise UpdateError(
                f"Failed to send update request to {self.settings.url}: {exc}",
            ) from exc

    def delete(self) -> None:
        """
        Delete entity using odata.
        """
        logger.info(f"Sending DELETE request to {self.settings.url}")

        if not isinstance(self.input, pd.DataFrame) or self.input.empty:
            logger.warning("No content data provided for delete.")
            return

        logger.debug("Serializing data for write")
        if not self.serializer:
            raise DeleteError("Data serializer not provided")

        try:
            url = self._build_resource_url()

            req = requests.Request(
                method="DELETE",
                url=url,
                headers=self.linked_service.settings.headers,
            )
            prepared = self.linked_service.connection.session.prepare_request(req)
            response = self.linked_service.connection.session.send(prepared)

            logger.debug(f"HTTP Response Info: {self._response_info(response)}")
            response.raise_for_status()
            self._set_output_from_response(response, fallback_to_input=True)
        except requests.exceptions.RequestException as exc:
            raise DeleteError(
                f"Failed to send DELETE request to {self.settings.url}: {exc}",
            ) from exc

    def rename(self) -> NoReturn:
        """
        List entity using odata.
        """
        raise NotSupportedError("List operation is not supported for Odata datasets")

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

    def close(self) -> None:
        """
        Just to be compliant with the Dataset interface, no need to close anything for Odata datasets since we
        are not maintaining any persistent connections or resources that require cleanup.

        Returns:
            None
        """
        pass

    def _set_output_from_response(self, response: Response, *, fallback_to_input: bool = False) -> None:
        if not response.content:
            if fallback_to_input and isinstance(self.input, pd.DataFrame):
                self.output = self.input.copy()
            else:
                self.output = pd.DataFrame()
            logger.info(
                "Request completed with empty response for %s.",
                self.settings.url,
            )
            return
        if not self.deserializer:
            if fallback_to_input and isinstance(self.input, pd.DataFrame):
                self.output = self.input.copy()
            else:
                self.output = pd.DataFrame()
            logger.info(
                "Request completed with no deserializer configured for %s.",
                self.settings.url,
            )
            return
        logger.debug("Deserializing response content.")
        response_content = response.json()

        if isinstance(response_content, dict) and "value" in response_content:
            self.output = self.deserializer(response_content["value"])
        else:
            try:
                self.output = self.deserializer(response_content)
            except ValueError:
                # Fall back to wrapping object responses in a list
                self.output = self.deserializer([response_content])
                logger.info(
                    "Successfully processed response with (%s) records for %s.",
                    len(self.output),
                    self.settings.url,
                )

    def _build_resource_url(self) -> str:
        r"""
        Build the full OData resource URL for a specific entity using configured primary keys.

        The method reads key values from the first row of `self.input` and appendsan OData key segment
        to `self.settings.url`. String values are quoted andnon\-string values are emitted as raw literals.

        Raises:
            Exception: If primary keys are not configured or if any primary key is missing in the input DataFrame.

        Examples:
        - Single key: `.../resource(id=123)`
        - Composite key: `.../resource(userPrincipalName='john.doe@example.com',tenantId=456)`

        Returns:
            str
        """
        if not self.settings.primary_keys:
            raise DeleteError("Primary keys must be provided to segment the URL.")

        try:
            segment = ",".join(
                f"{key}='{self.input.iloc[0][key]}'"
                if isinstance(self.input.iloc[0][key], str)
                else f"{key}={self.input.iloc[0][key]}"
                for key in self.settings.primary_keys
            )
        except KeyError as exc:
            raise DeleteError(f"Primary key {exc} not found in the payload.") from exc

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
