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

Example:
    >>> from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
    >>> from ds_protocol_odata_py_lib.dataset.odata import OdataDataset, OdataDatasetSettings
    >>> linked_service = HttpLinkedService(settings=...)
    >>> linked_service.connect()
    >>> settings = OdataDatasetSettings(
    ...     url="https://api.example.com/odata/Users",
    ...     top=100,
    ... )
    >>> dataset = OdataDataset(linked_service=linked_service, settings=settings)
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
class DeleteSettings:
    primary_keys: list[str]


@dataclass(kw_only=True)
class ReadSettings:
    # Odata specific properties
    top: int | None = 100
    filter: str | None = None
    skip: int | None = None
    select: str | None = None
    orderby: str | None = None
    search: str | None = None
    expand: str | None = None
    count: bool | None = None
    params: dict[str, Any] = field(init=False)

    def __post_init__(self) -> None:
        """
        Post-initialize the settings by building the OData query parameters dictionary.

        This method is called automatically after the dataclass is initialized. It converts
        individual OData parameter fields (top, filter, skip, etc.) into a dictionary with
        OData query parameter names (prefixed with $). Only non-None values are included
        in the resulting params dictionary.

        The params dictionary is used internally when constructing OData requests to avoid
        having to check each parameter individually.

        Returns:
            None
        """
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


@dataclass(kw_only=True)
class OdataDatasetSettings(DatasetSettings):
    """
    Settings for Odata dataset operations.
    """

    url: str

    read: ReadSettings | None = None
    delete: DeleteSettings | None = None


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
    OData dataset for CRUD operations via OData-compliant REST APIs.

    This class implements the TabularDataset interface for OData-compliant APIs,
    supporting read, create, update, and delete (CRUD) operations. Data is exchanged
    as JSON via pandas DataFrames, with optional pagination and OData query parameters.

    Raises:
        MismatchedLinkedServiceError: If linked_service is not an instance of HttpLinkedService.
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
        Read entities from OData resource and populate self.output.

        Sends GET request to the OData API URL with configured query parameters
        (top, filter, skip, select, orderby, search, expand, count). If pagination
        is enabled, automatically follows @odata.nextLink to fetch all pages.

        Data is deserialized from JSON response format and concatenated into a single
        pandas DataFrame in self.output.

        Returns:
            None: Result is stored in self.output as pandas DataFrame.

        Raises:
            ReadError: If HTTP request fails or response status indicates error.
        """

        logger.info(f"Sending GET request to {self.settings.url}")

        pagination_url: str | None = self.settings.url
        result = []
        _next = False
        while pagination_url:
            try:
                req = requests.Request(
                    method="GET",
                    url=pagination_url,
                    params=self.settings.read.params if self.settings.read and not _next else None,
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

                if not response_content.get("@odata.nextLink"):
                    logger.debug("No more pages to fetch. Terminating data fetch loop.")

                # Update the pagination URL
                pagination_url = response_content.get("@odata.nextLink")
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
        Create entities via OData POST request.

        Serializes the pandas DataFrame in self.input to JSON and sends a POST request
        to the configured URL. The response (if any) is deserialized and stored in
        self.output.

        Returns:
            None: Created entities (or echo of input) stored in self.output as pandas DataFrame.

        Raises:
            CreateError: If input is not a valid DataFrame, serializer is not configured, or HTTP
        request fails (including server rejecting data).

        """
        logger.info(f"Sending POST request to {self.settings.url}")

        if self.input is None or not isinstance(self.input, pd.DataFrame) or self.input.empty:
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
        Update entities via OData PATCH request (falls back to PUT if PATCH not allowed).

        Serializes the pandas DataFrame in self.input to JSON and sends a PATCH request
        to the configured URL. If the server rejects PATCH with 405 Method Not Allowed,
        retries with PUT. The response is deserialized and stored in self.output.

        Returns:
            None: Updated entities (or echo of input) stored in self.output as pandas DataFrame.
        """
        logger.info(f"Sending PATCH/PUT request to {self.settings.url}")

        if self.input is None or not isinstance(self.input, pd.DataFrame) or self.input.empty:
            logger.warning("No content data provided for update.")
            return

        logger.debug("Serializing data for update")
        if not self.serializer:
            raise UpdateError("Data serializer not provided")
        payload = self.serializer(self.input)

        try:
            req = requests.Request(
                method="PATCH",
                url=self.settings.url,
                headers=self.linked_service.settings.headers,
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
        Delete an entity via OData DELETE request.

        Constructs a single OData key segment URL from the current input using the
        configured primary_keys fields and sends one DELETE request. The response is
        deserialized and stored in self.output.

        Returns:
            None: Server response (or input fallback) stored in self.output as pandas DataFrame.

        Raises:
            DeleteError: If primary_keys are not configured, required primary key columns are missing
            from input, or the HTTP request fails.
        """
        logger.info(f"Sending DELETE request to {self.settings.url}")

        if self.input is None or not isinstance(self.input, pd.DataFrame) or self.input.empty:
            logger.warning("No content data provided for delete.")
            return
        try:
            url = self._build_delete_resource_url()

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
        Rename operation is not supported for OData datasets.

        OData endpoints typically handle entity identification via primary keys in URLs,
        not via separate rename operations. Renaming is typically done via update().

        Raises:
            NotSupportedError
        """
        raise NotSupportedError("Rename operation is not supported for Odata datasets")

    def list(self) -> NoReturn:
        """
        List operation is not supported for OData datasets.

        Use read() instead to retrieve entities from the OData resource.

        Raises:
            NotSupportedError
        """
        raise NotSupportedError("List operation is not supported for Odata datasets")

    def purge(self) -> NoReturn:
        """
        Purge operation is not supported for OData datasets.

        Use delete() to remove specific entities identified by primary keys.

        Raises:
            NotSupportedError
        """
        raise NotSupportedError("Purge operation is not supported for Odata")

    def upsert(self) -> NoReturn:
        """
        Upsert operation is not supported for OData datasets.

        Use create() or update() separately as needed.

        Raises:
            NotSupportedError
        """
        raise NotSupportedError("Upsert operation is not supported for Odata datasets")

    def close(self) -> None:
        """
        Close the dataset (no-op for OData datasets).

        This method is provided for compliance with the Dataset interface contract.
        OData datasets do not maintain persistent connections or resources requiring
        explicit cleanup. The actual connection lifecycle is managed by the linked_service.

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

    def _build_delete_resource_url(self) -> str:
        """
        Build the full OData resource URL for a specific entity using configured primary keys.

        The method reads key values from the first row of `self.input` and appends an OData
        key segment to `self.settings.url`. String values are quoted and non-string values
        are emitted as raw literals.

        Raises:
            DeleteError: If primary keys are not configured or if any primary key is missing in the input DataFrame.

        Returns:
            str: The OData resource URL with key segment appended.

        Examples
        --------
        - Single key: `.../resource(id=123)`
        - Composite key: `.../resource(userPrincipalName='john.doe@example.com',tenantId=456)`
        """
        if not self.settings.delete or not self.settings.delete.primary_keys:
            raise DeleteError("Primary keys must be provided to segment the URL.")

        try:
            segment = ",".join(
                f"{key}='{self.input.iloc[0][key]}'"
                if isinstance(self.input.iloc[0][key], str)
                else f"{key}={self.input.iloc[0][key]}"
                for key in self.settings.delete.primary_keys
            )
        except KeyError as exc:
            raise DeleteError(f"Primary key {exc} not found in the payload.") from exc

        return f"{self.settings.url}({segment})"

    @staticmethod
    def _response_info(response: requests.Response) -> dict[str, Any]:
        """
        Extract useful debugging information from an HTTP response.

        Args:
            response : requests.Response

        Returns:
            dict[str, Any]
        """
        return {
            "status_code": response.status_code,
            "url": response.url,
            "headers": response.headers,
            "reason": response.reason,
            "content": response.content[:500] if response.content else None,
        }
