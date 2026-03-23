"""
**File:** ``test_dataset.py``
**Region:** ``tests``

Minimal tests for OdataDataset with proper mocking.
"""

from __future__ import annotations

import uuid
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from ds_protocol_http_py_lib import HttpLinkedService
from ds_resource_plugin_py_lib.common.resource.dataset.errors import DeleteError
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError
from requests import Response

from ds_protocol_odata_py_lib.dataset.odata import OdataDataset, OdataDatasetSettings
from ds_protocol_odata_py_lib.enums import ResourceType


class TestBuildResourceUrl:
    """Test _build_resource_url() method."""


class TestOdataDataset:
    """Minimal tests for OdataDataset."""

    def setup_method(self) -> None:
        """Setup dataset for each test."""
        self.linked_service = Mock(spec=HttpLinkedService)
        self.settings = OdataDatasetSettings(
            url="https://example.com/api/people",
            primary_keys=["id"],
        )
        self.dataset = OdataDataset(
            linked_service=self.linked_service,
            settings=self.settings,
            id=uuid.uuid4(),
            name="test",
            version="1.0.0",
        )

    def test_build_url_string_key(self) -> None:
        """Test building URL with string key."""
        self.settings.primary_keys = ["username"]
        self.dataset.input = pd.DataFrame([{"username": "john"}])

        url = self.dataset._build_resource_url()

        assert url == "https://example.com/api/people(username='john')"

    def test_build_url_numeric_key(self) -> None:
        """Test building URL with numeric key."""
        self.dataset.input = pd.DataFrame([{"id": 123}])

        url = self.dataset._build_resource_url()

        assert url == "https://example.com/api/people(id=123)"

    def test_build_url_no_primary_keys(self) -> None:
        """Test that missing primary_keys raises DeleteError."""
        self.settings.primary_keys = None
        self.dataset.input = pd.DataFrame([{"id": 1}])

        with pytest.raises(DeleteError):
            self.dataset._build_resource_url()

    def test_build_url_missing_key(self) -> None:
        """Test that missing key raises DeleteError."""
        self.settings.primary_keys = ["id", "status"]
        self.dataset.input = pd.DataFrame([{"id": 1}])

        with pytest.raises(DeleteError):
            self.dataset._build_resource_url()


class TestResponseInfo:
    """Test _response_info() static method."""

    def test_response_info(self) -> None:
        """Test response info extraction."""
        mock_response = Mock(spec=Response)
        mock_response.status_code = 200
        mock_response.url = "https://example.com/api"
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.reason = "OK"
        mock_response.content = b"test"

        info = OdataDataset._response_info(mock_response)

        assert info["status_code"] == 200
        assert info["url"] == "https://example.com/api"

    def test_response_info_empty(self) -> None:
        """Test response info with empty content."""
        mock_response = Mock(spec=Response)
        mock_response.status_code = 204
        mock_response.url = "https://example.com"
        mock_response.headers = {}
        mock_response.reason = "No Content"
        mock_response.content = b""

        info = OdataDataset._response_info(mock_response)

        assert info["content"] is None

    def test_dataset_type_property(self) -> None:
        """Test that dataset type property returns DATASET_ODATA."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(url="https://example.com/api")

            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )

            assert dataset.type == ResourceType.DATASET_ODATA

    def test_dataset_close(self) -> None:
        """Test that close() method works."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(url="https://example.com/api")

            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )

            dataset.close()  # Should not raise

    def test_unsupported_operations(self) -> None:
        """Test that unsupported operations raise."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(url="https://example.com/api")

            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )

            with pytest.raises(NotSupportedError):
                dataset.rename()
            with pytest.raises(NotSupportedError):
                dataset.list()
            with pytest.raises(NotSupportedError):
                dataset.purge()
            with pytest.raises(NotSupportedError):
                dataset.upsert()
