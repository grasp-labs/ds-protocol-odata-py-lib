"""
**File:** ``test_read.py``
**Region:** ``tests``

Tests for OdataDataset read() operation.
"""

from __future__ import annotations

import uuid
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests
from ds_resource_plugin_py_lib.common.resource.dataset.errors import ReadError

from ds_protocol_odata_py_lib.dataset.odata import OdataDataset, OdataDatasetSettings


class TestOdataDatasetRead:
    """Test read() operation."""

    def test_read_empty_response(self) -> None:
        """Test read() with empty response."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(
                url="https://example.com/api/people",
            )
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )

            # Mock the response properly
            mock_response = Mock()
            mock_response.content = b'{"value": []}'
            mock_response.json.return_value = {"value": []}
            mock_response.url = "https://example.com/api/people"
            mock_response.raise_for_status.return_value = None

            mock_session = Mock()
            mock_session.prepare_request.return_value = Mock()
            mock_session.send.return_value = mock_response

            linked_service.connection = Mock()
            linked_service.connection.session = mock_session

            dataset.read()

            assert isinstance(dataset.output, pd.DataFrame)
            assert len(dataset.output) == 0

    def test_read_with_data(self) -> None:
        """Test read() with data in response."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(
                url="https://example.com/api/people",
            )
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )

            data = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
            mock_response = Mock()
            mock_response.content = b'{"value": [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]}'
            mock_response.json.return_value = {"value": data}
            mock_response.url = "https://example.com/api/people"
            mock_response.raise_for_status.return_value = None

            mock_session = Mock()
            mock_session.prepare_request.return_value = Mock()
            mock_session.send.return_value = mock_response

            linked_service.connection = Mock()
            linked_service.connection.session = mock_session

            dataset.read()

            assert isinstance(dataset.output, pd.DataFrame)
            assert len(dataset.output) == 2

    def test_read_with_pagination(self) -> None:
        """Test read() with pagination."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(
                url="https://example.com/api/people",
                paginate=True,
            )
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )

            # First page response
            page1_data = [{"id": 1, "name": "John"}]
            mock_response1 = Mock()
            mock_response1.content = b'{"value": [{"id": 1}], "@odata.nextLink": "https://example.com/api/people?skip=1"}'
            mock_response1.json.return_value = {
                "value": page1_data,
                "@odata.nextLink": "https://example.com/api/people?skip=1",
            }
            mock_response1.url = "https://example.com/api/people"
            mock_response1.raise_for_status.return_value = None

            # Second page response
            page2_data = [{"id": 2, "name": "Jane"}]
            mock_response2 = Mock()
            mock_response2.content = b'{"value": [{"id": 2}]}'
            mock_response2.json.return_value = {"value": page2_data}
            mock_response2.url = "https://example.com/api/people?skip=1"
            mock_response2.raise_for_status.return_value = None

            mock_session = Mock()
            mock_session.prepare_request.return_value = Mock()
            mock_session.send.side_effect = [mock_response1, mock_response2]

            linked_service.connection = Mock()
            linked_service.connection.session = mock_session

            dataset.read()

            assert len(dataset.output) == 2

    def test_read_without_deserializer(self) -> None:
        """Test read() when deserializer is None."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(
                url="https://example.com/api/people",
            )
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )
            dataset.deserializer = None

            mock_response = Mock()
            mock_response.content = b'{"value": [{"id": 1}]}'
            mock_response.json.return_value = {"value": [{"id": 1}]}
            mock_response.url = "https://example.com/api/people"
            mock_response.raise_for_status.return_value = None

            mock_session = Mock()
            mock_session.prepare_request.return_value = Mock()
            mock_session.send.return_value = mock_response

            linked_service.connection = Mock()
            linked_service.connection.session = mock_session

            dataset.read()

            # When no deserializer, output should be empty
            assert isinstance(dataset.output, pd.DataFrame)

    def test_read_http_error_raises_read_error(self) -> None:
        """Test read() wraps HTTPError in ReadError."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(url="https://example.com/api/people")
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )

            mock_response = Mock()
            mock_response.content = b"{}"
            mock_response.json.return_value = {}
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("boom")

            mock_session = Mock()
            mock_session.prepare_request.return_value = Mock()
            mock_session.send.return_value = mock_response

            linked_service.connection = Mock()
            linked_service.connection.session = mock_session

            with pytest.raises(ReadError):
                dataset.read()
