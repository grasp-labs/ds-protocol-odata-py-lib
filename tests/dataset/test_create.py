"""
**File:** ``test_create.py``
**Region:** ``tests``

Tests for OdataDataset create() operation.
"""

from __future__ import annotations

import uuid
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests
from ds_resource_plugin_py_lib.common.resource.dataset.errors import CreateError

from ds_protocol_odata_py_lib.dataset.odata import OdataDataset, OdataDatasetSettings


class TestCreateOperations:
    """Tests for create() operation."""

    def test_create_empty_input_noop(self) -> None:
        """Test create() with empty input returns early."""
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
            dataset.input = pd.DataFrame()
            dataset.create()  # Should return without error

    def test_create_none_input_noop(self) -> None:
        """Test create() with None input returns early."""
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
            dataset.input = None
            dataset.create()  # Should return without error

    def test_create_no_serializer_raises_error(self) -> None:
        """Test create() raises CreateError when serializer is None."""
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
            dataset.input = pd.DataFrame([{"name": "John"}])
            dataset.serializer = None

            with pytest.raises(CreateError):
                dataset.create()

    def test_create_request_exception_raises_create_error(self) -> None:
        """Test create() wraps RequestException in CreateError."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            linked_service.settings = Mock(headers={"Authorization": "Bearer x"})
            settings = OdataDatasetSettings(url="https://example.com/api/people")
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )
            dataset.input = pd.DataFrame([{"name": "John"}])

            mock_session = Mock()
            mock_session.prepare_request.return_value = Mock()
            mock_session.send.side_effect = requests.exceptions.RequestException("network")
            linked_service.connection = Mock(session=mock_session)

            with pytest.raises(CreateError):
                dataset.create()

    def test_create_successful_with_response(self) -> None:
        """Test create() succeeds and processes response."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            linked_service.settings = Mock(headers={"Authorization": "Bearer x"})
            settings = OdataDatasetSettings(url="https://example.com/api/people")
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )
            dataset.input = pd.DataFrame([{"name": "John"}])

            mock_response = Mock()
            mock_response.content = b'{"value": [{"id": 1, "name": "John"}]}'
            mock_response.json.return_value = {"value": [{"id": 1, "name": "John"}]}
            mock_response.raise_for_status.return_value = None

            mock_session = Mock()
            mock_session.prepare_request.return_value = Mock()
            mock_session.send.return_value = mock_response
            linked_service.connection = Mock(session=mock_session)

            dataset.create()

            assert isinstance(dataset.output, pd.DataFrame)
            assert len(dataset.output) == 1
