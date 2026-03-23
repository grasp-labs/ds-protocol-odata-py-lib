"""
**File:** ``test_write.py``
**Region:** ``tests``

Tests for OdataDataset write() operation.
"""

from __future__ import annotations

import uuid
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    DeleteError,
    UpdateError,
)

from ds_protocol_odata_py_lib.dataset.odata import OdataDataset, OdataDatasetSettings


class TestWriteOperationsMinimal:
    """Minimal tests for write operations."""

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

    def test_update_empty_input_noop(self) -> None:
        """Test update() with empty input returns early."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(
                url="https://example.com/api/people",
                primary_keys=["id"],
            )
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )
            dataset.input = pd.DataFrame()
            dataset.update()  # Should return without error

    def test_delete_empty_input_noop(self) -> None:
        """Test delete() with empty input returns early."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(
                url="https://example.com/api/people",
                primary_keys=["id"],
            )
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )
            dataset.input = pd.DataFrame()
            dataset.delete()  # Should return without error

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

    def test_update_no_primary_keys_raises_error(self) -> None:
        """Test update() raises UpdateError when primary_keys not set."""
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
            dataset.input = pd.DataFrame([{"id": 1, "name": "John"}])

            with pytest.raises(UpdateError):
                dataset.update()

    def test_delete_no_primary_keys_raises_error(self) -> None:
        """Test delete() raises DeleteError when primary_keys not set."""
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
            dataset.input = pd.DataFrame([{"id": 1}])

            with pytest.raises(DeleteError):
                dataset.delete()

    def test_create_request_exception_raises_create_error(self) -> None:
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

    def test_update_no_serializer_raises_update_error(self) -> None:
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(
                url="https://example.com/api/people",
                primary_keys=["id"],
            )
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )
            dataset.input = pd.DataFrame([{"id": 1, "name": "John"}])
            dataset.serializer = None

            with pytest.raises(UpdateError):
                dataset.update()

    def test_update_patch_method_not_allowed_falls_back_to_put(self) -> None:
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(
                url="https://example.com/api/people",
                primary_keys=["id"],
            )
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )
            dataset.input = pd.DataFrame([{"id": 1, "name": "John"}])

            response_405 = Mock(status_code=405, content=b"", json=Mock(return_value={}))
            response_405.raise_for_status.return_value = None
            response_200 = Mock(status_code=200, content=b"", json=Mock(return_value={}))
            response_200.raise_for_status.return_value = None

            mock_session = Mock()
            mock_session.prepare_request.return_value = Mock()
            mock_session.send.side_effect = [response_405, response_200]
            linked_service.connection = Mock(session=mock_session)

            dataset.update()

            assert mock_session.send.call_count == 2
            assert isinstance(dataset.output, pd.DataFrame)
            assert len(dataset.output) == 1

    def test_update_request_exception_raises_update_error(self) -> None:
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(
                url="https://example.com/api/people",
                primary_keys=["id"],
            )
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )
            dataset.input = pd.DataFrame([{"id": 1, "name": "John"}])

            mock_session = Mock()
            mock_session.prepare_request.return_value = Mock()
            mock_session.send.side_effect = requests.exceptions.RequestException("network")
            linked_service.connection = Mock(session=mock_session)

            with pytest.raises(UpdateError):
                dataset.update()

    def test_delete_no_serializer_raises_error(self) -> None:
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(
                url="https://example.com/api/people",
                primary_keys=["id"],
            )
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )
            dataset.input = pd.DataFrame([{"id": 1}])
            dataset.serializer = None

            with pytest.raises(DeleteError):
                dataset.delete()

    def test_delete_request_exception_raises_delete_error(self) -> None:
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            linked_service.settings = Mock(headers={"Authorization": "Bearer x"})
            settings = OdataDatasetSettings(
                url="https://example.com/api/people",
                primary_keys=["id"],
            )
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )
            dataset.input = pd.DataFrame([{"id": 1}])

            mock_session = Mock()
            mock_session.prepare_request.return_value = Mock()
            mock_session.send.side_effect = requests.exceptions.RequestException("network")
            linked_service.connection = Mock(session=mock_session)

            with pytest.raises(DeleteError):
                dataset.delete()
