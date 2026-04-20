"""
**File:** ``test_delete.py``
**Region:** ``tests``

Tests for OdataDataset delete() operation.
"""

from __future__ import annotations

import uuid
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests
from ds_resource_plugin_py_lib.common.resource.dataset.errors import DeleteError

from ds_protocol_odata_py_lib.dataset.odata import DeleteSettings, OdataDataset, OdataDatasetSettings


class TestDeleteOperations:
    """Tests for delete() operation."""

    def test_delete_empty_input_noop(self) -> None:
        """Test delete() with empty input returns early."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(url="https://example.com/api/people", delete=DeleteSettings(primary_keys=["id"]))
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )
            dataset.input = pd.DataFrame()
            dataset.delete()  # Should return without error

    def test_delete_no_primary_keys_raises_error(self) -> None:
        """Test delete() raises DeleteError when primary_keys not set."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            settings = OdataDatasetSettings(
                url="https://example.com/api/people",
                delete=DeleteSettings(primary_keys=None),
            )
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

    def test_delete_request_exception_raises_delete_error(self) -> None:
        """Test delete() wraps RequestException in DeleteError."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            linked_service.settings = Mock(headers={"Authorization": "Bearer x"})
            settings = OdataDatasetSettings(url="https://example.com/api/people", delete=DeleteSettings(primary_keys=["id"]))
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

    def test_delete_successful_with_response(self) -> None:
        """Test delete() succeeds and processes response."""
        with patch("ds_protocol_odata_py_lib.dataset.odata.isinstance", return_value=True):
            linked_service = Mock()
            linked_service.settings = Mock(headers={"Authorization": "Bearer x"})
            settings = OdataDatasetSettings(url="https://example.com/api/people", delete=DeleteSettings(primary_keys=["id"]))
            dataset = OdataDataset(
                linked_service=linked_service,
                settings=settings,
                id=uuid.uuid4(),
                name="test",
                version="1.0.0",
            )
            dataset.input = pd.DataFrame([{"id": 1}])

            mock_response = Mock()
            mock_response.content = b""
            mock_response.raise_for_status.return_value = None

            mock_session = Mock()
            mock_session.prepare_request.return_value = Mock()
            mock_session.send.return_value = mock_response
            linked_service.connection = Mock(session=mock_session)

            dataset.delete()

            assert isinstance(dataset.output, pd.DataFrame)
