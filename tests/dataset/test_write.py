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
