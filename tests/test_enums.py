"""
**File:** ``test_enums.py``
**Region:** ``tests``

Tests for enums module.

Tests all enum constants and their string values.
"""

from __future__ import annotations

from ds_protocol_odata_py_lib.enums import HttpMethod, ResourceType


class TestHttpMethod:
    """Test HttpMethod enum."""

    def test_http_method_get(self) -> None:
        """Test GET method constant."""
        assert HttpMethod.GET == "GET"
        assert HttpMethod.GET.value == "GET"

    def test_http_method_post(self) -> None:
        """Test POST method constant."""
        assert HttpMethod.POST == "POST"
        assert HttpMethod.POST.value == "POST"

    def test_http_method_put(self) -> None:
        """Test PUT method constant."""
        assert HttpMethod.PUT == "PUT"
        assert HttpMethod.PUT.value == "PUT"

    def test_http_method_delete(self) -> None:
        """Test DELETE method constant."""
        assert HttpMethod.DELETE == "DELETE"
        assert HttpMethod.DELETE.value == "DELETE"

    def test_http_method_patch(self) -> None:
        """Test PATCH method constant."""
        assert HttpMethod.PATCH == "PATCH"
        assert HttpMethod.PATCH.value == "PATCH"

    def test_http_method_all_members(self) -> None:
        """Test that all expected HTTP methods are present."""
        expected = {"GET", "POST", "PUT", "DELETE", "PATCH"}
        actual = {method.value for method in HttpMethod}
        assert actual == expected

    def test_http_method_str_conversion(self) -> None:
        """Test string conversion of HTTP methods."""
        assert str(HttpMethod.GET) == "GET"
        assert str(HttpMethod.POST) == "POST"


class TestResourceType:
    """Test ResourceType enum."""

    def test_resource_type_dataset_odata(self) -> None:
        """Test DATASET_ODATA resource type constant."""
        assert ResourceType.DATASET_ODATA == "DS.RESOURCE.DATASET.ODATA"
        assert ResourceType.DATASET_ODATA.value == "DS.RESOURCE.DATASET.ODATA"

    def test_resource_type_str_conversion(self) -> None:
        """Test string conversion of resource types."""
        assert str(ResourceType.DATASET_ODATA) == "DS.RESOURCE.DATASET.ODATA"

    def test_resource_type_is_str_enum(self) -> None:
        """Test that ResourceType is a StrEnum."""
        assert isinstance(ResourceType.DATASET_ODATA, str)
