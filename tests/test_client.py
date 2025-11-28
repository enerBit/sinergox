"""Tests for the Sinergox Client class."""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from sinergox import Client


class TestClientInit:
    """Tests for Client initialization."""

    def test_init_with_base_url(self):
        """Test client initialization with base URL."""
        client = Client(base_url="https://api.example.com")
        assert client.base_url == "https://api.example.com"
        assert client.api_key is None
        assert client.timeout == 30.0

    def test_init_with_trailing_slash(self):
        """Test that trailing slash is stripped from base URL."""
        client = Client(base_url="https://api.example.com/")
        assert client.base_url == "https://api.example.com"

    def test_init_with_api_key(self):
        """Test client initialization with API key."""
        client = Client(base_url="https://api.example.com", api_key="test-key")
        assert client.api_key == "test-key"

    def test_init_with_custom_timeout(self):
        """Test client initialization with custom timeout."""
        client = Client(base_url="https://api.example.com", timeout=60.0)
        assert client.timeout == 60.0


class TestClientHeaders:
    """Tests for Client header generation."""

    def test_headers_without_api_key(self):
        """Test headers without API key."""
        client = Client(base_url="https://api.example.com")
        headers = client._get_headers()
        assert headers["Content-Type"] == "application/json"
        assert headers["Accept"] == "application/json"
        assert "Authorization" not in headers

    def test_headers_with_api_key(self):
        """Test headers with API key."""
        client = Client(base_url="https://api.example.com", api_key="test-key")
        headers = client._get_headers()
        assert headers["Authorization"] == "Bearer test-key"


class TestClientAsyncContextManager:
    """Tests for Client async context manager."""

    @pytest.mark.asyncio
    async def test_context_manager_entry_exit(self):
        """Test async context manager properly opens and closes connection."""
        with patch.object(httpx.AsyncClient, "aclose", new_callable=AsyncMock) as mock_close:
            async with Client(base_url="https://api.example.com") as client:
                assert client._client is not None
            mock_close.assert_called_once()


class TestClientRequests:
    """Tests for Client HTTP methods."""

    @pytest.mark.asyncio
    async def test_get_request(self):
        """Test GET request."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": "test"}
        mock_response.raise_for_status = MagicMock()

        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            async with Client(base_url="https://api.example.com") as client:
                result = await client.get("/test")
                assert result == {"data": "test"}
                mock_get.assert_called_once_with("/test", params=None)

    @pytest.mark.asyncio
    async def test_get_request_with_params(self):
        """Test GET request with query parameters."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": "test"}
        mock_response.raise_for_status = MagicMock()

        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            async with Client(base_url="https://api.example.com") as client:
                await client.get("/test", params={"page": 1})
                mock_get.assert_called_once_with("/test", params={"page": 1})

    @pytest.mark.asyncio
    async def test_post_request(self):
        """Test POST request."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"id": 1}
        mock_response.raise_for_status = MagicMock()

        with patch.object(httpx.AsyncClient, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            async with Client(base_url="https://api.example.com") as client:
                result = await client.post("/test", data={"name": "test"})
                assert result == {"id": 1}
                mock_post.assert_called_once_with("/test", json={"name": "test"})

    @pytest.mark.asyncio
    async def test_put_request(self):
        """Test PUT request."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"updated": True}
        mock_response.raise_for_status = MagicMock()

        with patch.object(httpx.AsyncClient, "put", new_callable=AsyncMock) as mock_put:
            mock_put.return_value = mock_response
            async with Client(base_url="https://api.example.com") as client:
                result = await client.put("/test/1", data={"name": "updated"})
                assert result == {"updated": True}
                mock_put.assert_called_once_with("/test/1", json={"name": "updated"})

    @pytest.mark.asyncio
    async def test_delete_request(self):
        """Test DELETE request."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"deleted": True}
        mock_response.raise_for_status = MagicMock()

        with patch.object(httpx.AsyncClient, "delete", new_callable=AsyncMock) as mock_delete:
            mock_delete.return_value = mock_response
            async with Client(base_url="https://api.example.com") as client:
                result = await client.delete("/test/1")
                assert result == {"deleted": True}
                mock_delete.assert_called_once_with("/test/1")


class TestClientClose:
    """Tests for Client close method."""

    @pytest.mark.asyncio
    async def test_close_when_client_exists(self):
        """Test closing when client exists."""
        client = Client(base_url="https://api.example.com")
        await client._ensure_client()
        assert client._client is not None

        with patch.object(client._client, "aclose", new_callable=AsyncMock) as mock_close:
            await client.close()
            mock_close.assert_called_once()
        assert client._client is None

    @pytest.mark.asyncio
    async def test_close_when_client_is_none(self):
        """Test closing when client is None."""
        client = Client(base_url="https://api.example.com")
        assert client._client is None
        await client.close()  # Should not raise
        assert client._client is None
