"""Tests for the Sinergox Client class."""

import pytest
import httpx
from unittest.mock import AsyncMock, patch, MagicMock

from sinergox import Client


@pytest.fixture
def client():
    """Create a Client instance for testing."""
    return Client(base_url="https://api.example.com")


@pytest.mark.asyncio
async def test_client_initialization():
    """Test Client initialization with default and custom values."""
    # Default initialization
    client = Client()
    assert client._base_url == ""
    assert client._timeout == 30.0
    assert client._default_headers == {}
    
    # Custom initialization
    client = Client(
        base_url="https://api.example.com/",
        timeout=60.0,
        headers={"Authorization": "Bearer token"}
    )
    assert client._base_url == "https://api.example.com"  # trailing slash removed
    assert client._timeout == 60.0
    assert client._default_headers == {"Authorization": "Bearer token"}


@pytest.mark.asyncio
async def test_client_context_manager():
    """Test Client as async context manager."""
    async with Client(base_url="https://api.example.com") as client:
        assert client._client is not None
    
    # Client should be closed after exiting context
    assert client._client is None


@pytest.mark.asyncio
async def test_client_close():
    """Test Client close method."""
    client = Client(base_url="https://api.example.com")
    await client._get_client()  # Initialize the client
    assert client._client is not None
    
    await client.close()
    assert client._client is None
    
    # Calling close again should not raise
    await client.close()


@pytest.mark.asyncio
async def test_client_get():
    """Test Client GET request."""
    client = Client(base_url="https://api.example.com")
    
    mock_response = MagicMock()
    mock_response.json.return_value = {"id": 1, "name": "Test"}
    mock_response.raise_for_status = MagicMock()
    
    with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response
        
        result = await client.get("/users/1", params={"include": "profile"})
        
        mock_get.assert_called_once_with(
            "/users/1",
            params={"include": "profile"},
            headers=None
        )
        assert result == {"id": 1, "name": "Test"}
    
    await client.close()


@pytest.mark.asyncio
async def test_client_post():
    """Test Client POST request."""
    client = Client(base_url="https://api.example.com")
    
    mock_response = MagicMock()
    mock_response.json.return_value = {"id": 2, "name": "New User"}
    mock_response.raise_for_status = MagicMock()
    
    with patch.object(httpx.AsyncClient, "post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_response
        
        result = await client.post("/users", json={"name": "New User"})
        
        mock_post.assert_called_once_with(
            "/users",
            data=None,
            json={"name": "New User"},
            headers=None
        )
        assert result == {"id": 2, "name": "New User"}
    
    await client.close()


@pytest.mark.asyncio
async def test_client_put():
    """Test Client PUT request."""
    client = Client(base_url="https://api.example.com")
    
    mock_response = MagicMock()
    mock_response.json.return_value = {"id": 1, "name": "Updated User"}
    mock_response.raise_for_status = MagicMock()
    
    with patch.object(httpx.AsyncClient, "put", new_callable=AsyncMock) as mock_put:
        mock_put.return_value = mock_response
        
        result = await client.put("/users/1", json={"name": "Updated User"})
        
        mock_put.assert_called_once_with(
            "/users/1",
            data=None,
            json={"name": "Updated User"},
            headers=None
        )
        assert result == {"id": 1, "name": "Updated User"}
    
    await client.close()


@pytest.mark.asyncio
async def test_client_delete():
    """Test Client DELETE request."""
    client = Client(base_url="https://api.example.com")
    
    mock_response = MagicMock()
    mock_response.json.return_value = {"deleted": True}
    mock_response.raise_for_status = MagicMock()
    
    with patch.object(httpx.AsyncClient, "delete", new_callable=AsyncMock) as mock_delete:
        mock_delete.return_value = mock_response
        
        result = await client.delete("/users/1")
        
        mock_delete.assert_called_once_with(
            "/users/1",
            params=None,
            headers=None
        )
        assert result == {"deleted": True}
    
    await client.close()


@pytest.mark.asyncio
async def test_client_patch():
    """Test Client PATCH request."""
    client = Client(base_url="https://api.example.com")
    
    mock_response = MagicMock()
    mock_response.json.return_value = {"id": 1, "status": "active"}
    mock_response.raise_for_status = MagicMock()
    
    with patch.object(httpx.AsyncClient, "patch", new_callable=AsyncMock) as mock_patch:
        mock_patch.return_value = mock_response
        
        result = await client.patch("/users/1", json={"status": "active"})
        
        mock_patch.assert_called_once_with(
            "/users/1",
            data=None,
            json={"status": "active"},
            headers=None
        )
        assert result == {"id": 1, "status": "active"}
    
    await client.close()


@pytest.mark.asyncio
async def test_client_with_custom_headers():
    """Test Client requests with custom headers."""
    client = Client(
        base_url="https://api.example.com",
        headers={"Authorization": "Bearer token"}
    )
    
    mock_response = MagicMock()
    mock_response.json.return_value = {}
    mock_response.raise_for_status = MagicMock()
    
    with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response
        
        await client.get("/users", headers={"X-Custom": "value"})
        
        mock_get.assert_called_once_with(
            "/users",
            params=None,
            headers={"X-Custom": "value"}
        )
    
    await client.close()


@pytest.mark.asyncio
async def test_client_http_error():
    """Test Client handles HTTP errors properly."""
    client = Client(base_url="https://api.example.com")
    
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Not Found",
        request=MagicMock(),
        response=MagicMock(status_code=404)
    )
    
    with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response
        
        with pytest.raises(httpx.HTTPStatusError):
            await client.get("/nonexistent")
    
    await client.close()
