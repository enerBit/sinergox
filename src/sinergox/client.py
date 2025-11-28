"""Async client for Sinergox API."""

import logging
import types
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)


class Client:
    """Async client for interacting with the Sinergox API.

    This client provides async methods for making HTTP requests to the Sinergox API.
    It handles authentication, connection management, and provides a clean interface
    for API operations.

    Example:
        ```python
        import asyncio
        from sinergox import Client

        async def main():
            async with Client(base_url="https://api.sinergox.com") as client:
                data = await client.get("/endpoint")
                print(data)

        asyncio.run(main())
        ```
    """

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        """Initialize the Sinergox client.

        Args:
            base_url: The base URL for the Sinergox API.
            api_key: Optional API key for authentication.
            timeout: Request timeout in seconds. Defaults to 30.0.
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    def _get_headers(self) -> dict[str, str]:
        """Build request headers including authentication if configured."""
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def _ensure_client(self) -> httpx.AsyncClient:
        """Ensure the HTTP client is initialized."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers=self._get_headers(),
                timeout=self.timeout,
            )
        return self._client

    async def get(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        """Make an async GET request.

        Args:
            path: The API endpoint path.
            params: Optional query parameters.

        Returns:
            The JSON response data.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        client = await self._ensure_client()
        response = await client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    async def post(self, path: str, data: Optional[dict[str, Any]] = None) -> Any:
        """Make an async POST request.

        Args:
            path: The API endpoint path.
            data: Optional request body data.

        Returns:
            The JSON response data.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        client = await self._ensure_client()
        response = await client.post(path, json=data)
        response.raise_for_status()
        return response.json()

    async def put(self, path: str, data: Optional[dict[str, Any]] = None) -> Any:
        """Make an async PUT request.

        Args:
            path: The API endpoint path.
            data: Optional request body data.

        Returns:
            The JSON response data.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        client = await self._ensure_client()
        response = await client.put(path, json=data)
        response.raise_for_status()
        return response.json()

    async def delete(self, path: str) -> Any:
        """Make an async DELETE request.

        Args:
            path: The API endpoint path.

        Returns:
            The JSON response data.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        client = await self._ensure_client()
        response = await client.delete(path)
        response.raise_for_status()
        return response.json()

    async def close(self) -> None:
        """Close the HTTP client connection."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
            logger.debug("HTTP client closed")

    async def __aenter__(self) -> "Client":
        """Async context manager entry."""
        await self._ensure_client()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        """Async context manager exit - closes the connection automatically."""
        await self.close()
