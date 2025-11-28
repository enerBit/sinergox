"""
Sinergox Client implementation.

This module provides an async Client class for interacting with Sinergox services.
"""

from typing import Any

import httpx


class Client:
    """
    Async client for Sinergox services.

    This client provides async methods for interacting with Sinergox APIs.
    All methods are async and should be awaited when called.

    Example:
        ```python
        import asyncio
        from sinergox import Client

        async def main():
            client = Client(base_url="https://api.example.com")
            response = await client.get("/endpoint")
            print(response)
            await client.close()

        asyncio.run(main())
        ```

    The client can also be used as an async context manager:
        ```python
        async with Client(base_url="https://api.example.com") as client:
            response = await client.get("/endpoint")
            print(response)
        ```
    """

    def __init__(
        self,
        base_url: str = "",
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
    ) -> None:
        """
        Initialize the Sinergox client.

        Args:
            base_url: The base URL for API requests.
            timeout: Request timeout in seconds. Defaults to 30.0.
            headers: Optional default headers to include in all requests.
        """
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._default_headers = headers or {}
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the underlying HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=self._timeout,
                headers=self._default_headers,
            )
        return self._client

    async def close(self) -> None:
        """
        Close the client and release resources.

        This method should be called when the client is no longer needed.
        Alternatively, use the client as an async context manager.
        """
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "Client":
        """Enter the async context manager."""
        await self._get_client()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit the async context manager."""
        await self.close()

    async def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Perform an async GET request.

        Args:
            path: The API endpoint path.
            params: Optional query parameters.
            headers: Optional additional headers for this request.

        Returns:
            The JSON response as a dictionary.

        Raises:
            httpx.HTTPStatusError: If the response has an error status code.
        """
        client = await self._get_client()
        response = await client.get(path, params=params, headers=headers)
        response.raise_for_status()
        return response.json()

    async def post(
        self,
        path: str,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Perform an async POST request.

        Args:
            path: The API endpoint path.
            data: Optional form data to send.
            json: Optional JSON data to send.
            headers: Optional additional headers for this request.

        Returns:
            The JSON response as a dictionary.

        Raises:
            httpx.HTTPStatusError: If the response has an error status code.
        """
        client = await self._get_client()
        response = await client.post(path, data=data, json=json, headers=headers)
        response.raise_for_status()
        return response.json()

    async def put(
        self,
        path: str,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Perform an async PUT request.

        Args:
            path: The API endpoint path.
            data: Optional form data to send.
            json: Optional JSON data to send.
            headers: Optional additional headers for this request.

        Returns:
            The JSON response as a dictionary.

        Raises:
            httpx.HTTPStatusError: If the response has an error status code.
        """
        client = await self._get_client()
        response = await client.put(path, data=data, json=json, headers=headers)
        response.raise_for_status()
        return response.json()

    async def delete(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Perform an async DELETE request.

        Args:
            path: The API endpoint path.
            params: Optional query parameters.
            headers: Optional additional headers for this request.

        Returns:
            The JSON response as a dictionary.

        Raises:
            httpx.HTTPStatusError: If the response has an error status code.
        """
        client = await self._get_client()
        response = await client.delete(path, params=params, headers=headers)
        response.raise_for_status()
        return response.json()

    async def patch(
        self,
        path: str,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Perform an async PATCH request.

        Args:
            path: The API endpoint path.
            data: Optional form data to send.
            json: Optional JSON data to send.
            headers: Optional additional headers for this request.

        Returns:
            The JSON response as a dictionary.

        Raises:
            httpx.HTTPStatusError: If the response has an error status code.
        """
        client = await self._get_client()
        response = await client.patch(path, data=data, json=json, headers=headers)
        response.raise_for_status()
        return response.json()
