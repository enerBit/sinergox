# Sinergox

Async Python client library for the Sinergox API.

## Installation

```bash
pip install sinergox
```

## Quick Start

The library provides an async `Client` class for interacting with the Sinergox API. All methods are async and should be awaited.

### Basic Usage

```python
import asyncio
from sinergox import Client


async def main():
    # Create a client with async context manager
    async with Client(base_url="https://api.sinergox.com", api_key="your-api-key") as client:
        # Make async requests
        data = await client.get("/endpoint")
        print(data)


asyncio.run(main())
```

### Manual Connection Management

If you prefer to manage the connection lifecycle manually:

```python
import asyncio
from sinergox import Client


async def main():
    client = Client(base_url="https://api.sinergox.com", api_key="your-api-key")
    try:
        data = await client.get("/endpoint")
        print(data)
    finally:
        await client.close()


asyncio.run(main())
```

## API Reference

### Client

The main class for interacting with the Sinergox API.

#### Constructor

```python
Client(
    base_url: str,
    api_key: Optional[str] = None,
    timeout: float = 30.0,
)
```

**Parameters:**
- `base_url`: The base URL for the Sinergox API.
- `api_key`: Optional API key for authentication.
- `timeout`: Request timeout in seconds. Defaults to 30.0.

#### Methods

All methods are async and should be awaited:

##### `get(path, params=None)`
Make an async GET request.

```python
data = await client.get("/users", params={"page": 1})
```

##### `post(path, data=None)`
Make an async POST request.

```python
result = await client.post("/users", data={"name": "John"})
```

##### `put(path, data=None)`
Make an async PUT request.

```python
result = await client.put("/users/1", data={"name": "Jane"})
```

##### `delete(path)`
Make an async DELETE request.

```python
result = await client.delete("/users/1")
```

##### `close()`
Close the HTTP client connection.

```python
await client.close()
```

## Async Context Manager

The `Client` class supports the async context manager protocol, which ensures proper cleanup of resources:

```python
async with Client(base_url="https://api.sinergox.com") as client:
    # The connection is automatically managed
    data = await client.get("/endpoint")
# Connection is automatically closed when exiting the context
```

## Error Handling

The client raises `httpx.HTTPStatusError` for failed requests:

```python
import httpx
from sinergox import Client


async def main():
    async with Client(base_url="https://api.sinergox.com") as client:
        try:
            data = await client.get("/endpoint")
        except httpx.HTTPStatusError as e:
            print(f"Request failed: {e.response.status_code}")
```

## License

MIT License - see [LICENSE](LICENSE) for details.
