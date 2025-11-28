# Sinergox

An async Python client library for Sinergox services.

## Installation

```bash
pip install sinergox
```

## Quick Start

The library provides an async `Client` class for interacting with APIs. All methods are async and should be awaited.

### Basic Usage

```python
import asyncio
from sinergox import Client

async def main():
    # Create a client instance
    client = Client(base_url="https://api.example.com")
    
    # Perform async requests
    response = await client.get("/users")
    print(response)
    
    # Don't forget to close the client when done
    await client.close()

asyncio.run(main())
```

### Using as Context Manager

The recommended way to use the client is as an async context manager, which automatically handles resource cleanup:

```python
import asyncio
from sinergox import Client

async def main():
    async with Client(base_url="https://api.example.com") as client:
        # GET request
        users = await client.get("/users")
        
        # POST request with JSON data
        new_user = await client.post("/users", json={"name": "John", "email": "john@example.com"})
        
        # PUT request
        updated = await client.put("/users/1", json={"name": "John Doe"})
        
        # DELETE request
        await client.delete("/users/1")

asyncio.run(main())
```

### Configuration

The `Client` class accepts the following parameters:

- `base_url`: The base URL for API requests
- `timeout`: Request timeout in seconds (default: 30.0)
- `headers`: Default headers to include in all requests

```python
client = Client(
    base_url="https://api.example.com",
    timeout=60.0,
    headers={"Authorization": "Bearer your-token"}
)
```

### Available Methods

All methods are async and return the parsed JSON response:

- `await client.get(path, params=None, headers=None)` - GET request
- `await client.post(path, data=None, json=None, headers=None)` - POST request
- `await client.put(path, data=None, json=None, headers=None)` - PUT request
- `await client.patch(path, data=None, json=None, headers=None)` - PATCH request
- `await client.delete(path, params=None, headers=None)` - DELETE request
- `await client.close()` - Close the client and release resources

### Error Handling

The client raises `httpx.HTTPStatusError` for HTTP error responses:

```python
import httpx
from sinergox import Client

async def main():
    async with Client(base_url="https://api.example.com") as client:
        try:
            response = await client.get("/nonexistent")
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code}")
```

## Development

Install development dependencies:

```bash
pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```

## License

MIT License - see [LICENSE](LICENSE) for details.
