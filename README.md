
# Sinergox API de XM

## Description

Library to easily access Sinergox API data (XM) as tidy data.

## Installation

You can install the development version of Sinergox from GitHub with:

```sh
uv add sinergox
```

## Usage

```python
import sinergox

# Initialize the client
client = sinergox.Client()

metrtic = "generacion"
by = "recurso"
time_granularity = "horario"

# Fetch data
data = await client.get_data(
    Periodo.HORARIO,
    metric="VoluUtilDiarMasa",
    entity=EntityTypes.RECURSO,
    start=dt.datetime(2025, 11, 1),
    end=dt.datetime(2025, 11, 3),
)
print(data.head())
```
