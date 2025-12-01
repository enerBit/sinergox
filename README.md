# Sinergox API de XM

## Descripción

Sinergox es un cliente asíncrono para consultar la API pública de XM (<https://sinergox.xm.com.co/Paginas/Home.aspx>) y obtener
los datos en formato *tidy* listo para análisis con pandas.
El paquete proporciona utilidades para descubrir métricas disponibles, descargar series de
tiempo en diferentes resoluciones (horaria, diaria, mensual y anual) y trabajar
con un catálogo de entidades predefinidas.

## Requisitos

- Python 3.10 o superior.
- Dependencias gestionadas con `uv` (recomendado) o `pip`.
- Conexión a Internet hacia `https://servapibi.xm.com.co`.
- Los ejemplos usan la zona horaria oficial de Colombia (`America/Bogota`) a
    través del módulo estándar `zoneinfo`.

## Instalación

Instala la versión publicada en GitHub agregando el paquete a tu entorno:

```sh
uv add sinergox
```

Si prefieres `pip`:

```sh
pip install sinergox
```

## Ejemplo rápido

```python
import asyncio
from sinergox import Client


async def main() -> None:
    async with Client() as client:
        # Descarga los últimos siete días para la primera métrica que coincida
        # con la consulta, asumiendo la zona horaria de Bogotá.
        datos = await client.get_data_for(
            "volumen util",
        )
        print(datos.head())


if __name__ == "__main__":
    asyncio.run(main())
```

Todas las funciones públicas del `Client` son asíncronas; ejecútalas dentro de
una corrutina `async` y usa `await`, o encapsula la llamada con `asyncio.run`.

## Descubrimiento de métricas

La API incluye un catálogo con más de 150 métricas. Usa `client.find_metric`
para obtener coincidencias directas y homogéneas, o `client.search_metrics`
datos = await client.get_data_for(
    "VoluUtil",
    timezone=zona,
    start=dt.datetime(2025, 11, 1, tzinfo=zona),
    end=dt.datetime(2025, 11, 3, tzinfo=zona),
)

```

La búsqueda normaliza acentos y diacríticos, y combina coincidencias exactas
con similitud difusa.

## Obtención de datos

`client.get_data` descarga la serie en la granularidad indicada por `TimeResolution`
y devuelve un `DataFrame` indexado por `MetricName` y `Timestamp`. El cliente
divide automáticamente los rangos extensos en ventanas compatibles con la API
para evitar errores por límite de días. Si no indicas la zona horaria, las fechas
y horas se asumen en `America/Bogota` antes de convertirlas a UTC. Puedes controlar
el paralelismo con el parámetro `concurrency`.

```python
from zoneinfo import ZoneInfo

zona = ZoneInfo("America/Bogota")
datos_horarios = await client.get_data(
    TimeResolution.HORARIO,
    metric="DemaReal",
    entity=Entity.SISTEMA,
    start=dt.datetime.now(zona) - dt.timedelta(days=7),
    end=dt.datetime.now(zona) - dt.timedelta(days=1),
)
```

### Descarga directa por consulta

Para obtener datos sin revisar manualmente el catálogo, usa
`client.get_data_for`. La función toma la primera coincidencia de
`find_metric`, convierte automáticamente la columna `Entity` al `Entity`
correspondiente y, si no indicas un rango temporal, descarga los últimos
siete días por defecto.

```python
import datetime as dt
from zoneinfo import ZoneInfo

from sinergox import Entity

zona = ZoneInfo("America/Bogota")
datos = await client.get_data_for(
    "VoluUtil",
    entity=Entity.EMBALSE,
    timezone=zona,
    start=dt.datetime(2025, 11, 1, tzinfo=zona),
    end=dt.datetime(2025, 11, 3, tzinfo=zona),
)
```

Puedes ajustar la zona horaria, definir explícitamente `start` y `end`, fijar
un `TimeResolution` específico o restringir la búsqueda a una entidad concreta
mediante los argumentos opcionales.

## Caché del catálogo de métricas

El catálogo se mantiene en memoria y se refresca automáticamente cada hora. Si
necesitas un TTL distinto (o desactivar la expiración) configura el parámetro
`metrics_cache_ttl` al crear el cliente:

```python
client = Client(metrics_cache_ttl=dt.timedelta(hours=6))
```

Para forzar una actualización manual usa `await client.get_metrics(force_refresh=True)`.

## Pruebas automatizadas

Ejecuta la suite unitaria con:

```sh
uv run --group dev pytest
```

Las pruebas de integración dependen del servicio en vivo. Habilítalas
exportando la variable de entorno `RUN_INTEGRATION_TESTS`:

```pwsh
$Env:RUN_INTEGRATION_TESTS = "1"
uv run --group dev pytest -m integration -rs
```

Ten en cuenta que las pruebas omiten cualquier fallo externo (HTTP 4xx/5xx)
mediante `pytest.skip` para no interrumpir tu flujo de trabajo.
