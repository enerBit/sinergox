import asyncio

from sinergox import Client


async def main() -> None:
    async with Client() as client:
        # Descarga los últimos siete días para la primera métrica que coincida
        # con la consulta, asumiendo la zona horaria de Bogotá.
        datos = await client.get_data_for(
            "volumen util en energia por embalse",
        )
        print(datos.head())


if __name__ == "__main__":
    asyncio.run(main())
