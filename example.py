import asyncio
import datetime as dt

from sinergox import Client, EntityTypes, Periodo


async def main() -> None:
    async with Client() as client:
        # Optional helper to discover metric identifiers
        matches = await client.find_metric("VoluUtil")
        print(matches.head())

        data = await client.get_data(
            Periodo.DIARIO,
            metric="VoluUtilDiarMasa",
            entity=EntityTypes.EMBALSE,
            start=dt.datetime(2025, 11, 1),
            end=dt.datetime(2025, 11, 3),
        )
        print(data.head())


if __name__ == "__main__":
    print("Running sinergox example...")
    asyncio.run(main())
