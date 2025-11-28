import asyncio
import datetime as dt

import pytest

from sinergox import Client, Periodo


@pytest.fixture
def client() -> Client:
    instance = Client()
    try:
        yield instance
    finally:
        asyncio.run(instance.aclose())


def test_build_ranges_splits_large_intervals(client: Client) -> None:
    start = dt.datetime(2025, 1, 1)
    end = dt.datetime(2025, 2, 15)

    ranges = client._build_ranges(start, end, Periodo.HORARIO)

    assert ranges == [
        (dt.datetime(2025, 1, 1), dt.datetime(2025, 1, 31)),
        (dt.datetime(2025, 1, 31), dt.datetime(2025, 2, 15)),
    ]


def test_build_ranges_single_chunk_when_within_delta(client: Client) -> None:
    start = dt.datetime(2025, 1, 1)
    end = dt.datetime(2025, 1, 5)

    ranges = client._build_ranges(start, end, Periodo.HORARIO)

    assert ranges == [(start, end)]


def test_build_ranges_handles_equal_start_end(client: Client) -> None:
    start = dt.datetime(2025, 1, 1)
    end = start

    ranges = client._build_ranges(start, end, Periodo.HORARIO)

    assert ranges == [(start, end)]
