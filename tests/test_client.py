import asyncio
import datetime as dt
from typing import Any, Sequence

import pyarrow as pa
import pytest

from sinergox import Client, Entity, TimeResolution
from sinergox.client import DataPath


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

    ranges = client._build_ranges(start, end, TimeResolution.HORARIO)

    assert ranges == [
        (dt.datetime(2025, 1, 1), dt.datetime(2025, 1, 31)),
        (dt.datetime(2025, 1, 31), dt.datetime(2025, 2, 15)),
    ]


def test_build_ranges_single_chunk_when_within_delta(client: Client) -> None:
    start = dt.datetime(2025, 1, 1)
    end = dt.datetime(2025, 1, 5)

    ranges = client._build_ranges(start, end, TimeResolution.HORARIO)

    assert ranges == [(start, end)]


def test_build_ranges_handles_equal_start_end(client: Client) -> None:
    start = dt.datetime(2025, 1, 1)
    end = start

    ranges = client._build_ranges(start, end, TimeResolution.HORARIO)

    assert ranges == [(start, end)]


def test_ensure_datetime_assumes_bogota_timezone() -> None:
    naive = dt.datetime(2025, 1, 1, 0, 0)

    converted = Client._ensure_datetime(naive)
    assert converted == dt.datetime(2025, 1, 1, 5, 0)

    date_converted = Client._ensure_datetime(dt.date(2025, 1, 1))
    assert date_converted == dt.datetime(2025, 1, 1, 5, 0)


def test_to_tidy_hourly_produces_expected_columns() -> None:
    content = {
        "Metric": {"Name": "Sample Metric"},
        "Items": [
            {
                "Date": "2025-01-01",
                "HourlyEntities": [
                    {
                        "Id": "row-1",
                        "Values-code": "RecursoA",
                        "Values-name": "Recurso Uno",
                        "Values-value01": 10,
                        "Values-value02": 20,
                    }
                ],
            }
        ],
    }

    result = Client._to_tidy(content, DataPath.HORARIO, Entity.RECURSO)

    cols = result.column_names
    assert "Timestamp" in cols
    assert "Sample Metric" in cols
    assert Entity.RECURSO.value in cols
    assert Entity.RECURSO.value.lower() in cols

    rows = result.to_pylist()
    # Sort by timestamp to ensure order
    rows.sort(key=lambda x: x["Timestamp"])
    
    assert len(rows) == 2
    assert rows[0]["Timestamp"] == dt.datetime(2025, 1, 1, 0, 0)
    assert rows[1]["Timestamp"] == dt.datetime(2025, 1, 1, 1, 0)
    assert rows[0][Entity.RECURSO.value] == "RecursoA"
    assert rows[0][Entity.RECURSO.value.lower()] == "Recurso Uno"
    assert rows[0]["Sample Metric"] == 10.0
    assert rows[1]["Sample Metric"] == 20.0


def test_to_tidy_daily_produces_timestamp_without_hours() -> None:
    content = {
        "Metric": {"Name": "Sample Metric"},
        "Items": [
            {
                "Date": "2025-01-01",
                "DailyEntities": [
                    {
                        "Id": "row-1",
                        "Values-code": "RecursoA",
                        "Values-name": "Recurso Uno",
                        "Values-value": 5,
                    }
                ],
            }
        ],
    }

    result = Client._to_tidy(content, DataPath.DIARIO, Entity.RECURSO)

    cols = result.column_names
    assert "Timestamp" in cols
    assert "Sample Metric" in cols

    rows = result.to_pylist()
    assert len(rows) == 1
    assert rows[0]["Timestamp"] == dt.datetime(2025, 1, 1)
    assert rows[0]["Sample Metric"] == 5.0
    assert rows[0][Entity.RECURSO.value.lower()] == "Recurso Uno"


def test_find_metric_prefers_exact_matches(client: Client) -> None:
    sample = pa.Table.from_pylist(
        [
            {
                "MetricId": "VoluUtilDiarMasa",
                "MetricName": "Volumen Útil diario por Embalse",
                "MetricDescription": "Volumen util del embalse en masa",
            },
            {
                "MetricId": "EnerGenTotal",
                "MetricName": "Energía generada total neta",
                "MetricDescription": "Energia generada neta",
            },
        ]
    )
    client._metrics_cache = sample

    results = asyncio.run(client.find_metric("VoluUtilDiarMasa"))

    assert results.num_rows > 0
    first_row = results.to_pylist()[0]
    assert first_row["MetricId"] == "VoluUtilDiarMasa"
    assert "match_tier" not in results.column_names


def test_find_metric_handles_accents_and_tokens(client: Client) -> None:
    sample = pa.Table.from_pylist(
        [
            {
                "MetricId": "VoluUtilDiarMasa",
                "MetricName": "Volumen Útil diario por Embalse",
                "MetricDescription": "Volumen util del embalse en masa",
            },
            {
                "MetricId": "CostoMarginalSist",
                "MetricName": "Costo marginal del sistema",
                "MetricDescription": "Costo marginal spot",
            },
        ]
    )
    client._metrics_cache = sample

    results = asyncio.run(client.find_metric("volumen util embalse", limit=1))

    assert results.num_rows == 1
    first_row = results.to_pylist()[0]
    assert first_row["MetricId"] == "VoluUtilDiarMasa"
    assert "match_tier" not in results.column_names


def test_search_metrics_includes_scoring(client: Client) -> None:
    sample = pa.Table.from_pylist(
        [
            {
                "MetricId": "VoluUtilDiarMasa",
                "MetricName": "Volumen Útil diario por Embalse",
                "MetricDescription": "Volumen util del embalse en masa",
            },
            {
                "MetricId": "EnerGenTotal",
                "MetricName": "Energía generada total neta",
                "MetricDescription": "Energia generada neta",
            },
        ]
    )
    client._metrics_cache = sample

    results = asyncio.run(client.search_metrics("VoluUtilDiarMasa"))

    assert results.num_rows > 0
    cols = results.column_names
    assert "match_tier" in cols
    assert "levenshtein" in cols
    assert "token_overlap" in cols
    
    first_row = results.to_pylist()[0]
    assert first_row["match_tier"] == 0


def test_search_metrics_token_overlap_scores(client: Client) -> None:
    sample = pa.Table.from_pylist(
        [
            {
                "MetricId": "VoluUtilDiarMasa",
                "MetricName": "Volumen Útil diario por Embalse",
                "MetricDescription": "Volumen util del embalse en masa",
            },
            {
                "MetricId": "CostoMarginalSist",
                "MetricName": "Costo marginal del sistema",
                "MetricDescription": "Costo marginal spot",
            },
        ]
    )
    client._metrics_cache = sample

    results = asyncio.run(client.search_metrics("volumen util embalse", limit=1))

    assert results.num_rows == 1
    first_row = results.to_pylist()[0]
    assert first_row["MetricId"] == "VoluUtilDiarMasa"
    # match_tier <= 3 (exact=0, starts=1, substr=2, overlap=3)
    assert first_row["match_tier"] <= 3
    assert first_row["token_overlap"] > 0


def test_search_metrics_prefers_resolution_aliases(client: Client) -> None:
    sample = pa.Table.from_pylist(
        [
            {
                "MetricId": "GenerRecHoraria",
                "MetricName": "Generación por recurso",
                "MetricDescription": "Generación agregada por recurso",
                "Entity": "Recurso",
                "Type": "HourlyEntities",
            },
            {
                "MetricId": "GenerRecDiaria",
                "MetricName": "Generación por recurso",
                "MetricDescription": "Total diario por recurso",
                "Entity": "Recurso",
                "Type": "DailyEntities",
            },
        ]
    )
    client._metrics_cache = sample

    results = asyncio.run(client.search_metrics("generacion horaria por recurso"))

    assert results.num_rows > 0
    assert results.to_pylist()[0]["MetricId"] == "GenerRecHoraria"


def test_get_data_for_uses_first_match(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    sample = pa.Table.from_pylist(
        [
            {
                "MetricId": "VoluUtilDiarMasa",
                "MetricName": "Volumen Útil diario por Embalse",
                "MetricDescription": "Volumen util del embalse en masa",
                "Entity": "Embalse",
                "Type": "DailyEntities",
            },
            {
                "MetricId": "EnerGenTotal",
                "MetricName": "Energía generada total neta",
                "MetricDescription": "Energia generada neta",
                "Entity": "Sistema",
                "Type": "HourlyEntities",
            },
        ]
    )
    client._metrics_cache = sample

    captured: dict[str, Any] = {}

    async def fake_get_data(
        self: Client,
        period: TimeResolution,
        *,
        metric: str,
        entity: Entity,
        start: dt.datetime,
        end: dt.datetime,
        filter: Sequence[Any] | None = None,
        concurrency: int | None = None,
    ) -> pa.Table:
        captured.update(
            {
                "period": period,
                "metric": metric,
                "entity": entity,
                "start": start,
                "end": end,
            }
        )
        return pa.Table.from_pydict({"value": [1]})

    monkeypatch.setattr(Client, "get_data", fake_get_data, raising=False)

    result = asyncio.run(client.get_data_for("volumen util", timezone=dt.timezone.utc))

    assert result.num_rows > 0
    assert captured["metric"] == "VoluUtilDiarMasa"
    assert captured["entity"] == Entity.EMBALSE
    assert captured["period"] == TimeResolution.DIARIO
    assert captured["end"] - captured["start"] == dt.timedelta(days=7)


def test_get_data_for_explicit_start_defaults_end(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    sample = pa.Table.from_pylist(
        [
            {
                "MetricId": "VoluUtilDiarMasa",
                "MetricName": "Volumen Útil diario por Embalse",
                "MetricDescription": "Volumen util del embalse en masa",
                "Entity": "Embalse",
                "Type": "DailyEntities",
            }
        ]
    )
    client._metrics_cache = sample

    captured: dict[str, dt.datetime] = {}

    async def fake_get_data(
        self: Client,
        period: TimeResolution,
        *,
        metric: str,
        entity: Entity,
        start: dt.datetime,
        end: dt.datetime,
        filter: Sequence[Any] | None = None,
        concurrency: int | None = None,
    ) -> pa.Table:
        captured.update({"start": start, "end": end})
        return pa.Table.from_pydict({"dummy": [1]})

    monkeypatch.setattr(Client, "get_data", fake_get_data, raising=False)

    start_input = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    asyncio.run(
        client.get_data_for(
            "volumen util",
            timezone=dt.timezone.utc,
            start=start_input,
        )
    )

    assert captured["start"] == dt.datetime(2025, 1, 1)
    assert captured["end"] - captured["start"] == dt.timedelta(days=7)


def test_get_data_for_explicit_end_defaults_start(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    sample = pa.Table.from_pylist(
        [
            {
                "MetricId": "VoluUtilDiarMasa",
                "MetricName": "Volumen Útil diario por Embalse",
                "MetricDescription": "Volumen util del embalse en masa",
                "Entity": "Embalse",
                "Type": "DailyEntities",
            }
        ]
    )
    client._metrics_cache = sample

    captured: dict[str, dt.datetime] = {}

    async def fake_get_data(
        self: Client,
        period: TimeResolution,
        *,
        metric: str,
        entity: Entity,
        start: dt.datetime,
        end: dt.datetime,
        filter: Sequence[Any] | None = None,
        concurrency: int | None = None,
    ) -> pa.Table:
        captured.update({"start": start, "end": end})
        return pa.Table.from_pydict({"dummy": [1]})

    monkeypatch.setattr(Client, "get_data", fake_get_data, raising=False)

    end_input = dt.datetime(2025, 1, 15, tzinfo=dt.timezone.utc)
    asyncio.run(
        client.get_data_for(
            "volumen util",
            timezone=dt.timezone.utc,
            end=end_input,
        )
    )

    assert captured["end"] == dt.datetime(2025, 1, 15)
    assert captured["end"] - captured["start"] == dt.timedelta(days=7)


def test_get_data_for_respects_entity_filter(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    matches = pa.Table.from_pylist(
        [
            {
                "MetricId": "VoluUtilDiarMasa",
                "MetricName": "Volumen Útil diario por Embalse",
                "Entity": "Embalse",
                "Type": "DailyEntities",
            },
            {
                "MetricId": "GeneracionRecursoHor",
                "MetricName": "Generación horaria por recurso",
                "Entity": "Recurso",
                "Type": "HourlyEntities",
            },
        ]
    )

    async def fake_find_metric(
        self: Client,
        query: str,
        *,
        levenshtein_threshold: int = 3,
        limit: int | None = None,
    ) -> pa.Table:
        # Simplified slicing
        return matches.slice(0, limit or matches.num_rows)

    captured: dict[str, Any] = {}

    async def fake_get_data(
        self: Client,
        period: TimeResolution,
        *,
        metric: str,
        entity: Entity,
        start: dt.datetime,
        end: dt.datetime,
        filter: Sequence[Any] | None = None,
        concurrency: int | None = None,
    ) -> pa.Table:
        captured.update({"metric": metric, "entity": entity})
        return pa.Table.from_pydict({"dummy": [1]})

    monkeypatch.setattr(Client, "find_metric", fake_find_metric, raising=False)
    monkeypatch.setattr(Client, "get_data", fake_get_data, raising=False)

    asyncio.run(
        client.get_data_for(
            "generacion horaria",
            entity=Entity.RECURSO,
            timezone=dt.timezone.utc,
        )
    )

    assert captured["metric"] == "GeneracionRecursoHor"
    assert captured["entity"] == Entity.RECURSO


def test_get_data_for_entity_filter_raises_when_missing(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    matches = pa.Table.from_pylist(
        [
            {
                "MetricId": "VoluUtilDiarMasa",
                "MetricName": "Volumen Útil diario por Embalse",
                "Entity": "Embalse",
                "Type": "DailyEntities",
            }
        ]
    )

    async def fake_find_metric(
        self: Client,
        query: str,
        *,
        levenshtein_threshold: int = 3,
        limit: int | None = None,
    ) -> pa.Table:
        return matches.slice(0, limit or matches.num_rows)

    monkeypatch.setattr(Client, "find_metric", fake_find_metric, raising=False)

    with pytest.raises(ValueError):
        asyncio.run(
            client.get_data_for(
                "generacion",
                entity=Entity.RECURSO,
                timezone=dt.timezone.utc,
            )
        )


def test_get_data_for_raises_when_no_match(client: Client) -> None:
    client._metrics_cache = pa.Table.from_batches([], schema=pa.schema([]))

    with pytest.raises(ValueError):
        asyncio.run(client.get_data_for("no-existe"))
