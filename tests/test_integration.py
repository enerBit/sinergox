import asyncio
import datetime as dt
import os

import httpx
import pandas as pd
import pytest

from sinergox import Client, Entity, TimeResolution

_TRUTHY = {"1", "true", "yes", "on"}


def _env_flag(name: str, default: str = "0") -> bool:
    value = os.getenv(name, default)
    if value is None:
        return False
    return value.strip().lower() in _TRUTHY


RUN_INTEGRATION = _env_flag("RUN_INTEGRATION_TESTS")


def _skip_reason() -> str:
    return "set RUN_INTEGRATION_TESTS=1 to enable"


@pytest.mark.integration
@pytest.mark.skipif(not RUN_INTEGRATION, reason=_skip_reason())
@pytest.mark.parametrize(
    ("period", "metric_id", "entity", "start_offset", "end_offset"),
    [
        pytest.param(
            TimeResolution.DIARIO,
            "VoluUtilDiarMasa",
            Entity.EMBALSE,
            3,
            2,
            id="daily",
        ),
        pytest.param(
            TimeResolution.HORARIO,
            "DemaReal",
            Entity.SISTEMA,
            7,
            1,
            id="hourly",
        ),
        pytest.param(
            TimeResolution.MENSUAL,
            "FAER",
            Entity.SISTEMA,
            365,
            0,
            id="monthly",
        ),
    ],
)
def test_get_data_returns_rows(
    period: TimeResolution,
    metric_id: str,
    entity: Entity,
    start_offset: int,
    end_offset: int,
) -> None:
    async def _fetch() -> pd.DataFrame:
        async with Client() as client:
            now = dt.datetime.now(dt.UTC)
            start = now - dt.timedelta(days=start_offset)
            end = now - dt.timedelta(days=end_offset)
            return await client.get_data(
                period,
                metric=metric_id,
                entity=entity,
                start=start,
                end=end,
            )

    try:
        frame = asyncio.run(_fetch())
    except httpx.HTTPError as exc:  # pragma: no cover - skip when live API fails
        pytest.skip(f"API unavailable: {exc}")

    assert not frame.empty
    assert frame.index.names[-1] == "Timestamp"
    entity_names = {entity.value, entity.value.lower()}
    has_entity_index = any(name in frame.index.names for name in entity_names)
    assert has_entity_index or frame.index.names == ["Timestamp"]
    metric_columns = [
        column for column in frame.columns if column.lower() not in entity_names
    ]
    assert metric_columns, "expected at least one metric column"
    assert all(column != "value" for column in metric_columns)
    assert all(
        pd.api.types.is_numeric_dtype(frame[column]) for column in metric_columns
    )


@pytest.mark.integration
@pytest.mark.skipif(not RUN_INTEGRATION, reason=_skip_reason())
def test_get_data_annual_returns_rows() -> None:
    async def _fetch() -> pd.DataFrame:
        async with Client() as client:
            metrics = await client.get_metrics()
            annual = metrics[
                (metrics["Type"] == "AnnualEntities")
                & (metrics["Filter"].str.contains("No aplica", case=False, na=False))
            ]
            if annual.empty:
                pytest.skip("no annual metrics available from API")

            record = annual.iloc[0]
            try:
                entity = Entity(record["Entity"])
            except ValueError:
                pytest.skip(f"annual metric entity not supported: {record['Entity']!r}")

            now = dt.datetime.now(dt.UTC)
            start = now - dt.timedelta(days=5 * 366)

            return await client.get_data(
                TimeResolution.ANUAL,
                metric=record["MetricId"],
                entity=entity,
                start=start,
                end=now,
            )

    try:
        frame = asyncio.run(_fetch())
    except httpx.HTTPError as exc:  # pragma: no cover - skip when live API fails
        pytest.skip(f"API unavailable: {exc}")

    assert not frame.empty
    assert frame.index.names[-1] == "Timestamp"
    metric_columns = [
        column
        for column in frame.columns
        if column.lower() not in {"metricname", "value"}
    ]
    assert metric_columns, "expected at least one metric column"
    assert all(
        pd.api.types.is_numeric_dtype(frame[column]) for column in metric_columns
    )


@pytest.mark.integration
@pytest.mark.skipif(not RUN_INTEGRATION, reason=_skip_reason())
def test_get_metrics_returns_catalog() -> None:
    async def _fetch() -> pd.DataFrame:
        async with Client() as client:
            return await client.get_metrics(force_refresh=True)

    try:
        metrics = asyncio.run(_fetch())
    except httpx.HTTPError as exc:  # pragma: no cover - we skip on live failures
        pytest.skip(f"API unavailable: {exc}")

    assert not metrics.empty
    assert {"MetricId", "MetricName"}.issubset(metrics.columns)
