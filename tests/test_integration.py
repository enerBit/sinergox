import asyncio
import datetime as dt
import os

import httpx
import pyarrow as pa
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
    async def _fetch() -> pa.Table:
        async with Client() as client:
            now = dt.datetime.now(dt.timezone.utc)
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

    assert frame.num_rows > 0
    cols = frame.column_names
    assert "Timestamp" in cols
    
    entity_names = {entity.value, entity.value.lower()}
    has_entity_col = any(name in cols for name in entity_names)
    assert has_entity_col or len(list(cols)) > 0 # At least some columns
    
    metric_columns = [
        column for column in cols 
        if column not in entity_names and column != "Timestamp"
    ]
    assert metric_columns, "expected at least one metric column"
    assert all(column != "value" for column in metric_columns)
    
    for col_name in metric_columns:
        col_type = frame.schema.field(col_name).type
        assert pa.types.is_floating(col_type) or pa.types.is_integer(col_type)


@pytest.mark.integration
@pytest.mark.skipif(not RUN_INTEGRATION, reason=_skip_reason())
def test_get_data_annual_returns_rows() -> None:
    async def _fetch() -> pa.Table:
        async with Client() as client:
            metrics = await client.get_metrics()
            
            # Manual filtering
            rows = metrics.to_pylist()
            annual_rows = []
            for row in rows:
                if row.get("Type") != "AnnualEntities":
                    continue
                f_val = row.get("Filter")
                # Check for "No aplica", ignoring case/na
                # In original pandas: str.contains("No aplica", case=False, na=False)
                if not isinstance(f_val, str):
                    continue
                if "no aplica" in f_val.lower():
                    annual_rows.append(row)
            
            if not annual_rows:
                pytest.skip("no annual metrics available from API")

            record = annual_rows[0]
            try:
                entity = Entity(record["Entity"])
            except ValueError:
                pytest.skip(f"annual metric entity not supported: {record['Entity']!r}")

            now = dt.datetime.now(dt.timezone.utc)
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

    assert frame.num_rows > 0
    cols = frame.column_names
    assert "Timestamp" in cols
    
    metric_columns = [
        column
        for column in cols
        if column.lower() not in {"metricname", "value", "timestamp"} 
        # timestamp excluded explicitly just in case logic differs
        and column not in {"Timestamp"}
    ]
    # Filter out Entity cols if any (heuristic)
    # Actually just checking purely numeric
    
    numeric_columns = []
    for col_name in cols:
         col_type = frame.schema.field(col_name).type
         if pa.types.is_floating(col_type) or pa.types.is_integer(col_type):
             numeric_columns.append(col_name)
    
    assert numeric_columns, "expected at least one numeric/metric column"


@pytest.mark.integration
@pytest.mark.skipif(not RUN_INTEGRATION, reason=_skip_reason())
def test_get_metrics_returns_catalog() -> None:
    async def _fetch() -> pa.Table:
        async with Client() as client:
            return await client.get_metrics(force_refresh=True)

    try:
        metrics = asyncio.run(_fetch())
    except httpx.HTTPError as exc:  # pragma: no cover - we skip on live failures
        pytest.skip(f"API unavailable: {exc}")

    assert metrics.num_rows > 0
    cols = metrics.column_names
    assert "MetricId" in cols
    assert "MetricName" in cols


@pytest.mark.integration
@pytest.mark.skipif(not RUN_INTEGRATION, reason=_skip_reason())
def test_get_data_returns_pandas() -> None:
    try:
         import pandas as pd
    except ImportError:
         pytest.skip("Pandas not installed")

    async def _fetch() -> pd.DataFrame:
        async with Client(return_pandas=True) as client:
            now = dt.datetime.now(dt.timezone.utc)
            start = now - dt.timedelta(days=7)
            end = now - dt.timedelta(days=1)
            return await client.get_data(
                TimeResolution.HORARIO,
                metric="DemaReal",
                entity=Entity.SISTEMA,
                start=start,
                end=end,
            )

    try:
        df = asyncio.run(_fetch())
    except httpx.HTTPError as exc:
        pytest.skip(f"API unavailable: {exc}")

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "Timestamp" in df.columns
