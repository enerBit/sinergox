from __future__ import annotations

import asyncio
import datetime as dt
import logging
import re
from asyncio.log import logger
from enum import Enum
from typing import Any, Sequence

import httpx
import pandas as pd

logger = logging.getLogger(__name__)


class Periodo(Enum):
    DIARIO = "daily"
    HORARIO = "hourly"
    MENSUAL = "monthly"
    ANUAL = "annual"


class EndPoint(Enum):
    HORARIO = "HourlyEntities"
    DIARIO = "DailyEntities"
    MENSUAL = "MonthlyEntities"
    ANUAL = "AnnualEntities"


class EntityTypes(Enum):
    AGENTE = "Agente"
    ZONA = "Zona"
    SUBSISTEMA = "Subsistema"
    SISTEMA = "Sistema"
    RECURSO = "Recurso"
    COMBUSTIBLE = "Combustible"
    RECURSO_COMB = "RecursoComb"
    ENLACE = "Enlace"
    CIIU = "CIIU"
    MERCADO_COMERCIALIZACION = "MercadoComercializacion"
    AREA = "Area"
    SUBAREA = "SubArea"
    EMBALSE = "Embalse"
    RIO = "Rio"


PERIOD_CONFIG: dict[Periodo, dict[str, Any]] = {
    Periodo.HORARIO: {
        "max_delta_in_days": 30,
        "endpoint": EndPoint.HORARIO,
    },
    Periodo.DIARIO: {"max_delta_in_days": 30, "endpoint": EndPoint.DIARIO},
    Periodo.MENSUAL: {
        "max_delta_in_days": 732,
        "endpoint": EndPoint.MENSUAL,
    },
    Periodo.ANUAL: {"max_delta_in_days": 366, "endpoint": EndPoint.ANUAL},
}


DateLike = dt.datetime | dt.date


class Client:
    """Asynchronous client for the Sinergox API."""

    def __init__(
        self,
        *,
        base_url: str = "https://servapibi.xm.com.co",
        timeout: float | httpx.Timeout | None = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=timeout,
            transport=transport,
        )
        self._metrics_cache: pd.DataFrame | None = None

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get_data(
        self,
        period: Periodo,
        *,
        metric: str,
        entity: EntityTypes,
        start: DateLike,
        end: DateLike,
        filter: Sequence[Any] | None = None,
        concurrency: int | None = None,
    ) -> pd.DataFrame:
        start_dt = self._ensure_datetime(start)
        end_dt = self._ensure_datetime(end)
        if end_dt < start_dt:
            raise ValueError("'end' must be greater than or equal to 'start'.")

        logger.debug(
            f"Fetching data from {start_dt} to {end_dt} for metric {metric} and entity {entity}",
        )

        ranges = self._build_ranges(start_dt, end_dt, period)
        endpoint = PERIOD_CONFIG[period]["endpoint"]

        sem = asyncio.Semaphore(concurrency) if concurrency else None

        async def wrapped_fetch(
            chunk_start: dt.datetime, chunk_end: dt.datetime
        ) -> pd.DataFrame:
            if sem is None:
                return await self._fetch_data(
                    period,
                    metric_id=metric,
                    entity=entity,
                    start=chunk_start,
                    end=chunk_end,
                    data_path=endpoint,
                    filter=filter,
                )
            async with sem:
                return await self._fetch_data(
                    period,
                    metric_id=metric,
                    entity=entity,
                    start=chunk_start,
                    end=chunk_end,
                    data_path=endpoint,
                    filter=filter,
                )

        tasks = [
            wrapped_fetch(chunk_start, chunk_end) for chunk_start, chunk_end in ranges
        ]
        results = await asyncio.gather(*tasks)
        frames = [frame for frame in results if not frame.empty]
        if not frames:
            return pd.DataFrame()
        combined = pd.concat(frames)
        return combined.sort_index()

    async def get_metrics(self, *, force_refresh: bool = False) -> pd.DataFrame:
        if self._metrics_cache is not None and not force_refresh:
            return self._metrics_cache.copy()

        payload = {"MetricId": "ListadoMetricas"}
        content = await self._post("/Lists", json=payload)
        items = content.get("Items", [])
        metrics = pd.json_normalize(items, "ListEntities", sep=".")
        metrics.rename(
            columns={name: name.removeprefix("Values.") for name in metrics.columns},
            inplace=True,
        )
        self._metrics_cache = metrics
        return metrics.copy()

    async def find_metric(
        self,
        query: str,
        *,
        levenshtein_threshold: int = 3,
    ) -> pd.DataFrame:
        metrics = await self.get_metrics()
        try:
            from textdistance import levenshtein
        except (
            ModuleNotFoundError
        ) as exc:  # pragma: no cover - only raised when missing dep
            raise ModuleNotFoundError(
                "The 'textdistance' package is required for fuzzy metric search."
            ) from exc

        metrics_copy = metrics.copy()
        metrics_copy["levenshtein"] = metrics_copy["MetricId"].apply(
            lambda value: levenshtein(value.lower(), query.lower())
        )
        mask = metrics_copy["levenshtein"] <= levenshtein_threshold
        if mask.any():
            ordered = metrics_copy.loc[mask].sort_values("levenshtein")
            return metrics.loc[ordered.index].reset_index(drop=True)

        mask_alt = metrics_copy["MetricName"].str.contains(
            query, case=False, na=False
        ) | metrics_copy["MetricDescription"].str.contains(query, case=False, na=False)
        return metrics.loc[mask_alt].reset_index(drop=True)

    async def _fetch_data(
        self,
        period: Periodo,
        *,
        metric_id: str,
        entity: EntityTypes,
        start: dt.datetime,
        end: dt.datetime,
        data_path: EndPoint,
        filter: Sequence[Any] | None,
    ) -> pd.DataFrame:
        body: dict[str, Any] = {
            "MetricId": metric_id,
            "StartDate": self._serialize_date(start),
            "EndDate": self._serialize_date(end),
            "Entity": entity.value,
        }
        if filter:
            body["Filter"] = list(filter)

        content = await self._post(f"/{period.value}", json=body)
        return self._to_tidy(content, data_path, entity)

    async def _post(self, path: str, *, json: dict[str, Any]) -> dict[str, Any]:
        response = await self._client.post(path, json=json)
        response.raise_for_status()
        return response.json()

    def _build_ranges(
        self,
        start: dt.datetime,
        end: dt.datetime,
        period: Periodo,
    ) -> list[tuple[dt.datetime, dt.datetime]]:
        delta = dt.timedelta(days=PERIOD_CONFIG[period]["max_delta_in_days"])
        if delta <= dt.timedelta(0):
            return [(start, end)]

        ranges: list[tuple[dt.datetime, dt.datetime]] = []
        current = start
        while current < end:
            chunk_end = min(end, current + delta)
            ranges.append((current, chunk_end))
            if chunk_end == end:
                break
            current = chunk_end
        if not ranges:
            ranges.append((start, end))
        return ranges

    @staticmethod
    def _ensure_datetime(value: DateLike) -> dt.datetime:
        if isinstance(value, dt.datetime):
            if value.tzinfo is not None:
                return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
            return value
        if isinstance(value, dt.date):
            return dt.datetime.combine(value, dt.time())
        msg = "start/end must be datetime or date instances"
        raise TypeError(msg)

    @staticmethod
    def _serialize_date(value: dt.datetime) -> str:
        return value.date().isoformat()

    @staticmethod
    def _to_tidy(
        content: dict[str, Any],
        data_path: EndPoint,
        entity: EntityTypes,
    ) -> pd.DataFrame:
        patt = re.compile(r"(?P<hour_digits>\d{2})")

        metric_name = content.get("Metric", {}).get("Name", "Unknown Metric")
        items = content.get("Items", [])
        if not items:
            return pd.DataFrame()

        data = pd.json_normalize(
            items, record_path=[data_path.value], sep="-", meta=["Date"]
        )
        data["MetricName"] = metric_name
        data["Date"] = pd.to_datetime(data["Date"], format="%Y-%m-%d")
        if "Values-code" in data.columns:
            data.rename(columns={"Values-code": entity.value}, inplace=True)

        index_columns = ["MetricName", "Date"]
        if entity.value in data.columns:
            index_columns.append(entity.value)
        data = data.set_index(index_columns)
        if "Id" in data.columns:
            data = data.drop(columns=["Id"])

        if data_path is EndPoint.HORARIO:
            column_map = {
                column: int(match.group("hour_digits"))
                for column in data.columns
                if (match := patt.search(column))
            }
            if column_map:
                data = data.rename(columns=column_map)

        stacked = data.stack().reset_index()

        rename_map: dict[str, str] = {0: "Value"}
        if data_path is EndPoint.HORARIO:
            rename_map["level_3"] = "Hour"
        stacked.rename(columns=rename_map, inplace=True)

        if data_path is EndPoint.HORARIO:
            stacked["Timestamp"] = stacked["Date"] + pd.to_timedelta(
                stacked["Hour"] - 1, unit="h"
            )
            stacked.drop(columns=["Date", "Hour"], inplace=True)
        else:
            stacked.rename(columns={"Date": "Timestamp"}, inplace=True)

        final_index: list[str] = ["MetricName", "Timestamp"]
        if entity.value in stacked.columns:
            final_index.append(entity.value)
        return stacked.set_index(final_index)


__all__ = [
    "Client",
    "EntityTypes",
    "Periodo",
]
