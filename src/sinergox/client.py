from __future__ import annotations

import asyncio
import datetime as dt
import logging
import re
import unicodedata
from enum import Enum
from typing import Any, Sequence
from zoneinfo import ZoneInfo

import httpx
import pandas as pd

logger = logging.getLogger(__name__)


class TimeResolution(Enum):
    HORARIO = "hourly"
    DIARIO = "daily"
    MENSUAL = "monthly"
    ANUAL = "annual"


class DataPath(Enum):
    HORARIO = "HourlyEntities"
    DIARIO = "DailyEntities"
    MENSUAL = "MonthlyEntities"
    ANUAL = "AnnualEntities"


class Entity(Enum):
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


RESOLUTION_CONFIG: dict[TimeResolution, dict[str, Any]] = {
    TimeResolution.HORARIO: {"max_delta_in_days": 30, "endpoint": DataPath.HORARIO},
    TimeResolution.DIARIO: {"max_delta_in_days": 30, "endpoint": DataPath.DIARIO},
    TimeResolution.MENSUAL: {"max_delta_in_days": 732, "endpoint": DataPath.MENSUAL},
    TimeResolution.ANUAL: {"max_delta_in_days": 366, "endpoint": DataPath.ANUAL},
}

ENDPOINT_TO_RESOLUTION: dict[DataPath, TimeResolution] = {
    config["endpoint"]: period for period, config in RESOLUTION_CONFIG.items()
}


DateLike = dt.datetime | dt.date
HOUR_PATTERN = re.compile(r"(?P<hour_digits>\d{2})")
DEFAULT_TIMEZONE = ZoneInfo("America/Bogota")


class Client:
    """Async interface for the XM Sinergox API.

    This client exposes high-level helpers for exploring the metric catalog,
    retrieving tidy data frames for any metric and period, and performing
    fuzzy searches that are friendly to automated agents.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://servapibi.xm.com.co",
        timeout: float | httpx.Timeout | None = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
        metrics_cache_ttl: dt.timedelta | None = dt.timedelta(hours=1),
    ) -> None:
        """Build a client instance ready to query the API.

        Parameters
        ----------
        base_url:
            Base endpoint for the API. Override only for testing purposes.
        timeout:
            Total request timeout passed to :class:`httpx.AsyncClient`.
        transport:
            Optional custom transport (handy for mocking during tests).
        metrics_cache_ttl:
            Lifetime for the in-memory metric catalog. Use ``None`` to disable
            expiration or any positive :class:`datetime.timedelta` to refresh
            automatically.
        """
        self._base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=timeout,
            transport=transport,
        )
        self._metrics_cache_ttl = (
            metrics_cache_ttl
            if metrics_cache_ttl and metrics_cache_ttl > dt.timedelta(0)
            else None
        )
        self._metrics_cache: pd.DataFrame | None = None
        self._metrics_cache_expires_at: dt.datetime | None = None

    async def __aenter__(self) -> "Client":
        """Enter the async context manager returning the client itself."""
        return self

    async def __aexit__(self, *_: object) -> None:
        """Release network resources when leaving the context."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close the underlying :class:`httpx.AsyncClient`."""
        await self._client.aclose()

    async def get_data(
        self,
        period: TimeResolution,
        *,
        metric: str,
        entity: Entity,
        start: DateLike,
        end: DateLike,
        filter: Sequence[Any] | None = None,
        concurrency: int | None = None,
    ) -> pd.DataFrame:
        """Download tidy data for a metric/entity pair.

        Parameters
        ----------
        period:
            Target granularity for the time series.
        metric:
            Metric identifier as published by the catalog.
        entity:
            Entity type required by the metric.
        start, end:
            Temporal range (inclusive). Datetime inputs may contain timezones;
            they are normalized to naive UTC before the request payload is
            assembled.
        filter:
            Optional sequence of filter values accepted by some metrics.
        concurrency:
            Maximum number of simultaneous chunk requests when the time span
            must be split because of API limits. ``None`` disables throttling.

        Returns
        -------
        pandas.DataFrame
            Data indexed by metric name and timestamp with value columns ready
            for analysis.

        Raises
        ------
        ValueError
            If ``end`` precedes ``start``.
        """
        start_dt = self._ensure_datetime(start)
        end_dt = self._ensure_datetime(end)
        if end_dt < start_dt:
            raise ValueError("'end' must be greater than or equal to 'start'.")

        logger.debug(
            "Fetching data from %s to %s for metric %s and entity %s",
            start_dt,
            end_dt,
            metric,
            entity,
        )

        ranges = self._build_ranges(start_dt, end_dt, period)
        endpoint = RESOLUTION_CONFIG[period]["endpoint"]

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
        """Return the cached metric catalog, refreshing it when required.

        Parameters
        ----------
        force_refresh:
            Force a fresh fetch even if the TTL has not expired.

        Returns
        -------
        pandas.DataFrame
            Catalog with all metadata provided by the API.
        """
        now = dt.datetime.now(dt.timezone.utc)

        if not force_refresh and self._metrics_cache is not None:
            if (
                self._metrics_cache_expires_at is None
                or now < self._metrics_cache_expires_at
            ):
                return self._metrics_cache.copy()

        if force_refresh:
            self._metrics_cache = None
            self._metrics_cache_expires_at = None

        payload = {"MetricId": "ListadoMetricas"}
        content = await self._post("/Lists", json=payload)
        items = content.get("Items", [])
        metrics = pd.json_normalize(items, "ListEntities", sep=".")
        metrics.rename(
            columns={name: name.removeprefix("Values.") for name in metrics.columns},
            inplace=True,
        )
        self._metrics_cache = metrics
        if self._metrics_cache_ttl is None:
            self._metrics_cache_expires_at = None
        else:
            self._metrics_cache_expires_at = now + self._metrics_cache_ttl
        return metrics.copy()

    async def get_data_for(
        self,
        query: str,
        *,
        days: int = 7,
        period: TimeResolution | None = None,
        timezone: dt.tzinfo | None = DEFAULT_TIMEZONE,
    ) -> pd.DataFrame:
        """Fetch data for the first metric matching ``query``.

        This helper combines :meth:`find_metric` and :meth:`get_data`. It picks
        the first catalog match, converts the ``Entity`` column into the
        corresponding :class:`Entity` member, and infers the period from
        the catalog ``Type`` column when ``period`` is not provided.

        Parameters
        ----------
        query:
            Text that will be sent to :meth:`find_metric`.
        days:
            Rolling window size (up to ``now`` in the chosen timezone).
        period:
            Optional override for the inferred :class:`TimeResolution`.
        timezone:
            Datetime timezone used to anchor ``start`` and ``end`` (defaults to
            ``America/Bogota``).

        Returns
        -------
        pandas.DataFrame
            Tidy frame for the discovered metric.

        Raises
        ------
        ValueError
            If no matches are found or if required metadata is missing from the
            catalog entry.
        """
        matches = await self.find_metric(query, limit=1)
        if matches.empty:
            raise ValueError(f"No se encontraron métricas para la consulta {query!r}.")

        row = matches.iloc[0]
        metric_id = row.get("MetricId")
        entity_label = row.get("Entity")
        if not metric_id or not isinstance(metric_id, str):
            raise ValueError("La métrica encontrada no contiene un 'MetricId' válido.")
        if not entity_label or not isinstance(entity_label, str):
            raise ValueError(
                f"La métrica {metric_id!r} no tiene un valor de 'Entity' compatible."
            )

        try:
            entity = Entity(entity_label)
        except ValueError as exc:
            raise ValueError(
                f"El valor de entidad '{entity_label}' no pertenece a Entity."
            ) from exc

        resolved_period = period
        if resolved_period is None:
            type_label = row.get("Type")
            if not type_label or not isinstance(type_label, str):
                raise ValueError(
                    "No se especificó periodo y la métrica no provee la columna 'Type'."
                )
            try:
                endpoint = DataPath(type_label)
            except ValueError as exc:
                raise ValueError(
                    f"El tipo '{type_label}' de la métrica {metric_id!r} es desconocido."
                ) from exc
            try:
                resolved_period = ENDPOINT_TO_RESOLUTION[endpoint]
            except KeyError as exc:  # pragma: no cover - seguridad adicional
                raise ValueError(
                    f"No se pudo derivar el periodo para el endpoint '{endpoint.value}'."
                ) from exc

        tz = timezone or DEFAULT_TIMEZONE
        now = dt.datetime.now(tz)
        start = now - dt.timedelta(days=days)

        return await self.get_data(
            resolved_period,
            metric=metric_id,
            entity=entity,
            start=start,
            end=now,
        )

    async def find_metric(
        self,
        query: str,
        *,
        levenshtein_threshold: int = 3,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """Convenience wrapper around :meth:`search_metrics`.

        Returns the best matches without the scoring columns so that the
        resulting frame is ready for human or agent consumption.
        """
        results = await self.search_metrics(
            query,
            levenshtein_threshold=levenshtein_threshold,
            limit=limit,
        )
        return results.drop(
            columns=["match_tier", "levenshtein", "token_overlap"],
            errors="ignore",
        )

    async def search_metrics(
        self,
        query: str,
        *,
        levenshtein_threshold: int = 3,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """Return catalog matches enriched with scoring metadata.

        Parameters
        ----------
        query:
            Free-form text, accents and case are normalized.
        levenshtein_threshold:
            Maximum allowed edit distance for fuzzy candidates. Use ``None`` to
            keep all results regardless of distance.
        limit:
            Optional maximum number of rows to return.

        Returns
        -------
        pandas.DataFrame
            Catalog records plus ``match_tier``, ``levenshtein`` and
            ``token_overlap`` columns.

        Raises
        ------
        ValueError
            If ``query`` is empty or whitespace.
        ModuleNotFoundError
            When the optional dependency ``textdistance`` is missing.
        """
        if not query.strip():
            raise ValueError("query must not be empty")

        metrics = await self.get_metrics()
        if metrics.empty:
            return metrics

        try:
            from textdistance import levenshtein
        except (
            ModuleNotFoundError
        ) as exc:  # pragma: no cover - only raised when missing dep
            raise ModuleNotFoundError(
                "The 'textdistance' package is required for fuzzy metric search."
            ) from exc

        query_norm = self._normalize_text(query)
        query_tokens = self._tokenize(query_norm)

        def score_row(row: pd.Series) -> pd.Series:
            fields_raw = [
                row.get("MetricId", ""),
                row.get("MetricName", ""),
                row.get("MetricDescription", ""),
            ]
            fields_norm = [
                self._normalize_text(str(value))
                for value in fields_raw
                if isinstance(value, str) and value
            ]
            if not fields_norm:
                return pd.Series(
                    {
                        "match_tier": 5,
                        "levenshtein": float("inf"),
                        "token_overlap": 0.0,
                    }
                )

            best_lev = min(levenshtein(field, query_norm) for field in fields_norm)

            exact = any(field == query_norm for field in fields_norm)
            starts = any(field.startswith(query_norm) for field in fields_norm)
            substring = any(query_norm in field for field in fields_norm)

            token_overlap = 0.0
            if query_tokens:
                overlaps = []
                for field in fields_norm:
                    tokens = self._tokenize(field)
                    if tokens:
                        overlaps.append(len(query_tokens & tokens) / len(query_tokens))
                if overlaps:
                    token_overlap = max(overlaps)

            tier = 4
            if exact:
                tier = 0
            elif starts:
                tier = 1
            elif substring:
                tier = 2
            elif token_overlap > 0:
                tier = 3

            return pd.Series(
                {
                    "match_tier": tier,
                    "levenshtein": best_lev,
                    "token_overlap": token_overlap,
                }
            )

        scores = metrics.apply(score_row, axis=1)
        ranked = pd.concat([metrics, scores], axis=1)

        threshold = (
            levenshtein_threshold if levenshtein_threshold is not None else float("inf")
        )
        mask = (ranked["match_tier"] < 4) | (ranked["levenshtein"] <= threshold)
        matches = ranked.loc[mask]
        if matches.empty:
            return matches.reset_index(drop=True)

        matches = matches.sort_values(
            by=["match_tier", "levenshtein", "token_overlap", "MetricName"],
            ascending=[True, True, False, True],
        )
        if limit is not None:
            matches = matches.head(limit)

        return matches.reset_index(drop=True)

    async def _fetch_data(
        self,
        period: TimeResolution,
        *,
        metric_id: str,
        entity: Entity,
        start: dt.datetime,
        end: dt.datetime,
        data_path: DataPath,
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
        period: TimeResolution,
    ) -> list[tuple[dt.datetime, dt.datetime]]:
        delta = dt.timedelta(days=RESOLUTION_CONFIG[period]["max_delta_in_days"])
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
            if value.tzinfo is None:
                value = value.replace(tzinfo=DEFAULT_TIMEZONE)
            return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        if isinstance(value, dt.date):
            assumed = dt.datetime.combine(value, dt.time()).replace(
                tzinfo=DEFAULT_TIMEZONE
            )
            return assumed.astimezone(dt.timezone.utc).replace(tzinfo=None)
        msg = "start/end must be datetime or date instances"
        raise TypeError(msg)

    @staticmethod
    def _serialize_date(value: dt.datetime) -> str:
        return value.date().isoformat()

    @staticmethod
    def _to_tidy(
        content: dict[str, Any],
        data_path: DataPath,
        entity: Entity,
    ) -> pd.DataFrame:
        prepared = Client._prepare_frame(content, data_path, entity)
        if prepared.empty:
            return prepared
        return Client._reshape_frame(prepared, data_path, entity)

    @staticmethod
    def _prepare_frame(
        content: dict[str, Any],
        data_path: DataPath,
        entity: Entity,
    ) -> pd.DataFrame:
        metric_name = content.get("Metric", {}).get("Name", "Unknown Metric")
        items = content.get("Items", [])
        if not items:
            return pd.DataFrame()

        data = pd.json_normalize(
            items, record_path=[data_path.value], sep="-", meta=["Date"]
        )
        data["MetricName"] = metric_name
        data["Date"] = pd.to_datetime(data["Date"], format="%Y-%m-%d")

        code_keys = ["Values-code", "Values-Code", "Code"]
        for key in code_keys:
            if key in data.columns:
                data.rename(columns={key: entity.value}, inplace=True)
                break

        name_keys = ["Values-name", "Values-Name", "Name"]
        for key in name_keys:
            if key in data.columns:
                data.rename(columns={key: entity.value.lower()}, inplace=True)
                break

        if "Values-value" in data.columns:
            data.rename(columns={"Values-value": "value"}, inplace=True)

        if "Id" in data.columns:
            data = data.drop(columns=["Id"])

        if data_path is DataPath.HORARIO:
            column_map = {
                column: int(match.group("hour_digits"))
                for column in data.columns
                if isinstance(column, str) and (match := HOUR_PATTERN.search(column))
            }
            if column_map:
                data = data.rename(columns=column_map)

        data = data.set_index(["MetricName", "Date"], drop=True)
        return data

    @staticmethod
    def _reshape_frame(
        data: pd.DataFrame,
        data_path: DataPath,
        entity: Entity,
    ) -> pd.DataFrame:
        data_reset = data.reset_index()
        index_columns = ["MetricName", "Date"]

        value_columns, metadata_columns = Client._categorise_columns(
            data_reset.columns, index_columns, data_path
        )
        if not value_columns:
            return pd.DataFrame()

        var_name = "Hour" if data_path is DataPath.HORARIO else "Variable"
        value_col_name = "_value"
        tidy = data_reset.melt(
            id_vars=index_columns + metadata_columns,
            value_vars=value_columns,
            var_name=var_name,
            value_name=value_col_name,
        )

        tidy.rename(columns={value_col_name: "value"}, inplace=True)

        if data_path is DataPath.HORARIO:
            tidy["Hour"] = tidy["Hour"].astype(int)
            tidy["Timestamp"] = tidy["Date"] + pd.to_timedelta(
                tidy["Hour"] - 1, unit="h"
            )
            tidy.drop(columns=["Hour", "Date"], inplace=True)
        else:
            tidy["Timestamp"] = tidy["Date"]
            tidy.drop(columns=["Variable", "Date"], inplace=True, errors="ignore")

        tidy["value"] = pd.to_numeric(tidy["value"], errors="coerce")
        tidy.sort_values(["MetricName", "Timestamp"], inplace=True)
        tidy.reset_index(drop=True, inplace=True)
        return tidy.set_index(["MetricName", "Timestamp"])

    @staticmethod
    def _categorise_columns(
        columns: Sequence[Any],
        index_columns: Sequence[str],
        data_path: DataPath,
    ) -> tuple[list[Any], list[str]]:
        value_columns: list[Any] = []
        metadata_columns: list[str] = []
        for column in columns:
            if column in index_columns:
                continue
            if Client._is_value_column(column, data_path):
                value_columns.append(column)
            else:
                metadata_columns.append(column)  # type: ignore[arg-type]
        return value_columns, metadata_columns

    @staticmethod
    def _is_value_column(column: Any, data_path: DataPath) -> bool:
        if data_path is DataPath.HORARIO:
            return isinstance(column, int)
        if isinstance(column, str):
            lowered = column.lower()
            return lowered == "value" or lowered.endswith("value")
        return False

    @staticmethod
    def _normalize_text(value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value)
        ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
        return ascii_text.lower().strip()

    @staticmethod
    def _tokenize(value: str) -> set[str]:
        if not value:
            return set()
        return {token for token in re.split(r"[^0-9a-zA-Z]+", value) if token}


__all__ = [
    "Client",
    "Entity",
    "TimeResolution",
]
