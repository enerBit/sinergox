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
import pyarrow as pa
import pyarrow.compute as pc

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
RESOLUTION_ALIASES: dict[DataPath, tuple[str, ...]] = {
    DataPath.HORARIO: ("horario", "horaria", "hourly"),
    DataPath.DIARIO: ("diario", "diaria", "daily"),
    DataPath.MENSUAL: ("mensual", "monthly"),
    DataPath.ANUAL: ("anual", "annual"),
}


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
        return_pandas: bool = False,
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
        return_pandas:
            If True, methods will return pandas.DataFrame objects instead of
            pyarrow.Table. Requires pandas to be installed.
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
        self._metrics_cache: pa.Table | None = None
        self._metrics_cache_expires_at: dt.datetime | None = None
        self._return_pandas = return_pandas
        
        if self._return_pandas:
            try:
                import pandas as pd
                self._pandas_module = pd
            except ImportError as exc:
                raise ModuleNotFoundError(
                    "pandas is required when return_pandas=True. "
                    "Install it with 'pip install sinergox[pandas]'."
                ) from exc
        else:
            self._pandas_module = None

    def _convert_output(self, table: pa.Table) -> Any:
        if self._return_pandas:
            return table.to_pandas()
        return table

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
    ) -> Any:
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
        pyarrow.Table | pandas.DataFrame
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
        ) -> pa.Table:
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
        frames = [frame for frame in results if frame.num_rows > 0]
        if not frames:
            return self._convert_output(pa.Table.from_batches([], schema=pa.schema([])))
        
        combined = pa.concat_tables(frames)
        
        # Sort logic
        sort_keys = []
        if entity.value in combined.column_names:
            sort_keys.append((entity.value, "ascending"))
        elif entity.value.lower() in combined.column_names:
             sort_keys.append((entity.value.lower(), "ascending"))
        
        if "Timestamp" in combined.column_names:
            sort_keys.append(("Timestamp", "ascending"))
            
        if sort_keys:
            combined = combined.sort_by(sort_keys)
            
        return self._convert_output(combined)

    async def get_metrics(self, *, force_refresh: bool = False) -> Any:
        """Return the cached metric catalog, refreshing it when required.

        Parameters
        ----------
        force_refresh:
            Force a fresh fetch even if the TTL has not expired.

        Returns
        -------
        pyarrow.Table | pandas.DataFrame
            Catalog with all metadata provided by the API.
        """
        table = await self._get_metrics_table(force_refresh=force_refresh)
        return self._convert_output(table)

    async def _get_metrics_table(self, *, force_refresh: bool = False) -> pa.Table:
        now = dt.datetime.now(dt.timezone.utc)

        if not force_refresh and self._metrics_cache is not None:
            if (
                self._metrics_cache_expires_at is None
                or now < self._metrics_cache_expires_at
            ):
                return self._metrics_cache

        if force_refresh:
            self._metrics_cache = None
            self._metrics_cache_expires_at = None

        payload = {"MetricId": "ListadoMetricas"}
        content = await self._post("/Lists", json=payload)
        items = content.get("Items", [])
        
        flat_metrics = []
        for item in items:
            for entity_dict in item.get("ListEntities", []):
                # Flattening: keys are inside "Values" dict usually
                # The entity_dict might look like {'Id': '...', 'Values': { ... }}
                values = entity_dict.get("Values", {})
                if not values:
                    # Fallback if flattening didn't happen or different structure
                    # Try to use entity_dict itself if it has the keys
                    values = entity_dict

                clean_dict = {}
                for k, v in values.items():
                    # If keys still have prefixes (unlikely inside Values dict but possible)
                    key = k.removeprefix("Values.")
                    clean_dict[key] = v
                
                # Also include ID if needed? Pandas normalized it to "Id" maybe?
                # The test expects MetricId, MetricName which are inside Values.
                flat_metrics.append(clean_dict)
        
        if not flat_metrics:
            metrics = pa.Table.from_batches([], schema=pa.schema([]))
        else:
            metrics = pa.Table.from_pylist(flat_metrics)

        self._metrics_cache = metrics
        if self._metrics_cache_ttl is None:
            self._metrics_cache_expires_at = None
        else:
            self._metrics_cache_expires_at = now + self._metrics_cache_ttl
        return metrics

    async def get_data_for(
        self,
        query: str,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        period: TimeResolution | None = None,
        entity: Entity | None = None,
        timezone: dt.tzinfo | None = DEFAULT_TIMEZONE,
    ) -> Any:
        """Fetch data for the first metric matching ``query``.

        This helper combines :meth:`find_metric` and :meth:`get_data`. It picks
        the first catalog match, converts the ``Entity`` column into the
        corresponding :class:`Entity` member, and infers the period from
        the catalog ``Type`` column when ``period`` is not provided.

        Parameters
        ----------
        query:
            Text that will be sent to :meth:`find_metric`.
        start:
            Optional start of the range. When omitted, defaults to seven days
            before ``end`` (or now).
        end:
            Optional end of the range. When omitted, defaults to seven days
            after ``start`` (or now if both are missing).
        period:
            Optional override for the inferred :class:`TimeResolution`.
        entity:
            When provided, restricts candidate metrics to the specified
            :class:`Entity` before selecting the first match.
        timezone:
            Datetime timezone used to anchor ``start`` and ``end`` (defaults to
            ``America/Bogota``).

        Returns
        -------
        pyarrow.Table | pandas.DataFrame
            Tidy frame for the discovered metric.

        Raises
        ------
        ValueError
            If no matches are found or if required metadata is missing from the
            catalog entry.
        """
        # Note: we use self.find_metric which might return pandas if configured that way.
        # But we need PyArrow logic here internally if possible, OR adapt to handle both.
        # Easier to force pyarrow internally for logic, but self.find_metric obeys global setting.
        
        # We can temporarily bypass find_metric or handle both types.
        # But find_metric calls search_metrics.
        
        # Let's handle the return type of find_metric by checking isinstance or converting back if needed is messy.
        # Better approach: internal methods always return Table/Dict? 
        # No, find_metric is public.
        
        # We can implement a private _find_metric that returns Table and use it.
        # Or just cast it back if it is DataFrame (inefficient but safe).
        # Actually since we have full Table in `matches`, we can just use `matches` regardless of type 
        # if we are careful, but pandas vs pyarrow API is different.
        
        # Let's inspect `matches`.
        matches = await self.find_metric(query, limit=50 if entity is not None else 1)
        
        # If matches is DF, convert to Table or use DF API?
        # Converting back to Table is safest to share logic below.
        if self._return_pandas:
             import pandas as pd
             if isinstance(matches, pd.DataFrame):
                 matches = pa.Table.from_pandas(matches)

        if matches.num_rows == 0:
            raise ValueError(f"No se encontraron métricas para la consulta {query!r}.")

        if entity is not None:
            # Filter matches by Entity matches["Entity"] == entity.value
            # Use PyArrow compute
            mask = pc.equal(matches["Entity"], entity.value)
            matches = matches.filter(mask)
            if matches.num_rows == 0:
                raise ValueError(
                    "No se encontraron métricas que coincidan con la entidad "
                    f"{entity.value!r}."
                )

        # Take the first row
        row = matches.to_pylist()[0]
        metric_id = row.get("MetricId")
        entity_label = row.get("Entity")
        if not metric_id or not isinstance(metric_id, str):
            raise ValueError("La métrica encontrada no contiene un 'MetricId' válido.")
        if not entity_label or not isinstance(entity_label, str):
            raise ValueError(
                f"La métrica {metric_id!r} no tiene un valor de 'Entity' compatible."
            )

        try:
            entity_member = Entity(entity_label)
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

        def _normalize(value: DateLike | None) -> dt.datetime | None:
            if value is None:
                return None
            if isinstance(value, dt.datetime):
                if value.tzinfo is None:
                    return value.replace(tzinfo=tz)
                return value.astimezone(tz)
            if isinstance(value, dt.date):
                return dt.datetime.combine(value, dt.time()).replace(tzinfo=tz)
            raise TypeError("start/end must be datetime or date instances")

        start_local = _normalize(start)
        end_local = _normalize(end)

        if start_local is None and end_local is None:
            end_local = dt.datetime.now(tz)
            start_local = end_local - dt.timedelta(days=7)
        elif start_local is None:
            assert end_local is not None  # for type checkers
            start_local = end_local - dt.timedelta(days=7)
        elif end_local is None:
            end_local = start_local + dt.timedelta(days=7)

        start_dt = self._ensure_datetime(start_local)
        end_dt = self._ensure_datetime(end_local)

        return await self.get_data(
            resolved_period,
            metric=metric_id,
            entity=entity_member,
            start=start_dt,
            end=end_dt,
        )

    async def find_metric(
        self,
        query: str,
        *,
        levenshtein_threshold: int = 3,
        limit: int | None = None,
    ) -> Any:
        """Convenience wrapper around :meth:`search_metrics`.

        Returns the best matches without the scoring columns so that the
        resulting frame is ready for human or agent consumption.
        """
        # We need the inner search to return Table for our drop_columns logic if possible
        # Or we call search_metrics (which handles conversion) then convert back? 
        # Better: check result type
        results = await self.search_metrics(
            query,
            levenshtein_threshold=levenshtein_threshold,
            limit=limit,
        )
        
        # If Pandas, drop cols using pandas API
        if self._return_pandas and isinstance(results, self._pandas_module.DataFrame):
            cols_to_drop = ["match_tier", "levenshtein", "token_overlap"]
            return results.drop(columns=cols_to_drop, errors="ignore")
        
        # PyArrow
        cols_to_drop = ["match_tier", "levenshtein", "token_overlap"]
        present_cols = [c for c in cols_to_drop if c in results.column_names]
        if present_cols:
            results = results.drop_columns(present_cols)
        return results

    async def search_metrics(
        self,
        query: str,
        *,
        levenshtein_threshold: int = 3,
        limit: int | None = None,
    ) -> Any:
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
        pyarrow.Table | pandas.DataFrame
            Catalog records plus ``match_tier``, ``levenshtein`` and
            ``token_overlap`` columns.
        """
        if not query.strip():
            raise ValueError("query must not be empty")

        metrics_table = await self._get_metrics_table()
        if metrics_table.num_rows == 0:
            return metrics_table

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

        metrics_list = metrics_table.to_pylist()
        scored_rows = []

        for row in metrics_list:
            # Score logic
            fields_raw = [
                row.get("MetricId", ""),
                row.get("MetricName", ""),
                row.get("MetricDescription", ""),
                row.get("Entity", ""),
                row.get("Type", ""),
                row.get("Unit", ""),
            ]
            type_hint = row.get("Type")
            if isinstance(type_hint, str):
                try:
                    data_path = DataPath(type_hint)
                except ValueError:
                    data_path = None
                if data_path is not None:
                    aliases = " ".join(RESOLUTION_ALIASES.get(data_path, ()))
                    if aliases:
                        fields_raw.append(aliases)
            
            fields_norm = [
                self._normalize_text(str(value))
                for value in fields_raw
                if isinstance(value, str) and value
            ]

            if not fields_norm:
                row["match_tier"] = 5
                row["levenshtein"] = float("inf")
                row["token_overlap"] = 0.0
                scored_rows.append(row)
                continue

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

            row["match_tier"] = tier
            row["levenshtein"] = best_lev
            row["token_overlap"] = token_overlap
            scored_rows.append(row)

        # Filtering
        threshold = (
            levenshtein_threshold if levenshtein_threshold is not None else float("inf")
        )
        
        filtered_rows = [
            r for r in scored_rows 
            if r["match_tier"] < 4 or r["levenshtein"] <= threshold
        ]

        if not filtered_rows:
             # Return empty table with correct schema (including scoring columns)
             # Simplest way is to infer from the list if not empty, but it is empty.
             # We can create a dummy dict with all keys from metrics + scoring keys
             schema_keys = metrics_table.column_names + ["match_tier", "levenshtein", "token_overlap"]
             return pa.Table.from_batches([], schema=pa.schema([(k, pa.string()) for k in schema_keys])) # Approx schema

        # Sorting: match_tier (asc), levenshtein (asc), token_overlap (desc), MetricName (asc)
        # Python sort is stable. We sort in reverse order of significance
        
        filtered_rows.sort(key=lambda x: x.get("MetricName", ""))
        filtered_rows.sort(key=lambda x: x.get("token_overlap", 0.0), reverse=True)
        filtered_rows.sort(key=lambda x: x.get("levenshtein", float("inf")))
        filtered_rows.sort(key=lambda x: x.get("match_tier", 5))

        if limit is not None:
            filtered_rows = filtered_rows[:limit]
            
        return self._convert_output(pa.Table.from_pylist(filtered_rows))

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
    ) -> pa.Table:
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
    ) -> pa.Table:
        """Parses JSON content directly into a Tidy PyArrow Table."""
        metric_name = content.get("Metric", {}).get("Name", "Unknown Metric")
        items = content.get("Items", [])
        if not items:
            return pa.Table.from_batches([], schema=pa.schema([]))

        rows = []
        
        # Prepare lookup keys
        code_keys = ["Values-code", "Values-Code", "Code"]
        name_keys = ["Values-name", "Values-Name", "Name"]
        value_keys_lower = ["values-value", "value"]

        for item in items:
            date_str = item.get("Date")
            if not date_str:
                continue
            # Parse date once
            try:
                 base_date = dt.datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                 continue

            entity_records = item.get(data_path.value, [])
            for record in entity_records:
                # The record might be {'Id': '...', 'Values': { 'code': ..., 'val': ... }}
                # We need to look inside 'Values' or assume flattened keys if they exist
                
                # Check for explicit Values dict
                # Some endpoints might flatten it differently, but debug shows nested Values
                vals = record.get("Values", {})
                
                # Merge top level keys (excluding Values) with vals to mimic flattening
                # but careful about overwrites.
                # Actually we mainly care about what's in Values for code/name/metrics.
                # But 'Id' is at top level.
                
                # Build a combined lookup dict
                # Keys inside Values usually don't have "Values-" prefix in raw JSON, 
                # but pandas with sep="-" would create "Values-code".
                # My logic below checks for "Values-code" OR "Code".
                # If we use `vals` dict, the key is just "code".
                
                combined = record.copy()
                if "Values" in combined:
                    del combined["Values"]
                # Add vals keys. If we want to support "Values-code" style lookup, 
                # we can rely on "code" checking.
                combined.update(vals)

                # Identify Entity Code and Name
                entity_code = None
                # Check for "code", "Code", "Values-code" (if flattened upstream)
                for k in ["code", "Code", "Values-code", "Values-Code"]:
                    if k in combined:
                        entity_code = combined[k]
                        break
                
                entity_name = None
                for k in ["name", "Name", "Values-name", "Values-Name"]:
                     if k in combined:
                         entity_name = combined[k]
                         break
                
                if data_path == DataPath.HORARIO:
                     for k, v in combined.items():
                         # Check for hour pattern in key
                         match = HOUR_PATTERN.search(k)
                         if match:
                             hour = int(match.group("hour_digits"))
                             val = v
                             
                             # Construct Timestamp
                             # date + (hour - 1) hours
                             ts = base_date + dt.timedelta(hours=hour - 1)
                             
                             row = {
                                 "Timestamp": ts,
                                 metric_name: float(val) if val is not None else None
                             }
                             if entity_code is not None:
                                 row[entity.value] = entity_code
                             if entity_name is not None:
                                 row[entity.value.lower()] = entity_name
                             
                             rows.append(row)
                else:
                    # Daily or others
                    val = None
                    found_value = False
                    # Check combined keys
                    for k, v in combined.items():
                         lowered = k.lower()
                         # "value" or "values-value"
                         if lowered == "value" or lowered == "values-value" or lowered.endswith("value"):
                             val = v
                             found_value = True
                             break
                    
                    if found_value:
                        row = {
                             "Timestamp": base_date,
                             metric_name: float(val) if val is not None else None
                        }
                        if entity_code is not None:
                             row[entity.value] = entity_code
                        if entity_name is not None:
                             row[entity.value.lower()] = entity_name
                        rows.append(row)

        if not rows:
             return pa.Table.from_batches([], schema=pa.schema([]))

        return pa.Table.from_pylist(rows)


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
