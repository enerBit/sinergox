import asyncio
import datetime as dt
import re
from enum import Enum

import httpx
import pandas as pd
from IPython.display import display


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


PERIOD_CONFIG = {
    Periodo.HORARIO: {
        "max_delta_in_days": 30,
        "endpoint": EndPoint.HORARIO,
        "freq": "H",
    },
    Periodo.DIARIO: {"max_delta_in_days": 30, "endpoint": EndPoint.DIARIO, "freq": "D"},
    Periodo.MENSUAL: {
        "max_delta_in_days": 732,
        "endpoint": EndPoint.MENSUAL,
        "freq": "M",
    },
    Periodo.ANUAL: {"max_delta_in_days": 366, "endpoint": EndPoint.ANUAL, "freq": "Y"},
}



async def fetch_data(
    client,
    url: str,
    metric_id: str,
    entity: EntityTypes,
    start: dt.datetime,
    end: dt.datetime,
    data_path: EndPoint,
    filter: list | None = None,
) -> pd.DataFrame:
    body = {
        "MetricId": metric_id,
        "StartDate": start.strftime("%Y-%m-%d"),
        "EndDate": end.strftime("%Y-%m-%d"),
        "Entity": entity.value,
    }
    if filter is not None:
        body.update(
            {
                "Filter": filter,
            }
        )

    try:
        response = await client.post(url, json=body)
        response.raise_for_status()

    except httpx.HTTPStatusError as e:
        print(
            f"Error response {e} while requesting {e.request.url!r} with body {e.request.content}."
        )
        return pd.DataFrame()

    json_content = response.json()
    print(f"json_content: {json_content}")
    data = to_tidy(json_content, data_path, entity=entity)

    return data


def to_tidy(json_content, data_path: EndPoint, entity: EntityTypes) -> pd.DataFrame:
    patt = re.compile(r"(?P<hour_digits>\d{2})")

    # Get metadata
    metric_name = json_content.get("Metric", {}).get("Name", "Unknown Metric")
    metric_units = json_content.get("Metric", {}).get("Units", "Unknown Units")

    items = json_content.get("Items", [])
    if not items:
        return pd.DataFrame()

    # Normalize JSON data into DataFrame
    data: pd.DataFrame = pd.json_normalize(
        items, record_path=[data_path.value], sep="-", meta=["Date"]
    )

    # Add metadata columns
    data["MetricName"] = metric_name
    # data["MetricUnits"] = metric_units
    data["Date"] = pd.to_datetime(data["Date"], format="%Y-%m-%d")
    data.rename(columns={"Values-code": entity.value}, inplace=True)

    print(f"data.head(): {data.head()}")

    # Set index
    new_index = ["MetricName", "Date"]
    if entity.value in data.columns:
        new_index.append(entity.value)
    data = data.set_index(new_index)
    data = data.drop(columns=["Id"])

    # Rename columns to extract hour digits as integers
    if data_path in {EndPoint.HORARIO}:
        new_names = {
            c: int(patt.search(c).group("hour_digits"))
            for c in data.columns
            if patt.search(c)
        }
        data = data.copy().rename(columns=new_names)

    # Stack the DataFrame to convert horizontal data (columns not in index) into vertical data
    stacked = data.stack(future_stack=True).reset_index()

    # Rename the stacked columns
    new_names = {0: "Value"}
    if data_path in {EndPoint.HORARIO}:
        new_names["level_3"] = "Hour"
    stacked.rename(columns=new_names, inplace=True)

    # Add hour data to date to create a datetime column
    if data_path in {EndPoint.HORARIO}:
        stacked["Timestamp"] = stacked["Date"] + pd.to_timedelta(
            stacked["Hour"] - 1, unit="h"
        )
        stacked.drop(columns=["Date", "Hour"], inplace=True)
    else:
        stacked.rename(columns={"Date": "Timestamp"}, inplace=True)
    final_index = ["MetricName", "Timestamp"]
    if entity.value in stacked.columns:
        final_index.append(entity.value)
    stacked.set_index(final_index, inplace=True)
    return stacked


async def get_data(
    period: Periodo,
    metric: str,
    entity: EntityTypes,
    start: dt.datetime,
    end: dt.datetime,
    filter: list | None = None,
):
    url = f"https://servapibi.xm.com.co/{period.value}"

    max_delta_in_days = PERIOD_CONFIG[period]["max_delta_in_days"]
    freq = PERIOD_CONFIG[period]["freq"]
    number_of_days = (end - start).days
    starts = pd.date_range(
        start=start,
        end=end,
        freq=freq,
        inclusive="both",
        tz="America/Bogota",
        normalize=True,
    )
    ends = pd.date_range(
        start=start,
        end=end,
        freq=freq,
        inclusive="both",
        tz="America/Bogota",
        normalize=True,
    )
    print(f"starts: {starts}")
    print(f"ends: {ends}")

    ranges = list(zip(starts[:-1], ends[1:]))
    print(f"start: {start}, end: {end}")
    print(f"ranges_start: {ranges[0][0]}, ranges_end: {ranges[-1][1]}")
    print(f"ranges: {ranges}")

    body_template = {
        "metric_id": metric,
        "start": None,
        "end": None,
        "entity": entity,
    }

    if filter is not None:
        body_template.update(
            {
                "filter": filter,
            }
        )

    bodies = [
        body_template
        | {
            "start": r[0],
            "end": r[1],
        }
        for r in ranges
    ]
    async with httpx.AsyncClient() as client:
        tasks = [
            fetch_data(client, url, **body, data_path=PERIOD_CONFIG[period]["endpoint"])
            for body in bodies
        ]
        print(f"requesting {len(tasks)} tasks...")
        results = await asyncio.gather(*tasks)

    df = pd.concat(results)
    return df


async def get_metrics():
|    async with httpx.AsyncClient() as client:
        metrics_response = await client.post(
            "https://servapibi.xm.com.co/Lists", json={"MetricId": "ListadoMetricas"}
        )
        metrics_response.raise_for_status()
        metrics = metrics_response.json()["Items"]
    display(metrics[:2])  # Show only first 2 items for brevity
    metrics = pd.json_normalize(metrics, "ListEntities", sep=".")
    new_names = {name: name.removeprefix("Values.") for name in metrics.columns}
    metrics.rename(columns=new_names, inplace=True)
    return metrics


METRICS = await get_metrics()

def find_metric(query: str, levenshtein_threshold: int = 3) -> pd.DataFrame:
    """Busca métricas que contengan el texto de consulta.

    Args:
        query (str): Texto a buscar en los nombres de las métricas.

    Returns:
        pd.DataFrame: DataFrame con las métricas que coinciden con la consulta.
    """
    from textdistance import levenshtein

    metrics_copy = METRICS.copy()
    metrics_copy["levenshtein"] = metrics_copy["MetricId"].apply(
        lambda x: levenshtein(x.lower(), query.lower())
    )
    mask = pd.Series(False, index=metrics_copy.index)
    mask = mask | (metrics_copy["levenshtein"] <= levenshtein_threshold)
    if mask.any():
        return METRICS.loc[
            metrics_copy[mask].sort_values(by=["levenshtein"], ascending=True).index
        ]

    mask = mask | metrics_copy["MetricName"].str.contains(query, case=False, na=False)
    mask = mask | metrics_copy["MetricDescription"].str.contains(
        query, case=False, na=False
    )
    return METRICS[mask]


display(find_metric("PPPrecBolsNaci"))