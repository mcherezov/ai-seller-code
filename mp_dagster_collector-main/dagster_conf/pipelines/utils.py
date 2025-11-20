from dagster import op, Out, Dict, In
from datetime import datetime, timezone, timedelta
from typing import Any, Dict as TypingDict, List
from itertools import islice


@op(
    out=Out(str),
    description="Текущая дата в формате YYYY-MM-DD (UTC)"
)
def today_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()

@op(
    out=Out(Dict),
    description="Генерирует словарь фильтров {'date_from','date_to'} по переданной дате"
)
def make_today_filter_dict(date: str) -> TypingDict[str, str]:
    """
    Параметры:
    - date: строка в формате YYYY-MM-DD

    Возвращает:
    - словарь {'date_from': date, 'date_to': date}
    """
    return {"date_from": date, "date_to": date}


@op(
    out=Out(str),
    description="Текущая дата в формате YYYY-MM-DD (UTC)"
)
def today_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


@op(
    out=Out(Dict),
    description="Генерирует словарь фильтров {'date_from','date_to'} по переданной дате"
)
def make_today_filter_dict(date: str) -> TypingDict[str, str]:
    return {"date_from": date, "date_to": date}


@op(
    out=Out(Dict),
    description="Генерирует фильтр для отчёта остатков Wildberries"
)
def make_stocks_filters() -> TypingDict[str, Any]:
    return {
        "groupByBrand":   True,
        "groupBySubject": True,
        "groupBySa":      True,
        "groupByNm":      True,
        "groupByBarcode": True,
        "groupBySize":    True,
    }


@op(
    out=Out(Dict),
    description="Генерирует параметры для запроса wb_ad_info"
)
def make_ad_info_params() -> TypingDict[str, Any]:
    """
    Возвращает стандартные параметры для вызова wb_ad_info:
    - search: строка для поиска по объявлению
    - page: номер страницы
    - limit: размер страницы
    """
    return {"search": "", "page": 1, "limit": 100}


@op(
    out=Out(Dict),
    description="Генерирует filters для запроса воронки продаж по дате"
)
def make_sales_funnel_filter_dict(date: str) -> Dict[str, Any]:
    return {
        "nmIDs": [],
        "objectIDs": [],
        "tagIDs": [],
        "brandNames": [],
        "timezone": "Europe/Moscow",
        "period": {
            "begin": f"{date} 00:00:00",
            "end": f"{date} 23:59:59"
        },
        "page": 1
    }


@op(
    ins={
        "advert_ids": In(List[int], description="Список advert_id"),
        "date_filters": In(Dict[str, str], description="{'date_from':…, 'date_to':…}")
    },
    out=Out(List[Dict[str, Any]]),
    description="Строит payload для POST /adv/v2/fullstats"
)
def make_ad_stats_payload(advert_ids, date_filters):
    return [
        {"id": advert_id, "interval": {"begin": date_filters["date_from"], "end": date_filters["date_to"]}}
        for advert_id in advert_ids
    ]


@op
def make_ad_info_payload(advert_ids: List[int]) -> List[int]:
    return advert_ids


def chunked(iterable: List[int], size: int):
    """Генератор чанков фиксированной длины."""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk


def prev_hour_mskt(msk_dt: datetime) -> datetime:
    return (msk_dt - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
