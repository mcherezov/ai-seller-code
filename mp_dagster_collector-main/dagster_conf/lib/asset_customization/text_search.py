from __future__ import annotations
from typing import Any, Dict
from datetime import datetime
import sqlalchemy as sa

DEFAULT_COMMON_PARAMS = {
    "ab_testing": "false",
    "appType":    32,
    "curr":       "rub",
    "dest":       -1257786,
    "lang":       "ru",
    "resultset":  "catalog",
    "sort":       "popular",
    "uclusters":  0,
    "uiv":        0,
}

async def build_text_search_requests(context, business_dttm: datetime, company_id: int) -> Dict[str, Any]:
    """
    Возвращает ОДИН словарь именованных параметров для метода
    wildberries_client.fetch_www_text_search_page(...).
    Ключи ДОЛЖНЫ совпадать с сигнатурой метода: query, page, dest, app_type, ...
    """
    target_date = business_dttm.date()

    sql = sa.text("""
        SELECT search_term
        FROM core.search_results_tracking
        LIMIT 1
    """)

    # sql = sa.text("""
    #     SELECT kws.keyword
    #     FROM silver_v2.wb_adv_keyword_stats_1d AS kws
    #     WHERE kws.company_id = :cid
    #       AND kws.business_dttm::date = :biz_date
    #     ORDER BY kws.clicks DESC NULLS LAST
    #     LIMIT 1
    # """)

    keyword = None
    async with context.resources.postgres() as session:
        res = await session.execute(sql, {"cid": company_id, "biz_date": target_date})
        row = res.first()
        if row:
            # row может быть tuple/Row; берём через _mapping для надёжности
            m = getattr(row, "_mapping", None)
            keyword = (m["keyword"] if m and "keyword" in m else row[0])

    if not keyword:
        # фолбэк, чтобы не уходить в TypeError/500, если ключей нет
        keyword = "wildberries"  # можно заменить на твой дефолт/индикатор пустого набора

    params = {
        "query": str(keyword),
        "page": 1,
        # остальное — как в старом ассете
        "dest":       DEFAULT_COMMON_PARAMS["dest"],
        "app_type":   DEFAULT_COMMON_PARAMS["appType"],
        "lang":       DEFAULT_COMMON_PARAMS["lang"],
        "curr":       DEFAULT_COMMON_PARAMS["curr"],
        "resultset":  DEFAULT_COMMON_PARAMS["resultset"],
        "sort":       DEFAULT_COMMON_PARAMS["sort"],
        "uclusters":  DEFAULT_COMMON_PARAMS["uclusters"],
        "uiv":        DEFAULT_COMMON_PARAMS["uiv"],
        "base_url":   "https://search.wb.ru/exactmatch/ru/common/v14/search",
        # token_override подставит фабрика автоматически, т.к. метод его принимает
    }
    return params
