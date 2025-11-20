from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any
from sqlalchemy import text as sa_text, bindparam, Integer, DateTime

MSK = ZoneInfo("Europe/Moscow")


async def build_product_params(context, business_dttm: datetime, company_id: int) -> dict:
    """
    Выбираем DISTINCT nm_id из silver_v2.wb_sales_funnels_1d за сутки для конкретного company_id
    """
    biz_msk = business_dttm.astimezone(MSK) if business_dttm.tzinfo else business_dttm.replace(tzinfo=MSK)

    day_start = datetime.combine(biz_msk.date(), time.min, tzinfo=MSK)
    day_end   = day_start + timedelta(days=1)

    q = sa_text("""
        SELECT DISTINCT nm_id
          FROM silver_v2.wb_sales_funnels_1d
         WHERE company_id = :cid
           AND business_dttm >= :start_msk
           AND business_dttm <  :end_msk
    """).bindparams(
        bindparam("cid", type_=Integer()),
        bindparam("start_msk", type_=DateTime(timezone=True)),
        bindparam("end_msk",   type_=DateTime(timezone=True)),
    )

    async with context.resources.postgres() as session:
        rows = await session.execute(q, {"cid": company_id, "start_msk": day_start, "end_msk": day_end})
        ids = [int(r[0]) for r in rows.fetchall()]

    context.log.info(
        "[build_product_params] собрано %d nm_id на %s (MSK) из silver_v2.wb_sales_funnels_1d",
        len(ids), biz_msk
    )

    return {
        "nmIds": ids, 
        "date": day_start.strftime('%Y-%m-%d')
    }


async def build_product_search_texts_params(context, business_dttm: datetime, company_id: int) -> Dict[str, Any]:
    """
    Берём nm_ids из silver_v2.wb_sales_funnels_1d,
    и маппим поля под клиентский метод fetch_product_search_texts_batch(...)
    """
    p = await build_product_params(context, business_dttm, company_id)
    return {
        "payload": {
            "nmIds": p["nmIds"],
            "currentPeriod": {
                "start": p["date"], 
                "end": p["date"]
            },
            "orderBy": {"field": "openCard", "mode": "desc"},
            "limit": 30,
        },
    }
