from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any
from sqlalchemy import text as sa_text, bindparam, Integer, DateTime

MSK = ZoneInfo("Europe/Moscow")


async def build_fullstats_params(context, business_dttm: datetime, company_id: int) -> dict:
    """
    Отбираем рекламные кампании и запрашиваем fullstats на дату.
    """
    biz_msk = business_dttm.astimezone(MSK) if business_dttm.tzinfo else business_dttm.replace(tzinfo=MSK)
    d_iso = biz_msk.date().isoformat()

    start_msk = datetime.combine(biz_msk.date(), time.min, tzinfo=MSK)

    params = {
        "cid": company_id, 
        "start_msk": start_msk, 
    }

    # Берем кампании, которые активны сейчас или меняли статус в day_start или позже
    q = sa_text("""
        SELECT DISTINCT advert_id
          FROM silver_v2.wb_adv_promotions_1d
         WHERE company_id = :cid
           AND business_dttm >= :start_msk
           AND advert_id IS NOT NULL
           AND (advert_status IN (7, 9, 11) OR change_time >= :start_msk);
    """).bindparams(
        bindparam("cid", type_=Integer()),
        bindparam("start_msk", type_=DateTime(timezone=True)),
    )

    context.log.info(f"""fullstats: Выполняем SQL: 
        {q}, 
        params={params}
    """)

    async with context.resources.postgres() as session:
        rows = await session.execute(q, params)
        ids = [int(r[0]) for r in rows.fetchall()]

    context.log.info(
        "fullstats: собрано %d advert_id на %s (MSK) из silver_v2.wb_adv_promotions_1d",
        len(ids), d_iso
    )

    return {"ids": ids, "begin_date": d_iso, "end_date": d_iso}


async def build_keywords_params(context, business_dttm: datetime, company_id: int) -> Dict[str, Any]:
    """
    Берём advert_ids так же, как для fullstats (из silver_v2.wb_adv_promotions_1d),
    и маппим поля под клиентский метод fetch_keywords_batch(...).
    """
    p = await build_fullstats_params(context, business_dttm, company_id)
    return {
        "advert_ids": p["ids"],
        "date_from":  p["begin_date"],
        "date_to":    p["end_date"],
    }
