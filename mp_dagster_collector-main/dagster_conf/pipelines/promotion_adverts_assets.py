import uuid
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from dagster import asset, RetryPolicy, Failure
from dagster_conf.resources.wb_client import wildberries_client
from dagster_conf.resources.pg_resource import postgres_resource

from src.db.bronze.models import WbAdvPromotionAdverts1d, WbAdvPromotionAdverts1h
from src.db.silver.models import (
    SilverWbAdvCampaigns1d,
    SilverWbAdvCampaigns1h,
    SilverWbAdvProductRates1d,
    SilverWbAdvProductRates1h,
)
from dagster_conf.pipelines.wb_bronze_ops import extract_metadata_from_headers
from dagster_conf.pipelines.utils import prev_hour_mskt

MSK = ZoneInfo("Europe/Moscow")
ACTIVE_STATUSES = {4, 9, 11}  # используется в 1d-режиме


def _parse_iso_any_tz(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


@asset(
    config_schema={"time_grain": str},  # '1d' | '1h'
    resource_defs={"wildberries_client": wildberries_client, "postgres": postgres_resource},
    required_resource_keys={"wildberries_client", "postgres"},
    key_prefix=["bronze"],
    name="wb_adv_promotion_adverts",
    description="Сохраняет ответ /adv/v1/promotion/adverts в bronze.{1d,1h}. "
                "IDs кампаний берём из silver.wb_adv_promotions_1h (и для 1d, и для 1h). "
                "Фолбэк к API /promotion/count при отсутствии срезов в silver.",
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
async def bronze_wb_adv_promotion_adverts(context) -> None:
    grain = context.op_config.get("time_grain", "1d")

    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    if not scheduled_iso:
        raise Failure("Missing tag 'dagster/scheduled_execution_time'")
    run_schedule_dttm = _parse_iso_any_tz(scheduled_iso).astimezone(MSK)

    run_uuid = context.run_id
    run_dttm = datetime.now(MSK)
    api_token_id = context.resources.wildberries_client.token_id

    # целевые даты
    if grain == "1h":
        target_hour = prev_hour_mskt(run_schedule_dttm)
        date_from = date_to = target_hour.date().isoformat()
        model = WbAdvPromotionAdverts1h
    elif grain == "1d":
        day_start = (run_schedule_dttm - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end   = day_start + timedelta(days=1)
        date_from = date_to = day_start.date().isoformat()
        model = WbAdvPromotionAdverts1d
    else:
        raise Failure(f"Unsupported time_grain: {grain}")

    # 1) получаем advert_ids из silver.wb_adv_promotions_1h
    async with context.resources.postgres() as session:
        company_id = (await session.execute(
            text("SELECT company_id FROM core.tokens WHERE token_id = :tid"),
            {"tid": api_token_id},
        )).scalar_one()

        advert_ids: list[int] = []

        if grain == "1h":
            # cur vs prev-hour включающие «архивные, если вчера ещё работали»
            rows = await session.execute(text("""
                WITH cur AS (
                  SELECT advert_id, advert_status
                    FROM silver.wb_adv_promotions_1h
                   WHERE company_id = :cid AND business_dttm = :h
                ),
                prev AS (
                  SELECT advert_id, advert_status
                    FROM silver.wb_adv_promotions_1h
                   WHERE company_id = :cid AND business_dttm = (:h - INTERVAL '1 hour')
                )
                SELECT DISTINCT c.advert_id
                  FROM cur c
             LEFT JOIN prev p USING (advert_id)
                 WHERE c.advert_status <> 7
                    OR (c.advert_status = 7 AND COALESCE(p.advert_status, 7) <> 7)
            """), {"cid": company_id, "h": target_hour})
            advert_ids = [r[0] for r in rows.fetchall()]

            if not advert_ids:
                # фолбэк: возьмём последний доступный час
                last_hour = (await session.execute(text("""
                    SELECT MAX(business_dttm) FROM silver.wb_adv_promotions_1h WHERE company_id=:cid
                """), {"cid": company_id})).scalar()
                if last_hour:
                    context.log.warning("[promotion_adverts_1h] Нет данных за %s, используем %s",
                                        target_hour.isoformat(), last_hour.isoformat())
                    rows = await session.execute(text("""
                        WITH cur AS (
                          SELECT advert_id, advert_status
                            FROM silver.wb_adv_promotions_1h
                           WHERE company_id = :cid AND business_dttm = :h
                        ),
                        prev AS (
                          SELECT advert_id, advert_status
                            FROM silver.wb_adv_promotions_1h
                           WHERE company_id = :cid AND business_dttm = (:h - INTERVAL '1 hour')
                        )
                        SELECT DISTINCT c.advert_id
                          FROM cur c
                     LEFT JOIN prev p USING (advert_id)
                         WHERE c.advert_status <> 7
                            OR (c.advert_status = 7 AND COALESCE(p.advert_status, 7) <> 7)
                    """), {"cid": company_id, "h": last_hour})
                    advert_ids = [r[0] for r in rows.fetchall()]

        else:  # 1d
            # логика «за сутки: был <>7 ИЛИ была смена статуса»
            rows = await session.execute(text("""
                WITH day_rows AS (
                  SELECT advert_id, advert_status, business_dttm
                    FROM silver.wb_adv_promotions_1h
                   WHERE company_id = :cid AND business_dttm >= :start_dt AND business_dttm < :end_dt
                ),
                lagged AS (
                  SELECT advert_id,
                         advert_status,
                         LAG(advert_status) OVER (PARTITION BY advert_id ORDER BY business_dttm) AS prev_status
                    FROM day_rows
                )
                SELECT DISTINCT advert_id
                  FROM (
                        SELECT advert_id FROM day_rows WHERE advert_status <> 7
                        UNION
                        SELECT advert_id FROM lagged
                         WHERE prev_status IS NOT NULL AND prev_status <> advert_status
                  ) q
            """), {"cid": company_id, "start_dt": day_start, "end_dt": day_end})
            advert_ids = [r[0] for r in rows.fetchall()]

    # 2) фолбэк к API, если silver ничего не дал (актуально в бэкфиллах)
    if not advert_ids:
        context.log.warning("[promotion_adverts_%s] В silver нет кампаний за %s — фолбэк к /promotion/count",
                            grain, date_from)
        async with context.resources.wildberries_client as client:
            raw = await client.get_advert_list_depr(date_from, date_from)
        advert_ids = [a["advertId"] for a in (raw or []) if a.get("status") in ACTIVE_STATUSES]

    if not advert_ids:
        context.log.info("[promotion_adverts_%s] Нет кампаний за %s — пропускаю", grain, date_from)
        return

    # 3) собственно запрос /adv/v1/promotion/adverts
    async with context.resources.wildberries_client as client:
        raw_data, headers = await client.fetch_ad_info(advert_ids)
        _, response_dttm = extract_metadata_from_headers(headers)
        response_code = int(headers.get("status", 200))

    record = {
        "api_token_id":       api_token_id,
        "run_uuid":           run_uuid,
        "run_dttm":           run_dttm,
        "request_uuid":       uuid.uuid4(),
        "request_dttm":       run_dttm,
        "request_parameters": {"advert_ids": advert_ids, "date_from": date_from, "date_to": date_to},
        "request_body":       None,
        "response_dttm":      response_dttm,
        "response_code":      response_code,
        "response_body":      json.dumps(raw_data, ensure_ascii=False),
    }

    async with context.resources.postgres() as session:
        await session.execute(model.__table__.insert().values(**record))
        await session.commit()

    context.log.info("[promotion_adverts_%s] записано: ids=%d", grain, len(advert_ids))



@asset(
    config_schema={"time_grain": str},  # "1d" или "1h"
    deps=["wb_adv_promotion_adverts"],
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    key_prefix=["silver"],
    name="wb_adv_campaigns",
    description="Переносит поля кампаний из bronze → silver, зависит от time_grain (1d/1h)",
)
async def silver_wb_adv_campaigns(context):
    grain = context.op_config.get("time_grain", "1d")
    run_id = context.run_id

    # Выбор нужной модели
    if grain == "1d":
        bronze_model = WbAdvPromotionAdverts1d
        silver_model = SilverWbAdvCampaigns1d
    elif grain == "1h":
        bronze_model = WbAdvPromotionAdverts1h
        silver_model = SilverWbAdvCampaigns1h
    else:
        raise ValueError(f"Unsupported time_grain: {grain}")

    async with context.resources.postgres() as session:
        result = await session.execute(
            select(bronze_model).where(bronze_model.run_uuid == run_id)
        )
        bronze_rows = result.scalars().all()

        total = 0
        for bronze in bronze_rows:
            batch = json.loads(bronze.response_body) or []

            for entry in batch:
                rec = {
                    "advert_id":         entry.get("advertId"),
                    "request_uuid":      bronze.request_uuid,
                    "run_dttm":          bronze.run_dttm,
                    "name":              entry.get("name"),
                    "create_time":       entry.get("createTime"),
                    "change_time":       entry.get("changeTime"),
                    "start_time":        entry.get("startTime"),
                    "end_time":          entry.get("endTime"),
                    "status":            entry.get("status"),
                    "type":              entry.get("type"),
                    "payment_type":      entry.get("paymentType"),
                    "daily_budget":      entry.get("dailyBudget"),
                    "search_pluse_state": entry.get("searchPluseState"),
                }

                upsert = pg_insert(silver_model).values(**rec)
                upsert = upsert.on_conflict_do_update(
                    index_elements=[
                        silver_model.advert_id,
                        silver_model.request_uuid,
                        silver_model.run_dttm,
                    ],
                    set_=rec,
                )
                await session.execute(upsert)
                total += 1

        await session.commit()

    context.log.info(f"[silver_wb_adv_campaigns] перенесено {total} записей в {silver_model.__tablename__}")


@asset(
    config_schema={"time_grain": str},  # "1d" или "1h"
    deps=["wb_adv_promotion_adverts"],
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    key_prefix=["silver"],
    name="wb_adv_product_rates",
    description="Переносит ставки из bronze → silver (параметризованный ассет: 1d/1h)",
)
async def silver_wb_adv_product_rates(context):
    grain = context.op_config.get("time_grain", "1d")
    run_id = context.run_id

    # Выбор нужной модели
    if grain == "1d":
        bronze_model = WbAdvPromotionAdverts1d
        silver_model = SilverWbAdvProductRates1d
    elif grain == "1h":
        bronze_model = WbAdvPromotionAdverts1h
        silver_model = SilverWbAdvProductRates1h
    else:
        raise ValueError(f"Unsupported time_grain: {grain}")

    async with context.resources.postgres() as session:
        result = await session.execute(
            select(bronze_model).where(bronze_model.run_uuid == run_id)
        )
        bronze_rows = result.scalars().all()
        total_inserted = 0

        for bronze in bronze_rows:
            batch = json.loads(bronze.response_body) or []

            for entry in batch:
                advert_id = entry.get("advertId")
                campaign_type = entry.get("type")
                status = entry.get("status")
                params_list = []

                # Авто-кампании (type == 8)
                if campaign_type == 8:
                    auto = entry.get("autoParams") or {}
                    base = {
                        "advert_status":   status,
                        "advert_type":     campaign_type,
                        "cpm_first":       auto.get("cpm"),
                        "active_carousel": auto.get("active", {}).get("carousel"),
                        "active_recom":    auto.get("active", {}).get("recom"),
                        "active_booster":  auto.get("active", {}).get("booster"),
                        "subject_id":      auto.get("subject", {}).get("id"),
                        "cpm_catalog":     None,
                        "cpm_search":      None,
                    }
                    for m in auto.get("nmCPM", []):
                        params_list.append({
                            **base,
                            "nm_id":       m.get("nm"),
                            "cpm_current": m.get("cpm"),
                        })

                # Аукционы (type == 9)
                elif campaign_type == 9:
                    for ap in entry.get("auction_multibids", []):
                        params_list.append({
                            "nm_id":         ap.get("nm"),
                            "cpm_current":   ap.get("bid"),
                            "cpm_catalog":   None,
                            "cpm_search":    None,
                            "cpm_first":     None,
                            "advert_status": status,
                            "advert_type":   campaign_type,
                            "active_carousel": None,
                            "active_recom":    None,
                            "active_booster":  None,
                            "subject_id":      None,
                        })
                    for up in entry.get("unitedParams", []):
                        base = {
                            "advert_status": status,
                            "advert_type":   campaign_type,
                            "cpm_catalog":   up.get("catalogCPM"),
                            "cpm_search":    up.get("searchCPM"),
                            "subject_id":    up.get("subject", {}).get("id"),
                        }
                        for nm in up.get("nms", []):
                            params_list.append({**base, "nm_id": nm})

                # Прочие кампании
                else:
                    for ap in entry.get("params", []):
                        price = ap.get("price")
                        subject_id = ap.get("subjectId")
                        nms_list = ap.get("nms") or []
                        if isinstance(nms_list, dict):
                            nms_list = [nms_list]
                        for nm_entry in nms_list:
                            params_list.append({
                                "nm_id":         nm_entry.get("nm"),
                                "cpm_current":   price,
                                "cpm_catalog":   None,
                                "cpm_search":    None,
                                "cpm_first":     None,
                                "advert_status": status,
                                "advert_type":   campaign_type,
                                "active_carousel": None,
                                "active_recom":    None,
                                "active_booster":  None,
                                "subject_id":      subject_id,
                            })

                # Запись
                for p in params_list:
                    company_id = (
                        await session.execute(
                            text("SELECT company_id FROM core.tokens WHERE token_id = :tid"),
                            {"tid": bronze.api_token_id},
                        )
                    ).scalar_one()

                    rec = {
                        "company_id":   company_id,
                        "request_uuid": bronze.request_uuid,
                        "inserted_at":  bronze.inserted_at,
                        "run_dttm":     bronze.run_dttm,
                        "advert_id":    advert_id,
                        **p,
                    }

                    upsert = pg_insert(silver_model).values(**rec)
                    upsert = upsert.on_conflict_do_update(
                        index_elements=[
                            silver_model.run_dttm,
                            silver_model.advert_id,
                            silver_model.nm_id,
                        ],
                        set_=rec,
                    )
                    await session.execute(upsert)
                    total_inserted += 1

        await session.commit()

    context.log.info(f"[silver_wb_adv_product_rates] перенесено {total_inserted} записей в {silver_model.__tablename__}")