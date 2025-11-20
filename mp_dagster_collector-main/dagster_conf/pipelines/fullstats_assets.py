from dagster import asset, AssetIn, AssetKey, RetryPolicy, Failure
import asyncio
from aiohttp import ClientResponseError
from datetime import datetime, timedelta, timezone, time
from zoneinfo import ZoneInfo
from typing import Union
import uuid
import json
import sqlalchemy as sa
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from dagster_conf.resources.wb_client import wildberries_client
from dagster_conf.resources.pg_resource import postgres_resource
from dagster_conf.pipelines.wb_bronze_ops import extract_metadata_from_headers
from src.db.bronze.models import WbAdvFullstats1d, WbAdvFullstats1h
from src.db.silver.models import (
    SilverWbAdvProductStats1d, SilverWbAdvProductStats1h, SilverWbAdvProductPositions1d, SilverWbAdvProductPositions1h
)
from dagster_conf.pipelines.utils import prev_hour_mskt


# Максимальное число ID за один запрос
CHUNK_SIZE = 100
# Пауза между чанками (секунды)
RATE_LIMIT_DELAY = 60
ACTIVE_STATUSES = {4, 9, 11}
MSK = ZoneInfo("Europe/Moscow")
MAX_SKEW = timedelta(minutes=15)


def chunked(it, size):
    """Генератор списков длины size из итерации it."""
    for i in range(0, len(it), size):
        yield it[i : i + size]


async def safe_fetch(client, payload, context):
    """
    Вызывает client.fetch_ad_stats и при 429 повторяет запрос после паузы.
    Гарантированно возвращает (result, headers).
    """
    try:
        res = await client.fetch_ad_stats(payload)
        if isinstance(res, tuple) and len(res) == 2:
            data, headers = res
        else:
            data, headers = res, {}

        try:
            h = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
        except Exception:
            h = {}
        context.log.debug("[fullstats] headers(raw): %s", json.dumps(h, ensure_ascii=False))
        context.log.info(
            "[fullstats] date=%s | x-request-id=%s | status=%s | content-length=%s",
            h.get("date"),
            h.get("x-request-id") or h.get("x-response-id"),
            h.get(":status") or h.get("status"),
            h.get("content-length"),
        )

        return data, headers

    except ClientResponseError as e:
        if e.status == 429:
            try:
                eh = {str(k).lower(): str(v) for k, v in (getattr(e, "headers", {}) or {}).items()}
            except Exception:
                eh = {}
            context.log.warning("429 Too Many Requests – ждём %s сек и повторяем… headers=%s",
                                RATE_LIMIT_DELAY, json.dumps(eh, ensure_ascii=False))
            await asyncio.sleep(RATE_LIMIT_DELAY)
            # повтор
            res = await client.fetch_ad_stats(payload)
            if isinstance(res, tuple) and len(res) == 2:
                return res
            return res, {}
        else:
            raise


def business_hour_start(run_dttm_msk: datetime) -> datetime:
    """
    Начало бизнес-периода для current-state (1h):
    date_trunc('hour', run_dttm) - 1 hour, в МСК.
    """
    base = run_dttm_msk.replace(minute=0, second=0, microsecond=0)
    return base - timedelta(hours=1)


def _parse_iso_any_tz(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


@asset(
    config_schema={"time_grain": str},  # "1d" или "1h"
    resource_defs={"wildberries_client": wildberries_client, "postgres": postgres_resource},
    required_resource_keys={"wildberries_client", "postgres"},
    key_prefix=["bronze"],
    name="wb_adv_fullstats",
    description="Сохраняет fullstats в bronze.{1d,1h}",
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
async def bronze_wb_adv_fullstats(context) -> None:
    time_grain = context.op_config.get("time_grain", "1d")
    run_uuid = context.run_id

    # ── плановое время запуска (МСК) ────────────────────────────────────────────
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    if not scheduled_iso:
        raise Failure("Missing tag 'dagster/scheduled_execution_time'")

    run_schedule_dttm = _parse_iso_any_tz(scheduled_iso).astimezone(MSK)
    run_dttm = datetime.now(MSK)

    api_token_id = context.resources.wildberries_client.token_id

    # ── параметры периода ───────────────────────────────────────────────────────
    if time_grain == "1d":
        # вчера и позавчера (окна в МСК)
        day_1_start = (run_schedule_dttm - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_2_start = day_1_start - timedelta(days=1)
        day_1_end   = day_1_start + timedelta(days=1)
        day_2_end   = day_2_start + timedelta(days=1)

        date_1 = day_1_start.date().isoformat()  # вчера
        date_2 = day_2_start.date().isoformat()  # позавчера

        business_dttm_static = day_1_start       # якорь суточного бизнес-периода = начало вчерашних суток (МСК)
        model = WbAdvFullstats1d

    elif time_grain == "1h":
        # целевой час — предыдущий к плановому
        dt_hour = prev_hour_mskt(run_schedule_dttm)
        date_1 = dt_hour.date().isoformat()
        date_2 = date_1  # не используется в 1h
        business_dttm_static = dt_hour
        model = WbAdvFullstats1h
    else:
        raise ValueError(f"Invalid time_grain: {time_grain}")

    context.log.info(
        f"[bronze_wb_adv_fullstats_{time_grain}] "
        f"run_uuid={run_uuid} run_schedule_dttm={run_schedule_dttm} run_dttm(actual)={run_dttm} "
        f"business_dttm={business_dttm_static}"
    )

    # ────────────────────────────────────────────────────────────────────────────
    # Получение списка кампаний
    # ────────────────────────────────────────────────────────────────────────────
    if time_grain == "1h":
        # Берём id кампаний из silver.wb_adv_promotions_1h по правилу H-1 (с фолбэком)
        async with context.resources.postgres() as session:
            company_id = (await session.execute(
                text("SELECT company_id FROM core.tokens WHERE token_id = :tid"),
                {"tid": api_token_id},
            )).scalar_one()

            target_hour = prev_hour_mskt(run_schedule_dttm)

            # есть ли записи в silver за целевой час?
            exists = (await session.execute(text("""
                SELECT 1
                  FROM silver.wb_adv_promotions_1h
                 WHERE company_id = :cid
                   AND business_dttm = :h
                 LIMIT 1
            """), {"cid": company_id, "h": target_hour})).scalar()

            if not exists:
                latest_hour = (await session.execute(text("""
                    SELECT MAX(business_dttm)
                      FROM silver.wb_adv_promotions_1h
                     WHERE company_id = :cid
                """), {"cid": company_id})).scalar()
                if latest_hour:
                    context.log.warning(
                        "[fullstats_1h] Нет данных в silver за %s, используем последний срез %s",
                        target_hour.isoformat(), latest_hour.isoformat()
                    )
                    target_hour = latest_hour
                else:
                    context.log.warning(
                        "[fullstats_1h] В silver.wb_adv_promotions_1h нет ни одного среза для company_id=%s",
                        company_id
                    )
                    return

            rows = await session.execute(text("""
                WITH cur AS (
                    SELECT advert_id, advert_status
                      FROM silver.wb_adv_promotions_1h
                     WHERE company_id = :cid
                       AND business_dttm = :h
                ),
                prev AS (
                    SELECT advert_id, advert_status
                      FROM silver.wb_adv_promotions_1h
                     WHERE company_id = :cid
                       AND business_dttm = (:h - INTERVAL '1 hour')
                )
                SELECT DISTINCT c.advert_id
                  FROM cur c
                  LEFT JOIN prev p USING (advert_id)
                 WHERE c.advert_status <> 7
                    OR (c.advert_status = 7 AND COALESCE(p.advert_status, 7) <> 7)
            """), {"cid": company_id, "h": target_hour})
            ids_day1 = {r[0] for r in rows.fetchall()}
            ids_day2 = set()

    else:
        # Новая логика для 1d: проверяем ОТДЕЛЬНО вчерашние и позавчерашние 24 часа (МСК)
        async with context.resources.postgres() as session:
            company_id = (await session.execute(
                text("SELECT company_id FROM core.tokens WHERE token_id = :tid"),
                {"tid": api_token_id},
            )).scalar_one()

            # общее правило включения кампаний за сутки:
            #  - был хоть один час со статусом <> 7
            #  - ИЛИ фиксировалась смена статуса в пределах суток
            q_ids_day = text("""
                WITH day_rows AS (
                    SELECT advert_id, advert_status, business_dttm
                      FROM silver.wb_adv_promotions_1h
                     WHERE company_id = :cid
                       AND business_dttm >= :start_dt
                       AND business_dttm <  :end_dt
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
            """)

            rows1 = await session.execute(q_ids_day, {"cid": company_id, "start_dt": day_1_start, "end_dt": day_1_end})
            rows2 = await session.execute(q_ids_day, {"cid": company_id, "start_dt": day_2_start, "end_dt": day_2_end})

            ids_day1 = {r[0] for r in rows1.fetchall()}
            ids_day2 = {r[0] for r in rows2.fetchall()}

        # if not (ids_day1 or ids_day2):
        #     context.log.warning(
        #         "[fullstats_1d] В silver.wb_adv_promotions_1h за окна %s..%s и %s..%s не найдено ни одной кампании",
        #         day_1_start.isoformat(), day_1_end.isoformat(), day_2_start.isoformat(), day_2_end.isoformat()
        #     )
        #     return

        # --- Fallback к API, если в silver пусто по одним/обоим суткам ---
        need_fallback_d1 = not ids_day1
        need_fallback_d2 = not ids_day2

        if need_fallback_d1 or need_fallback_d2:
            context.log.warning(
                "[fullstats_1d] Нет данных в silver.wb_adv_promotions_1h за сутки "
                "(d1=%s, d2=%s) — используем фолбэк к API /adv/v1/promotion/count",
                date_1, date_2
            )
            # локально открываем клиент только для фолбэка
            async with context.resources.wildberries_client as client:
                if need_fallback_d1:
                    raw_day1 = await client.get_advert_list_depr(date_1, date_1)
                    ids_day1 = {
                        a["advertId"]
                        for a in (raw_day1 or [])
                        if a.get("status") in ACTIVE_STATUSES  # {4,9,11}
                    }

                if need_fallback_d2:
                    raw_day2 = await client.get_advert_list_depr(date_2, date_2)
                    ids_day2 = {
                        a["advertId"]
                        for a in (raw_day2 or [])
                        if a.get("status") in ACTIVE_STATUSES  # {4,9,11}
                    }

            if not (ids_day1 or ids_day2):
                context.log.warning(
                    "[fullstats_1d] Фолбэк к API не вернул активных кампаний за %s или %s — пропускаю загрузку",
                    date_1, date_2
                )
                return

    # деление на группы: обе даты / только вчера / только позавчера
    ids_both      = ids_day1 & ids_day2
    ids_only_day1 = ids_day1 - ids_both
    ids_only_day2 = ids_day2 - ids_both

    context.log.info(
        "[fullstats_%s] |both|=%s |only_d1|=%s |only_d2|=%s",
        time_grain, len(ids_both), len(ids_only_day1), len(ids_only_day2)
    )

    # ────────────────────────────────────────────────────────────────────────────
    # HTTP-запросы fullstats и запись в бронзу
    # ────────────────────────────────────────────────────────────────────────────
    async with context.resources.wildberries_client as client:
        async def process_chunk_group(ids, make_payload):
            if not ids:
                return
            for chunk in chunked(list(ids), CHUNK_SIZE):
                payload = make_payload(chunk)

                # момент отправки (MSK)
                request_dttm = datetime.now(MSK)

                # HTTP-запрос
                context.log.debug("sample payload: %s", json.dumps(payload[:2], ensure_ascii=False))
                raw_list, headers = await safe_fetch(client, payload, context)

                receive_dttm = datetime.now(MSK)
                _, response_dttm = extract_metadata_from_headers(headers)
                response_code = int((headers or {}).get("status", 200))

                error_text = (headers or {}).get("wb-error")

                # ── проверка "15 минут" только для 1h ────────────────────────────
                if time_grain == "1h":
                    if run_dttm.tzinfo is None or (response_dttm and response_dttm.tzinfo is None):
                        context.log.error(
                            "[fullstats_1h] tzinfo отсутствует: run_dttm=%r, response_dttm=%r; запись пропущена",
                            run_dttm, response_dttm
                        )
                        await asyncio.sleep(RATE_LIMIT_DELAY)
                        continue

                    msk_run  = run_dttm.astimezone(MSK)
                    msk_resp = (response_dttm or request_dttm).astimezone(MSK)
                    delta = msk_resp - msk_run
                    if delta > MAX_SKEW:
                        context.log.warning(
                            "[fullstats_1h] Пропуск записи: response_dttm=%s (MSK=%s) > run_dttm=%s (MSK=%s) на %s (> %s). "
                            "run_uuid=%s, chunk_size=%d",
                            response_dttm, msk_resp, run_dttm, msk_run, delta, MAX_SKEW, run_uuid, len(chunk)
                        )
                        await asyncio.sleep(RATE_LIMIT_DELAY)
                        continue

                record_common = {
                    "run_uuid": run_uuid,
                    "run_dttm": run_dttm,
                    "run_schedule_dttm": run_schedule_dttm,
                    "business_dttm": business_dttm_static,
                    "request_uuid": uuid.uuid4(),
                    "request_dttm": request_dttm,
                    "request_parameters": {"wb_error": error_text} if (response_code >= 400 and error_text) else None,
                    "request_body": payload,                       # JSONB
                    "response_dttm": response_dttm or request_dttm,
                    "receive_dttm": receive_dttm,
                    "response_code": response_code,
                    "response_body": json.dumps(raw_list),         # TEXT
                    "api_token_id": api_token_id,
                }

                # защита: берём только те ключи, которые реально есть в модели
                cols = {c.name for c in model.__table__.columns}
                payload_for_model = {k: v for k, v in record_common.items() if k in cols}

                async with context.resources.postgres() as session:
                    await session.execute(model.__table__.insert().values(**payload_for_model))
                    await session.commit()

                context.log.info(
                    f"[bronze_wb_adv_fullstats_{time_grain}] записан чанк на {len(chunk)} кампаний, run_uuid={run_uuid}"
                )
                await asyncio.sleep(RATE_LIMIT_DELAY)

        if time_grain == "1d":
            await process_chunk_group(
                ids_both,
                lambda chunk: [{"id": aid, "dates": [date_1, date_2]} for aid in chunk],
            )

        # B) кампании только вчера (для 1d) ИЛИ весь набор ids_day1 (для 1h)
        await process_chunk_group(
            ids_only_day1 if time_grain == "1d" else ids_day1,
            lambda chunk: [{"id": aid, "dates": [date_1]} for aid in chunk],
        )

        # C) кампании только позавчера (только 1d)
        if time_grain == "1d":
            await process_chunk_group(
                ids_only_day2,
                lambda chunk: [{"id": aid, "dates": [date_2]} for aid in chunk],
            )


async def safe_upsert(session, stmt, context):
    try:
        await session.execute(stmt)
    except Exception as e:
        raise


@asset(
    config_schema={"time_grain": str},
    deps=[bronze_wb_adv_fullstats],
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    key_prefix=["silver"],
    name="wb_adv_fullstats",
    description="Переносит свежие fullstats из bronze → silver (stats и positions включая boosterStats)",
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
async def silver_wb_adv_fullstats(context):
    from src.db.silver.models import (
        SilverWbAdvProductStats1d,
        SilverWbAdvProductStats1h,
        SilverWbAdvProductPositions1d,
        SilverWbAdvProductPositions1h,
    )
    from src.db.bronze.models import WbAdvFullstats1d, WbAdvFullstats1h

    time_grain = context.op_config.get("time_grain", "1d")
    is_hourly = (time_grain == "1h")
    run_id = context.run_id

    # плановое время запуска (для логов)
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    raw_dt = datetime.fromisoformat(scheduled_iso) if scheduled_iso else datetime.now(timezone.utc)
    run_dttm = raw_dt.astimezone(MSK)
    context.log.info(f"silver_wb_adv_fullstats start: time_grain={time_grain}, run_dttm={run_dttm}")

    # выбираем нужные модели
    if is_hourly:
        bronze_model           = WbAdvFullstats1h
        silver_stats_model     = SilverWbAdvProductStats1h
        silver_positions_model = SilverWbAdvProductPositions1h
    else:
        bronze_model           = WbAdvFullstats1d
        silver_stats_model     = SilverWbAdvProductStats1d
        silver_positions_model = SilverWbAdvProductPositions1d

    def _to_msk(dt_like: Union[str, datetime]) -> datetime:
        """
        Принимает ISO-строку или datetime (aware/naive) и возвращает datetime в Europe/Moscow.
        """
        if isinstance(dt_like, datetime):
            dt = dt_like
        elif isinstance(dt_like, str):
            s = dt_like.strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
        else:
            raise TypeError(f"unsupported type: {type(dt_like)!r}")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(MSK)

    async with context.resources.postgres() as session:
        # бронзовые чанки этого запуска
        result = await session.execute(
            select(bronze_model).where(bronze_model.run_uuid == run_id)
        )
        bronze_rows = result.scalars().all()
        context.log.info("bronze chunks: %s", len(bronze_rows))

        total_rows = 0

        for bronze in bronze_rows:
            try:
                # company_id по токену
                company_id = (await session.execute(
                    text("SELECT company_id FROM core.tokens WHERE token_id = :token_id"),
                    {"token_id": bronze.api_token_id}
                )).scalar_one()

                raw_list = json.loads(bronze.response_body) or []

                # общие для апдейта поля ODS
                response_dttm_bronze = _to_msk(bronze.response_dttm)
                request_dttm_bronze  = _to_msk(bronze.request_dttm)

                processed = 0
                for raw in raw_list:
                    advert_id = raw["advertId"]

                    # ────────────────────────────────────────────────────────────────
                    # 1) Обработка блока "days" → stats
                    # ────────────────────────────────────────────────────────────────
                    if is_hourly:
                        # целевой бизнес-час из бронзы обязателен
                        biz = bronze.business_dttm
                        if biz is None:
                            context.log.error("bronze.business_dttm is NULL, skip request_uuid=%s", bronze.request_uuid)
                            continue
                        biz = _to_msk(biz)

                        target_date = biz.date()
                        # ищем один день, соответствующий целевому часу
                        day = next((d for d in raw.get("days", []) if _to_msk(d["date"]).date() == target_date), None)
                        if not day:
                            context.log.info("skip hourly: no day for %s (advert_id=%s)", target_date.isoformat(), advert_id)
                            # позиции всё равно попробуем ниже из boosterStats
                        else:
                            day_dttm = _to_msk(day["date"])
                            day_start = day_dttm.replace(hour=0, minute=0, second=0, microsecond=0)

                            for app in day.get("apps", []):
                                app_type = app["appType"]
                                for nm in app.get("nm", []):
                                    nm_id = nm["nmId"]

                                    # кумулятив за день из текущего ответа
                                    cumul = {
                                        "views":   nm.get("views", 0),
                                        "clicks":  nm.get("clicks", 0),
                                        "cost":    nm.get("sum", 0.0),
                                        "carts":   nm.get("atbs", 0),
                                        "orders":  nm.get("orders", 0),
                                        "items":   nm.get("shks", 0),
                                        "revenue": nm.get("sum_price", 0.0),
                                    }

                                    # предыдущие часы текущего бизнес-дня < biz
                                    prev = await session.execute(text(f"""
                                        SELECT  COALESCE(SUM(views),   0) AS views,
                                                COALESCE(SUM(clicks),  0) AS clicks,
                                                COALESCE(SUM(cost),    0) AS cost,
                                                COALESCE(SUM(carts),   0) AS carts,
                                                COALESCE(SUM(orders),  0) AS orders,
                                                COALESCE(SUM(items),   0) AS items,
                                                COALESCE(SUM(revenue), 0) AS revenue
                                          FROM silver.{silver_stats_model.__tablename__}
                                         WHERE company_id   = :company_id
                                           AND advert_id    = :advert_id
                                           AND app_type     = :app_type
                                           AND nm_id        = :nm_id
                                           AND business_dttm >= :day_start
                                           AND business_dttm <  :current_biz
                                    """), {
                                        "company_id": company_id,
                                        "advert_id": advert_id,
                                        "app_type":  app_type,
                                        "nm_id":     nm_id,
                                        "day_start": day_start,
                                        "current_biz": biz,
                                    })
                                    prev_map = prev.mappings().one()
                                    delta = {
                                        "views":   int(cumul["views"]   - prev_map["views"]),
                                        "clicks":  int(cumul["clicks"]  - prev_map["clicks"]),
                                        "cost":    float(cumul["cost"]   - prev_map["cost"]),
                                        "carts":   int(cumul["carts"]   - prev_map["carts"]),
                                        "orders":  int(cumul["orders"]  - prev_map["orders"]),
                                        "items":   int(cumul["items"]   - prev_map["items"]),
                                        "revenue": float(cumul["revenue"] - prev_map["revenue"]),
                                    }

                                    rec = {
                                        "company_id":    company_id,
                                        "request_uuid":  bronze.request_uuid,
                                        "advert_id":     advert_id,
                                        "app_type":      app_type,
                                        "nm_id":         nm_id,
                                        "request_dttm":  bronze.request_dttm,
                                        "business_dttm": bronze.business_dttm,
                                        "response_dttm": bronze.response_dttm,
                                        **delta,
                                    }

                                    ins = pg_insert(silver_stats_model).values(**rec)
                                    upsert = ins.on_conflict_do_update(
                                        index_elements=[
                                            silver_stats_model.business_dttm,
                                            silver_stats_model.advert_id,
                                            silver_stats_model.app_type,
                                            silver_stats_model.nm_id,
                                        ],
                                        set_=rec,
                                        where=sa.and_(
                                            ins.excluded.response_dttm.isnot(None),
                                            sa.or_(
                                                silver_stats_model.response_dttm.is_(None),
                                                ins.excluded.response_dttm > silver_stats_model.response_dttm,
                                            ),
                                        ),
                                    )
                                    await session.execute(upsert)
                                    processed += 1
                    else:
                        # DAILY: обрабатываем все дни, которые пришли в ответе (вчера/позавчера и т.п.)
                        for day in raw.get("days", []):
                            day_dttm = _to_msk(day["date"])
                            biz_day_start = day_dttm.replace(hour=0, minute=0, second=0, microsecond=0)

                            for app in day.get("apps", []):
                                app_type = app["appType"]
                                for nm in app.get("nm", []):
                                    nm_id = nm["nmId"]
                                    metrics = {
                                        "views":   nm.get("views", 0),
                                        "clicks":  nm.get("clicks", 0),
                                        "cost":    nm.get("sum", 0.0),
                                        "carts":   nm.get("atbs", 0),
                                        "orders":  nm.get("orders", 0),
                                        "items":   nm.get("shks", 0),
                                        "revenue": nm.get("sum_price", 0.0),
                                    }

                                    rec = {
                                        "company_id":    company_id,
                                        "request_uuid":  bronze.request_uuid,
                                        "advert_id":     advert_id,
                                        "app_type":      app_type,
                                        "nm_id":         nm_id,
                                        "request_dttm":  request_dttm_bronze,
                                        "business_dttm": bronze.business_dttm,
                                        "response_dttm": bronze.response_dttm,
                                        **metrics,
                                    }

                                    ins = pg_insert(silver_stats_model).values(**rec)
                                    upsert = ins.on_conflict_do_update(
                                        index_elements=[
                                            silver_stats_model.business_dttm,
                                            silver_stats_model.advert_id,
                                            silver_stats_model.app_type,
                                            silver_stats_model.nm_id,
                                        ],
                                        set_=rec,
                                        where=sa.and_(
                                            ins.excluded.response_dttm.isnot(None),
                                            sa.or_(
                                                silver_stats_model.response_dttm.is_(None),
                                                ins.excluded.response_dttm > silver_stats_model.response_dttm,
                                            ),
                                        ),
                                    )
                                    await session.execute(upsert)
                                    processed += 1

                    # ────────────────────────────────────────────────────────────────
                    # 2) Обработка boosterStats → positions
                    # ────────────────────────────────────────────────────────────────
                    for raw_boost in raw.get("boosterStats", []):
                        bst_date = _to_msk(raw_boost["date"])

                        if is_hourly:
                            # целевой «ведомый» час берём из бронзы (он уже от scheduled tag)
                            target_hour = bronze.business_dttm  # tz-aware MSK, начало часа

                            # у WB часто date = конец окна (HH:00) → переводим в начало окна
                            bst_bucket = business_hour_start(bst_date)

                            # берём только записи, попадающие в нужный час
                            if bst_bucket != target_hour:
                                continue

                            rec_pos = {
                                "company_id": company_id,
                                "request_uuid": bronze.request_uuid,
                                "advert_id": advert_id,
                                "nm_id": raw_boost["nm"],
                                "date": bst_date,
                                "avg_position": raw_boost.get("avg_position"),
                                "business_dttm": target_hour,  # ← ключевой момент: строго из бронзы
                                "response_dttm": response_dttm_bronze,
                            }

                            ins_pos = pg_insert(silver_positions_model).values(**rec_pos)
                            upsert_pos = ins_pos.on_conflict_do_update(
                                index_elements=[
                                    silver_positions_model.request_uuid,
                                    silver_positions_model.advert_id,
                                    silver_positions_model.nm_id,
                                    silver_positions_model.date,
                                ],
                                set_=rec_pos,
                            )
                        else:
                            # DAILY: ключ = (business_dttm(day), advert_id, nm_id)
                            biz_day_start = bst_date.replace(hour=0, minute=0, second=0, microsecond=0)
                            rec_pos = {
                                "company_id":    company_id,
                                "request_uuid":  bronze.request_uuid,
                                "advert_id":     advert_id,
                                "nm_id":         raw_boost["nm"],
                                "date":          bst_date,
                                "avg_position":  raw_boost.get("avg_position"),
                                "business_dttm": biz_day_start,
                                "response_dttm": response_dttm_bronze,
                            }
                            ins_pos = pg_insert(silver_positions_model).values(**rec_pos)
                            upsert_pos = ins_pos.on_conflict_do_update(
                                index_elements=[
                                    silver_positions_model.business_dttm,
                                    silver_positions_model.advert_id,
                                    silver_positions_model.nm_id,
                                ],
                                set_=rec_pos,
                                where=sa.and_(
                                    ins_pos.excluded.response_dttm.isnot(None),
                                    sa.or_(
                                        silver_positions_model.response_dttm.is_(None),
                                        ins_pos.excluded.response_dttm > silver_positions_model.response_dttm,
                                    ),
                                ),
                            )

                        await session.execute(upsert_pos)

                await session.commit()
                total_rows += processed
                context.log.info(
                    "chunk committed: request_uuid=%s, rows=%s",
                    bronze.request_uuid, processed
                )

            except Exception as e:
                await session.rollback()
                context.log.exception(
                    "chunk rollback: request_uuid=%s — %s",
                    bronze.request_uuid, e
                )
                continue

        context.log.info(
            "[silver_wb_adv_fullstats_%s] processed %s bronze chunks, inserted/updated rows ≈ %s",
            time_grain, len(bronze_rows), total_rows
        )

