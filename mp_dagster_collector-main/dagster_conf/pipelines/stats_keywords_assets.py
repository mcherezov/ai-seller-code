import asyncio
import json
import uuid
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from aiohttp import ClientResponseError

import dagster as dg
from dagster import asset, In, RetryPolicy, Failure
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from dagster_conf.resources.wb_client import wildberries_client
from dagster_conf.resources.pg_resource import postgres_resource
from dagster_conf.pipelines.wb_bronze_ops import extract_metadata_from_headers
from src.db.bronze.models import WbAdvStatsKeywords1d, WbAdvStatsKeywords1h
from src.db.silver.models  import SilverWbAdvKeywordStats1d, SilverWbAdvKeywordStats1h
from dagster_conf.pipelines.utils import prev_hour_mskt


# скорость не более 4 req/sec
PER_ADVERT_DELAY = 0.3
ACTIVE_STATUSES = {4, 9, 11}  # используется в 1d-режиме
MSK = ZoneInfo("Europe/Moscow")


def _parse_iso_any_tz(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


@asset(
    resource_defs={"wildberries_client": wildberries_client, "postgres": postgres_resource},
    required_resource_keys={"wildberries_client", "postgres"},
    key_prefix=["bronze"],
    name="wb_adv_stats_keywords",
    description="Сохраняет ключевые слова за вчера (1d) или за текущий час (1h) в bronze.wb_adv_stats_keywords_{1d,1h}. "
                "IDs кампаний берём из silver.wb_adv_promotions_1h, при отсутствии — фолбэк к /promotion/count.",
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    config_schema={"time_grain": str},
)
async def bronze_wb_adv_stats_keywords(context) -> None:
    time_grain = context.op_config.get("time_grain", "1d")

    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    if not scheduled_iso:
        raise Failure("Missing tag 'dagster/scheduled_execution_time'")
    run_schedule_dttm = _parse_iso_any_tz(scheduled_iso).astimezone(MSK)

    run_uuid     = context.run_id
    run_dttm     = datetime.now(MSK)
    api_token_id = context.resources.wildberries_client.token_id

    if time_grain == "1h":
        target_hour = prev_hour_mskt(run_schedule_dttm)
        date_from = date_to = target_hour.date().isoformat()
        table = WbAdvStatsKeywords1h
    elif time_grain == "1d":
        day_start = (run_schedule_dttm - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end   = day_start + timedelta(days=1)
        date_from = date_to = day_start.date().isoformat()
        table = WbAdvStatsKeywords1d
    else:
        raise Failure(f"Unsupported time_grain: {time_grain}")

    # 1) список кампаний из silver.wb_adv_promotions_1h
    async with context.resources.postgres() as session:
        company_id = (await session.execute(
            text("SELECT company_id FROM core.tokens WHERE token_id = :tid"),
            {"tid": api_token_id},
        )).scalar_one()

        advert_ids: list[int] = []
        if time_grain == "1h":
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
                last_hour = (await session.execute(text("""
                    SELECT MAX(business_dttm) FROM silver.wb_adv_promotions_1h WHERE company_id=:cid
                """), {"cid": company_id})).scalar()
                if last_hour:
                    context.log.warning("[keywords_1h] Нет данных за %s, используем %s",
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
        else:
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

    # 2) фолбэк к API, если silver пуст (актуально в исторических окнах)
    if not advert_ids:
        context.log.warning("[keywords_%s] В silver нет кампаний за %s — фолбэк к /promotion/count",
                            time_grain, date_from)
        async with context.resources.wildberries_client as client:
            raw = await client.get_advert_list_depr(date_from, date_from)
        advert_ids = [a["advertId"] for a in (raw or []) if a.get("status") in ACTIVE_STATUSES]

    if not advert_ids:
        context.log.info("[keywords_%s] Нет кампаний за %s — пропускаю", time_grain, date_from)
        return

    # 3) по каждой кампании — запрос keywords и запись в бронзу
    async with context.resources.wildberries_client as client:
        for advert_id in advert_ids:
            try:
                (raw_data, headers) = (await client.fetch_keywords_batch([advert_id], date_from, date_to))[0]
            except Exception as e:
                context.log.warning("[keywords_%s] Ошибка для advert_id=%s: %s", time_grain, advert_id, e)
                await asyncio.sleep(PER_ADVERT_DELAY)
                continue

            _, response_dttm = extract_metadata_from_headers(headers)
            response_code = int(headers.get("status", 200))

            record = {
                "api_token_id":       api_token_id,
                "run_uuid":           run_uuid,
                "run_dttm":           run_dttm,
                "request_uuid":       uuid.uuid4(),
                "request_dttm":       run_dttm,
                "request_parameters": {"advert_id": advert_id, "date_from": date_from, "date_to": date_to},
                "request_body":       None,
                "response_dttm":      response_dttm,
                "response_code":      response_code,
                "response_body":      json.dumps(raw_data.get("keywords", []), ensure_ascii=False),
            }

            async with context.resources.postgres() as session:
                await session.execute(table.__table__.insert().values(**record))
                await session.commit()

            await asyncio.sleep(PER_ADVERT_DELAY)


@asset(
    deps=[bronze_wb_adv_stats_keywords],
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    kinds={"postgres"},
    key_prefix=["silver"],
    name="wb_adv_keyword_stats",
    description="Статистика показов и кликов по ключевым фразам в рекламных кампаниях.",
    metadata={
        "link_to_docs": dg.MetadataValue.url("https://wiki.yandex.ru/homepage/product/potoki-dannyx/wb-adv-stats-keywords/"),
        "snippet": dg.MetadataValue.md("# Статистика РК по ключевым фразам.\n Для автоматических кампаний и аукционов"),
        "dagster/uri": "postgresql://rc1a-6ic32g9da782j8kh.mdb.yandexcloud.net:6432/app/silver.wb_adv_keyword_stats_1d",
        "dagster/table_name": "silver.wb_adv_keyword_stats_1d",
        "dagster/column_schema": dg.TableSchema(
            columns=[
                dg.TableColumn(
                    name="company_id",
                    type="int",
                    description="ID юридического лица селлера",
                ),
                dg.TableColumn(
                    name="request_uuid",
                    type="string",
                    description="Уникальный идентификатор запроса к API",
                ),
                dg.TableColumn(
                    name="inserted_at",
                    type="timestamp",
                    description="Время вставки записи в данную таблицу",
                ),
                dg.TableColumn(
                    name="advert_id",
                    type="int",
                    description="ID рекламной кампании",
                    tags={"NK":""},
                ),
                dg.TableColumn(
                    name="date",
                    type="timestamp",
                    description="Дата, к которой относится статистика",
                    tags={"NK":""},
                ),
                dg.TableColumn(
                    name="keyword",
                    type="string",
                    description="Ключевое слово, по которому показалась реклама",
                ),
                dg.TableColumn(
                    name="views",
                    type="int",
                    description="Количество показов",
                ),
                dg.TableColumn(
                    name="clicks",
                    type="int",
                    description="Количество кликов",
                ),
                dg.TableColumn(
                    name="ctr",
                    type="float",
                    description="Click-through rate (CTR), отношение кликов к показам",
                    constraints=dg.TableColumnConstraints(
                        nullable=True,
                        unique=False,
                        other=[
                            "equal to 0 if views = 0",
                        ],
                    ),
                ),
                dg.TableColumn(
                    name="cost",
                    type="float",
                    description="Затраты на показы и клики по ключевому слову",
                ),
            ],
            constraints = dg.TableConstraints(
                other = [
                    "views > clicks",
                    "views, clicks, cost >= 0",
                    "ctr = clicks / views",
                ],
            ),
        ),
        "dagster/column_lineage": dg.TableColumnLineage(
            deps_by_column={
                "request_uuid": [
                    dg.TableColumnDep(
                        asset_key=dg.AssetKey("wb_adv_stats_keywords_1d"),
                        column_name="request_uuid",
                    ),
                ],
                "company_id": [
                    dg.TableColumnDep(
                        asset_key=dg.AssetKey("wb_adv_stats_keywords_1d"),
                        column_name="api_token_id",
                    ),
                ],
            }
        ),
    },
)
async def silver_wb_adv_keyword_stats(context):
    run_id     = context.run_id
    time_grain = context.op_config.get("time_grain", "1d")
    scheduled  = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    run_dttm   = datetime.fromisoformat(scheduled).astimezone(ZoneInfo("Europe/Moscow"))

    context.log.info(f"[silver_wb_adv_keyword_stats] start: grain={time_grain}, run_dttm={run_dttm}")

    # Выбор моделей
    if time_grain == "1d":
        bronze_table = WbAdvStatsKeywords1d
        silver_table = SilverWbAdvKeywordStats1d
        target_date  = run_dttm.date() - timedelta(days=1)
    elif time_grain == "1h":
        bronze_table = WbAdvStatsKeywords1h
        silver_table = SilverWbAdvKeywordStats1h
        target_date  = run_dttm.date()
    else:
        raise ValueError(f"Unsupported time_grain: {time_grain}")

    async with context.resources.postgres() as session:
        # Получаем все записи бронзы текущего запуска
        rows = await session.execute(
            select(bronze_table).where(bronze_table.run_uuid == run_id)
        )
        bronze_rows = rows.scalars().all()

        for bronze in bronze_rows:
            # company_id
            company_id = (await session.execute(
                text("SELECT company_id FROM core.tokens WHERE token_id = :tid"),
                {"tid": bronze.api_token_id},
            )).scalar_one()

            advert_id = bronze.request_parameters["advert_id"]
            raw_list  = json.loads(bronze.response_body)

            # Собираем все даты, логируем их
            available = [
                datetime.fromisoformat(d["date"]).astimezone(ZoneInfo("Europe/Moscow"))
                for d in raw_list
            ]
            for dt in available:
                context.log.info(f"  API keyword datetime: {dt.isoformat()}")

            # Фильтрация по дате и выбор нужной метки
            if time_grain == "1h":
                valid = [dt for dt in available if dt.date() == target_date]
                if not valid:
                    context.log.info("  нет часовых данных за сегодня, пропускаем")
                    continue
                stat_date = max(valid)
            else:
                valid = [dt for dt in available if dt.date() == target_date]
                if not valid:
                    continue
                stat_date = valid[0]

            # Находим объект kw_day для выбранной метки
            kw_day = next(
                d for d in raw_list
                if datetime.fromisoformat(d["date"]).astimezone(ZoneInfo("Europe/Moscow")) == stat_date
            )

            # Собираем текущие метрики
            current_data = {
                stat["keyword"]: {
                    "views":  stat.get("views", 0),
                    "clicks": stat.get("clicks", 0),
                    "cost":   stat.get("sum", 0.0),
                    "ctr":    stat.get("ctr", 0.0),
                }
                for stat in kw_day.get("stats", [])
            }

            # Для часового режима вычитаем накопленное за предыдущие часы
            if time_grain == "1h":
                start_dt = stat_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_dt   = run_dttm.replace(minute=0, second=0, microsecond=0)
                q = text("""
                    SELECT keyword,
                           SUM(views) AS views,
                           SUM(clicks) AS clicks,
                           SUM(cost)   AS cost
                      FROM silver.wb_adv_keyword_stats_1h
                     WHERE advert_id = :advert_id
                       AND date >= :start_dt
                       AND date <  :end_dt
                     GROUP BY keyword
                """)
                prev = await session.execute(q, {
                    "advert_id": advert_id,
                    "start_dt":  start_dt,
                    "end_dt":    end_dt,
                })
                previous = {
                    r.keyword: {"views": r.views or 0, "clicks": r.clicks or 0, "cost": r.cost or 0.0}
                    for r in prev
                }

                # Вычитаем
                for kw, stats in current_data.items():
                    base = previous.get(kw, {"views": 0, "clicks": 0, "cost": 0.0})
                    stats["views"]  = max(0, stats["views"]  - base["views"])
                    stats["clicks"] = max(0, stats["clicks"] - base["clicks"])
                    stats["cost"]   = max(0, stats["cost"]   - base["cost"])
                    stats["ctr"]    = (stats["clicks"] / stats["views"]) if stats["views"] else 0.0

            # Upsert в silver
            for keyword, stats in current_data.items():
                rec = {
                    "company_id":   company_id,
                    "request_uuid": bronze.request_uuid,
                    "inserted_at":  bronze.run_dttm,
                    "advert_id":    advert_id,
                    "date":         stat_date,
                    "keyword":      keyword,
                    "views":        stats["views"],
                    "clicks":       stats["clicks"],
                    "ctr":          stats["ctr"],
                    "cost":         stats["cost"],
                }
                upsert = (
                    pg_insert(silver_table)
                    .values(**rec)
                    .on_conflict_do_update(
                        index_elements=[
                            silver_table.request_uuid,
                            silver_table.advert_id,
                            silver_table.date,
                            silver_table.keyword,
                        ],
                        set_=rec,
                    )
                )
                await session.execute(upsert)

        await session.commit()

    context.log.info(f"[silver_wb_adv_keyword_stats] Обработано записей: {len(bronze_rows)}")