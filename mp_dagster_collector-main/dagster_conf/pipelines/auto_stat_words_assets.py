import uuid
import json
import asyncio
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import dagster as dg
from dagster import asset
from sqlalchemy import text, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db.bronze.models import WbAdvAutoStatWords1d
from src.db.silver.models import SilverWbAdvKeywordClusters1d
from dagster_conf.resources.wb_client import wildberries_client
from dagster_conf.resources.pg_resource import postgres_resource
from dagster_conf.pipelines.wb_bronze_ops import extract_metadata_from_headers


# скорость не более 4 req/sec
PER_ADVERT_DELAY = 0.3


@asset(
    resource_defs={
        "wildberries_client": wildberries_client,
        "postgres":           postgres_resource,
    },
    required_resource_keys={"wildberries_client", "postgres"},
    key_prefix=["bronze"],
    name="wb_adv_auto_stat_words_1d",
    description="Сохраняет кластеры ключевых слов за вчерашний день в bronze.wb_adv_auto_stat_words_1d",
)
async def bronze_wb_adv_auto_stat_words_1d(context) -> None:
    # 1) Метаданные запуска
    run_uuid      = context.run_id
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    raw_dt        = (
        datetime.fromisoformat(scheduled_iso)
        if scheduled_iso
        else datetime.now(timezone.utc)
    )
    run_dttm      = raw_dt.astimezone(ZoneInfo("Europe/Moscow"))
    api_token_id  = context.resources.wildberries_client.token_id

    # 2) Вычисляем вчерашний день МСК
    yesterday = (run_dttm.date() - timedelta(days=1)).isoformat()

    async with context.resources.wildberries_client as client:
        # 3) Берём список кампаний + их статус за вчера
        raw_adverts = await client.get_advert_list_depr(yesterday, yesterday)

        # 4) Фильтруем по ACTIVE_STATUSES
        ACTIVE_STATUSES   = {4, 9, 11}
        active_advert_ids = [
            a["advertId"]
            for a in raw_adverts
            if a.get("status") in ACTIVE_STATUSES
        ]

        # 5) Для каждого advert_id запрашиваем кластеры
        for advert_id in active_advert_ids:
            context.log.info(f"[clusters] advert_id={advert_id}")
            try:
                # fetch_clusters_batch возвращает List[Tuple[data, headers]]
                raw_data, headers = (await client.fetch_clusters_batch([advert_id]))[0]
            except Exception as e:
                context.log.warning(f"[clusters] Ошибка для advert_id={advert_id}: {e}")
                continue

            # 6) Метаданные ответа
            response_uuid, response_dttm = extract_metadata_from_headers(headers)
            response_code                = int(headers.get("status", 200))

            # 7) Сохраняем в бронзу
            record = {
                "api_token_id":       api_token_id,
                "run_uuid":           run_uuid,
                "run_dttm":           run_dttm,
                "request_uuid":       uuid.uuid4(),
                "request_dttm":       run_dttm,
                "request_parameters": {"advert_id": advert_id},
                "request_body":       None,
                "response_dttm":      response_dttm,
                "response_code":      response_code,
                "response_body":      json.dumps(raw_data, ensure_ascii=False),
            }

            async with context.resources.postgres() as session:
                await session.execute(
                    WbAdvAutoStatWords1d.__table__.insert().values(**record)
                )
                await session.commit()

            # 8) Интервал между запросами
            await asyncio.sleep(PER_ADVERT_DELAY)


@asset(
    deps=[bronze_wb_adv_auto_stat_words_1d],
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    key_prefix=["silver"],
    name="wb_adv_keyword_clusters_1d",
    description="Переносит кластеры слов и исключённые слова из bronze.wb_adv_auto_stat_words_1d → silver.wb_adv_keyword_clusters_1d",
)
async def silver_wb_adv_keyword_clusters_1d(context):
    run_id = context.run_id

    async with context.resources.postgres() as session:
        # 1) грузим все бронзовые записи этого запуска
        result = await session.execute(
            select(WbAdvAutoStatWords1d).where(WbAdvAutoStatWords1d.run_uuid == run_id)
        )
        bronze_rows = result.scalars().all()

        for bronze in bronze_rows:
            # 2) тащим company_id из core.tokens по api_token_id
            company_id = (
                await session.execute(
                    text("SELECT company_id FROM core.tokens WHERE token_id = :token_id"),
                    {"token_id": bronze.api_token_id},
                )
            ).scalar_one()

            # 3) единственный advert_id из request_parameters
            advert_id = bronze.request_parameters["advert_id"]

            # 4) парсим ответ
            payload = json.loads(bronze.response_body)
            run_dttm = bronze.run_dttm

            # 5) раскладываем по кластерам
            for cluster in payload.get("clusters", []):
                cluster_name  = cluster["cluster"]
                cluster_views = cluster["count"]
                for keyword in cluster.get("keywords", []):
                    rec = {
                        "company_id":           company_id,
                        "request_uuid":         bronze.request_uuid,
                        "inserted_at":          bronze.inserted_at,
                        "run_dttm":             run_dttm,
                        "advert_id":            advert_id,
                        "keyword_cluster":      cluster_name,
                        "keyword_cluster_views": cluster_views,
                        "keyword":              keyword,
                        "is_excluded":          False,
                    }
                    upsert = pg_insert(SilverWbAdvKeywordClusters1d).values(**rec)
                    upsert = upsert.on_conflict_do_update(
                        index_elements=[
                            SilverWbAdvKeywordClusters1d.run_dttm,
                            SilverWbAdvKeywordClusters1d.advert_id,
                            SilverWbAdvKeywordClusters1d.keyword_cluster,
                            SilverWbAdvKeywordClusters1d.keyword,
                        ],
                        set_=rec,
                    )
                    await session.execute(upsert)

            # 6) отмечаем исключённые слова
            for excluded_kw in payload.get("excluded", []):
                rec = {
                    "company_id":           company_id,
                    "request_uuid":         bronze.request_uuid,
                    "inserted_at":          bronze.inserted_at,
                    "run_dttm":             run_dttm,
                    "advert_id":            advert_id,
                    "keyword_cluster":      excluded_kw,
                    "keyword_cluster_views": 0,
                    "keyword":              "",
                    "is_excluded":          True,
                }
                upsert = pg_insert(SilverWbAdvKeywordClusters1d).values(**rec)
                upsert = upsert.on_conflict_do_update(
                    index_elements=[
                        SilverWbAdvKeywordClusters1d.run_dttm,
                        SilverWbAdvKeywordClusters1d.advert_id,
                        SilverWbAdvKeywordClusters1d.keyword_cluster,
                        SilverWbAdvKeywordClusters1d.keyword,
                    ],
                    set_=rec,
                )
                await session.execute(upsert)

        # 7) один финальный коммит
        await session.commit()

    context.log.info(
        f"[silver_wb_adv_keyword_clusters_1d] Перенесено {len(bronze_rows)} бронзовых записей → silver"
    )
