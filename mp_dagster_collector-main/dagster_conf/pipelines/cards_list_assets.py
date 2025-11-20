import json
import uuid
import asyncio
from datetime import datetime, timezone as std_timezone
from zoneinfo import ZoneInfo
from typing import List, Dict, Any

from dagster import asset
from sqlalchemy import text, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from dagster_conf.resources.wb_client import wildberries_client
from dagster_conf.resources.pg_resource import postgres_resource
from dagster_conf.pipelines.wb_bronze_ops import extract_metadata_from_headers

from src.db.bronze.models import WbCardsList1d
from src.db.silver.models import SilverWbMpSkus1d


@asset(
    resource_defs={
        "wildberries_client": wildberries_client,
        "postgres":          postgres_resource,
    },
    required_resource_keys={"wildberries_client", "postgres"},
    key_prefix=["bronze"],
    name="wb_cards_list_1d",
    description="Сохраняет все карточки SKU в bronze.wb_cards_list_1d",
)
async def bronze_wb_cards_list_1d(context) -> None:
    # 1) Метаданные запуска
    run_uuid      = context.run_id
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    raw_dt        = (
        datetime.fromisoformat(scheduled_iso)
        if scheduled_iso
        else datetime.now(std_timezone.utc)
    )
    run_dttm      = raw_dt.astimezone(ZoneInfo("Europe/Moscow"))
    api_token_id  = context.resources.wildberries_client.token_id

    # 2) Постранично забираем все карточки
    async with context.resources.wildberries_client as client:
        context.log.info("[cards] fetching all SKU cards")
        try:
            cards, headers = await client.fetch_sku_cards_all()
        except Exception as e:
            context.log.error(f"[cards] fetch error: {e}")
            raise

    # 3) HTTP-метаданные
    request_uuid, response_dttm = extract_metadata_from_headers(headers)
    response_code = int(headers.get("status", 200))

    # 4) Подготовка записи
    record = {
        "api_token_id":       api_token_id,
        "run_uuid":           run_uuid,
        "run_dttm":           run_dttm,
        "request_uuid":       request_uuid or uuid.uuid4(),
        "request_dttm":       run_dttm,
        "request_parameters": {"withPhoto": -1, "limit": 100},
        "request_body":       None,
        "response_dttm":      response_dttm,
        "response_code":      response_code,
        # сохраняем весь список карточек в response_body
        "response_body":      json.dumps(cards, ensure_ascii=False),
    }

    # 5) Вставка в бронзу
    async with context.resources.postgres() as session:
        await session.execute(
            WbCardsList1d.__table__.insert().values(**record)
        )
        await session.commit()

    context.log.info(f"[bronze_wb_cards_list_1d] Вставлена запись run_uuid={run_uuid}")


@asset(
    deps=[bronze_wb_cards_list_1d],
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    key_prefix=["silver"],
    name="wb_mp_skus_1d",
    description="Переносит карточки SKU из bronze.wb_cards_list_1d → silver.wb_mp_skus_1d",
)
async def silver_wb_mp_skus_1d(context):
    run_id = context.run_id

    async with context.resources.postgres() as session:
        # 1) Берём все бронзовые записи по этому запуску
        result = await session.execute(
            select(WbCardsList1d).where(WbCardsList1d.run_uuid == run_id)
        )
        bronze_rows = result.scalars().all()

        for bronze in bronze_rows:
            # 2) company_id по token_id
            company_id = await session.execute(
                text(
                    "SELECT company_id FROM core.tokens WHERE token_id = :token_id"
                ),
                {"token_id": bronze.api_token_id},
            )
            company_id = company_id.scalar_one()

            # 3) JSON-массив карточек
            cards = json.loads(bronze.response_body)

            # 4) Для каждой карточки формируем запись
            for item in cards:
                sizes = item.get("sizes") or []
                first_size = sizes[0] if sizes else {}
                skus = first_size.get("skus") or []

                rec = {
                    "request_uuid": bronze.request_uuid,
                    "response_dttm": bronze.response_dttm,
                    "company_id": company_id,
                    "nm_id": item.get("nmID"),
                    "imt_id": item.get("imtID"),
                    "subject_id": item.get("subjectID"),
                    "subject_name": item.get("subjectName"),
                    "supplier_article": item.get("vendorCode"),
                    "brand": item.get("brand"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "length": item.get("dimensions", {}).get("length"),
                    "width": item.get("dimensions", {}).get("width"),
                    "height": item.get("dimensions", {}).get("height"),
                    "weight": item.get("dimensions", {}).get("weightBrutto"),
                    "chrt_id": first_size.get("chrtID"),
                    "tech_size": first_size.get("techSize"),
                    "barcode": skus[0] if skus else None,
                }

                # 5) UPSERT по составному PK (request_uuid, response_dttm)
                upsert = pg_insert(SilverWbMpSkus1d).values(**rec)
                upsert = upsert.on_conflict_do_update(
                    index_elements=[
                        SilverWbMpSkus1d.request_uuid,
                        SilverWbMpSkus1d.response_dttm,
                        SilverWbMpSkus1d.barcode,
                        SilverWbMpSkus1d.company_id,
                    ],
                    set_=rec,
                )
                await session.execute(upsert)

        # 6) Финальный коммит
        await session.commit()

    context.log.info(
        f"[silver_wb_mp_skus_1d] Перенесено {len(bronze_rows)} бронзовых записей → silver.wb_mp_skus_1d"
    )
