import json
import uuid
from datetime import datetime, timezone as std_timezone
from zoneinfo import ZoneInfo

from dagster import asset
from sqlalchemy import text, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from dagster_conf.resources.pg_resource import postgres_resource
from dagster_conf.resources.wb_client import wildberries_client
from dagster_conf.pipelines.wb_bronze_ops import extract_metadata_from_headers
from src.db.bronze.models import WbTariffsCommission1d
from src.db.silver.models import SilverWbCommission1d



@asset(
    resource_defs={
        "wildberries_client": wildberries_client,
        "postgres":          postgres_resource,
    },
    required_resource_keys={"wildberries_client", "postgres"},
    key_prefix=["bronze"],
    name="wb_commission_1d",
    description="Сохраняет комиссии по категориям (GET /tariffs/commission) в bronze.wb_commission_1d",
)
async def bronze_wb_commission_1d(context) -> None:
    # 1) Метаданные запуска
    run_uuid      = context.run_id
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    raw_dt        = (
        datetime.fromisoformat(scheduled_iso)
        if scheduled_iso
        else datetime.now(std_timezone.utc)
    )
    # приводим к Московскому времени
    run_dttm      = raw_dt.astimezone(ZoneInfo("Europe/Moscow"))
    api_token_id  = context.resources.wildberries_client.token_id

    # 2) Запрос к API
    async with context.resources.wildberries_client as client:
        context.log.info("[commission] fetching wb commission tariffs")
        try:
            raw_data, headers = await client.fetch_commission(locale="ru")
        except Exception as e:
            context.log.error(f"[commission] ошибка при fetch_commission: {e}")
            raise

    # 3) Метаданные HTTP
    request_uuid, response_dttm = extract_metadata_from_headers(headers)
    response_code = int(headers.get("status", 200))

    # 4) Собираем запись для бронзы
    record = {
        "api_token_id":       api_token_id,
        "run_uuid":           run_uuid,
        "run_dttm":           run_dttm,
        "request_uuid":       request_uuid or uuid.uuid4(),
        "request_dttm":       run_dttm,
        "request_parameters": {"locale": "ru"},
        "request_body":       None,
        "response_dttm":      response_dttm,
        "response_code":      response_code,
        "response_body": json.dumps(raw_data, ensure_ascii=False),
    }

    # 5) Вставляем в таблицу bronze.wb_commission_1d
    async with context.resources.postgres() as session:
        await session.execute(
            WbTariffsCommission1d.__table__.insert().values(**record)
        )
        await session.commit()

    context.log.info(f"[bronze_wb_commission_1d] Вставлена запись run_uuid={run_uuid}")


@asset(
    deps=[bronze_wb_commission_1d],
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    key_prefix=["silver"],
    name="wb_commission_1d",
    description="Переносит комиссии из bronze.wb_tariffs_commission_1d → silver.wb_commission_1d",
)
async def silver_wb_commission_1d(context):
    run_id = context.run_id

    async with context.resources.postgres() as session:
        # 1) Получаем все бронзовые записи этого запуска
        result = await session.execute(
            select(WbTariffsCommission1d)
            .where(WbTariffsCommission1d.run_uuid == run_id)
        )
        bronze_rows = result.scalars().all()

        for bronze in bronze_rows:
            # 2) тащим company_id по api_token_id
            company_id = (
                await session.execute(
                    text("SELECT company_id FROM core.tokens WHERE token_id = :token_id"),
                    {"token_id": bronze.api_token_id},
                )
            ).scalar_one()

            # 3) парсим JSON-массив из response_body
            payload = json.loads(bronze.response_body)

            # 4) Для каждого элемента payload['commission'] создаём запись
            for item in payload.get("report", []):
                response_dttm = bronze.response_dttm
                rec = {
                    "request_uuid":           bronze.request_uuid,
                    "response_dttm":          response_dttm,
                    "date":                   response_dttm,
                    "kgvp_booking":           item.get("kgvpBooking"),
                    "kgvp_marketplace":       item.get("kgvpMarketplace"),
                    "kgvp_pick_up":           item.get("kgvpPickup"),
                    "kgvp_supplier":          item.get("kgvpSupplier"),
                    "kgvp_supplier_express":  item.get("kgvpSupplierExpress"),
                    "paid_storage_kgvp":      item.get("paidStorageKgvp"),
                    "category_id":            item.get("parentID"),
                    "category_name":          item.get("parentName"),
                    "subject_id":             item.get("subjectID"),
                    "subject_name":           item.get("subjectName"),
                }

                # 5) UPSERT в silver.wb_commission_1d по составному ключу (date, subject_id)
                upsert = pg_insert(SilverWbCommission1d).values(**rec)
                upsert = upsert.on_conflict_do_update(
                    index_elements=[
                        SilverWbCommission1d.date,
                        SilverWbCommission1d.subject_id,
                    ],
                    set_=rec,
                )
                await session.execute(upsert)

        # 6) финальный коммит
        await session.commit()

    context.log.info(
        f"[silver_wb_commission_1d] Перенесено {len(bronze_rows)} бронзовых записей → silver.wb_commission_1d"
    )
