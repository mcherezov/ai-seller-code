import json
import uuid
import asyncio
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import text, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from dagster import asset

from dagster_conf.resources.wb_client import wildberries_client
from dagster_conf.resources.pg_resource import postgres_resource
from dagster_conf.pipelines.wb_bronze_ops import extract_metadata_from_headers
from src.db.bronze.models import WbPaidStorage1d
from src.db.silver.models import SilverWBPaidStorage1d


@asset(
    resource_defs={"wildberries_client": wildberries_client, "postgres": postgres_resource},
    required_resource_keys={"wildberries_client", "postgres"},
    key_prefix=["bronze"],
    name="wb_paid_storage_1d",
    description="Сохраняет отчет о платном хранении в bronze.wb_paid_storage_1d",
)
async def bronze_wb_paid_storage_1d(context):
    # 1. Метаданные запуска
    run_uuid = context.run_id
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    raw_dt = datetime.fromisoformat(scheduled_iso) if scheduled_iso else datetime.now(timezone.utc)
    run_dttm = raw_dt.astimezone(ZoneInfo("Europe/Moscow"))
    api_token_id = context.resources.wildberries_client.token_id

    # 2. Запрос данных по API
    report_date = run_dttm.date() - timedelta(days=1)
    date_str     = report_date.isoformat()

    async with context.resources.wildberries_client as client:
        context.log.info("[paid_storage] requesting report")
        try:
            resp = await client.fetch_paid_storage(date_from=date_str, date_to=date_str)
            task_id = resp.get("data", {}).get("taskId") or resp.get("taskId")
            if not task_id:
                raise RuntimeError(f"[paid_storage] taskId not found in response: {resp}")
        except Exception as e:
            context.log.error(f"[paid_storage] fetch error: {e}")
            raise

        # 3. Ожидание готовности
        while True:
            try:
                status_resp = await client.get_paid_storage_status(task_id)
                status = status_resp.get("data", {}).get("status")
                context.log.info(f"[paid_storage] status = {status}")
                if status == "done":
                    break
                await asyncio.sleep(5)
            except Exception as e:
                context.log.error(f"[paid_storage] status check error: {e}")
                raise

        # 4. Загрузка отчета
        try:
            raw_bytes, headers = await client.download_paid_storage(task_id)
            decoded = raw_bytes.decode("utf-8")
            parsed = json.loads(decoded)
        except Exception as e:
            context.log.error(f"[paid_storage] download/decode error: {e}")
            raise

    # 5. Метаданные ответа
    request_uuid, response_dttm = extract_metadata_from_headers(headers)
    response_code = int(headers.get("status", 200))

    # 6. Подготовка записи
    record = {
        "api_token_id":       api_token_id,
        "run_uuid":           run_uuid,
        "run_dttm":           run_dttm,
        "request_uuid":       request_uuid or uuid.uuid4(),
        "request_dttm":       run_dttm,
        "request_parameters": {"date_from": date_str, "date_to": date_str},
        "request_body":       None,
        "response_dttm":      response_dttm,
        "response_code":      response_code,
        "response_body":      json.dumps(parsed, ensure_ascii=False),
    }

    # 7. Вставка в бронзу
    async with context.resources.postgres() as session:
        await session.execute(WbPaidStorage1d.__table__.insert().values(**record))
        await session.commit()

    context.log.info(f"[bronze_wb_paid_storage_1d] Вставлена запись run_uuid={run_uuid}")


@asset(
    deps=[bronze_wb_paid_storage_1d],
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    key_prefix=["silver"],
    name="paid_storage_1d",
    description="Переносит данные из bronze.wb_paid_storage_1d → silver.paid_storage_1d",
)
async def silver_paid_storage_1d(context):
    run_id = context.run_id

    async with context.resources.postgres() as session:
        result = await session.execute(
            select(WbPaidStorage1d).where(WbPaidStorage1d.run_uuid == run_id)
        )
        bronze_rows = result.scalars().all()

        for bronze in bronze_rows:
            company_id = await session.execute(
                text("SELECT company_id FROM core.tokens WHERE token_id = :token_id"),
                {"token_id": bronze.api_token_id},
            )
            company_id = company_id.scalar_one()
            rows = json.loads(bronze.response_body)

            for row in rows:
                rec = {
                    "request_uuid": bronze.request_uuid,
                    "response_dttm": bronze.response_dttm,
                    "company_id": company_id,
                    "date": datetime.strptime(row.get("date"), "%Y-%m-%d").date() if row.get("date") else None,
                    "log_warehouse_coef": float(row.get("logWarehouseCoef")) if row.get(
                        "logWarehouseCoef") is not None else None,
                    "warehouse_id": int(row.get("officeId")) if row.get("officeId") is not None else None,
                    "warehouse_name": row.get("warehouse"),
                    "warehouse_coef": float(row.get("warehouseCoef")) if row.get("warehouseCoef") is not None else None,
                    "income_id": int(row.get("giId")) if row.get("giId") is not None else None,
                    "chrt_id": int(row.get("chrtId")) if row.get("chrtId") is not None else None,
                    "tech_size": row.get("size"),
                    "barcode": row.get("barcode"),
                    "supplier_article": row.get("vendorCode"),
                    "nm_id": int(row.get("nmId")) if row.get("nmId") is not None else None,
                    "volume": float(row.get("volume")) if row.get("volume") is not None else None,
                    "calc_type": row.get("calcType"),
                    "warehouse_price": float(row.get("warehousePrice")) if row.get(
                        "warehousePrice") is not None else None,
                    "barcodes_count": int(row.get("barcodesCount")) if row.get("barcodesCount") is not None else None,
                    "pallet_place_code": int(row.get("palletPlaceCode")) if row.get(
                        "palletPlaceCode") is not None else None,
                    "pallet_count": float(row.get("palletCount")) if row.get("palletCount") is not None else None,
                    "loyalty_discount": float(row.get("loyaltyDiscount")) if row.get(
                        "loyaltyDiscount") is not None else None,
                }

                stmt = pg_insert(SilverWBPaidStorage1d).values(**rec).on_conflict_do_update(
                    index_elements=[
                        SilverWBPaidStorage1d.response_dttm,
                        SilverWBPaidStorage1d.date,
                        SilverWBPaidStorage1d.warehouse_name,
                        SilverWBPaidStorage1d.income_id,
                        SilverWBPaidStorage1d.barcode,
                        SilverWBPaidStorage1d.calc_type,
                    ],
                    set_=rec,
                )
                await session.execute(stmt)

        await session.commit()
    context.log.info(f"[silver_paid_storage_1d] Перенесено {len(bronze_rows)} отчётов")


