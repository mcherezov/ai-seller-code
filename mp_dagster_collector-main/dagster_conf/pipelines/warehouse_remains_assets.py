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
from src.db.bronze.models import WbWarehouseRemains1d
from src.db.silver.models import SilverWbStocks1d


@asset(
    resource_defs={
        "wildberries_client": wildberries_client,
        "postgres": postgres_resource,
    },
    required_resource_keys={"wildberries_client", "postgres"},
    key_prefix=["bronze"],
    name="wb_stocks_report_1d",
    description="Сохраняет отчет об остатках по складам в bronze.wb_stocks_report",
)
async def bronze_wb_stocks_report_1d(context):
    # 1) Метаданные запуска
    run_uuid = context.run_id
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    raw_dt = (
        datetime.fromisoformat(scheduled_iso)
        if scheduled_iso
        else datetime.now(timezone.utc)
    )
    run_dttm = raw_dt.astimezone(ZoneInfo("Europe/Moscow"))
    api_token_id = context.resources.wildberries_client.token_id

    # 2) Дата отчёта — вчерашняя
    report_date = run_dttm.date() - timedelta(days=1)
    date_str    = report_date.isoformat()

    # 3) Фильтры
    filters = {
        "groupByBrand":   True,
        "groupBySubject": True,
        "groupBySa":      True,
        "groupByNm":      True,
        "groupByBarcode": True,
        "groupBySize":    True,
    }

    # 4) Запрос отчета через API
    async with context.resources.wildberries_client as client:
        context.log.info("[stocks] requesting warehouse remains")
        try:
            task_resp = await client.request_stocks_report(filters)
            task_id = task_resp.get("data", {}).get("taskId")
            if not task_id:
                raise RuntimeError(f"[stocks] taskId not found in {task_resp}")
        except Exception as e:
            context.log.error(f"[stocks] fetch error: {e}")
            raise

        # 5) Ожидание готовности
        while True:
            status_resp = await client.get_stocks_report_status(task_id)
            status = status_resp.get("data", {}).get("status")
            context.log.info(f"[stocks] status = {status}")
            if status == "done":
                break
            await asyncio.sleep(5)

        # 6) Загрузка отчета
        try:
            raw_bytes, headers = await client.download_stocks_report(task_id)
            decoded = raw_bytes.decode("utf-8")
            stocks = json.loads(decoded)
        except Exception as e:
            context.log.error(f"[stocks] decode error: {e}")
            raise

    # 7) Метаданные ответа
    request_uuid, response_dttm = extract_metadata_from_headers(headers)
    response_code = int(headers.get("status", 200))

    # 8) Подготовка записи
    record = {
        "api_token_id":       api_token_id,
        "run_uuid":           run_uuid,
        "run_dttm":           run_dttm,
        "request_uuid":       request_uuid or uuid.uuid4(),
        "request_dttm":       run_dttm,
        "request_parameters": filters,
        "request_body":       None,
        "response_dttm":      response_dttm,
        "response_code":      response_code,
        "response_body":      json.dumps(stocks, ensure_ascii=False),
    }

    # 9) Вставка в бронзу
    async with context.resources.postgres() as session:
        await session.execute(WbWarehouseRemains1d.__table__.insert().values(**record))
        await session.commit()

    context.log.info(f"[bronze_wb_stocks_report_1d] Вставлена запись run_uuid={run_uuid}")


@asset(
    deps=[bronze_wb_stocks_report_1d],
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    key_prefix=["silver"],
    name="wb_stocks_1d",
    description="Переносит данные из bronze.wb_stocks_report → silver.wb_stocks_1d",
)
async def silver_wb_stocks_1d(context):
    run_id = context.run_id

    async with context.resources.postgres() as session:
        result = await session.execute(
            select(WbWarehouseRemains1d).where(WbWarehouseRemains1d.run_uuid == run_id)
        )
        bronze_rows = result.scalars().all()

        for bronze in bronze_rows:
            company_id = await session.execute(
                text("SELECT company_id FROM core.tokens WHERE token_id = :token_id"),
                {"token_id": bronze.api_token_id},
            )
            company_id = company_id.scalar_one()
            rows = json.loads(bronze.response_body)
            date = bronze.run_dttm.date()

            for row in rows:
                warehouses = row.get("warehouses", [])
                in_way_to_client = None
                in_way_from_client = None
                total = None

                # Сначала достаём агрегаты
                for w in warehouses:
                    name = w.get("warehouseName", "").replace(" ", "").lower()
                    quantity = w.get("quantity")
                    if name == "впутидополучателей":
                        in_way_to_client = quantity
                    elif name == "впутивозвратынаскладwb":
                        in_way_from_client = quantity
                    elif name == "всегонаходитсянаскладах":
                        total = quantity

                # Общие поля — определяем заранее
                common_fields = {
                    "request_uuid": bronze.request_uuid,
                    "response_dttm": bronze.response_dttm,
                    "company_id": company_id,
                    "date": date,
                    "supplier_article": row.get("vendorCode"),
                    "nm_id": row.get("nmId"),
                    "barcode": row.get("barcode"),
                    "tech_size": row.get("techSize"),
                    "volume": row.get("volume"),
                    "in_way_to_client": in_way_to_client,
                    "in_way_from_client": in_way_from_client,
                    "total": total,
                }

                # Теперь можно использовать common_fields внутри цикла
                for w in warehouses:
                    warehouse_name = w.get("warehouseName")
                    normalized = warehouse_name.replace(" ", "").lower()

                    if normalized in {
                        "впутидополучателей",
                        "впутивозвратынаскладwb",
                        "всегонаходитсянаскладах"
                    }:
                        continue

                    quantity = w.get("quantity")

                    rec = {
                        **common_fields,
                        "warehouse_name": warehouse_name,
                        "quantity": quantity,
                    }

                    stmt = pg_insert(SilverWbStocks1d).values(**rec)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[
                            SilverWbStocks1d.request_uuid,
                            SilverWbStocks1d.response_dttm,
                            SilverWbStocks1d.company_id,
                            SilverWbStocks1d.date,
                            SilverWbStocks1d.barcode,
                            SilverWbStocks1d.warehouse_name,
                        ],
                        set_=rec,
                    )
                    await session.execute(stmt)

        await session.commit()
    context.log.info(f"[silver_wb_stocks_1d] Перенесено {len(bronze_rows)} отчётов")
