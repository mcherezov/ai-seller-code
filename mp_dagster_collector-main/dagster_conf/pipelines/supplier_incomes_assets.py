import json
import uuid
from datetime import datetime, timezone as std_timezone,timedelta
from zoneinfo import ZoneInfo

from dagster import asset
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from dagster_conf.resources.pg_resource import postgres_resource
from dagster_conf.resources.wb_client import wildberries_client
from dagster_conf.pipelines.wb_bronze_ops import extract_metadata_from_headers

from src.db.bronze.models import WbSupplierIncomes1d
from src.db.silver.models import SilverWbSupplies1d


@asset(
    resource_defs={
        "wildberries_client": wildberries_client,
        "postgres":          postgres_resource,
    },
    required_resource_keys={"wildberries_client", "postgres"},
    key_prefix=["bronze"],
    name="wb_supplier_incomes_1d",
    description="Сохраняет отчет о поставках товара на маркетплейс (GET /supplier/incomes) в bronze.wb_supplier_incomes_1d",
)
async def bronze_wb_supplier_incomes_1d(context) -> None:
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

    # 2) Интервал за вчерашний день
    data_date      = run_dttm.date() - timedelta(days=1)
    date_from_iso  = data_date.isoformat()
    request_params = {"dateFrom": date_from_iso}

    # 3) Запрос к API
    async with context.resources.wildberries_client as client:
        context.log.info(f"[supplies] fetching wb supplier incomes for {data_date}")
        try:
            raw_data, headers = await client.fetch_suppliers(
                date_from=date_from_iso,
            )
        except Exception as e:
            context.log.error(f"[supplies] error in fetch_suppliers: {e}")
            raise

    # 4) HTTP-метаданные
    try:
        request_uuid, response_dttm = extract_metadata_from_headers(headers)
    except ValueError:
        context.log.warning(f"[supplies] empty headers {headers!r}, using run_dttm")
        request_uuid  = uuid.uuid4()
        response_dttm = run_dttm
    response_code = int(headers.get("status", 200))

    # 5) Формируем запись для бронзы
    record = {
        "api_token_id":       api_token_id,
        "run_uuid":           run_uuid,
        "run_dttm":           run_dttm,
        "request_uuid":       request_uuid,
        "request_dttm":       run_dttm,
        "request_parameters": request_params,
        "request_body":       None,
        "response_dttm":      response_dttm,
        "response_code":      response_code,
        "response_body":      json.dumps(raw_data, ensure_ascii=False),
    }

    # 6) Вставка в bronze.wb_supplier_incomes_1d
    async with context.resources.postgres() as session:
        await session.execute(
            WbSupplierIncomes1d.__table__.insert().values(**record)
        )
        await session.commit()

    context.log.info(
        f"[bronze_wb_supplier_incomes_1d] inserted run_uuid={run_uuid} for {data_date}"
    )


@asset(
    deps=[bronze_wb_supplier_incomes_1d],
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    key_prefix=["silver"],
    name="wb_supplies_1d",
    description="Переносит доходы поставщика из bronze.wb_supplier_incomes_1d → silver.wb_supplies_1d",
)
async def silver_wb_supplies_1d(context):
    run_uuid = context.run_id

    async with context.resources.postgres() as session:
        # 1) Получаем все бронзовые записи этого запуска
        result = await session.execute(
            select(WbSupplierIncomes1d).where(WbSupplierIncomes1d.run_uuid == run_uuid)
        )
        bronze_rows = result.scalars().all()

        for bronze in bronze_rows:
            # 2) Тащим company_id по api_token_id
            company_id = await session.execute(
                text("SELECT company_id FROM core.tokens WHERE token_id = :token_id"),
                {"token_id": bronze.api_token_id},
            )
            company_id = company_id.scalar_one()

            # 3) Парсим JSON-массив из response_body
            payload = json.loads(bronze.response_body)

            # 4) Для каждого элемента payload создаём запись и UPSERT в silver
            for item in payload:
                # Парсим даты из ISO-строк
                date          = datetime.fromisoformat(item["date"]).date()
                last_change   = datetime.fromisoformat(item["lastChangeDate"])
                date_close    = (
                    datetime.fromisoformat(item["dateClose"])
                    if item.get("dateClose")
                    else None
                )

                rec = {
                    "request_uuid":      bronze.request_uuid,
                    "response_dttm":     bronze.response_dttm,
                    "company_id":        company_id,
                    "income_id":         item.get("incomeId"),
                    "date":              date,
                    "last_change_date":  last_change,
                    "supplier_article":  item.get("supplierArticle"),
                    "tech_size":         item.get("techSize"),
                    "barcode":           item.get("barcode"),
                    "quantity":          item.get("quantity"),
                    "total_price":       item.get("totalPrice"),
                    "date_close":        date_close,
                    "warehouse_name":    item.get("warehouseName"),
                    "status":            item.get("status"),
                    "nm_id":             item.get("nmId"),
                }

                upsert = pg_insert(SilverWbSupplies1d).values(**rec)
                upsert = upsert.on_conflict_do_update(
                    index_elements=[
                        SilverWbSupplies1d.request_uuid,
                        SilverWbSupplies1d.company_id,
                        SilverWbSupplies1d.income_id,
                        SilverWbSupplies1d.barcode,
                    ],
                    set_=rec,
                )
                await session.execute(upsert)

        # 5) Коммит
        await session.commit()

    context.log.info(
        f"[silver_wb_supplies_1d] migrated {len(bronze_rows)} bronze rows to silver.wb_supplies_1d"
    )
