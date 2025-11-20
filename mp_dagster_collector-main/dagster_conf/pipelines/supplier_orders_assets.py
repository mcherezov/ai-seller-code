import uuid
import json
import asyncio
from datetime import datetime, timedelta, timezone as std_timezone
from zoneinfo import ZoneInfo

from dagster import asset, Field, Int, String
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db.bronze.models import WbSupplierOrders1d
from src.db.silver.models import SilverOrderItems1d
from dagster_conf.resources.pg_resource import postgres_resource
from dagster_conf.resources.wb_client import wildberries_client
from dagster_conf.pipelines.wb_bronze_ops import extract_metadata_from_headers


# ────────────────────────────── BRONZE ──────────────────────────────
@asset(
    resource_defs={"wildberries_client": wildberries_client, "postgres": postgres_resource},
    required_resource_keys={"wildberries_client", "postgres"},
    key_prefix=["bronze"],
    name="wb_supplier_orders_1d",
    description=(
        "Загружает заказы поставщика по last_change_date (flag=0) "
        "с 25-часовым нахлёстом в bronze.wb_supplier_orders_1d"
    ),
    config_schema={
        "flag": Field(Int, default_value=0, description="WB API flag"),
        "date_from": Field(String, is_required=False, description="Override dateFrom (MSK, no TZ)"),
    },
)
async def bronze_wb_supplier_orders_1d(context) -> None:
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    scheduled_utc = datetime.fromisoformat(scheduled_iso) if scheduled_iso else datetime.now(std_timezone.utc)
    run_schedule_dttm_msk = scheduled_utc.astimezone(ZoneInfo("Europe/Moscow"))
    run_actual_dttm_msk   = datetime.now(std_timezone.utc).astimezone(ZoneInfo("Europe/Moscow"))

    run_uuid     = context.run_id
    api_token_id = context.resources.wildberries_client.token_id

    flag = int(context.op_config.get("flag", 0))
    date_from_cfg = context.op_config.get("date_from")
    date_from_http = (date_from_cfg.strip() if isinstance(date_from_cfg, str) and date_from_cfg.strip()
                      else (run_schedule_dttm_msk - timedelta(hours=25)).strftime("%Y-%m-%dT%H:%M:%S"))

    request_dttm_msk = datetime.now(std_timezone.utc).astimezone(ZoneInfo("Europe/Moscow"))

    async with context.resources.wildberries_client as client:
        context.log.info(f"[orders] fetching supplier orders: flag={flag}, dateFrom={date_from_http}")
        raw_data, headers = await client.fetch_orders(date_from=date_from_http, flag=flag)

    receive_dttm_msk = datetime.now(std_timezone.utc).astimezone(ZoneInfo("Europe/Moscow"))

    request_uuid, response_dttm = extract_metadata_from_headers(headers)
    response_code = int(headers.get("status", 200))

    # point-in-time (за «вчера» от ПЛАНОВОГО времени)
    business_dttm_msk = (run_schedule_dttm_msk - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    record = {
        "api_token_id":       api_token_id,
        "run_uuid":           run_uuid,
        "run_dttm":           run_actual_dttm_msk,
        "run_schedule_dttm":  run_schedule_dttm_msk,
        "request_uuid":       request_uuid or uuid.uuid4(),
        "request_dttm":       request_dttm_msk,
        "request_parameters": {"flag": flag, "dateFrom": date_from_http},
        "request_body":       None,
        "response_dttm":      response_dttm,
        "receive_dttm":       receive_dttm_msk,
        "response_code":      response_code,
        "response_body":      json.dumps(raw_data, ensure_ascii=False),
        "business_dttm":      business_dttm_msk,
    }

    async with context.resources.postgres() as session:
        await session.execute(WbSupplierOrders1d.__table__.insert().values(**record))
        await session.commit()

    context.log.info(f"[bronze_wb_supplier_orders_1d] saved run={run_uuid} flag={flag} dateFrom={date_from_http}")


# ────────────────────────────── SILVER ──────────────────────────────
def _to_msk(dt_str: str | None):
    if not dt_str:
        return None
    s = dt_str.strip()
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=ZoneInfo("Europe/Moscow"))
    return dt.astimezone(ZoneInfo("Europe/Moscow"))



@asset(
    deps=[bronze_wb_supplier_orders_1d],
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    key_prefix=["silver"],
    name="order_items_1d",
    description=(
        "Переносит заказы поставщика из bronze.wb_supplier_orders_1d → silver.wb_order_items_1d "
        "c идемпотентностью по (sr_id, last_change_date) — до смены PK"
    ),
)
async def silver_order_items_1d(context):
    run_id = context.run_id

    async with context.resources.postgres() as session:
        result = await session.execute(select(WbSupplierOrders1d).where(WbSupplierOrders1d.run_uuid == run_id))
        bronze_rows = result.scalars().all()

        total_attempts = 0

        for bronze in bronze_rows:
            company_id = (await session.execute(
                text("SELECT company_id FROM core.tokens WHERE token_id = :token_id"),
                {"token_id": bronze.api_token_id},
            )).scalar_one()

            payload = json.loads(bronze.response_body) or []

            response_dttm = bronze.response_dttm
            business_dttm = bronze.business_dttm
            now_msk = datetime.now(std_timezone.utc).astimezone(ZoneInfo("Europe/Moscow"))
            fallback_business = (now_msk - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

            for item in payload:
                dt        = _to_msk(item.get("date"))
                last_dt   = _to_msk(item.get("lastChangeDate"))
                cancel_dt = _to_msk(item.get("cancelDate")) if item.get("cancelDate") not in (None, "0001-01-01T00:00:00") else None

                if last_dt is None:
                    continue

                rec = {
                    "request_uuid":         bronze.request_uuid,
                    "response_dttm":        response_dttm or now_msk,
                    "business_dttm":        business_dttm or fallback_business,

                    "g_number":             item.get("gNumber")             or "",
                    "sr_id":                item.get("srid"),
                    "date":                 dt,
                    "last_change_date":     last_dt,
                    "warehouse_name":       item.get("warehouseName")       or "",
                    "warehouse_type":       item.get("warehouseType")       or "",
                    "country_name":         item.get("countryName")         or "",
                    "oblast_okrug_name":    item.get("oblastOkrugName")     or "",
                    "region_name":          item.get("regionName")          or "",
                    "supplier_article":     item.get("supplierArticle")     or "",
                    "nm_id":                item.get("nmId"),
                    "barcode":              item.get("barcode")             or "",
                    "category":             item.get("category")            or "",
                    "subject":              item.get("subject")             or "",
                    "brand":                item.get("brand")               or "",
                    "tech_size":            item.get("techSize")            or "",
                    "income_id":            item.get("incomeID")            or 0,
                    "total_price":          item.get("totalPrice")          or 0.0,
                    "discount_percent":     item.get("discountPercent")     or 0.0,
                    "spp":                  item.get("spp")                 or 0,
                    "finished_price":       item.get("finishedPrice")       or 0.0,
                    "price_with_discount":  item.get("priceWithDisc")       or 0.0,
                    "is_cancel":            item.get("isCancel", False),
                    "cancel_date":          cancel_dt,
                    "sticker":              item.get("sticker")             or "",
                    "company_id":           company_id,
                }

                upsert = pg_insert(SilverOrderItems1d).values(**rec)
                upsert = upsert.on_conflict_do_nothing(constraint="wb_order_items_1d_pkey")
                await session.execute(upsert)
                total_attempts += 1

        await session.commit()

    context.log.info(f"[silver_order_items_1d] processed bronze_rows={len(bronze_rows)}, attempted_inserts={total_attempts}")
