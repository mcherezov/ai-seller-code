import json
import uuid
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Any
from zoneinfo import ZoneInfo
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from dagster import asset

from dagster_conf.resources.wb_client import wildberries_client
from dagster_conf.resources.pg_resource import postgres_resource
from dagster_conf.pipelines.wb_bronze_ops import extract_metadata_from_headers
from src.db.bronze.models import WbAcceptanceReport1d
from src.db.silver.models import SilverWBPaidAcceptances1d


@asset(
    resource_defs={"wildberries_client": wildberries_client, "postgres": postgres_resource},
    required_resource_keys={"wildberries_client", "postgres"},
    key_prefix=["bronze"],
    name="wb_paid_acceptance_1d",
    description="Сохраняет отчет о платной приёмке в bronze.wb_paid_acceptance_1d",
)
async def bronze_wb_paid_acceptance_1d(context):
    # 1. Метаданные запуска
    run_uuid = context.run_id
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    raw_dt = datetime.fromisoformat(scheduled_iso) if scheduled_iso else datetime.now(timezone.utc)
    run_dttm = raw_dt.astimezone(ZoneInfo("Europe/Moscow"))
    api_token_id = context.resources.wildberries_client.token_id

    # 2. Дата отчёта
    report_date = run_dttm.date() - timedelta(days=1)
    date_str     = report_date.isoformat()

    async with context.resources.wildberries_client as client:
        context.log.info("[paid_acceptance] requesting report")
        try:
            resp = await client.fetch_paid_acceptions(date_from=date_str, date_to=date_str)
            task_id = resp.get("data", {}).get("taskId") or resp.get("taskId")
            if not task_id:
                raise RuntimeError(f"[paid_acceptance] taskId not found in response: {resp}")
        except Exception as e:
            context.log.error(f"[paid_acceptance] fetch error: {e}")
            raise

        # Wait for completion
        while True:
            try:
                status_resp = await client.get_paid_acceptions_status(task_id)
                status = status_resp.get("data", {}).get("status")
                context.log.info(f"[paid_acceptance] status = {status}")
                if status == "done":
                    break
                await asyncio.sleep(5)
            except Exception as e:
                context.log.error(f"[paid_acceptance] status check error: {e}")
                raise

        try:
            raw_bytes, headers = await client.download_paid_acceptions(task_id)
            decoded = raw_bytes.decode("utf-8")
            parsed = json.loads(decoded)
        except Exception as e:
            context.log.error(f"[paid_acceptance] download/decode error: {e}")
            raise

    request_uuid, response_dttm = extract_metadata_from_headers(headers)
    response_code = int(headers.get("status", 200))

    record = {
        "api_token_id": api_token_id,
        "run_uuid": run_uuid,
        "run_dttm": run_dttm,
        "request_uuid": request_uuid or uuid.uuid4(),
        "request_dttm": run_dttm,
        "request_parameters": {"date_from": date_str, "date_to": date_str},
        "request_body": None,
        "response_dttm": response_dttm,
        "response_code": response_code,
        "response_body": json.dumps(parsed, ensure_ascii=False),
    }

    async with context.resources.postgres() as session:
        await session.execute(WbAcceptanceReport1d.__table__.insert().values(**record))
        await session.commit()

    context.log.info(f"[bronze_wb_paid_acceptance_1d] Вставлена запись run_uuid={run_uuid}")


@asset(
    deps=[bronze_wb_paid_acceptance_1d],
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    key_prefix=["silver"],
    name="wb_paid_acceptances_1d",
    description="Переносит данные из bronze.wb_acceptance_report_1d → silver.wb_paid_acceptances_1d",
)
async def silver_wb_paid_acceptances_1d(context):
    run_id = context.run_id

    async with context.resources.postgres() as session:
        result = await session.execute(
            select(WbAcceptanceReport1d).where(WbAcceptanceReport1d.run_uuid == run_id)
        )
        bronze_rows = result.scalars().all()

        for bronze in bronze_rows:
            company_id_result = await session.execute(
                text("SELECT company_id FROM core.tokens WHERE token_id = :token_id"),
                {"token_id": bronze.api_token_id},
            )
            company_id = company_id_result.scalar_one()
            rows = json.loads(bronze.response_body)

            for row in rows:
                rec = {
                    "request_uuid": bronze.request_uuid,
                    "response_dttm": bronze.response_dttm,
                    "company_id": company_id,
                    "quantity": int(row.get("count", 0)),
                    "gi_create_date": datetime.strptime(row.get("giCreateDate"), "%Y-%m-%d").date() if row.get("giCreateDate") else None,
                    "date": datetime.strptime(row.get("shkCreateDate"), "%Y-%m-%d").date() if row.get("shkCreateDate") else None,
                    "income_id": int(row["incomeId"]),
                    "nm_id": int(row["nmId"]),
                    "total_price": float(row.get("total", 0)),
                }

                stmt = pg_insert(SilverWBPaidAcceptances1d).values(**rec).on_conflict_do_update(
                    index_elements=[
                        SilverWBPaidAcceptances1d.response_dttm,
                        SilverWBPaidAcceptances1d.income_id,
                        SilverWBPaidAcceptances1d.nm_id,
                    ],
                    set_=rec,
                )
                await session.execute(stmt)

        await session.commit()

    context.log.info(f"[silver_wb_paid_acceptances_1d] Перенесено {len(bronze_rows)} отчётов")
