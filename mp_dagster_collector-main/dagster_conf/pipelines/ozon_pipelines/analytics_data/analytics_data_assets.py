import json
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta, timezone as std_timezone
from zoneinfo import ZoneInfo
from email.utils import parsedate_to_datetime

from dagster import asset, RetryPolicy
from src.db.bronze.ozon_models import OzonAnalyticsData1d
from dagster_conf.resources.ozon_client import ozon_client
from dagster_conf.resources.pg_resource import postgres_resource


def _parse_response_meta(headers: Dict[str, str]) -> tuple[Optional[uuid.UUID], Optional[datetime], int]:
    req_id = headers.get("x-request-id") or headers.get("X-Request-Id") or headers.get("request-id")
    try:
        request_uuid = uuid.UUID(req_id) if req_id else None
    except Exception:
        request_uuid = None

    date_hdr = headers.get("date") or headers.get("Date")
    try:
        dt_utc = parsedate_to_datetime(date_hdr) if date_hdr else None
        response_dttm = dt_utc.astimezone(ZoneInfo("Europe/Moscow")) if dt_utc else None
    except Exception:
        response_dttm = None

    try:
        status_code = int(headers.get("status", 200))
    except Exception:
        status_code = 200

    return request_uuid, response_dttm, status_code


@asset(
    resource_defs={"ozon_client": ozon_client, "postgres": postgres_resource},
    required_resource_keys={"ozon_client", "postgres"},
    key_prefix=["bronze"],
    name="ozon_analytics_data_1d",
    description="Сохраняет сырые ответы /v1/analytics/data (воронка продаж) в bronze.ozon_analytics_data_1d. Каждый запрос — отдельная строка.",
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
async def bronze_ozon_analytics_data_1d(context) -> None:
    # 1) Метаданные запуска
    run_uuid      = context.run_id
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    raw_dt        = datetime.fromisoformat(scheduled_iso) if scheduled_iso else datetime.now(std_timezone.utc)
    run_dttm      = raw_dt.astimezone(ZoneInfo("Europe/Moscow"))

    # 2) Даты + параметры
    date_from = context.op_config.get("date_from")
    date_to   = context.op_config.get("date_to")
    if not (date_from and date_to):
        yest = (run_dttm - timedelta(days=1)).date()
        date_from = date_to = yest.strftime("%Y-%m-%d")

    metrics   = context.op_config.get("metrics")   or ["view_count", "session_count"]
    dimension = context.op_config.get("dimension") or ["sku"]

    limit  = int(context.op_config.get("limit", 1000))
    offset = int(context.op_config.get("offset", 0))

    total_rows = 0

    # 3) Пагинация и вставка «1 запрос = 1 строка»
    async with context.resources.ozon_client as client:
        api_token_id = client.token_id

        while True:
            context.log.info(f"[ozon analytics] fetch {date_from}..{date_to} offset={offset} limit={limit}")
            try:
                data, headers, http_status = await client.fetch_sales_funnel(
                    date_from=date_from, date_to=date_to,
                    metrics=metrics, dimension=dimension,
                    limit=limit, offset=offset
                )
            except Exception as e:
                context.log.error(f"[ozon analytics] fetch error at offset={offset}: {e}")
                raise

            # метаданные ответа
            request_uuid, response_dttm, status_code = _parse_response_meta(headers)
            request_uuid  = request_uuid  or uuid.uuid4()
            response_dttm = response_dttm or run_dttm

            # «сырое тело» как текст
            response_text = json.dumps(data, ensure_ascii=False)

            record = {
                "api_token_id":       api_token_id,
                "run_uuid":           run_uuid,
                "run_dttm":           run_dttm,
                "request_uuid":       request_uuid,
                "request_dttm":       run_dttm,
                "request_parameters": {
                    "date_from": date_from,
                    "date_to":   date_to,
                    "metrics":   metrics,
                    "dimension": dimension,
                    "limit":     limit,
                    "offset":    offset,
                },
                "request_body":       None,
                "response_dttm":      response_dttm,
                "response_code":      status_code or http_status,
                "response_body":      response_text,
            }

            async with context.resources.postgres() as session:
                await session.execute(OzonAnalyticsData1d.__table__.insert().values(**record))
                await session.commit()
                total_rows += 1

            # условие выхода: если страница пустая или неполная — дальше данных нет
            try:
                rows = (data or {}).get("result", {}).get("data", [])
            except Exception:
                rows = []
            if not rows or len(rows) < limit:
                break

            offset += limit

    context.log.info(f"[bronze_ozon_analytics_data_1d] inserted {total_rows} rows for {date_from}..{date_to}")
