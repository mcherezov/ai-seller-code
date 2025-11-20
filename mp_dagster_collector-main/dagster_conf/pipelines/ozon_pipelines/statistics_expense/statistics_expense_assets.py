import uuid
from typing import Dict, Optional
from datetime import datetime, timedelta, timezone as std_timezone
from zoneinfo import ZoneInfo
from email.utils import parsedate_to_datetime

from dagster import asset, RetryPolicy
from src.db.bronze.ozon_models import OzonAdvExpense1d

# ресурсы
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

    # resp.raise_for_status() уже сработал в клиенте → считаем 200
    return request_uuid, response_dttm, 200


@asset(
    resource_defs={
        "ozon_client": ozon_client,
        "postgres":    postgres_resource,
    },
    required_resource_keys={"ozon_client", "postgres"},
    key_prefix=["bronze"],
    name="ozon_adv_expense_1d",
    description="Сохраняет CSV расходов РК Ozon Performance в bronze.ozon_adv_expense_1d (response_body=BYTEA)",
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
async def bronze_ozon_adv_expense_1d(context) -> None:
    # 1) Метаданные запуска
    run_uuid      = context.run_id
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    raw_dt        = datetime.fromisoformat(scheduled_iso) if scheduled_iso else datetime.now(std_timezone.utc)
    run_dttm      = raw_dt.astimezone(ZoneInfo("Europe/Moscow"))

    # 2) Даты: по умолчанию — вчера (по Мск), можно переопределить в op_config
    date_from_cfg = context.op_config.get("date_from")
    date_to_cfg   = context.op_config.get("date_to")
    if date_from_cfg and date_to_cfg:
        date_from, date_to = date_from_cfg, date_to_cfg
    else:
        yest = (run_dttm - timedelta(days=1)).date()
        date_from = date_to = yest.strftime("%Y-%m-%d")

    # 3) Вызов API
    async with context.resources.ozon_client as client:
        api_token_id = client.token_id
        context.log.info(f"[ozon expense] fetch CSV for {date_from}..{date_to}")
        payload_bytes, headers = await client.fetch_ad_campaign_expense(date_from, date_to)

    # 4) Метаданные ответа
    request_uuid, response_dttm, status_code = _parse_response_meta(headers)
    request_uuid  = request_uuid or uuid.uuid4()
    response_dttm = response_dttm or run_dttm

    # 5) Формируем запись: response_body ← байты CSV (BYTEA)
    record = {
        "api_token_id":       api_token_id,
        "run_uuid":           run_uuid,
        "run_dttm":           run_dttm,
        "request_uuid":       request_uuid,
        "request_dttm":       run_dttm,
        "request_parameters": {"dateFrom": date_from, "dateTo": date_to},
        "request_body":       None,
        "response_dttm":      response_dttm,
        "response_code":      status_code,
        "response_body":      payload_bytes,  # <-- RAW CSV BYTES
    }

    # 6) Вставка в бронзу
    Session = context.resources.postgres
    async with Session() as session:
        await session.execute(OzonAdvExpense1d.__table__.insert().values(**record))
        await session.commit()

    context.log.info(
        f"[bronze_ozon_adv_expense_1d] inserted run_uuid={run_uuid} "
        f"dateFrom={date_from} dateTo={date_to} bytes={len(payload_bytes)}"
    )
