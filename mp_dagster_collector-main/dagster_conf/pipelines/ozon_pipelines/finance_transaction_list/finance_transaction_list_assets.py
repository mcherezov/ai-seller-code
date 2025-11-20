import uuid
import json
from typing import Dict, Optional, Any
from datetime import datetime, timedelta, timezone as std_timezone
from zoneinfo import ZoneInfo
from email.utils import parsedate_to_datetime

from dagster import asset, RetryPolicy

from src.db.bronze.ozon_models import OzonFinanceTransactionList1d
from dagster_conf.resources.ozon_client import ozon_client
from dagster_conf.resources.pg_resource import postgres_resource


def _extract_items(resp: Dict[str, Any]) -> Optional[list]:
    if not isinstance(resp, dict):
        return None
    r = resp.get("result", resp)
    for key in ("operations", "list", "items", "transactions"):
        arr = r.get(key)
        if isinstance(arr, list):
            return arr
    if isinstance(r, list):
        return r
    return None


def _parse_response_meta(headers: Dict[str, str]) -> tuple[Optional[uuid.UUID], Optional[datetime]]:
    """Извлекаем возможный request-id и дату ответа из заголовков."""
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

    return request_uuid, response_dttm


@asset(
    resource_defs={
        "ozon_client": ozon_client,
        "postgres":    postgres_resource,
    },
    required_resource_keys={"ozon_client", "postgres"},
    key_prefix=["bronze"],
    name="ozon_finance_transaction_list_1d",
    description="POST /v3/finance/transaction/list → сохраняет сырые JSON-страницы в bronze.ozon_finance_transaction_list_1d",
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
async def bronze_ozon_finance_transaction_list_1d(context) -> None:
    # 1) Метаданные запуска
    run_uuid      = context.run_id
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    raw_dt        = datetime.fromisoformat(scheduled_iso) if scheduled_iso else datetime.now(std_timezone.utc)
    run_dttm      = raw_dt.astimezone(ZoneInfo("Europe/Moscow"))

    # 2) Входные параметры (опционально через op_config)
    date_from_cfg = context.op_config.get("date_from")  # ISO: 'YYYY-MM-DDTHH:MM:SSZ' или 'YYYY-MM-DD'
    date_to_cfg   = context.op_config.get("date_to")
    page_size     = int(context.op_config.get("page_size", 1000))

    if date_from_cfg and date_to_cfg:
        date_from_iso = date_from_cfg
        date_to_iso   = date_to_cfg
    else:
        # по умолчанию — вчерашние сутки по Москве (00:00..23:59:59)
        y = (run_dttm - timedelta(days=1)).date()
        date_from_iso = f"{y}T00:00:00+03:00"
        date_to_iso   = f"{y}T23:59:59+03:00"

    Session = context.resources.postgres
    async with context.resources.ozon_client as client, Session() as session:
        api_token_id = client.token_id

        page = 1
        inserted = 0

        while True:
            payload_req = {
                "filter": {"date": {"from": date_from_iso, "to": date_to_iso}},
                "page": page,
                "page_size": page_size,
            }

            context.log.info(f"[ozon finance] page={page} size={page_size} from={date_from_iso} to={date_to_iso}")

            # ⬇️ теперь клиент возвращает (data, headers, status)
            resp_dict, headers, status = await client.fetch_sales(
                date_from=date_from_iso, date_to=date_to_iso, page=page, page_size=page_size
            )

            # сырые данные → JSON строкой без форматирования
            resp_text = json.dumps(resp_dict, ensure_ascii=False, separators=(",", ":"))

            # метаданные ответа
            req_uuid_from_hdr, resp_dt_from_hdr = _parse_response_meta(headers)
            request_uuid  = req_uuid_from_hdr or uuid.uuid4()
            response_dttm = resp_dt_from_hdr or run_dttm

            # запись одной страницы (1 запрос = 1 строка)
            record = {
                "api_token_id":       api_token_id,
                "run_uuid":           run_uuid,
                "run_dttm":           run_dttm,
                "request_uuid":       request_uuid,
                "request_dttm":       run_dttm,
                "request_parameters": None,
                "request_body":       payload_req,
                "response_dttm":      response_dttm,
                "response_code":      int(status),
                "response_body":      resp_text,
            }
            await session.execute(OzonFinanceTransactionList1d.__table__.insert().values(**record))
            inserted += 1

            # пагинация: если вернулось меньше page_size или массив пуст — заканчиваем
            items = _extract_items(resp_dict)
            if not items or (isinstance(items, list) and len(items) < page_size):
                await session.commit()
                break

            page += 1

        context.log.info(
            f"[bronze_ozon_finance_transaction_list_1d] inserted_pages={inserted} "
            f"from={date_from_iso} to={date_to_iso} page_size={page_size}"
        )
