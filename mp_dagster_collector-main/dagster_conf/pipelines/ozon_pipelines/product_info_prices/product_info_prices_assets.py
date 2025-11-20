import uuid
from typing import Dict, Optional
from datetime import datetime, timedelta, timezone as std_timezone
from zoneinfo import ZoneInfo
from dagster import asset, RetryPolicy
from email.utils import parsedate_to_datetime
import json

from src.db.bronze.ozon_models import OzonProductInfoPrices1d
from dagster_conf.resources.ozon_client import ozon_client
from dagster_conf.resources.pg_resource import postgres_resource


def _next_cursor(resp: Dict) -> Optional[str]:
    if not isinstance(resp, dict):
        return None
    r = resp.get("result") or resp
    if isinstance(r.get("cursor"), str) and r.get("cursor"):
        return r["cursor"]
    for k in ("next_cursor", "next"):
        v = r.get(k)
        if isinstance(v, str) and v:
            return v
    if r.get("has_next") is False or r.get("is_last_page") is True:
        return None
    return None


def _parse_response_meta(headers: Dict[str, str]):
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
    resource_defs={"ozon_client": ozon_client, "postgres": postgres_resource},
    required_resource_keys={"ozon_client", "postgres"},
    key_prefix=["bronze"],
    name="ozon_product_info_prices_1d",
    description="POST /v5/product/info/prices → сырые JSON-страницы в bronze.ozon_product_info_prices_1d (1 запрос = 1 строка)",
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
async def bronze_ozon_product_info_prices_1d(context) -> None:
    # 1) Метаданные запуска
    run_uuid      = context.run_id
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    raw_dt        = datetime.fromisoformat(scheduled_iso) if scheduled_iso else datetime.now(std_timezone.utc)
    run_dttm      = raw_dt.astimezone(ZoneInfo("Europe/Moscow"))

    # 2) Параметры
    visibility = context.op_config.get("visibility", "ALL")
    limit      = int(context.op_config.get("limit", 1000))
    cursor     = context.op_config.get("cursor", "")
    page       = 1
    inserted   = 0

    # 3) Пагинация: каждая страница — отдельная строка
    Session = context.resources.postgres
    async with context.resources.ozon_client as client, Session() as session:
        api_token_id = client.token_id

        while True:
            payload_req = {"filter": {"visibility": visibility}, "cursor": cursor, "limit": limit}
            context.log.info(f"[ozon prices] page={page} limit={limit} cursor={'<empty>' if not cursor else cursor[:24]+'...'}")

            # ⬇️ теперь принимаем (data, headers, status)
            resp_dict, headers, status = await client.fetch_commission(
                visibility=visibility, limit=limit, cursor=cursor
            )

            # метаданные из заголовков
            req_uuid_from_hdr, resp_dt_from_hdr = _parse_response_meta(headers)
            request_uuid  = req_uuid_from_hdr or uuid.uuid4()
            response_dttm = resp_dt_from_hdr or run_dttm

            # сырое тело → текст JSON
            resp_text = json.dumps(resp_dict, ensure_ascii=False, separators=(",", ":"))

            record = {
                "api_token_id":       api_token_id,
                "run_uuid":           run_uuid,
                "run_dttm":           run_dttm,
                "request_uuid":       request_uuid,
                "request_dttm":       run_dttm,
                "request_parameters": None,
                "request_body":       payload_req,     # что отправили
                "response_dttm":      response_dttm,
                "response_code":      int(status),     # ⬅️ используем статус из клиента
                "response_body":      resp_text,
            }

            await session.execute(OzonProductInfoPrices1d.__table__.insert().values(**record))
            inserted += 1

            nxt = _next_cursor(resp_dict)
            if not nxt:
                await session.commit()
                break

            cursor = nxt
            page  += 1

    context.log.info(f"[bronze_ozon_product_info_prices_1d] inserted_pages={inserted} visibility={visibility} limit={limit}")
