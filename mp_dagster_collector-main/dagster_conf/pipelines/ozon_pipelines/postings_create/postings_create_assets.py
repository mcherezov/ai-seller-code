import uuid
import asyncio
from typing import Dict, Optional
from datetime import datetime, timedelta, timezone as std_timezone
from zoneinfo import ZoneInfo
from email.utils import parsedate_to_datetime

from dagster import asset, RetryPolicy

from src.db.bronze.ozon_models import OzonPostingsCreate1d
from dagster_conf.resources.ozon_client import ozon_client
from dagster_conf.resources.pg_resource import postgres_resource


def _parse_response_meta(headers: Dict[str, str]) -> tuple[Optional[uuid.UUID], Optional[datetime]]:
    """Пытаемся достать request_uuid и время ответа из заголовков."""
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
    name="ozon_report_postings_file_1d",
    description=(
        "Создаёт отчёт заказов, ждёт готовности и СКАЧИВАЕТ XLSX. "
        "В бронзу сохраняется только финальный HTTP-запрос скачивания (1 запрос = 1 строка; response_body=BYTEA)."
    ),
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
async def bronze_ozon_report_postings_file_1d(context) -> None:
    # 1) Метаданные запуска
    run_uuid      = context.run_id
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    raw_dt        = datetime.fromisoformat(scheduled_iso) if scheduled_iso else datetime.now(std_timezone.utc)
    run_dttm      = raw_dt.astimezone(ZoneInfo("Europe/Moscow"))

    # 2) Конфиг: даты и схема доставки
    date_from = context.op_config.get("date_from")
    date_to   = context.op_config.get("date_to")
    if not (date_from and date_to):
        # по умолчанию — вчерашний день по Мск
        y = (run_dttm - timedelta(days=1)).date()
        date_from = f"{y}T00:00:00+03:00"
        date_to   = f"{y}T23:59:59+03:00"

    delivery_schema = context.op_config.get("delivery_schema", "fbo")  # "fbo" | "fbs"
    poll_delay_sec  = int(context.op_config.get("poll_delay_sec", 15))
    max_checks      = int(context.op_config.get("max_checks", 40))

    # 3) Создание → ожидание → скачивание.
    Session = context.resources.postgres
    async with context.resources.ozon_client as client, Session() as session:
        api_token_id = client.token_id

        # 3.1 Создать отчёт → получить code
        context.log.info(f"[ozon report create] {delivery_schema} {date_from}..{date_to}")
        code = await client.create_orders_report(date_from=date_from, date_to=date_to, delivery_schema=delivery_schema)
        if not code:
            raise RuntimeError("Не получили code отчёта от /v1/report/postings/create")

        # 3.2 Ожидать готовности, достать file_url
        file_url: Optional[str] = None
        for i in range(max_checks):
            context.log.info(f"[ozon report status] try={i+1}/{max_checks} code={code}")
            # ожидается новая сигнатура: (data, headers, status)
            info_data, _, _ = await client.get_report_status(code)

            if not isinstance(info_data, dict):
                status_str = str(info_data).lower()
                if status_str in {"success", "done", "ready"}:
                    raise RuntimeError("Статус готов, но метод get_report_status не вернул JSON с file_url.")
                if status_str in {"failed", "error"}:
                    raise RuntimeError(f"Отчёт завершился со статусом '{status_str}'")
                await asyncio.sleep(poll_delay_sec)
                continue

            result = info_data.get("result") or {}
            status_str = (result.get("status") or "").lower()
            if status_str in {"success", "done", "ready"}:
                file_url = result.get("file_url") or result.get("file") or result.get("url")
                break
            if status_str in {"failed", "error"}:
                raise RuntimeError(f"Отчёт завершился со статусом '{status_str}'")
            await asyncio.sleep(poll_delay_sec)

        if not file_url:
            raise TimeoutError("Не получили file_url: отчёт не подготовился за отведённое число попыток.")

        # 3.3 Скачивание файла
        context.log.info(f"[ozon report download] {file_url}")
        file_bytes, dl_headers, dl_status = await client.download_orders_report(file_url)

        # метаданные ответа
        req_uuid, resp_dttm = _parse_response_meta(dl_headers)
        request_uuid  = req_uuid or uuid.uuid4()
        response_dttm = resp_dttm or run_dttm

        # формируем одну запись: 1 HTTP запрос (download) = 1 строка
        record = {
            "api_token_id":       api_token_id,
            "run_uuid":           run_uuid,
            "run_dttm":           run_dttm,
            "request_uuid":       request_uuid,
            "request_dttm":       run_dttm,
            "request_parameters": {
                "date_from": date_from,
                "date_to":   date_to,
                "delivery_schema": delivery_schema,
                "code": code,
            },
            "request_body":       {"file_url": file_url},
            "response_dttm":      response_dttm,
            "response_code":      int(dl_status),
            "response_body":      file_bytes,
        }

        await session.execute(OzonPostingsCreate1d.__table__.insert().values(**record))
        await session.commit()

        context.log.info(
            f"[bronze_ozon_report_postings_file_1d] saved file for code={code} "
            f"schema={delivery_schema} range={date_from}..{date_to} bytes={len(file_bytes)}"
        )
