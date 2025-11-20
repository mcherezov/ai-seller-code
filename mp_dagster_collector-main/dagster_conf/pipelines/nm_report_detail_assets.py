import json
import uuid
import asyncio
from datetime import datetime, timezone, timedelta, time
from zoneinfo import ZoneInfo
from typing import Dict, Any
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from aiohttp import ClientResponseError
from dagster import asset, Failure

from dagster_conf.pipelines.wb_bronze_ops import extract_metadata_from_headers
from dagster_conf.resources.wb_client import wildberries_client
from dagster_conf.resources.pg_resource import postgres_resource

from src.db.bronze.models import WbNmReportDetail1d
from src.db.silver.models import (
    SilverWbBuyoutsPercent1d,
    SilverWbSalesFunnels1d
)

MSK = ZoneInfo("Europe/Moscow")
RATE_LIMIT_SEC = 20


def _parse_iso_any_tz(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


@asset(
    resource_defs={"wildberries_client": wildberries_client, "postgres": postgres_resource},
    required_resource_keys={"wildberries_client", "postgres"},
    key_prefix=["bronze"],
    name="wb_nm_report_detail_1d",
    description="Сохраняет детализированную воронку продаж за последние 30 дней в bronze.wb_nm_report_detail_1d с расчётом business_dttm/метаданных запуска.",
)
async def bronze_wb_nm_report_detail_1d(context) -> None:
    logger = context.log
    t0 = datetime.now()

    # ── мета ранa ─────────────────────────────────────────────────────────────
    run_uuid = context.run_id
    all_tags = dict(context.dagster_run.tags or {})
    logger.info(f"[nm_report_detail] ▶ run meta: run_id={run_uuid}, tags={all_tags}")

    scheduled_iso = all_tags.get("dagster/scheduled_execution_time")
    if not scheduled_iso:
        logger.error("[nm_report_detail] ✖ нет тега dagster/scheduled_execution_time; доступные теги см. выше")
        raise Failure("Missing tag 'dagster/scheduled_execution_time'")

    # разжёвываем таймзоны
    try:
        run_schedule_dttm_utc = _parse_iso_any_tz(scheduled_iso)
        run_schedule_dttm = run_schedule_dttm_utc.astimezone(MSK)
        logger.info(
            "[nm_report_detail] clock: "
            f"scheduled_utc={run_schedule_dttm_utc.isoformat()} scheduled_msk={run_schedule_dttm.isoformat()}"
        )
    except Exception as e:
        logger.error(f"[nm_report_detail] ❌ parse scheduled tag: {scheduled_iso!r} -> {e}")
        raise

    run_dttm = datetime.now(MSK)
    api_token_id = context.resources.wildberries_client.token_id
    logger.info(
        "[nm_report_detail] ▶ start "
        f"run_uuid={run_uuid} run_dttm={run_dttm.isoformat()} api_token_id={api_token_id}"
    )

    payload = {
        "nmIDs":      [],
        "objectIDs":  [],
        "tagIDs":     [],
        "brandNames": [],
        "timezone":   "Europe/Moscow",
        "period":     {"begin": None, "end": None},
        "page":       None,
    }

    RATE_LIMIT_SEC = 1.0
    last_request_utc: datetime | None = None

    total_days = total_pages = total_inserts = 0
    total_retries_429 = total_retries_5xx = 0

    async with context.resources.wildberries_client as client, \
               context.resources.postgres() as session:

        today_msk = run_schedule_dttm.date()
        start_date = today_msk - timedelta(days=1)
        logger.info(f"[nm_report_detail] time window: today_msk={today_msk}, start_date={start_date}, days=30")

        for delta in range(0, 30):
            target_date = start_date - timedelta(days=delta)
            biz_dttm = datetime.combine(target_date, time.min, tzinfo=MSK)

            day_pages = day_inserts = day_retries_429 = day_retries_5xx = 0
            logger.info(f"[nm_report_detail] ▷ day_begin={target_date} (business_dttm={biz_dttm.isoformat()})")

            page = 1
            while True:
                payload["period"]["begin"] = f"{target_date} 00:00:00"
                payload["period"]["end"]   = f"{target_date} 23:59:59"
                payload["page"] = page

                # rate-limit
                if last_request_utc is not None:
                    elapsed = (datetime.now(timezone.utc) - last_request_utc).total_seconds()
                    if elapsed < RATE_LIMIT_SEC:
                        wait_s = round(RATE_LIMIT_SEC - elapsed, 3)
                        logger.debug(f"[nm_report_detail] ⏳ throttle wait={wait_s}s (page={page} date={target_date})")
                        await asyncio.sleep(wait_s)

                request_dttm = datetime.now(MSK)

                while True:
                    try:
                        logger.debug(f"[nm_report_detail] → fetch page={page} period={payload['period']}")
                        data, headers = await client.fetch_sales_funnel(payload)
                        break
                    except ClientResponseError as e:
                        # тело ошибки мы прокинули в message/wb-error (см. патч клиента)
                        if e.status == 429:
                            day_retries_429 += 1; total_retries_429 += 1
                            logger.warning(f"[nm_report_detail] 429 (page={page} date={target_date}); sleep=60s; err={e}")
                            await asyncio.sleep(60); continue
                        if 500 <= e.status < 600:
                            day_retries_5xx += 1; total_retries_5xx += 1
                            logger.warning(f"[nm_report_detail] {e.status} (page={page} date={target_date}); sleep=60s; err={e}")
                            await asyncio.sleep(60); continue
                        logger.error(f"[nm_report_detail] ❌ abort HTTP {e.status} (page={page} date={target_date}) err={e}")
                        raise

                receive_dttm = datetime.now(MSK)
                last_request_utc = datetime.now(timezone.utc)

                # HTTP-метаданные
                try:
                    request_uuid, response_dttm = extract_metadata_from_headers(headers)
                except Exception as meta_err:
                    logger.warning(f"[nm_report_detail] headers parse failed, fallback request_uuid=rand; headers={headers}")
                    request_uuid, response_dttm = (uuid.uuid4(), request_dttm)

                response_code = int(headers.get("status", 200))
                is_next = bool(data.get("data", {}).get("isNextPage", False))
                data_obj = data.get("data") if isinstance(data, dict) else None
                cards = (data_obj or {}).get("cards", [])
                approx_size = len(json.dumps(data, ensure_ascii=False)) if isinstance(data, (dict, list)) else 0
                wb_error = headers.get("wb-error")

                logger.info(
                    "[nm_report_detail] ✓ page=%s date=%s status=%s next=%s cards=%s size≈%sB "
                    "resp_dttm=%s req_uuid=%s wb_error=%s",
                    page, target_date, response_code, is_next, len(cards), approx_size,
                    (response_dttm or request_dttm).isoformat(), request_uuid, (wb_error[:200] if wb_error else None),
                )
                logger.debug(
                    "[nm_report_detail] keys: %s",
                    list((data_obj or {}).keys())[:10]
                )

                record = {
                    "api_token_id":       api_token_id,
                    "run_uuid":           run_uuid,
                    "run_dttm":           run_dttm,
                    "run_schedule_dttm":  run_schedule_dttm,
                    "business_dttm":      biz_dttm,
                    "request_uuid":       request_uuid or uuid.uuid4(),
                    "request_dttm":       request_dttm,
                    "request_parameters": None,
                    "request_body":       payload,  # JSONB
                    "response_dttm":      response_dttm or request_dttm,
                    "receive_dttm":       receive_dttm,
                    "response_code":      response_code,
                    "response_body":      json.dumps(data, ensure_ascii=False),
                }
                await session.execute(WbNmReportDetail1d.__table__.insert().values(**record))
                logger.debug("[nm_report_detail] ⬇ bronze insert ok (date=%s page=%s)", target_date, page)

                day_pages += 1; day_inserts += 1; total_pages += 1; total_inserts += 1
                if not is_next:
                    break
                page += 1

            total_days += 1
            logger.info(
                "[nm_report_detail] ◁ day_end=%s: pages=%s inserts=%s retries_429=%s retries_5xx=%s",
                target_date, day_pages, day_inserts, day_retries_429, day_retries_5xx
            )

        await session.commit()
        logger.info("[nm_report_detail] commit OK")

    dt = (datetime.now() - t0).total_seconds()
    logger.info(
        "[nm_report_detail] ■ DONE days=%s pages=%s inserts=%s retries_429=%s retries_5xx=%s elapsed=%.1fs",
        total_days, total_pages, total_inserts, total_retries_429, total_retries_5xx, dt
    )


@asset(
    deps=[bronze_wb_nm_report_detail_1d],
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    key_prefix=["silver"],
    name="wb_buyouts_percent_1d",
    description="Переносит buyoutsPercent из bronze → silver.wb_buyouts_percent_1d (PK: business_dttm, nm_id).",
)
async def silver_wb_buyouts_percent_1d(context):
    log = context.log
    run_id = context.run_id
    log.info(f"[silver_buyouts] ▶ start run_id={run_id}")

    async with context.resources.postgres() as session:
        bronze_rows = (await session.execute(
            select(WbNmReportDetail1d).where(WbNmReportDetail1d.run_uuid == run_id)
        )).scalars().all()
        log.info(f"[silver_buyouts] bronze rows fetched: {len(bronze_rows)}")

        inserted = 0
        skipped_no_period = 0
        unique_keys = set()

        for bronze in bronze_rows:
            company_id = (await session.execute(
                text("SELECT company_id FROM core.tokens WHERE token_id = :tid"),
                {"tid": bronze.api_token_id},
            )).scalar_one()
            payload = json.loads(bronze.response_body) if bronze.response_body else {}
            cards   = payload.get("data", {}).get("cards", [])
            if not cards:
                log.debug(f"[silver_buyouts] empty cards for request_uuid={bronze.request_uuid}")
            for item in cards:
                sel = item.get("statistics", {}).get("selectedPeriod", {})
                begin = sel.get("begin")
                if not begin:
                    skipped_no_period += 1
                    continue

                date_dt = _parse_iso_any_tz(begin).astimezone(MSK)
                biz_dttm = date_dt.replace(hour=0, minute=0, second=0, microsecond=0)

                key = (biz_dttm, item.get("nmID"))
                unique_keys.add(key)

                rec = {
                    "request_uuid":     bronze.request_uuid,
                    "response_dttm":    bronze.response_dttm or bronze.request_dttm,
                    "company_id":       company_id,
                    "date":             date_dt,
                    "business_dttm":    biz_dttm,
                    "nm_id":            item.get("nmID"),
                    "buyouts_percent":  sel.get("conversions", {}).get("buyoutsPercent") or 0.0,
                }

                ins = pg_insert(SilverWbBuyoutsPercent1d).values(**rec)
                upsert = ins.on_conflict_do_update(
                    index_elements=[
                        SilverWbBuyoutsPercent1d.business_dttm,
                        SilverWbBuyoutsPercent1d.nm_id,
                    ],
                    set_=rec,
                )
                await session.execute(upsert)
                inserted += 1

        await session.commit()
        log.info(
            "[silver_buyouts] ◼ upserted=%s unique_keys=%s skipped_no_period=%s",
            inserted, len(unique_keys), skipped_no_period
        )


@asset(
    deps=[bronze_wb_nm_report_detail_1d],
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    key_prefix=["silver"],
    name="wb_sales_funnels_1d",
    description="Переносит воронку продаж за вчера из bronze → silver.wb_sales_funnels_1д (PK: business_dttm, nm_id).",
)
async def silver_wb_sales_funnels_1d(context):

    log = context.log
    tags = dict(context.dagster_run.tags or {})
    log.info(f"[silver_funnel] ▶ tags={tags}")

    scheduled_iso = tags.get("dagster/scheduled_execution_time")
    if not scheduled_iso:
        log.error("[silver_funnel] ✖ нет тега dagster/scheduled_execution_time")
        raise Failure("Missing tag 'dagster/scheduled_execution_time'")

    run_schedule_dttm = _parse_iso_any_tz(scheduled_iso).astimezone(MSK)
    yesterday = (run_schedule_dttm - timedelta(days=1)).date()
    business_dttm = datetime.combine(yesterday, time.min, tzinfo=MSK)
    run_id = context.run_id

    log.info(
        "[silver_funnel] window: scheduled_msk=%s yesterday=%s business_dttm=%s run_id=%s",
        run_schedule_dttm.isoformat(), yesterday, business_dttm.isoformat(), run_id
    )

    async with context.resources.postgres() as session:
        bronze_rows = (await session.execute(
            select(WbNmReportDetail1d).where(WbNmReportDetail1d.run_uuid == run_id)
        )).scalars().all()
        log.info(f"[silver_funnel] bronze rows fetched: {len(bronze_rows)}")

        count_rows = 0
        mismatched_dates = 0
        empty_cards = 0
        sample_nm = []

        for bronze in bronze_rows:
            company_id = (await session.execute(
                text("SELECT company_id FROM core.tokens WHERE token_id = :tid"),
                {"tid": bronze.api_token_id},
            )).scalar_one()

            payload = json.loads(bronze.response_body) if bronze.response_body else {}
            cards   = payload.get("data", {}).get("cards", [])
            if not cards:
                empty_cards += 1
            for item in cards:
                sel = item.get("statistics", {}).get("selectedPeriod", {})
                begin = sel.get("begin")
                if not begin:
                    continue
                item_date = _parse_iso_any_tz(begin).astimezone(MSK).date()
                if item_date != yesterday:
                    mismatched_dates += 1
                    continue

                obj = item.get("object") or {}
                nm_id = item.get("nmID")
                if len(sample_nm) < 5 and nm_id not in sample_nm:
                    sample_nm.append(nm_id)

                rec = {
                    "request_uuid":      bronze.request_uuid,
                    "response_dttm":     bronze.response_dttm or bronze.request_dttm,
                    "company_id":        company_id,
                    "business_dttm":     business_dttm,
                    "date":              item_date,
                    "nm_id":             nm_id,
                    "supplier_article":  (item.get("vendorCode") or ""),
                    "brand":             (item.get("brandName") or ""),
                    "subject_id":        int(obj.get("id") or 0),
                    "subject_name":      (obj.get("name") or ""),
                    "open_card_count":   int(sel.get("openCardCount") or 0),
                    "add_to_cart_count": int(sel.get("addToCartCount") or 0),
                    "orders_count":      int(sel.get("ordersCount") or 0),
                    "orders_sum":        float(sel.get("ordersSumRub") or 0.0),
                    "buyouts_count":     int(sel.get("buyoutsCount") or 0),
                    "buyouts_sum":       int(sel.get("buyoutsSumRub") or 0),
                    "cancel_count":      int(sel.get("cancelCount") or 0),
                    "cancel_sum":        float(sel.get("cancelSumRub") or 0.0),
                    "avg_price":         float(sel.get("avgPriceRub") or 0.0),
                    "stocks_mp":         float((item.get("stocks") or {}).get("stocksMp") or 0.0),
                    "stocks_wb":         float((item.get("stocks") or {}).get("stocksWb") or 0.0),
                }

                ins = pg_insert(SilverWbSalesFunnels1d).values(**rec)
                upsert = ins.on_conflict_do_update(
                    index_elements=[
                        SilverWbSalesFunnels1d.business_dttm,
                        SilverWbSalesFunnels1d.nm_id,
                    ],
                    set_=rec,
                )
                await session.execute(upsert)
                count_rows += 1

        await session.commit()
        log.info(
            "[silver_funnel] ◼ upserted=%s empty_cards=%s mismatched_dates=%s sample_nm_ids=%s",
            count_rows, empty_cards, mismatched_dates, sample_nm
        )


def build_nm_report_detail_params(business_dttm: datetime, company_id: int) -> Dict[str, Any]:
    """
    Сборщик параметров для analytics v2 / nm-report/detail (sync_api-фабрика).
    Возвращает dict с ключом "payload", чтобы фабрика вызвала client.fetch_sales_funnel(payload=...).

    Поля payload:
      - nmIDs / objectIDs / tagIDs / brandNames — пустые списки (без фильтров).
      - timezone — "Europe/Moscow".
      - period.begin / period.end — границы ДНЯ партиции по MSK (строки 'YYYY-MM-DD HH:MM:SS').
      - page — 1 (если вызывается метод БЕЗ встроенной пагинации).
    """
    day = business_dttm.astimezone(MSK).date().isoformat()
    payload = {
        "nmIDs": [],
        "objectIDs": [],
        "tagIDs": [],
        "brandNames": [],
        "timezone": "Europe/Moscow",
        "period": {
            "begin": f"{day} 00:00:00",
            "end":   f"{day} 23:59:59",
        },
        "page": 1,
    }
    return {"payload": payload}