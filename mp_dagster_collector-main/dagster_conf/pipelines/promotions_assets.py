from dagster import asset, RetryPolicy
from aiohttp import ClientResponseError
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional
import json
import uuid

from dagster_conf.resources.wb_client import wildberries_client
from dagster_conf.resources.pg_resource import postgres_resource
from dagster_conf.pipelines.wb_bronze_ops import extract_metadata_from_headers
from dagster_conf.pipelines.utils import prev_hour_mskt

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db.bronze.models import WbAdvPromotions1h
from src.db.silver.models import SilverWbAdvPromotions1h


MSK = ZoneInfo("Europe/Moscow")


# ──────────────────────────────────────────────────────────────────────────────
# BRONZE
# ──────────────────────────────────────────────────────────────────────────────
@asset(
    name="wb_adv_promotions_1h",
    key_prefix=["bronze"],
    description="Материализует список РК (GET /adv/v1/promotion/count) в bronze.wb_adv_promotions_1h",
    resource_defs={"wildberries_client": wildberries_client, "postgres": postgres_resource},
    required_resource_keys={"wildberries_client", "postgres"},
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
async def bronze_wb_adv_promotions_1h(context) -> None:
    """
    1) Берём плановое время запуска (Dagster tag) → считаем business_dttm как предыдущий час по МСК.
    2) Делаем GET /adv/v1/promotion/count без параметров.
    3) Сохраняем полный payload и метаданные ответа в bronze.wb_adv_promotions_1h.
    """
    run_uuid = context.run_id

    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    raw_dt = datetime.fromisoformat(scheduled_iso) if scheduled_iso else datetime.now(timezone.utc)
    run_schedule_dttm = raw_dt.astimezone(MSK)

    business_dttm = prev_hour_mskt(run_schedule_dttm)
    run_dttm      = datetime.now(MSK)
    request_uuid  = uuid.uuid4()
    api_token_id  = context.resources.wildberries_client.token_id

    try:
        request_dttm = datetime.now(MSK)
        async with context.resources.wildberries_client as client:
            payload, headers = await client.get_advert_list()

        # метаданные из заголовков
        response_uuid, response_dttm = extract_metadata_from_headers(headers)
        response_code = int(headers.get("status", 200))
        receive_dttm  = datetime.now(MSK)

        if not response_dttm:
            response_dttm = receive_dttm

        record = {
            "run_uuid":           run_uuid,
            "run_dttm":           run_dttm,
            "run_schedule_dttm":  run_schedule_dttm,
            "business_dttm":      business_dttm,
            "receive_dttm":       receive_dttm,

            "request_uuid":       request_uuid,
            "request_dttm":       request_dttm,
            "request_parameters": None,            # GET без query
            "request_body":       None,            # GET без body

            "response_dttm":      response_dttm,
            "response_code":      response_code,
            "response_body":      json.dumps(payload, ensure_ascii=False),

            "api_token_id":       api_token_id,
        }

        cols = {c.name for c in WbAdvPromotions1h.__table__.columns}
        payload_for_model = {k: v for k, v in record.items() if k in cols}

        async with context.resources.postgres() as session:
            await session.execute(WbAdvPromotions1h.__table__.insert().values(**payload_for_model))
            await session.commit()

        context.log.info(
            "[bronze_wb_adv_promotions_1h] saved 1 row | run_uuid=%s | business_dttm=%s | "
            "api_token_id=%s | response_code=%s | response_dttm=%s | adverts_groups=%s",
            run_uuid, business_dttm.isoformat(), api_token_id, response_code, response_dttm.isoformat(),
            len((payload or {}).get("adverts", []) if isinstance(payload, dict) else []),
        )

    except ClientResponseError as e:
        context.log.error(
            "[bronze_wb_adv_promotions_1h] HTTP error: status=%s, message=%s", getattr(e, "status", None), str(e)
        )
        raise
    except Exception as e:
        context.log.error("[bronze_wb_adv_promotions_1h] unexpected error: %s", str(e))
        raise


# ──────────────────────────────────────────────────────────────────────────────
# SILVER helpers
# ──────────────────────────────────────────────────────────────────────────────
def _flatten_promotion_count(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Преобразует ответ /adv/v1/promotion/count в список:
      {advertId, status, changeTime, type}
    """
    out: List[Dict[str, Any]] = []
    for group in (payload or {}).get("adverts", []) or []:
        g_status = group.get("status")
        g_type   = group.get("type")
        for adv in group.get("advert_list", []) or []:
            adv_id = adv.get("advertId")
            if adv_id is None:
                continue
            out.append({
                "advertId":   adv_id,
                "status":     g_status,
                "changeTime": adv.get("changeTime"),
                "type":       adv.get("type", g_type),
            })
    return out


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# SILVER
# ──────────────────────────────────────────────────────────────────────────────
@asset(
    name="silver_adv_promotions_1h",
    key_prefix=["silver"],
    description=(
        "Парсит bronze.wb_adv_promotions_1h (GET /adv/v1/promotion/count) "
        "в silver.adv_promotions_1h. PK: (advert_id, business_dttm)."
    ),
    deps=["wb_adv_promotions_1h"],
    resource_defs={"postgres": postgres_resource, "wildberries_client": wildberries_client},
    required_resource_keys={"postgres", "wildberries_client"},
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
async def silver_adv_promotions_1h(context) -> None:
    run_uuid = context.run_id
    token_id = context.resources.wildberries_client.token_id

    async with context.resources.postgres() as session:
        q_cur = (
            select(WbAdvPromotions1h)
            .where(
                WbAdvPromotions1h.run_uuid == run_uuid,
                WbAdvPromotions1h.api_token_id == token_id,
            )
        )
        bronze_rows = (await session.execute(q_cur)).scalars().all()

        if not bronze_rows:
            context.log.warning(
                "[silver_adv_promotions_1h] Нет bronze для текущего run; берём последний срез для token_id=%s",
                token_id,
            )
            q_fb = (
                select(WbAdvPromotions1h)
                .where(WbAdvPromotions1h.api_token_id == token_id)
                .order_by(WbAdvPromotions1h.business_dttm.desc())
                .limit(1)
            )
            bronze_rows = (await session.execute(q_fb)).scalars().all()

        if not bronze_rows:
            context.log.warning("[silver_adv_promotions_1h] Bronze данных не найдено — пропускаю загрузку.")
            return

        company_id = (await session.execute(
            select(text("company_id")).select_from(text("core.tokens")).where(text("token_id = :tid")),
            {"tid": token_id},
        )).scalar_one()

        total = 0
        for br in bronze_rows:
            business_dttm = br.business_dttm
            response_dttm = br.response_dttm

            try:
                payload = json.loads(br.response_body) if br.response_body else {}
            except Exception:
                context.log.warning("[silver_adv_promotions_1h] Некорректный JSON в response_body, пропускаю запись")
                continue

            records = _flatten_promotion_count(payload)

            for item in records:
                rec = {
                    "request_uuid":  br.request_uuid,
                    "company_id":    company_id,
                    "business_dttm": business_dttm,
                    "response_dttm": response_dttm,
                    "advert_id":     item.get("advertId"),
                    "advert_status": item.get("status"),
                    "advert_type":   item.get("type"),
                    "change_time":   _parse_dt(item.get("changeTime")),
                }

                upsert = pg_insert(SilverWbAdvPromotions1h).values(**rec)
                upsert = upsert.on_conflict_do_update(
                    index_elements=[
                        SilverWbAdvPromotions1h.advert_id,
                        SilverWbAdvPromotions1h.business_dttm,
                    ],
                    set_=rec,
                )
                await session.execute(upsert)
                total += 1

        await session.commit()
        context.log.info(
            "[silver_adv_promotions_1h] upsert %s записей в silver.adv_promotions_1h (token_id=%s, company_id=%s)",
            total, token_id, company_id,
        )
