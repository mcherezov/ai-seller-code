"""
backfill_product_stats_1h.py
-----------------------------------------------------------------
Разовый прогон: берёт строки из bronze.wb_adv_fullstats_1h
и раскладывает их в silver.wb_adv_product_stats_1h_backfill_test
по логике ассета 1h (bucket_end, окно [01:00; <hour]).

Примеры:
    python backfill_product_stats_1h.py --since 2025-08-06
    python backfill_product_stats_1h.py --since 2025-08-06 --until 2025-08-10 --log-level DEBUG
"""
import argparse
import asyncio
import json
import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db.base import AsyncSessionLocal
from src.db.bronze.models import WbAdvFullstats1h
from src.db.silver.models import SilverWbAdvProductStats1h as SilverModel

# ─── timezones ────────────────────────────────────────────────────────────────
TZ_MSK = ZoneInfo("Europe/Moscow")
TZ_UTC = ZoneInfo("UTC")

def msk(dt_utc: datetime) -> datetime:
    return dt_utc.astimezone(TZ_MSK)

def to_utc_start_of_day_msk(d: date) -> datetime:
    return datetime.combine(d, time.min, TZ_MSK).astimezone(TZ_UTC)

def to_utc_next_day_msk(d: date) -> datetime:
    return to_utc_start_of_day_msk(d + timedelta(days=1))

# ─── logging ─────────────────────────────────────────────────────────────────
def setup_logging(level: str) -> logging.Logger:
    os.makedirs("logs", exist_ok=True)
    ts = datetime.now(TZ_MSK).strftime("%Y%m%d_%H%M%S")
    logfile = f"logs/backfill_fullstats_1h_{ts}.log"

    logger = logging.getLogger("backfill_fullstats_1h")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = RotatingFileHandler(logfile, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    # избегаем дублирования хендлеров при повторном вызове
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)

    logger.info("Лог пишем в файл: %s", logfile)
    return logger

# ─── json helper (с причиной ошибки) ─────────────────────────────────────────
def safe_json_with_reason(body: str | None):
    if body is None:
        return None, "empty (None)"
    txt = body.strip()
    if not txt:
        return None, "empty string"
    low = txt.lower()
    if low in ("null", "none"):
        return None, f"placeholder '{low}'"
    try:
        return json.loads(txt), None
    except json.JSONDecodeError as e:
        return None, f"json decode error: {e}"

# ─── pretty принтер для метрик ───────────────────────────────────────────────
def fmt_metrics(d: dict) -> str:
    keys = ("views","clicks","cost","carts","orders","items","revenue")
    return ", ".join(f"{k}={d.get(k,0)}" for k in keys)

# ─── backfill ────────────────────────────────────────────────────────────────
async def backfill(since: date, until: date | None, loglevel: str):
    log = setup_logging(loglevel)

    since_utc = to_utc_start_of_day_msk(since)
    until_utc = to_utc_next_day_msk(until) if until else None
    log.info("СТАРТ backfill: since(MSK)=%s until(MSK)=%s",
             since.isoformat(), until.isoformat() if until else "—")

    async with AsyncSessionLocal() as session:
        # 1) читаем бронзу по run_dttm
        stmt = select(WbAdvFullstats1h).order_by(WbAdvFullstats1h.run_dttm.asc())
        stmt = stmt.where(WbAdvFullstats1h.run_dttm >= since_utc)
        if until_utc:
            stmt = stmt.where(WbAdvFullstats1h.run_dttm < until_utc)

        result = await session.execute(stmt)
        rows = result.scalars().all()
        log.info("Найдено %s бронзовых чанков (run_dttm ∈ [%s; %s))",
                 len(rows), since_utc.isoformat(), until_utc.isoformat() if until_utc else "∞")

        for i, bronze in enumerate(rows, start=1):
            # 2) определяем бакет
            run_msk = msk(bronze.request_dttm)
            bucket_end = run_msk.replace(minute=0, second=0, microsecond=0)
            bucket_day_start = (
                (bucket_end - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                if bucket_end.hour == 0 else
                bucket_end.replace(hour=0, minute=0, second=0, microsecond=0)
            )
            target_date = bucket_day_start.date()
            prev_start = bucket_day_start + timedelta(hours=1)

            log.info("Чанк %s/%s: req_uuid=%s token_id=%s request_dttm=%s run_dttm=%s "
                     "[bucket_end=%s target_date=%s prev_start=%s]",
                     i, len(rows), bronze.request_uuid, bronze.api_token_id,
                     run_msk.isoformat(), msk(bronze.run_dttm).isoformat(),
                     bucket_end.isoformat(), target_date, prev_start.time())

            # 3) company_id
            company_id = (await session.execute(
                text("SELECT company_id FROM core.tokens WHERE token_id = :tid"),
                {"tid": bronze.api_token_id},
            )).scalar_one_or_none()
            if company_id is None:
                log.warning("SKIP chunk: token_id=%s нет в core.tokens", bronze.api_token_id)
                continue

            # 4) парсим JSON с причиной
            adv_list, reason = safe_json_with_reason(bronze.response_body)
            if not adv_list:
                log.warning("SKIP chunk: request_uuid=%s — response_body not parsed: %s",
                            bronze.request_uuid, reason)
                continue

            # 5) по кампаниям
            wrote_rows = 0
            for raw in adv_list:
                advert_id = raw.get("advertId")
                if advert_id is None:
                    log.warning("SKIP record (нет advertId) request_uuid=%s", bronze.request_uuid)
                    continue
                log.info("  → Кампания advert_id=%s", advert_id)

                day_obj = next(
                    (d for d in raw.get("days", [])
                     if msk(datetime.fromisoformat(d["date"])).date() == target_date),
                    None,
                )
                if day_obj is None:
                    log.info("    skip: нет day для %s (WB ещё не отдал срез)", target_date)
                    continue

                for app in day_obj.get("apps", []):
                    app_type = app.get("appType")
                    for nm in app.get("nm", []):
                        nm_id = nm.get("nmId")
                        if nm_id is None:
                            continue

                        cumul = {
                            "views":   nm.get("views", 0),
                            "clicks":  nm.get("clicks", 0),
                            "cost":    nm.get("sum", 0.0),
                            "carts":   nm.get("atbs", 0),
                            "orders":  nm.get("orders", 0),
                            "items":   nm.get("shks", 0),
                            "revenue": nm.get("sum_price", 0.0),
                        }
                        log.debug("    cumul (aid=%s, app=%s, nm=%s): %s",
                                  advert_id, app_type, nm_id, fmt_metrics(cumul))

                        prev = (await session.execute(text(f"""
                            SELECT  COALESCE(SUM(views),0)   AS views,
                                    COALESCE(SUM(clicks),0)  AS clicks,
                                    COALESCE(SUM(cost),0)    AS cost,
                                    COALESCE(SUM(carts),0)   AS carts,
                                    COALESCE(SUM(orders),0)  AS orders,
                                    COALESCE(SUM(items),0)   AS items,
                                    COALESCE(SUM(revenue),0) AS revenue
                            FROM silver.{SilverModel.__tablename__}
                            WHERE company_id  = :company_id
                              AND advert_id    = :advert_id
                              AND app_type     = :app_type
                              AND nm_id        = :nm_id
                              AND request_dttm >= :prev_start
                              AND request_dttm <  :bucket_end
                        """), {
                            "company_id": company_id,
                            "advert_id": advert_id,
                            "app_type": app_type,
                            "nm_id": nm_id,
                            "prev_start": prev_start,
                            "bucket_end": bucket_end,
                        })).mappings().one()
                        log.debug("    prev   (aid=%s, app=%s, nm=%s): %s",
                                  advert_id, app_type, nm_id, fmt_metrics(prev))

                        delta = {k: max(0, cumul[k] - prev[k]) for k in cumul}
                        log.debug("    delta  (aid=%s, app=%s, nm=%s, hour=%s): %s",
                                  advert_id, app_type, nm_id, bucket_end, fmt_metrics(delta))

                        if any(prev[k] > cumul[k] for k in cumul):
                            log.warning(
                                "    prev>cumul (aid=%s app=%s nm=%s hour=%s) prev=%s cumul=%s",
                                advert_id, app_type, nm_id, bucket_end,
                                fmt_metrics(prev), fmt_metrics(cumul)
                            )

                        record = {
                            "company_id": company_id,
                            "request_uuid": bronze.request_uuid,
                            "advert_id": advert_id,
                            "request_dttm": bucket_end,
                            "app_type": app_type,
                            "nm_id": nm_id,
                            **delta,
                        }

                        stmt = pg_insert(SilverModel).values(**record).on_conflict_do_update(
                            index_elements=[
                                SilverModel.request_uuid,
                                SilverModel.advert_id,
                                SilverModel.app_type,
                                SilverModel.nm_id,
                                SilverModel.request_dttm,
                            ],
                            set_={
                                "request_uuid": record["request_uuid"],
                                "views":   record["views"],
                                "clicks":  record["clicks"],
                                "cost":    record["cost"],
                                "carts":   record["carts"],
                                "orders":  record["orders"],
                                "items":   record["items"],
                                "revenue": record["revenue"],
                            },
                        )

                        try:
                            await session.execute(stmt)
                            wrote_rows += 1
                            log.debug("    upsert OK (aid=%s app=%s nm=%s hour=%s)",
                                      advert_id, app_type, nm_id, bucket_end)
                        except Exception as e:
                            log.exception("    upsert FAIL (aid=%s app=%s nm=%s hour=%s): %s",
                                          advert_id, app_type, nm_id, bucket_end, e)
                            await session.rollback()    # откат только неуспешного стейтмента
                            # продолжаем со следующей записью
                            continue

            # коммитим весь чанк один раз
            try:
                await session.commit()
                log.info("Чанк %s/%s commit OK (hour=%s, inserted/updated=%s)",
                         i, len(rows), bucket_end, wrote_rows)
            except Exception as e:
                log.exception("Commit FAIL for chunk %s/%s (hour=%s): %s",
                              i, len(rows), bucket_end, e)
                await session.rollback()


# ─── cli ─────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill 1h stats начиная с даты (МСК).")
    p.add_argument("--since", required=True, help="YYYY-MM-DD (MSK)")
    p.add_argument("--until", required=False, help="YYYY-MM-DD (MSK)")
    p.add_argument("--log-level", default="INFO", help="INFO/DEBUG/WARNING/ERROR")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    since = datetime.fromisoformat(args.since).date()
    until = datetime.fromisoformat(args.until).date() if args.until else None
    asyncio.run(backfill(since=since, until=until, loglevel=args.log_level))
