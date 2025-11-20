from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
from dagster import sensor, RunRequest, RunsFilter, DagsterRunStatus, SkipReason

from dagster_conf.pipelines.promotions_job import wb_adv_promotions_1h_job
from dagster_conf.pipelines.fullstats_jobs import wb_adv_fullstats_1h_job
from dagster_conf.pipelines.promotion_adverts_job import wb_adv_promotion_adverts_1h_job
from dagster_conf.pipelines.stats_keywords_jobs import wb_adv_stats_keywords_1h_job

MSK = ZoneInfo("Europe/Moscow")

TARGET_JOBS = {
    "wb_adv_fullstats_1h_job": wb_adv_fullstats_1h_job,
    "wb_adv_promotion_adverts_1h_job": wb_adv_promotion_adverts_1h_job,
    "wb_adv_stats_keywords_1h_job": wb_adv_stats_keywords_1h_job,
}


def _parse_iso_any_tz(dt_iso: str | None) -> datetime | None:
    """Парсит ISO (naive/aware, Z/offset) → datetime | None."""
    if not dt_iso:
        return None
    s = dt_iso.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _build_run_configs_from_source_run(src_run, scheduled_time_iso_msk: str):
    """
    Копируем ресурсы (token/token_id) от source-run и собираем
    run_config для целевых джоб (time_grain='1h' там, где это требуется схемой).
    """
    src_rc = src_run.run_config or {}
    resources_cfg = (src_rc.get("resources") or {}).copy()

    token_id = None
    try:
        token_id = resources_cfg["wildberries_client"]["config"]["token_id"]
    except Exception:
        pass

    run_cfgs = {
        # fullstats_1h: и бронза, и сильвер требуют time_grain
        "wb_adv_fullstats_1h_job": {
            "resources": resources_cfg,
            "ops": {
                "bronze__wb_adv_fullstats": {"config": {"time_grain": "1h"}},
                "silver__wb_adv_fullstats": {"config": {"time_grain": "1h"}},
            },
            "tags": {
                "dagster/scheduled_execution_time": scheduled_time_iso_msk,
                "source_run_id": src_run.run_id,
                **({"token_id": str(token_id)} if token_id is not None else {}),
            },
        },
        # promotion_adverts_1h: бронза + ОБА сильвера требуют time_grain
        "wb_adv_promotion_adverts_1h_job": {
            "resources": resources_cfg,
            "ops": {
                "bronze__wb_adv_promotion_adverts": {"config": {"time_grain": "1h"}},
                "silver__wb_adv_campaigns":         {"config": {"time_grain": "1h"}},
                "silver__wb_adv_product_rates":     {"config": {"time_grain": "1h"}},
            },
            "tags": {
                "dagster/scheduled_execution_time": scheduled_time_iso_msk,
                "source_run_id": src_run.run_id,
                **({"token_id": str(token_id)} if token_id is not None else {}),
            },
        },
        # stats_keywords_1h: если твои ассеты имеют config_schema — оставь как есть;
        # если нет — удали соответствующие строки "ops".
        "wb_adv_stats_keywords_1h_job": {
            "resources": resources_cfg,
            "ops": {
                "bronze__wb_adv_stats_keywords": {"config": {"time_grain": "1h"}},
                "silver__wb_adv_keyword_stats":   {"config": {"time_grain": "1h"}},
            },
            "tags": {
                "dagster/scheduled_execution_time": scheduled_time_iso_msk,
                "source_run_id": src_run.run_id,
                **({"token_id": str(token_id)} if token_id is not None else {}),
            },
        },
    }
    return run_cfgs


def _safe_run_dt(instance, run) -> datetime:
    """
    Возвращает datetime (aware, MSK) для запуска:
    1) из тега dagster/scheduled_execution_time;
    2) из RunStats (start_time / end_time / launch_time / enqueue_time);
    3) fallback: now(MSK).
    """
    # 1) тег планирования
    iso = (run.tags or {}).get("dagster/scheduled_execution_time")
    dt = _parse_iso_any_tz(iso)
    if dt:
        return dt.astimezone(MSK)

    # 2) статистика запуска
    try:
        stats = instance.get_run_stats(run_id=run.run_id)
        # значения — unix timestamp (секунды)
        for ts in (getattr(stats, "start_time", None),
                   getattr(stats, "end_time", None),
                   getattr(stats, "launch_time", None),
                   getattr(stats, "enqueue_time", None)):
            if ts:
                return datetime.fromtimestamp(ts, tz=MSK)
    except Exception:
        pass

    # 3) запасной путь
    return datetime.now(MSK)


@sensor(
    name="trigger_hourly_adv_jobs_after_promotions",
    minimum_interval_seconds=60,
    jobs=[wb_adv_fullstats_1h_job, wb_adv_promotion_adverts_1h_job, wb_adv_stats_keywords_1h_job],
)
def trigger_hourly_adv_jobs_after_promotions(context):
    """
    Запускает 3 часовые джобы (fullstats/promotion_adverts/stats_keywords)
    ПОСЛЕ успешных запусков wb_adv_promotions_1h_job за ТЕКУЩИЙ ЧАС по МСК.
    Идемпотентность: один комплект на (YYYYMMDDHH|token_id) через sensor cursor.
    """
    instance = context.instance

    # грузим курсор с ключами вида 'YYYYMMDDHH|<token_id>'
    try:
        processed = set(json.loads(context.cursor)) if context.cursor else set()
    except Exception:
        processed = set()

    # целевой верх часа (МСК)
    target_hour_msk = datetime.now(MSK).replace(minute=0, second=0, microsecond=0)
    hour_key_prefix = target_hour_msk.strftime("%Y%m%d%H")

    # берём SUCCESS источники за последние 2 часа (TZ-нейтрально)
    window_start = (target_hour_msk - timedelta(hours=2))
    src_runs = instance.get_runs(
        filters=RunsFilter(
            job_name="wb_adv_promotions_1h_job",
            statuses=[DagsterRunStatus.SUCCESS],
            created_after=window_start,
        ),
        limit=200,
    )
    if not src_runs:
        return SkipReason("Нет SUCCESS wb_adv_promotions_1h_job за последние 2 часа")

    # оставляем только те, чей scheduled_execution_time (в любой TZ) попадает ровно в target_hour_msk
    latest_runs = []
    for r in src_runs:
        dt = _safe_run_dt(instance, r)
        dt_hour_msk = dt.astimezone(MSK).replace(minute=0, second=0, microsecond=0)
        if dt_hour_msk == target_hour_msk:
            latest_runs.append(r)

    if not latest_runs:
        return SkipReason(f"Нет SUCCESS wb_adv_promotions_1h_job за час {target_hour_msk.isoformat()}")

    # дедуп по token_id (если один токен дал несколько SUCCESS за час)
    runs_by_token = {}
    for r in latest_runs:
        token_id = None
        try:
            token_id = (r.run_config or {}).get("resources", {})["wildberries_client"]["config"]["token_id"]
        except Exception:
            pass
        key = str(token_id) if token_id is not None else f"run:{r.run_id}"
        runs_by_token.setdefault(key, r)

    scheduled_time_iso_msk = target_hour_msk.isoformat()
    emitted = False

    for token_key, src_run in runs_by_token.items():
        # нормализуем token_id как строку (ключ и тег)
        token_id = None
        try:
            token_id = (src_run.run_config or {}).get("resources", {})["wildberries_client"]["config"]["token_id"]
        except Exception:
            pass
        token_id_str = str(token_id) if token_id is not None else token_key

        dedup_key = f"{hour_key_prefix}|{token_id_str}"
        if dedup_key in processed:
            context.log.info(f"[trigger] уже запускали цели для {dedup_key} — скипаем")
            continue

        run_cfgs = _build_run_configs_from_source_run(src_run, scheduled_time_iso_msk)

        for job_name in TARGET_JOBS.keys():
            run_key = f"after_promotions:{hour_key_prefix}:{token_id_str}:{job_name}"
            context.log.info(
                f"Scheduling {job_name} after wb_adv_promotions_1h_job "
                f"(scheduled(MSK)={scheduled_time_iso_msk}, token={token_id_str})"
            )
            yield RunRequest(
                run_key=run_key,
                job_name=job_name,
                run_config={
                    "resources": run_cfgs[job_name]["resources"],
                    "ops": run_cfgs[job_name]["ops"],
                },
                tags=run_cfgs[job_name]["tags"],
            )

        processed.add(dedup_key)
        emitted = True

    # обновляем курсор (держим 48 часов)
    if emitted:
        cutoff = datetime.now(MSK) - timedelta(hours=48)
        fresh = set()
        for k in processed:
            try:
                ts = datetime.strptime(k.split("|", 1)[0], "%Y%m%d%H").replace(tzinfo=MSK)
                if ts >= cutoff:
                    fresh.add(k)
            except Exception:
                pass
        context.update_cursor(json.dumps(sorted(fresh)))
    else:
        return SkipReason("Новых токенов для текущего часа нет")
