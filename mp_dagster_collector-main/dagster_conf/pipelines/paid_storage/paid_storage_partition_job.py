from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import yaml

from dagster import AssetSelection, define_asset_job, schedule, RunRequest, MultiPartitionKey

# Ассеты пайплайна
from dagster_conf.pipelines.paid_storage.paid_storage_partition_assets import (
    bronze_wb_paid_storage_1d_new,
    silver_wb_paid_storage_1d_new,
)

# Конфиг‑драйв партиции
from dagster_conf.lib.partitions import build_multipartitions
from dagster_conf.lib.timeutils import MSK, yesterday_msk_date_iso

# Загружаем YAML и извлекаем секцию нужного пайплайна
CFG_PATH = Path(__file__).with_name("config.yml")
CFG = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))
PIPE_NAME = "wb_paid_storage_1d"
PIPE = CFG["pipelines"][PIPE_NAME]

# Мультипартиции
PARTS = build_multipartitions(PIPE)

# Комбинированный job: bronze → silver
paid_storage_1d_job_v2 = define_asset_job(
    name=f"{PIPE_NAME}_job_v2",
    selection=AssetSelection.assets(
        bronze_wb_paid_storage_1d_new,
        silver_wb_paid_storage_1d_new,
    ),
    partitions_def=PARTS,
    description=(
        "WB paid_storage (bronze→silver), мультипартиции (business_dttm × company_id). "
        "Токен берётся внутри bronze из core.tokens; dynamic company_id обновляется отдельным schedule."
    ),
)

# Ежедневное расписание пайплайна (берём cron и TZ из YAML)
@schedule(
    cron_schedule=PIPE["schedule"],
    job=paid_storage_1d_job_v2,
    execution_timezone=PIPE.get("timezone", "Europe/Moscow"),
    name=f"{PIPE_NAME}_daily",
)
def paid_storage_daily(context):
    """
    Каждый день создаём по run'у для каждой актуальной компании (DynamicPartitionsDefinition("company_id"))
    на «вчера» (MSK). Список компаний предварительно обновляется отдельным daily‑schedule.
    """
    date_iso = yesterday_msk_date_iso(getattr(context, "scheduled_execution_time", None))
    # Текущие значения dynamic‑партиции company_id в инстансе Dagster
    companies = context.instance.get_dynamic_partitions("company_id")
    for cid in companies:
        pk = MultiPartitionKey({"business_dttm": date_iso, "company_id": str(cid)})
        yield RunRequest(partition_key=pk, run_config={})


__all__ = [
    "paid_storage_1d_job_v2",
    "paid_storage_daily",
    "PIPE",
]
