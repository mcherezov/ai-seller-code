from __future__ import annotations

from pathlib import Path
from datetime import datetime
import yaml

def _build_params(business_dttm: datetime, company_id: int) -> dict:
    """WB ожидает суточный диапазон."""
    iso = business_dttm.date().isoformat()
    return {"date_from": iso, "date_to": iso}

# фабрика
from dagster_conf.lib.asset_factories.task_asset_factory import make_pipeline_from_config

# ресурсы/модели БД (как и раньше)
from dagster_conf.resources.pg_resource import postgres_resource
from src.db.bronze.models import BronzeWbPaidStorage1DNew
from src.db.silver.models import SilverWbPaidStorage1DNew

# путь к YML
CFG_PATH = Path(__file__).with_name("config.yml")

# какой пайплайн брать из config.yml
PIPELINE_NAME = "wb_paid_storage_1d"

# грузим конфиг
with CFG_PATH.open("r", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

# TODO:  1. Создавать пайплайны в цикле сразу все в одном файле на базе конфига
#        2. Создавать модели БД из config.yaml
#        3. Добавить два других типа фабрик ассетов

# собираем ассеты из фабрики (бронза/сильвер), всё остальное тянется из YAML
bronze_wb_paid_storage_1d_new, silver_wb_paid_storage_1d_new = make_pipeline_from_config(
    PIPELINE_NAME,
    CFG,
    bronze_model=BronzeWbPaidStorage1DNew,
    silver_model=SilverWbPaidStorage1DNew,
)

__all__ = [
    "bronze_wb_paid_storage_1d_new",
    "silver_wb_paid_storage_1d_new",
]
