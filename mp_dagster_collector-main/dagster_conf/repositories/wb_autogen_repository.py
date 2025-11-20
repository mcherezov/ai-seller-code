from __future__ import annotations
from pathlib import Path
from dagster import repository

from dagster_conf.lib.auto_pipeline_builder import build_all_from_config
from dagster_conf.lib.auto_model_builder import build_models_from_config

from dagster_conf.resources.pg_resource import postgres_resource
from dagster_conf.resources.wildberries_client_v2 import wildberries_client_v2
from dagster_conf.resources.selenium_remote import selenium_remote

from dagster_conf.pipelines._gold__vm_product_orders_1d import refresh_vm_product_orders_1d_schedule


CFG_PATH = Path(__file__).parents[1] / "lib" / "config.yml"


@repository
def wb_autogen_repository():
    # 1) Генерим модели (bronze/silver) из config.yml и получаем реестр
    model_registry, _, _ = build_models_from_config(config_path=CFG_PATH)

    # 2) Регистрируем ресурсы.
    #    В РАМКАХ ОДНОГО RUN → ОДНА ПАРТИЦИЯ → ОДИН ТОКЕН.
    #    Токен и token_id подставляются фабрикой при выполнении ассета (через resolve_auth).
    #    Здесь они остаются пустыми плейсхолдерами.
    resources = {
        "postgres": postgres_resource,

        "wildberries_client_v2": wildberries_client_v2.configured({
            "token": "",
            "token_id": 0,
            # при необходимости можно прокинуть дополнительные параметры клиента:
            # "rps": 5, "timeout": 120, "retries": 3, "backoff": 0.5
        }),

        # Алиас для обратной совместимости: если где‑то остались ссылки на старый ключ
        "wildberries_client": wildberries_client_v2.configured({
            "token": "",
            "token_id": 0,
        }),
        "selenium_remote": selenium_remote.configured({
            "grid_url": "http://158.160.57.59:4444/wd/hub",
            "chrome_profiles": {
                "1": "Profile WB inter",
                "2": "Profile WB ut",
                "3": "Profile WB kravchik",
                "4": "Profile WB pomazanova",
                "5": "Profile WB avangard",
                "45": "Profile WB petflat"
            }
        })
    }

    # 3) Строим ассеты/джобы/расписания для всех pipelines из конфига
    assets, jobs, schedules = build_all_from_config(
        config_path=CFG_PATH,
        resources=resources,
        model_registry=model_registry,
        # при желании можно задать client_resource_key по умолчанию на уровне билдера
        # default_client_resource_key="wildberries_client_v2",
    )
    return [
        *assets, 
        *jobs, 
        *schedules,

        # @TODO: сделать фабрику для обновления всех мат.представлений в слое gold
        refresh_vm_product_orders_1d_schedule,
    ]
