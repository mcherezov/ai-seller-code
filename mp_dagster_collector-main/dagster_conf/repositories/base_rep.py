from dagster import repository
from dagster_conf.repositories.wb_rep import wb_repository
from dagster_conf.repositories.ozon_rep import ozon_repository
from dagster_conf.repositories.wb_autogen_repository import wb_autogen_repository

@repository
def base_repository():
    """
    Базовый репозиторий, объединяющий все остальные (WB, OZON и т. д.) в единое пространство имен.
    Dagster будет видеть все job‐ы и schedules, которые возвращают под‐репозитории.
    """
    return [
        *wb_repository(),
        # *ozon_repository(),
        *wb_autogen_repository(),
    ]