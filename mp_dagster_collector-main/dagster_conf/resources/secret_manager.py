import os
import json
from dagster import resource


@resource
def secret_manager(_context) -> dict:
    """
    Простой ресурс для выдачи секретов / конфигураций.
    Берёт путь до config.json из переменной окружения CONFIG_PATH,
    или пытается найти файл "config.json" в корне проекта.
    Возвращает распарсенный JSON как dict.
    """
    config_path = os.getenv("CONFIG_PATH", "config.json")

    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"secret_manager: файл конфигурации {config_path} не найден")

    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data
