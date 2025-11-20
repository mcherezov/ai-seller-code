import json
import os

def load_config(config_path="config.json"):
    """Загружает настройки из JSON файла."""
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    with open(config_path, "r") as file:
        return json.load(file)
