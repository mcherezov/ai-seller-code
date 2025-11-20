import requests
import pandas as pd
from config_loader import load_config
from google_sheets_utils import add_data_to_sheet

CONFIG = load_config()


def fetch_commission_data(seller_legal: str, spreadsheet_id: str):
    """
    Получает данные комиссии через API Wildberries и добавляет их в Google Sheets.

    :param seller_legal: Юридическое лицо ("inter" или "ut").
    :param spreadsheet_id: Идентификатор Google Sheets.
    """
    # Настройка URL API
    base_url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"

    # Получаем API-ключ для текущего юридического лица
    api_key = CONFIG["marketplace_keys"].get(seller_legal)
    if not api_key:
        raise Exception(f"API ключ для юридического лица '{seller_legal}' не найден в конфигурации.")

    headers = {
        "Authorization": api_key
    }

    # Выполняем запрос
    response = requests.get(base_url, headers=headers)

    # Проверяем статус ответа
    if response.status_code != 200:
        raise Exception(f"Ошибка API Wildberries: {response.status_code}, {response.text}")

    # Обработка данных
    try:
        data = response.json()

        # Проверяем, есть ли ключ "report" в данных
        report_data = data.get("report", [])
        if not report_data:
            print("Нет данных для загрузки.")
            return

        # Разворачиваем вложенные данные
        flattened_data = []
        for item in report_data:
            flat_item = {key: value for key, value in item.items()}  # Разворачиваем вложенные словари
            flattened_data.append(flat_item)

        # Преобразуем развернутые данные в DataFrame
        df = pd.DataFrame(flattened_data)

        # Заполняем пустые значения
        df.fillna("", inplace=True)

        # Добавляем новый столбец "Name" с фиксированным значением "report"
        df.insert(0, "Name", "report")

    except Exception as e:
        raise Exception(f"Ошибка обработки данных: {e}")

    # Добавляем данные в Google Sheets
    try:
        add_data_to_sheet(spreadsheet_id, df, "Commission(ext)")
        print("Данные добавлены на вкладку Commission(ext).")
    except Exception as e:
        raise Exception(f"Ошибка при добавлении данных в Google Sheets: {e}")


# Тестовая функция
if __name__ == "__main__":
    def test_fetch_commission_data():
        try:
            fetch_commission_data("ut", "1Gjx1UFXW0nZPUiDIrzNedBiJad1B__YQmTjH0uTSnws")
            print("Данные успешно добавлены в таблицу.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")


    test_fetch_commission_data()


