import requests
import pandas as pd
from config_loader import load_config
from google_sheets_utils import add_data_to_sheet

CONFIG = load_config()

def fetch_supplies_report(date_from: str, date_to: str, seller_legal: str, spreadsheet_id: str):
    """
    Получает данные о поставках через API Wildberries и добавляет их в Google Sheets.

    :param date_from: Дата начала выборки в формате DD.MM.YYYY.
    :param date_to: Дата окончания выборки в формате DD.MM.YYYY.
    :param seller_legal: Юридическое лицо ("inter" или "ut").
    :param spreadsheet_id: Идентификатор Google Sheets.
    """
    from datetime import datetime

    # Преобразуем даты в формат YYYY-MM-DD
    try:
        formatted_date_from = datetime.strptime(date_from, "%d.%m.%Y").strftime("%Y-%m-%d")
        formatted_date_to = datetime.strptime(date_to, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError as e:
        raise Exception(f"Неверный формат даты. Ожидается DD.MM.YYYY. {str(e)}")

    # Настройка URL и параметров API
    base_url = "https://statistics-api.wildberries.ru/api/v1/supplier/incomes"
    params = {
        "dateFrom": formatted_date_from,
        "dateTo": formatted_date_to
    }

    # Получаем API-ключ для текущего юридического лица
    api_key = CONFIG["marketplace_keys"].get(seller_legal)
    if not api_key:
        raise Exception(f"API ключ для юридического лица '{seller_legal}' не найден в конфигурации.")

    headers = {
        "Authorization": api_key
    }

    # Выполняем запрос
    response = requests.get(base_url, params=params, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Ошибка API Wildberries: {response.status_code}, {response.text}")

    # Преобразуем данные JSON
    try:
        data = response.json()

        # Проверяем, есть ли данные
        if not data:
            print("Нет данных для выбранного периода.")
            return

        # Преобразуем список словарей в DataFrame
        df = pd.DataFrame(data)

        # Заполняем пустые значения
        df.fillna("", inplace=True)

    except Exception as e:
        raise Exception(f"Ошибка обработки данных: {e}")

    # Добавляем данные в Google Sheets
    try:
        add_data_to_sheet(spreadsheet_id, df, "Supplies(ext)")
        print("Данные добавлены на вкладку Supplies(ext).")
    except Exception as e:
        raise Exception(f"Ошибка при добавлении данных в Google Sheets: {e}")


# Тестовая функция
if __name__ == "__main__":
    def test_fetch_supplies():
        try:
            fetch_supplies_report("01.11.2024", "12.11.2024", "ut", "1Gjx1UFXW0nZPUiDIrzNedBiJad1B__YQmTjH0uTSnws")
            print("Данные успешно добавлены в таблицу.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    test_fetch_supplies()
