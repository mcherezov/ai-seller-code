import requests
import pandas as pd
from google_sheets_utils import add_data_to_sheet_without_headers_with_format
from config_loader import load_config
from datetime import datetime

CONFIG = load_config()

def fetch_stocks(date_from: str, seller_legal: str, spreadsheet_id: str):
    """
    Получает данные об остатках через API Wildberries и добавляет их в Google Sheets.

    :param date_from: Дата начала выборки в формате DD.MM.YYYY.
    :param seller_legal: Юридическое лицо ("inter" или "ut").
    :param spreadsheet_id: Идентификатор Google Sheets.
    """
    # Преобразуем дату в формат YYYY-MM-DD
    try:
        formatted_date_from = datetime.strptime(date_from, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError as e:
        raise Exception(f"Неверный формат даты. Ожидается DD.MM.YYYY. {str(e)}")

    # Настройка URL и параметров API
    base_url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
    params = {
        "dateFrom": formatted_date_from
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

    # Проверяем статус ответа
    if response.status_code != 200:
        raise Exception(f"Ошибка API Wildberries: {response.status_code}, {response.text}")

    # Преобразуем ответ в DataFrame
    data = response.json()
    df = pd.DataFrame(data)

    # Проверяем, есть ли данные
    if df.empty:
        print("Нет данных для выбранного периода.")
        return

    # Обработка данных: добавляем юрлицо и текущую дату
    if seller_legal == "inter":
        df["legal_entity"] = "ИНТЕР"
    elif seller_legal == "ut":
        df["legal_entity"] = "АТ"
    elif seller_legal == "kravchik":
        df["legal_entity"] = "КРАВЧИК"
    else:
        df["legal_entity"] = seller_legal

    df["Дата Добавления"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df["period_id"] = datetime.strptime(date_from, "%d.%m.%Y").strftime("%d.%m.%Y")

    # Удаляем дубликаты по ключевым столбцам
    key_columns = ["Название склада", "Артикул продавца", "Артикул WB", "Баркод"]
    if all(col in df.columns for col in key_columns):
        df = df.drop_duplicates(subset=key_columns)

    # Добавляем данные в Google Sheets
    try:
        add_data_to_sheet_without_headers_with_format(spreadsheet_id, df, "StocksReport(ext)")
        print("Данные успешно добавлены на вкладку StocksReport(ext).")
    except Exception as e:
        raise Exception(f"Ошибка при добавлении данных в Google Sheets: {e}")

if __name__ == "__main__":
    # Тестовая функция
    def test_fetch_stocks():
        try:
            fetch_stocks("29.11.2024", "kravchik", "1Gjx1UFXW0nZPUiDIrzNedBiJad1B__YQmTjH0uTSnws")
            print("Тест успешно завершен.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    # Запуск теста
    test_fetch_stocks()