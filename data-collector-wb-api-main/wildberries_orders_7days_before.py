import requests
import pandas as pd
from google_sheets_utils import add_data_to_sheet
from config_loader import load_config
from datetime import datetime, timedelta
from time import sleep

CONFIG = load_config()

def fetch_with_retry(url, params, headers, max_retries=5):
    """
    Выполняет GET-запрос с повтором в случае ошибки 429.
    """
    for attempt in range(max_retries):
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            return response
        elif response.status_code == 429:
            wait_time = 5 * (2 ** attempt)
            print(f"⚠️ 429 Too Many Requests. Повтор {attempt + 1}/{max_retries} через {wait_time} сек...")
            sleep(wait_time)
            continue
        else:
            raise Exception(f"Ошибка API Wildberries: {response.status_code}, {response.text}")
    raise Exception("Превышено количество повторов после 429 ошибки")

def fetch_orders_7_days(date_from: str, seller_legal: str, spreadsheet_id: str):
    """
    Получает данные о заказах за предыдущие 7 дней через API Wildberries и добавляет их в Google Sheets.
    """
    try:
        date_from_dt = datetime.strptime(date_from, "%d.%m.%Y")
    except ValueError as e:
        raise Exception(f"Неверный формат даты. Ожидается DD.MM.YYYY. {str(e)}")

    date_from_request_dt = date_from_dt - timedelta(days=7)
    formatted_date_from = date_from_request_dt.strftime("%Y-%m-%d")

    base_url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    params = {
        "dateFrom": formatted_date_from,
        "flag": 0
    }

    api_key = CONFIG["marketplace_keys"].get(seller_legal)
    if not api_key:
        raise Exception(f"API ключ для юридического лица '{seller_legal}' не найден в конфигурации.")

    headers = {"Authorization": api_key}

    response = fetch_with_retry(base_url, params, headers)
    data = response.json()
    df = pd.DataFrame(data)

    if df.empty:
        print("Нет данных для выбранного периода.")
        return

    df['date_datetime'] = pd.to_datetime(df['date'], format="%Y-%m-%dT%H:%M:%S")

    start_date = date_from_dt - timedelta(days=7)
    end_date = date_from_dt
    df = df[(df['date_datetime'] >= start_date) & (df['date_datetime'] < end_date)]
    df.drop(columns=['date_datetime'], inplace=True)

    if df.empty:
        print("Нет данных после фильтрации по дате.")
        return

    df = df[['date', 'warehouseName', 'supplierArticle', 'nmId', 'isCancel']]

    add_data_to_sheet(spreadsheet_id, df, "OrdersReport(ext) - 7 days")
    print("✅ Данные добавлены на вкладку 'OrdersReport(ext) - 7 days'.")

if __name__ == "__main__":
    def test_fetch_orders_7_days():
        try:
            date_from = "04.11.2024"
            seller_legal = "ut"
            spreadsheet_id = "1Gjx1UFXW0nZPUiDIrzNedBiJad1B__YQmTjH0uTSnws"
            fetch_orders_7_days(date_from, seller_legal, spreadsheet_id)
            print("Данные успешно добавлены в таблицу.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    test_fetch_orders_7_days()
