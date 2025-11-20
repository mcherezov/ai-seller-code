import requests
import pandas as pd
from datetime import datetime, timedelta
from google_sheets_utils import add_data_to_sheet
from config_loader import load_config
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


def fetch_orders(
    date_from: str,
    date_to: str,
    seller_legal: str,
    spreadsheet_id: str,
    write_to_sheet: bool = True
) -> pd.DataFrame:
    """
    Получает данные о заказах через API Wildberries и добавляет их в Google Sheets.
    """
    try:
        formatted_date_from = datetime.strptime(date_from, "%d.%m.%Y").strftime("%Y-%m-%d")
        formatted_date_to   = datetime.strptime(date_to, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError as e:
        raise Exception(f"Неверный формат даты. Ожидается DD.MM.YYYY. {str(e)}")

    base_url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    params = {
        "dateFrom": formatted_date_from,
        "dateTo": formatted_date_to,
        "flag": 0
    }

    api_key = CONFIG["marketplace_keys"].get(seller_legal)
    if not api_key:
        raise Exception(f"API ключ для юридического лица '{seller_legal}' не найден в конфигурации.")

    headers = {
        "Authorization": api_key
    }

    response = fetch_with_retry(base_url, params, headers)
    data = response.json()
    df = pd.DataFrame(data)

    if df.empty:
        print("Нет данных для выбранного периода.")
        return df

    date_from_dt = datetime.strptime(date_from, "%d.%m.%Y")
    date_to_dt   = datetime.strptime(date_to, "%d.%m.%Y") + timedelta(days=1) - timedelta(seconds=1)

    df['date_datetime'] = pd.to_datetime(df['date'], format="%Y-%m-%dT%H:%M:%S")
    df = df[(df['date_datetime'] >= date_from_dt) & (df['date_datetime'] <= date_to_dt)]
    df.drop(columns=['date_datetime'], inplace=True)

    if df.empty:
        print("Нет данных после фильтрации по дате.")
        return df

    if write_to_sheet:
        add_data_to_sheet(spreadsheet_id, df, "OrdersReport(ext)")
        print(f"✅ Данные добавлены на вкладку OrdersReport(ext).")

    return df


if __name__ == "__main__":
    def test_fetch_orders():
        try:
            fetch_orders(
                "10.11.2024",
                "14.11.2024",
                "kravchik",
                "1Gjx1UFXW0nZPUiDIrzNedBiJad1B__YQmTjH0uTSnws",
                write_to_sheet=True
            )
            print("Данные успешно добавлены в таблицу.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    test_fetch_orders()
