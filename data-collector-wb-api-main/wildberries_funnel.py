import requests
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
from google_sheets_utils import add_data_to_sheet
from config_loader import load_config

CONFIG = load_config()

def flatten_data(data):
    return pd.json_normalize(data, sep="_")

def adjust_end_date(date_to):
    moscow_time = datetime.utcnow() + timedelta(hours=3)
    if moscow_time.date() == date_to.date() and moscow_time.time() < datetime.strptime("23:59:59", "%H:%M:%S").time():
        corrected_end = moscow_time - timedelta(minutes=5)
        return corrected_end.strftime('%Y-%m-%d %H:%M:%S')
    return date_to.strftime('%Y-%m-%d 23:59:59')

def fetch_sales_funnel(date_from: str,
                       date_to: str,
                       seller_legal: str,
                       spreadsheet_id: str,
                       write_to_sheet: bool = True) -> pd.DataFrame:

    def fetch_range(formatted_from, formatted_to) -> pd.DataFrame:
        payload = {
            "timezone": "Europe/Moscow",
            "period": {
                "begin": formatted_from,
                "end": formatted_to
            },
            "page": 1
        }

        all_data = []
        retries = 5
        backoff_base = 5

        while True:
            try:
                while True:
                    response = requests.post(base_url, json=payload, headers=headers)

                    if response.status_code == 429:
                        for attempt in range(retries):
                            wait_time = backoff_base * (2 ** attempt)
                            print(f"⚠️ 429 Too Many Requests. Повтор {attempt+1}/{retries} через {wait_time} сек...")
                            sleep(wait_time)
                            response = requests.post(base_url, json=payload, headers=headers)
                            if response.status_code != 429:
                                break
                        else:
                            raise Exception(f"Превышен лимит запросов API WB после {retries} попыток")

                    if response.status_code != 200:
                        raise Exception(f"Ошибка API Wildberries: {response.status_code}, {response.text}")

                    data = response.json()
                    if data.get("error", False):
                        raise Exception(f"Ошибка API: {data.get('errorText', 'Неизвестная ошибка')}")

                    page_data = data.get("data", {}).get("cards", [])
                    if not page_data:
                        break

                    all_data.extend(page_data)
                    if not data.get("data", {}).get("isNextPage", False):
                        break

                    payload["page"] += 1
                break  # основной цикл завершился успешно
            except Exception as e:
                raise Exception(f"⛔ Ошибка при получении данных из WB API: {e}")

        return flatten_data(all_data) if all_data else pd.DataFrame()

    try:
        date_from_dt = datetime.strptime(date_from, '%d.%m.%Y')
        date_to_dt = datetime.strptime(date_to, '%d.%m.%Y')

        formatted_date_from = f"{date_from_dt.strftime('%Y-%m-%d')} 00:00:00"
        formatted_date_to = adjust_end_date(date_to_dt)

        date_from_4 = (date_from_dt - timedelta(days=4)).strftime('%Y-%m-%d 00:00:00')
        date_to_4_dt = (date_to_dt - timedelta(days=4))
        formatted_date_to_4 = adjust_end_date(date_to_4_dt)
    except ValueError as e:
        raise Exception(f"Неверный формат даты. Ожидается DD.MM.YYYY. {str(e)}")

    base_url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail"
    api_key = CONFIG["marketplace_keys"].get(seller_legal)
    if not api_key:
        raise Exception(f"API ключ для юридического лица '{seller_legal}' не найден в конфигурации.")

    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }

    # --- Основная выгрузка ---
    df_main = fetch_range(formatted_date_from, formatted_date_to)
    if not df_main.empty and write_to_sheet:
        try:
            add_data_to_sheet(spreadsheet_id, df_main, "SalesFunnel(ext)")
            print("✅ Данные добавлены на вкладку SalesFunnel(ext).")
        except Exception as e:
            raise Exception(f"Ошибка при добавлении данных в Google Sheets: {e}")
    elif df_main.empty:
        print("⚠️ Нет данных для основной выгрузки.")

    # --- Дополнительная выгрузка -4 дня ---
    df_back = fetch_range(date_from_4, formatted_date_to_4)
    if not df_back.empty and write_to_sheet:
        try:
            add_data_to_sheet(spreadsheet_id, df_back, "SalesFunnel(ext) - 2")
            print(f"✅ Данные добавлены на вкладку SalesFunnel(ext) - 2 за {date_to_4_dt.strftime('%d.%m.%Y')}.")
        except Exception as e:
            raise Exception(f"Ошибка при добавлении дополнительных данных: {e}")
    elif df_back.empty:
        print(f"⚠️ Нет данных для дополнительной выгрузки за {date_to_4_dt.strftime('%d.%m.%Y')}.")

    return df_main


if __name__ == "__main__":
    def test_fetch_sales_funnel():
        try:
            fetch_sales_funnel(
                "01.11.2024",
                "29.11.2024",
                "inter",
                "1Gjx1UFXW0nZPUiDIrzNedBiJad1B__YQmTjH0uTSnws",
                True
            )
            print("Тест успешно завершен.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    test_fetch_sales_funnel()
