import requests
import functools
import pandas as pd
import time
from datetime import datetime, timedelta
from time import sleep
from config_loader import load_config
from google_sheets_utils import add_data_to_sheet

CONFIG = load_config()


def retry_on_429(max_retries=5, wait_seconds=60):
    """
    Декоратор для повторного выполнения функции при получении ответа 429 (Too Many Requests).
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                response = func(*args, **kwargs)
                if isinstance(response, requests.Response):
                    if response.status_code == 200:
                        return response
                    elif response.status_code == 429:
                        print(f"⚠️ Попытка {attempt}/{max_retries}: Превышен лимит WB. Ждём {wait_seconds} сек...")
                        time.sleep(wait_seconds)
                    else:
                        response.raise_for_status()
                else:
                    raise ValueError("Ожидался объект requests.Response")
            raise Exception("Не удалось получить успешный ответ после повторных попыток.")
        return wrapper
    return decorator



@retry_on_429(max_retries=5, wait_seconds=60)
def safe_request_get(url, **kwargs):
    return requests.get(url, **kwargs)


def fetch_paid_storage(date_from: str, date_to: str, seller_legal: str, spreadsheet_id: str):
    """
    Получает данные о платном хранении через API Wildberries и добавляет их в Google Sheets.

    :param date_from: Дата начала выборки в формате DD.MM.YYYY.
    :param date_to: Дата окончания выборки в формате DD.MM.YYYY.
    :param seller_legal: Юридическое лицо ("inter" или "ut").
    :param spreadsheet_id: Идентификатор Google Sheets.
    """
    try:
        start_date = datetime.strptime(date_from, "%d.%m.%Y")
        end_date = datetime.strptime(date_to, "%d.%m.%Y")
    except ValueError as e:
        raise Exception(f"Неверный формат даты. Ожидается DD.MM.YYYY. {str(e)}")

    # Получаем API-ключ для текущего юридического лица
    api_key = CONFIG["marketplace_keys"].get(seller_legal)
    if not api_key:
        raise Exception(f"API ключ для юридического лица '{seller_legal}' не найден в конфигурации.")

    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }

    base_url_create = "https://seller-analytics-api.wildberries.ru/api/v1/paid_storage"
    base_url_status = "https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/status"
    base_url_download = "https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/download"

    all_data = []
    current_start_date = start_date

    while current_start_date <= end_date:
        # Определяем конец текущего интервала
        current_end_date = min(current_start_date + timedelta(days=6), end_date)
        payload = {
            "dateFrom": current_start_date.strftime("%Y-%m-%d"),
            "dateTo": current_end_date.strftime("%Y-%m-%d")
        }

        # Создание отчета
        response_create = safe_request_get(base_url_create, params=payload, headers=headers)
        if response_create.status_code != 200:
            raise Exception(f"Ошибка при создании отчета: {response_create.status_code}, {response_create.text}")

        task_id = response_create.json().get("data", {}).get("taskId")
        if not task_id:
            raise Exception("Не удалось получить taskID из ответа API.")

        print(f"Отчет создан. Task ID: {task_id}. Ожидание статуса...")

        # Проверка статуса отчета
        status_url = base_url_status.format(task_id=task_id)
        while True:
            sleep(5)  # Ждем 5 секунд между проверками статуса
            response_status = safe_request_get(status_url, headers=headers)
            if response_status.status_code != 200:
                raise Exception(f"Ошибка при проверке статуса отчета: {response_status.status_code}, {response_status.text}")

            status = response_status.json().get("data", {}).get("status")
            if status == "done":
                print(f"Отчет {task_id} готов. Начинаем скачивание...")
                break
            elif status == "error":
                raise Exception(f"Ошибка в отчете {task_id}: {response_status.json()}")

        # Скачивание отчета
        download_url = base_url_download.format(task_id=task_id)
        response_download = safe_request_get(download_url, headers=headers)
        if response_download.status_code != 200:
            raise Exception(f"Ошибка при скачивании отчета: {response_download.status_code}, {response_download.text}")

        report_data = response_download.json()
        all_data.extend(report_data)

        print(f"Данные за период {payload['dateFrom']} - {payload['dateTo']} успешно получены.")

        # Переход к следующему интервалу
        current_start_date = current_end_date + timedelta(days=1)

        # Ждем минуту перед следующим запросом, если это не последний интервал
        if current_start_date <= end_date:
            sleep(60)  # Ждем минуту (ограничение API)


    # Преобразуем данные в DataFrame
    if not all_data:
        print("Нет данных для загрузки в таблицу.")
        return

    df = pd.DataFrame(all_data)
    df.fillna("", inplace=True)  # Заполняем пустые значения

    # Добавляем данные в Google Sheets
    try:
        add_data_to_sheet(spreadsheet_id, df, "PaidStorage(ext)")
        print("Данные добавлены на вкладку PaidStorage(ext).")
    except Exception as e:
        raise Exception(f"Ошибка при добавлении данных в Google Sheets: {e}")

# Тестовая функция
if __name__ == "__main__":
    def test_fetch_paid_storage():
        try:
            fetch_paid_storage("01.11.2024", "05.11.2024", "inter", "1Gjx1UFXW0nZPUiDIrzNedBiJad1B__YQmTjH0uTSnws")
            print("Данные успешно добавлены в таблицу.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    test_fetch_paid_storage()
