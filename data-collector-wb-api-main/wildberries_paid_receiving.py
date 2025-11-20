import requests
import pandas as pd
from config_loader import load_config
from google_sheets_utils import add_data_to_sheet
import time
from datetime import datetime, timedelta

CONFIG = load_config()

def fetch_acceptance_report(date_from: str, date_to: str, seller_legal: str, spreadsheet_id: str):
    """
    Получает данные о платной приёмке через новый асинхронный API Wildberries и
    добавляет их в Google Sheets.
    """
    # 1) Преобразуем даты DD.MM.YYYY -> YYYY-MM-DD
    try:
        dfmt = "%d.%m.%Y"
        date_from_iso = datetime.strptime(date_from, dfmt).strftime("%Y-%m-%d")
        date_to_iso   = datetime.strptime(date_to,   dfmt).strftime("%Y-%m-%d")
    except ValueError as e:
        raise Exception(f"Неверный формат даты. Ожидается DD.MM.YYYY. {e}")

    # 2) Собираем общие параметры
    api_key = CONFIG["marketplace_keys"].get(seller_legal)
    if not api_key:
        raise Exception(f"API ключ для '{seller_legal}' не найден.")
    headers = {"Authorization": api_key}
    base = "https://seller-analytics-api.wildberries.ru/api/v1"
    params = {"dateFrom": date_from_iso, "dateTo": date_to_iso}

    # 3) Создаём задачу на сбор отчёта
    resp = requests.get(f"{base}/acceptance_report", params=params, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"Ошибка создания задачи: {resp.status_code} {resp.text}")
    task_id = resp.json().get("data", {}).get("taskId")
    if not task_id:
        print("⚠️ От Wildberries не получили taskId —, вероятно, за период нет данных.")
        return

    # 4) Опрашиваем статус (макс. 60 попыток с паузой 5 сек)
    status = None
    for i in range(60):
        r2 = requests.get(f"{base}/acceptance_report/tasks/{task_id}/status", headers=headers)
        if r2.status_code != 200:
            raise Exception(f"Ошибка статуса: {r2.status_code} {r2.text}")
        status = r2.json().get("data", {}).get("status")
        if status == "done":
            break
        if status == "error":
            raise Exception("Wildberries вернуло статус error при генерации отчёта.")
        time.sleep(5)
    if status != "done":
        raise Exception("Не удалось дождаться завершения отчёта в отведённое время.")

    # 5) Скачиваем готовый отчёт
    r3 = requests.get(f"{base}/acceptance_report/tasks/{task_id}/download", headers=headers)
    if r3.status_code != 200:
        raise Exception(f"Ошибка при скачивании: {r3.status_code} {r3.text}")
    data = r3.json()  # ожидаем список словарей

    if not isinstance(data, list) or not data:
        print("Нет записей платной приёмки за указанный период.")
        return

    # 6) Конвертируем в DataFrame и заливаем в Google Sheets
    df = pd.json_normalize(data)
    df.fillna("", inplace=True)
    # можно добавить колонку с тем, за какой период или seller_legal
    df.insert(0, "seller", seller_legal)
    add_data_to_sheet(spreadsheet_id, df, "PaidReceiving(ext)")
    print(f"Данные добавлены на вкладку PaidReceiving(ext).")
