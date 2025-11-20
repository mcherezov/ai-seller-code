import requests
import pandas as pd
from datetime import datetime
from config_loader import load_config
from google_sheets_utils import add_data_to_sheet

CONFIG = load_config()


def fetch_running_campaigns(seller_legal: str):
    """
    Получает список ID кампаний с состоянием CAMPAIGN_STATE_RUNNING через API Ozon.

    :param seller_legal: Юридическое лицо ("inter" или "ut").
    :return: Список ID кампаний.
    """
    base_url = "https://api-performance.ozon.ru:443/api/client/campaign"
    api_key = CONFIG["marketplace_keys"].get(seller_legal)
    if not api_key:
        raise Exception(f"API ключ для юридического лица '{seller_legal}' не найден в конфигурации.")

    headers = {
        "Client-Id": CONFIG["ozon"]["client_id"],
        "Api-Key": api_key
    }

    response = requests.get(base_url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Ошибка API Ozon: {response.status_code}, {response.text}")

    data = response.json()
    campaigns = [item["id"] for item in data.get("result", []) if item.get("state") == "CAMPAIGN_STATE_RUNNING"]

    print(f"Найдено {len(campaigns)} кампаний с состоянием CAMPAIGN_STATE_RUNNING.")
    return campaigns


def fetch_ad_campaign_stats(date_from: str, date_to: str, seller_legal: str, spreadsheet_id: str):
    """
    Получает статистику по рекламным кампаниям и добавляет данные в Google Sheets.

    :param date_from: Дата начала выборки в формате DD.MM.YYYY.
    :param date_to: Дата окончания выборки в формате DD.MM.YYYY.
    :param seller_legal: Юридическое лицо ("inter" или "ut").
    :param spreadsheet_id: Идентификатор Google Sheets.
    """
    base_url = "https://api-performance.ozon.ru:443/api/client/statistics"
    api_key = CONFIG["marketplace_keys"].get(seller_legal)

    if not api_key:
        raise Exception(f"API ключ для юридического лица '{seller_legal}' не найден в конфигурации.")

    headers = {
        "Client-Id": CONFIG["ozon"]["client_id"],
        "Api-Key": api_key
    }

    # Преобразуем даты в формат YYYY-MM-DD
    try:
        formatted_date_from = datetime.strptime(date_from, "%d.%m.%Y").strftime("%Y-%m-%d")
        formatted_date_to = datetime.strptime(date_to, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError as e:
        raise Exception(f"Неверный формат даты. Ожидается DD.MM.YYYY. {str(e)}")

    campaigns = fetch_running_campaigns(seller_legal)
    if not campaigns:
        print("Нет доступных рекламных кампаний с состоянием CAMPAIGN_STATE_RUNNING.")
        return

    payload = {
        "campaigns": campaigns,
        "dateFrom": formatted_date_from,
        "dateTo": formatted_date_to
    }

    response = requests.post(base_url, json=payload, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Ошибка API Ozon: {response.status_code}, {response.text}")

    try:
        data = response.json()

        # Проверяем, есть ли данные
        if not data.get("result"):
            print("Нет данных для выбранного периода.")
            return

        # Преобразуем данные в DataFrame
        df = pd.DataFrame(data["result"])

        # Добавляем новый столбец "Name" с фиксированным значением "report"
        df.insert(0, "Name", "report")

        # Заполняем пустые значения
        df.fillna("", inplace=True)

        # Отладочный вывод для проверки данных
        print("Полученные данные для добавления в таблицу:")
        print(df.head())

    except Exception as e:
        raise Exception(f"Ошибка обработки данных: {e}")

    # Добавляем данные в Google Sheets
    try:
        add_data_to_sheet(spreadsheet_id, df, "AdCampaign(ext)")
        print("Данные добавлены на вкладку AdCampaign(ext).")
    except Exception as e:
        raise Exception(f"Ошибка при добавлении данных в Google Sheets: {e}")


# Тестовая функция
if __name__ == "__main__":
    def test_fetch_ad_campaign_stats():
        try:
            fetch_ad_campaign_stats("01.11.2024", "03.11.2024", "ut", "1Gjx1UFXW0nZPUiDIrzNedBiJad1B__YQmTjH0uTSnws")
            print("Данные успешно добавлены в таблицу.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    test_fetch_ad_campaign_stats()