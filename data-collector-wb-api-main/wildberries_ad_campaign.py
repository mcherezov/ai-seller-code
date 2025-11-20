import requests
import pandas as pd
from datetime import datetime
import time
from config_loader import load_config
from google_sheets_utils import add_data_to_sheet

CONFIG = load_config()


def flatten_and_filter_data(data):
    """
    Преобразует данные fullstats в плоский формат и оставляет только нужные столбцы.

    :param data: Входные данные fullstats.
    :return: Список словарей с отфильтрованными полями.
    """
    result = []
    for item in data:
        advert_id = item.get("advertId")
        for day in item.get("days", []):
            day_date = day.get("date")
            for app in day.get("apps", []):
                app_type = app.get("appType")
                avg_sum = app.get("sum", 0) / max(len(app.get("nm", [])), 1)  # Среднее значение суммы
                for nm in app.get("nm", []):
                    result.append({
                        "nm_id": nm.get("nmId"),
                        "advert_id": advert_id,
                        "sum": avg_sum if app_type == 0 else nm.get("sum"),
                        "app_type": app_type,
                        "date": day_date,
                        "views": nm.get("views", 0),
                        "clicks": nm.get("clicks", 0),
                        "atbs": nm.get("atbs", 0),
                        "orders": nm.get("orders", 0),
                        "ctr": nm.get("ctr", 0),
                    })
    return result


def chunk_list(lst, chunk_size):
    """
    Разделяет список на подсписки заданного размера.

    :param lst: Исходный список.
    :param chunk_size: Размер подсписка.
    :return: Генератор подсписков.
    """
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def fetch_ad_campaigns(date_from: str, date_to: str, seller_legal: str, spreadsheet_id: str):
    """
    Получает данные о рекламных кампаниях через API Wildberries и добавляет их в Google Sheets.

    :param date_from: Дата начала выборки в формате DD.MM.YYYY.
    :param date_to: Дата окончания выборки в формате DD.MM.YYYY.
    :param seller_legal: Юридическое лицо ("inter" или "ut").
    :param spreadsheet_id: Идентификатор Google Sheets.
    """
    # Преобразуем даты в формат YYYY-MM-DD
    try:
        formatted_date_from = datetime.strptime(date_from, "%d.%m.%Y").strftime("%Y-%m-%d")
        formatted_date_to = datetime.strptime(date_to, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError as e:
        raise Exception(f"Неверный формат даты. Ожидается DD.MM.YYYY. {str(e)}")

    # Настройка API
    url_promotion_count = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
    url_fullstats = "https://advert-api.wildberries.ru/adv/v2/fullstats"

    # Получаем API-ключ для текущего юридического лица
    api_key = CONFIG["marketplace_keys"].get(seller_legal)
    if not api_key:
        raise Exception(f"API ключ для юридического лица '{seller_legal}' не найден в конфигурации.")

    headers = {
        "Authorization": api_key
    }

    # Получаем список ID кампаний
    response_promotion_count = requests.get(
        url_promotion_count,
        params={"dateFrom": formatted_date_from, "dateTo": formatted_date_to},
        headers=headers
    )

    if response_promotion_count.status_code != 200:
        raise Exception(f"Ошибка API Wildberries (promotion/count): {response_promotion_count.status_code}, {response_promotion_count.text}")

    try:
        # Получаем JSON-ответ
        campaigns_data = response_promotion_count.json()

        # Проверяем, что это словарь
        if not isinstance(campaigns_data, dict):
            raise Exception(
                f"Некорректный формат ответа от API (promotion/count): ожидается словарь, получено {type(campaigns_data)}")

        # Извлекаем список кампаний из adverts
        adverts = campaigns_data.get("adverts", [])
        if not isinstance(adverts, list):
            raise Exception("Некорректный формат данных в поле 'adverts': ожидается список.")

        # Собираем ID кампаний из каждого элемента adverts
        advert_ids = [
            advert["advertId"]
            for advert_group in adverts
            for advert in advert_group.get("advert_list", [])
            if "advertId" in advert
        ]

        if not advert_ids:
            print("Нет доступных кампаний за указанный период.")
            return
    except Exception as e:
        raise Exception(f"Ошибка обработки данных из API (promotion/count): {e}")

    # Подготовка к обработке ID кампаний по частям
    all_filtered_data = []
    chunks = list(chunk_list(advert_ids, 100))

    for index, chunk in enumerate(chunks):
        print(f"Обработка группы {index + 1} из {len(chunks)} (ID кампаний: {len(chunk)}).")
        fullstats_payload = [
            {"id": advert_id, "interval": {"begin": formatted_date_from, "end": formatted_date_to}}
            for advert_id in chunk
        ]

        # Выполняем запрос
        response_fullstats = requests.post(url_fullstats, json=fullstats_payload, headers=headers)

        if response_fullstats.status_code != 200:
            print(f"Ошибка API Wildberries (fullstats): {response_fullstats.status_code}, {response_fullstats.text}")
            continue

        try:
            fullstats = response_fullstats.json()
            if not isinstance(fullstats, list):
                raise Exception("Ответ API (fullstats) не является списком.")

            # Распаковываем данные и фильтруем
            filtered_data = flatten_and_filter_data(fullstats)
            all_filtered_data.extend(filtered_data)
        except Exception as e:
            print(f"Ошибка обработки данных из API (fullstats): {e}")
            continue

        # Ожидание между запросами
        if index < len(chunks) - 1:
            print("Ждем 60 секунд перед следующим запросом...")
            time.sleep(60)

    # Преобразуем данные в DataFrame
    if not all_filtered_data:
        print("Нет данных для загрузки в таблицу.")
        return

    df = pd.DataFrame(all_filtered_data)
    df.fillna(value="", inplace=True)  # Заменяем NaN на пустые строки

    # Добавляем данные в Google Sheets
    try:
        add_data_to_sheet(spreadsheet_id, df, "AdCampaign(ext)")
        print("Данные успешно добавлены в Google Sheets.")
    except Exception as e:
        raise Exception(f"Ошибка при добавлении данных в Google Sheets: {e}")


# Тестовая функция
if __name__ == "__main__":
    def test_fetch_ad_campaigns():
        try:
            fetch_ad_campaigns("01.11.2024", "02.11.2024", "inter", "1Gjx1UFXW0nZPUiDIrzNedBiJad1B__YQmTjH0uTSnws")
            print("Тест успешно завершен.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")


    test_fetch_ad_campaigns()