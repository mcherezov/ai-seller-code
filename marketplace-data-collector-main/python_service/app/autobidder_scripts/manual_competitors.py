# import pandas as pd
# from datetime import date
# from urllib.parse import quote
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.support.ui import WebDriverWait
# import requests
# import sys
# import os
# import json
#
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#
# advert_ids = [22501210, 22500426]
#
# # === Загрузка конфига ===
# def load_config():
#     config_path = os.path.join(os.path.dirname(__file__), "../config.json")
#     with open(config_path, 'r') as file_stream:
#         return json.load(file_stream)
#
# # === Получение ключей по API WB ===
# def get_keyword_stats(advert_id, legal_entity, date_from, date_to):
#     print(f"📡 Получаем ключевые слова для РК {advert_id}")
#     url = "https://advert-api.wildberries.ru/adv/v0/stats/keywords"
#     headers = {
#         "Authorization": config["wb_marketplace_keys"][legal_entity]
#     }
#     params = {
#         "advert_id": advert_id,
#         "from": date_from,
#         "to": date_to
#     }
#     response = requests.get(url, headers=headers, params=params)
#     response.raise_for_status()
#     return response.json()
#
# # === Получение топ-10 ключей ===
# def fetch_keywords(advert_ids, legal_entity):
#     result = {}
#     for advert_id in advert_ids:
#         stats = get_keyword_stats(advert_id, legal_entity, date.today().isoformat(), date.today().isoformat()).get("keywords", [])
#         rows = []
#         for entry in stats:
#             for s in entry.get("stats", []):
#                 rows.append({"keyword": s.get("keyword"), "sum": s.get("sum", 0)})
#         df = pd.DataFrame(rows)
#         all_keywords = df.sort_values(by="sum", ascending=False)["keyword"].tolist()
#         print(f"🔑 Топ ключей для {advert_id}: {all_keywords}")
#         result[advert_id] = all_keywords
#     return result
#
# # === Запуск Chrome ===
# def _setup_chrome_driver(seller_legal: str, chrome_profiles: dict, download_dir: str):
#     try:
#         profile_config = chrome_profiles[seller_legal]
#     except KeyError:
#         print(f"Конфигурация для '{seller_legal}' не найдена в 'chrome_profiles'.")
#         raise
#
#     chrome_options = Options()
#     chrome_options.add_argument("user-data-dir=/google_chrome_users/")
#     chrome_options.add_argument(f"--profile-directory={profile_config['profile_directory']}")
#     chrome_options.add_argument("--no-first-run")
#     chrome_options.add_argument("--no-default-browser-check")
#     chrome_options.add_argument("--disable-session-crashed-bubble")
#     chrome_options.add_argument("--disable-infobars")
#     chrome_options.add_argument("--disable-popup-blocking")
#     chrome_options.add_argument("--disable-features=DownloadShelf")
#
#     prefs = {
#         "download.default_directory": download_dir,
#         "download.prompt_for_download": False,
#         "download.directory_upgrade": True,
#         "safebrowsing.enabled": True,
#         "profile.default_content_settings.popups": 0,
#     }
#     chrome_options.add_experimental_option("prefs", prefs)
#
#     selenium_grid_url = os.getenv("SELENIUM_GRID_URL", "http://selenium:4444/wd/hub")
#     try:
#         driver = webdriver.Remote(command_executor=selenium_grid_url, options=chrome_options)
#     except Exception as e:
#         print("Не удалось подключиться к Selenium Grid", exc_info=True)
#         raise
#
#     return driver
#
#
# # === Получение session с авторизованными куками ===
# def use_authenticated_requests(driver):
#     print("🔐 Получаем cookies из браузера...")
#     session = requests.Session()
#     driver.get("https://app.mpmgr.ru")
#     WebDriverWait(driver, 10).until(lambda d: "mpmgr" in d.current_url)
#     cookies = driver.get_cookies()
#     for cookie in cookies:
#         session.cookies.set(cookie["name"], cookie["value"], domain=cookie["domain"])
#     print(f"✅ Установлено cookies: {len(cookies)}")
#     return session
#
# def fetch_competitor_data(session, keyword, campaign_id, org_id):
#     from datetime import datetime
#
#     print(f"📅 {datetime.now().isoformat()} | 📋 Обработка ключа: '{keyword}'")
#     encoded_keyword = quote(keyword)
#     url = (
#         f"https://app.mpmgr.ru/api/wb/v3/organizations/{org_id}/"
#         f"bids/by-keyword?type=All&region=Moscow&sex=Any&keyword={encoded_keyword}&page=1"
#     )
#     resp = session.get(url)
#     resp.raise_for_status()
#     items = resp.json()
#
#     if not isinstance(items, list):
#         print(f"⚠️ Ожидался список, но получен тип: {type(items)}")
#         return []
#
#     # Оставляем только первые 30 строк
#     items = items[:30]
#     our_position = next((item.get("advertPosition") for item in items if int(item.get("campaignExternalId", -1)) == campaign_id), None)
#
#     if our_position:
#         filtered = [item for item in items if item.get("advertPosition") is not None and item["advertPosition"] <= our_position]
#     else:
#         filtered = items
#
#     df = pd.DataFrame([{
#         "ad_campaign_id": campaign_id,
#         "ad_keyword": keyword,
#         "id_competitor": item.get("productExternalId"),
#         "ad_campaign_type": item.get("type"),
#         "ad_campaign_rate": item.get("bid"),
#         "avg_position": item.get("position"),
#         "ad_campaign_avg_position": item.get("advertPosition"),
#         "delivery_time": item.get("hours"),
#         "category_id": item.get("subjectExternalId"),
#         "discount_percent": item.get("discount"),
#         "price": item.get("price"),
#         "promotion_name": item.get("promotionName"),
#         "has_promotions": bool(item.get("promotionName") and str(item.get("promotionName")).strip())
#     } for item in filtered])
#
#     return df
#
# # === Главный запуск ===
# if __name__ == "__main__":
#     print("📦 Загрузка конфигурации...")
#     config = load_config()
#     chrome_profiles = config["mpm_selenium"]["chrome_profiles"]
#     legal_entity = "inter"
#     org_id = chrome_profiles[legal_entity]["org_id"]
#     download_dir = "/tmp"
#
#     print("🧪 Сбор топ-ключей по API Wildberries...")
#     keyword_map = fetch_keywords(advert_ids, legal_entity)
#
#     print("🌐 Запуск браузера и сессии...")
#     driver = _setup_chrome_driver(legal_entity, chrome_profiles, download_dir)
#     session = use_authenticated_requests(driver)
#
#     results = []
#     for advert_id, keywords in keyword_map.items():
#         for keyword in keywords:
#             df = fetch_competitor_data(session, keyword, advert_id, org_id)
#             if not df.empty:
#                 results.append(df)
#
#     driver.quit()
#     print("🧹 Браузер закрыт.")
#
#     # Объединение всех результатов
#     df = pd.concat(results, ignore_index=True)
#     output_path = "/app/keyword_stats_report.xlsx"
#     df.to_excel(output_path, index=False)
#     print(f"✅ Отчёт сохранён: {output_path}")
#


import pandas as pd
from datetime import date, datetime
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
import requests
import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

advert_ids = [22501210, 22500426]

# === Загрузка конфига ===
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "../config.json")
    with open(config_path, 'r') as file_stream:
        return json.load(file_stream)

# === Получение ключей по API WB ===
def get_keyword_stats(advert_id, legal_entity, date_from, date_to):
    print(f"📡 Получаем ключевые слова для РК {advert_id}")
    url = "https://advert-api.wildberries.ru/adv/v0/stats/keywords"
    headers = {
        "Authorization": config["wb_marketplace_keys"][legal_entity]
    }
    params = {
        "advert_id": advert_id,
        "from": date_from,
        "to": date_to
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

# === Получение всех ключей ===
def fetch_keywords(advert_ids, legal_entity):
    result = {}
    for advert_id in advert_ids:
        stats = get_keyword_stats(advert_id, legal_entity, date.today().isoformat(), date.today().isoformat()).get("keywords", [])
        rows = []
        for entry in stats:
            for s in entry.get("stats", []):
                rows.append({"keyword": s.get("keyword"), "sum": s.get("sum", 0)})
        df = pd.DataFrame(rows)
        all_keywords = df.sort_values(by="sum", ascending=False)["keyword"].tolist()
        print(f"🔑 Ключи для {advert_id}: {all_keywords}")
        result[advert_id] = all_keywords
    return result

# === Запуск Chrome ===
def _setup_chrome_driver(seller_legal: str, chrome_profiles: dict, download_dir: str):
    try:
        profile_config = chrome_profiles[seller_legal]
    except KeyError:
        print(f"Конфигурация для '{seller_legal}' не найдена в 'chrome_profiles'.")
        raise

    chrome_options = Options()
    chrome_options.add_argument("user-data-dir=/google_chrome_users/")
    chrome_options.add_argument(f"--profile-directory={profile_config['profile_directory']}")
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--no-default-browser-check")
    chrome_options.add_argument("--disable-session-crashed-bubble")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-features=DownloadShelf")

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_settings.popups": 0,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    selenium_grid_url = os.getenv("SELENIUM_GRID_URL", "http://selenium:4444/wd/hub")
    try:
        driver = webdriver.Remote(command_executor=selenium_grid_url, options=chrome_options)
    except Exception as e:
        print("❌ Не удалось подключиться к Selenium Grid", exc_info=True)
        raise

    return driver

# === Получение session с авторизованными куками ===
def use_authenticated_requests(driver):
    print("🔐 Получаем cookies из браузера...")
    session = requests.Session()
    driver.get("https://app.mpmgr.ru")
    WebDriverWait(driver, 10).until(lambda d: "mpmgr" in d.current_url)
    cookies = driver.get_cookies()
    for cookie in cookies:
        session.cookies.set(cookie["name"], cookie["value"], domain=cookie["domain"])
    print(f"✅ Установлено cookies: {len(cookies)}")
    return session

# === Поиск только нашей кампании по ключу ===
def fetch_own_campaign_data(session, keyword, campaign_id, org_id):
    print(f"📅 {datetime.now().isoformat()} | 🔍 Поиск нашей кампании '{campaign_id}' по ключу '{keyword}'")
    encoded_keyword = quote(keyword)
    page = 1
    found_items = []

    while True:
        url = (
            f"https://app.mpmgr.ru/api/wb/v3/organizations/{org_id}/"
            f"bids/by-keyword?type=All&region=Moscow&sex=Any&keyword={encoded_keyword}&page={page}"
        )
        resp = session.get(url)
        resp.raise_for_status()
        items = resp.json()

        if not items:
            print(f"⚠️ На странице {page} по ключу '{keyword}' нет данных.")
            break

        for item in items:
            if int(item.get("campaignExternalId", -1)) == campaign_id:
                found_items.append({
                    "ad_campaign_id": campaign_id,
                    "ad_keyword": keyword,
                    "id_competitor": item.get("productExternalId"),
                    "ad_campaign_avg_position": item.get("advertPosition"),
                    "ad_campaign_rate": item.get("bid"),
                })
                print(f"✅ Найдена кампания на странице {page}: {item.get('advertPosition')} позиция")
                return pd.DataFrame(found_items)

        page += 1
        if page > 20:  # Ограничение для защиты от бесконечного цикла
            print(f"🛑 Превышено максимальное количество страниц на ключ '{keyword}'")
            break

    return pd.DataFrame(found_items)

# === Главный запуск ===
if __name__ == "__main__":
    print("📦 Загрузка конфигурации...")
    config = load_config()
    chrome_profiles = config["mpm_selenium"]["chrome_profiles"]
    legal_entity = "inter"
    org_id = chrome_profiles[legal_entity]["org_id"]
    download_dir = "/tmp"

    print("🧪 Сбор всех ключей по API Wildberries...")
    keyword_map = fetch_keywords(advert_ids, legal_entity)

    print("🌐 Запуск браузера и авторизация...")
    driver = _setup_chrome_driver(legal_entity, chrome_profiles, download_dir)
    session = use_authenticated_requests(driver)

    results = []
    for advert_id, keywords in keyword_map.items():
        for keyword in keywords:
            df = fetch_own_campaign_data(session, keyword, advert_id, org_id)
            if not df.empty:
                results.append(df)

    driver.quit()
    print("🧹 Браузер закрыт.")

    # Объединение всех результатов
    if results:
        df = pd.concat(results, ignore_index=True)
        output_path = "/app/keyword_stats_report_own.xlsx"
        df.to_excel(output_path, index=False)
        print(f"✅ Отчёт сохранён: {output_path}")
    else:
        print("⚠️ Нет данных для сохранения.")
