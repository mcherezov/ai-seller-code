import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
from datetime import date
import pandas as pd
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from db import get_top_keywords_by_campaign
import requests
from functools import wraps

logger = logging.getLogger(__name__)


# === Декоратор и сессия requests с куками из Selenium ===
def use_authenticated_requests(driver):
    logging.debug("🔐 Запрашиваем cookies для авторизации...")
    session = requests.Session()
    driver.get("https://app.mpmgr.ru")
    WebDriverWait(driver, 10).until(lambda d: "mpmgr" in d.current_url)

    cookies = driver.get_cookies()
    logging.debug(f"🍪 Получено {len(cookies)} cookies")
    for cookie in cookies:
        session.cookies.set(cookie['name'], cookie['value'], domain=cookie.get('domain'))
    logging.debug("✅ Cookies установлены в сессию requests")
    return session


def _setup_chrome_driver(seller_legal: str, chrome_profiles: dict, download_dir: str):
    try:
        profile_config = chrome_profiles[seller_legal]
    except KeyError:
        logger.error(f"Конфигурация для '{seller_legal}' не найдена в 'chrome_profiles'.")
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
        logger.error("Не удалось подключиться к Selenium Grid", exc_info=True)
        raise

    return driver


def fetch_competitor_data(session: requests.Session, keyword: str, campaign_id: int, org_id: str) -> pd.DataFrame:
    logging.debug(f"🔍 Получение данных по ключу '{keyword}', кампания {campaign_id}")
    encoded_keyword = quote(keyword)
    all_items = []

    url = (
        f"https://app.mpmgr.ru/api/wb/v3/organizations/{org_id}/"
        f"bids/by-keyword?type=All&region=Moscow&sex=Any&"
        f"keyword={encoded_keyword}&page=1"
    )

    response = session.get(url)
    response.raise_for_status()
    items = response.json()

    if not isinstance(items, list):
        raise ValueError("❌ Непредвиденный формат ответа — ожидался список")

    all_items = items

    logging.debug(f"📊 Всего получено {len(all_items)} позиций")
    found_item = next((item for item in all_items if int(item.get("campaignExternalId", -1)) == campaign_id), None)

    if found_item:
        max_advert_position = found_item.get("advertPosition")
        logging.debug(f"✅ Кампания найдена на позиции {max_advert_position}")
        relevant_items = [
            item for item in all_items if item.get("advertPosition") is not None and item["advertPosition"] <= max_advert_position
        ]
    else:
        logging.debug(f"⚠️ Кампания {campaign_id} не найдена, отбираем позиции до 100")
        relevant_items = [
            item for item in all_items if item.get("advertPosition") is not None and item["advertPosition"] <= 100
        ]

    return pd.DataFrame([{
        "productExternalId": item.get("productExternalId"),
        "type": item.get("type"),
        "bid": item.get("bid"),
        "position": item.get("position"),
        "advertPosition": item.get("advertPosition"),
        "hours": item.get("hours"),
        "subjectExternalId": item.get("subjectExternalId"),
        "discount": item.get("discount"),
        "price": item.get("price"),
        "promotionName": item.get("promotionName"),
        "campaign_id": item.get("campaignExternalId"),
        "keyword": keyword
    } for item in relevant_items])


def fetch_mpm_competitors(config: dict, today: date, hour: int) -> pd.DataFrame:
    logging.info("Старт загрузки конкурентов из MPManager...")
    chrome_profiles = config["mpm_selenium"]["chrome_profiles"]
    seller_legal = "inter"
    profile_config = chrome_profiles[seller_legal]
    org_id = profile_config["org_id"]
    download_dir = "/home/seluser/Downloads"

    driver = _setup_chrome_driver(seller_legal, chrome_profiles, download_dir)
    session = use_authenticated_requests(driver)

    all_results = []

    try:
        campaign_keywords = get_top_keywords_by_campaign(limit=50)
        logging.debug(f"🔑 Загружено {len(campaign_keywords)} кампаний")

        for campaign_id, keywords in campaign_keywords.items():
            logging.debug(f"📂 Обработка кампании {campaign_id}")
            if not keywords:
                continue
            for keyword in keywords:
                try:
                    df = fetch_competitor_data(session, keyword, campaign_id, org_id=org_id)
                    if not df.empty:
                        logging.debug(f"✅ Добавлено {len(df)} строк по ключу '{keyword}'")
                        all_results.append(df)
                    else:
                        logging.debug(f"⚠️ Пустой результат по ключу '{keyword}'")
                except Exception as e:
                    logger.warning(f"❌ Ошибка для ключа {keyword}: {e}")
    finally:
        driver.quit()
        logging.debug("🧹 Закрытие браузера завершено")

    if all_results:
        logging.debug("📊 Объединение всех результатов...")
        all_results = [df for df in all_results if not df.empty]
        final_df = pd.concat(all_results, ignore_index=True)
        final_df["date"] = today
        final_df["hour"] = hour - 1

        final_df.rename(columns={
            "productExternalId": "id_competitor",
            "type": "ad_campaign_type",
            "bid": "ad_campaign_rate",
            "position": "avg_position",
            "advertPosition": "ad_campaign_avg_position",
            "hours": "delivery_time",
            "subjectExternalId": "category_id",
            "discount": "discount_percent",
            "campaign_id": "ad_campaign_id",
            "keyword": "ad_keyword",
            "promotionName": "promotion_name"
        }, inplace=True)

        final_df["has_promotions"] = final_df["promotion_name"].apply(lambda x: bool(x and str(x).strip()))

        final_df = final_df.astype({
            "hour": int,
            "ad_campaign_id": "Int64",
            "ad_keyword": str,
            "id_competitor": str,
            "ad_campaign_type": str,
            "ad_campaign_rate": "float64",
            "avg_position": "float64",
            "ad_campaign_avg_position": "float64",
            "delivery_time": "float64",
            "category_id": "float64",
            "discount_percent": "float64",
            "price": "float64",
            "has_promotions": bool,
            "promotion_name": str
        })

        final_df = final_df.where(pd.notna(final_df), None)
        logging.debug(f"✅ Финальный датафрейм сформирован: {len(final_df)} строк")
        return final_df[[
            "date", "hour", "ad_campaign_id", "ad_keyword",
            "id_competitor", "ad_campaign_type", "ad_campaign_rate", "avg_position",
            "ad_campaign_avg_position", "delivery_time", "category_id",
            "discount_percent", "price", "has_promotions", "promotion_name"
        ]]

    logger.info("⚠️ Нет данных для возврата.")
    logging.debug("❌ Возвращён пустой DataFrame.")
    return pd.DataFrame()


if __name__ == '__main__':
    from config_loader import load_config
    config = load_config()
    fetch_mpm_competitors(config, date.today(), 15)
