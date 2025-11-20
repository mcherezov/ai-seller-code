import os
import time
import glob
import logging
import pandas as pd
import numpy as np

from time import sleep
from datetime import datetime
from typing import Optional, Dict

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options

from google_sheets_utils import add_data_to_sheet_without_headers_with_format


import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

logger = logging.getLogger(__name__)


def convert_date(date_str: str) -> str:
    """
    Преобразует дату из формата dd.mm.yyyy в YYYY-MM-DD.
    """
    try:
        return datetime.strptime(date_str, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Неверный формат даты: {date_str}. Ожидается dd.mm.yyyy.") from e


def _setup_chrome_driver(seller_legal: str, chrome_profiles: dict, download_dir: str):
    """
    Настраивает и запускает ChromeDriver через Selenium Grid с параметрами профиля и загрузки.

    Args:
        seller_legal (str): Идентификатор юридического лица, используемый для выбора профиля Chrome.
        chrome_profiles (dict): Конфигурация профилей Chrome.
        download_dir (str): Директория, куда будут загружаться файлы.

    Returns:
        webdriver.Remote: Настроенный экземпляр Chrome WebDriver.
    """
    try:
        profile_config = chrome_profiles[seller_legal]
    except KeyError:
        logger.error(f"Конфигурация для '{seller_legal}' не найдена в 'chrome_profiles'.")
        raise

    chrome_options = Options()
    # chrome_options.add_argument(f"user-data-dir=/home/seluser/.config/google-chrome/")
    # if seller_legal == "inter":
    #     chrome_options.add_argument("--profile-directory=Profile Ozon inter")
    # elif seller_legal == "ut":
    #     chrome_options.add_argument("--profile-directory=Profile Ozon ut")
    # else:
    #     raise ValueError(f"Неизвестное юридическое лицо: {seller_legal}")
    chrome_options.add_argument("user-data-dir=/google_chrome_users/")
    chrome_options.add_argument(f"--profile-directory={profile_config['profile_directory']}")

    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--no-default-browser-check")
    chrome_options.add_argument("--disable-session-crashed-bubble")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-features=DownloadShelf")

    prefs = {
        "download.default_directory": "/home/seluser/Downloads",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_settings.popups": 0,
    }
    chrome_options.add_experimental_option("prefs", prefs)


def download_file(
    driver: webdriver.Remote,
    base_url: str,
    date_from: str,
    date_to: str,
    download_dir: str,
    timeout: int = 30
) -> str:
    """
    Скачивает .xlsx через прямой GET с CSRF-заголовками.
    """
    full_url = f"{base_url}?from={date_from}&to={date_to}"
    logger.info(f"Переходим по URL: {full_url}")

    # зайти на главную для «прогрузки» профиля
    driver.get("https://seller.ozon.ru/")
    sleep(2)

    # подтянуть CSRF
    csrf = driver.get_cookie("X-CSRF-TOKEN") or {}
    headers = {
        "x-csrf-token": csrf.get("value", ""),
        "X-Requested-With": "Fetch",
        "Referer": "https://seller.ozon.ru/",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9"
    }
    driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": headers})

    driver.get(full_url)
    deadline = time.time() + timeout
    while time.time() < deadline:
        files = glob.glob(os.path.join(download_dir, "*.xlsx"))
        if files:
            return max(files, key=os.path.getctime)
        sleep(1)
    raise TimeoutException("Файл не был загружен за отведённое время.")


def process_excel_file(
    file_path: str,
    spreadsheet_id: str,
    seller_legal: str
):
    """
    Читает Excel, очищает NaN/Inf и отправляет в Google Sheets.
    """
    df = pd.read_excel(file_path)
    df = df.fillna(0).replace([np.inf, -np.inf], 0)
    add_data_to_sheet_without_headers_with_format(spreadsheet_id, df, "PaidStorage(ext)")
    logger.info(f"Данные для '{seller_legal}' успешно отправлены.")


def process_ozon_reports(
    date_from: str,
    date_to: str,
    seller_legal: str,
    spreadsheet_id: str
):
    """
    Основная функция: скачивает отчёты Ozon и выгружает их в Google Sheets.
    """
    d_from = convert_date(date_from)
    d_to   = convert_date(date_to)

    download_dir = os.path.join(os.path.dirname(__file__), "downloads")
    os.makedirs(download_dir, exist_ok=True)

    # вот здесь берём профиль из конфига:
    chrome_profiles = CONFIG["ozon_selenium"]["chrome_profiles"]  # :contentReference[oaicite:2]{index=2}:contentReference[oaicite:3]{index=3}
    driver = _setup_chrome_driver(seller_legal, chrome_profiles, download_dir)

    file_path = None
    try:
        try:
            file_path = download_file(
                driver,
                CONFIG["ozon_paid_storage"][seller_legal],
                d_from, d_to,
                download_dir
            )
        except TimeoutException:
            logger.warning("Не получилось по основному URL, пробую альтернативу.")
            file_path = download_file(
                driver,
                CONFIG["ozon_paid_storage_second_path"][seller_legal],
                d_from, d_to,
                download_dir
            )

        process_excel_file(file_path, spreadsheet_id, seller_legal)

    except Exception as e:
        logger.error(f"Ошибка при обработке {seller_legal}: {e}", exc_info=True)
    finally:
        driver.quit()
        if file_path and os.path.exists(file_path):
            os.remove(file_path)


if __name__ == "__main__":
    process_ozon_reports(
        "15.05.2025",
        "15.05.2025",
        "ut",
        "13hcF4pqxmeJSnYmVLqb7OMWKUaII7WGQVd6UdBRZkD4"
    )
