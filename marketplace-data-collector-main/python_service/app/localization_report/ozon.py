import os
import time
import logging
import glob
import pandas as pd
import numpy as np

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


logger = logging.getLogger(__name__)


def _setup_chrome_driver(seller_legal: str, chrome_profiles: dict, download_dir: str):
    """
    Настраивает и запускает ChromeDriver через Selenium Grid с параметрами профиля и загрузки.

    Args:
        seller_legal (str): Идентификатор юридического лица, используемый для выбора профиля Chrome.
        chrome_profiles (dict): Конфигурация профилей Chrome.
        download_dir (str): Директория, куда будут загружаться файлы.

    Returns:
        webdriver.Remote: Настроенный экземпляр Chrome WebDriver.

    Raises:
        KeyError: Если отсутствует конфигурация для переданного `seller_legal` в `chrome_profiles`.
    """
    try:
        profile_config = chrome_profiles[seller_legal]
    except KeyError:
        logger.error(f"Конфигурация для '{seller_legal}' не найдена в 'chrome_profiles'.")
        raise

    chrome_options = Options()
    chrome_options.add_argument(f"user-data-dir=/home/seluser/.config/google-chrome/")
    if seller_legal == "inter":
        chrome_options.add_argument(f"--profile-directory=Profile Ozon inter")
    else:
        chrome_options.add_argument(f"--profile-directory=Profile Ozon ut")
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--no-default-browser-check")
    chrome_options.add_argument("--disable-session-crashed-bubble")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-features=DownloadShelf")

    # Настройки загрузки файлов
    prefs = {
        "download.default_directory": "/home/seluser/Downloads",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_settings.popups": 0,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Подключение к Selenium Grid
    selenium_grid_url = os.getenv("SELENIUM_GRID_URL", "http://selenium:4444/wd/hub")
    try:
        driver = webdriver.Remote(command_executor=selenium_grid_url, options=chrome_options)

    except Exception as e:
        logger.error(f"Не удалось подключиться к Selenium Grid ({selenium_grid_url}). Проверьте, запущен ли он.",
                     exc_info=True)
        raise
    return driver


def _download_localization_report(driver, download_dir: str, seller_legal: str):
    """
    Скачивает отчет о локализации товаров с Ozon Seller.

    Args:
        driver (webdriver.Chrome): Экземпляр веб-драйвера Selenium.
        download_dir (str): Путь к каталогу загрузки файлов.
        seller_legal (str): Юридическое название селлера для именования файла.

    Returns:
        str: Полный путь к загруженному и переименованному файлу.

    Raises:
        TimeoutException: Если файл не был загружен в течение установленного времени ожидания.
    """
    url = "https://seller.ozon.ru/app/analytics/sales-geography/local-packaging"
    logger.info(f"Переход на страницу: {url}")
    driver.get(url)

    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

    button_css_selector = "#app > main > div.index_content_7dUsB > div.index_wrapper_1r7Ff > div > div > div > div:nth-child(1) > div > div:nth-child(2) > section.styles_bottomWidgets_JqrVS > div.styles_tableControls_3rdiE > div > button:nth-child(2)"
    download_css_selector = "#app > main > div.index_content_7dUsB > div.index_wrapper_1r7Ff > div > div > div > div:nth-child(1) > div > div:nth-child(2) > section.styles_bottomWidgets_JqrVS > div.styles_tableControls_3rdiE > button"

    try:
        # Ожидаем кнопку "По товарам" и кликаем
        button = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.CSS_SELECTOR, button_css_selector)))
        button.click()
        logger.info("Клик по кнопке 'По товарам' выполнен.")

        # Ожидаем кнопку "Скачать" и кликаем
        download_button = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.CSS_SELECTOR,
                                                                                      download_css_selector)))
        download_button.click()
        logger.info("Клик по кнопке 'Скачать' выполнен.")

        logger.info("Ожидание загрузки файла...")
        start_time = time.time()

        while time.time() - start_time < 180:
            downloaded_files = glob.glob(os.path.join(download_dir, "localization_index_*.xlsx"))

            if downloaded_files:
                downloaded_file = max(downloaded_files, key=os.path.getctime)
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                new_file_path = os.path.join(download_dir, f"{seller_legal}_localization_{timestamp}.xlsx")
                os.rename(downloaded_file, new_file_path)
                return new_file_path

            time.sleep(1)

        raise TimeoutException("Файл не был загружен в течение отведенного времени.")

    except TimeoutException as e:
        logger.error(f"Ошибка {e}: Не удалось найти кнопку или загрузить файл.", exc_info=True)
        raise


def _process_excel_file(file_path: str):
    """
    Извлекает нужную вкладку из Excel и готовит данные для загрузки.

    Args:
        file_path (str): Путь к файлу Excel, содержащему данные.

    Returns:
        pd.DataFrame: Обработанный DataFrame с данными из вкладки "Только товары".
    """
    try:
        xl = pd.ExcelFile(file_path)
        if "Только товары" not in xl.sheet_names:
            logger.error(f"❌ В файле {file_path} нет вкладки 'Только товары'. Пропускаем обработку.")
            return pd.DataFrame()

        # Пропускаем первые 4 строки и переименовываем столбцы в соответствии с таблицей БД
        df = xl.parse(sheet_name="Только товары", skiprows=4)
        df = df.iloc[:, :9]  # Берем только столбцы A:I
        df.columns = [
            "productName", "article", "sku", "supplyScheme",
            "totalOrdered", "localOrdered", "nonLocalOrdered",
            "localOrderPercent", "nonLocalOrderPercent"
        ]

        # Приводим к корректным типам данных
        numeric_columns = ["sku", "totalOrdered", "localOrdered", "nonLocalOrdered", "localOrderPercent", "nonLocalOrderPercent"]
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)

        df["legalEntity"] = ""

        return df
    except Exception as e:
        logger.error(f"❌ Ошибка обработки Excel-файла {file_path}: {e}", exc_info=True)
        return pd.DataFrame()


def sync_localization_reports_into_db(chrome_profiles, today):
    """
    Скачивает отчеты о локализации товаров для всех юридических лиц,
    обрабатывает их и загружает в PostgreSQL.

    Workflow:
        1. Получает конфигурацию БД и список юридических лиц.
        2. Настраивает директорию загрузки.
        3. Для каждого юридического лица:
            - Запускает Chrome с нужным профилем.
            - Переходит на страницу и скачивает отчет.
            - Обрабатывает Excel-файл, очищая данные.
            - Добавляет данные в общий DataFrame.
        4. Загружает объединенные данные в PostgreSQL.

    Raises:
        Exception: Если при скачивании, обработке или загрузке в БД возникает ошибка.
    """
    table_name = "localization_idx_reports"
    sheet_mapping = {"inter": "ИНТЕР", "ut": "АТ"}

    download_dir = "/home/seluser/Downloads"
    os.makedirs(download_dir, exist_ok=True)

    all_data = []

    for seller_legal, legal_entity in sheet_mapping.items():
        driver = None
        file_path = None

        try:
            driver = _setup_chrome_driver(seller_legal,chrome_profiles, download_dir)
            file_path = _download_localization_report(driver, download_dir, seller_legal)
            df = _process_excel_file(file_path)

            if df.empty:
                logger.warning(f"⚠️ Отчет для {seller_legal} пуст. Пропускаем загрузку.")
                continue

            df["legalEntity"] = legal_entity
            df["updated_at"] = today.strftime("%Y-%m-%d")
            all_data.append(df)

        except Exception as e:
            logger.error(f"❌ Ошибка при обработке {seller_legal}: {e}", exc_info=True)
            continue
        finally:
            if driver:
                driver.quit()

            if file_path:
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except OSError as e:
                        logger.warning(f"⚠️ Не удалось удалить файл {file_path}: {e}")
                else:
                    logger.warning(f"⚠️ Файл {file_path} отсутствует, возможно, скачивание не произошло.")

    if not all_data:
        logger.warning("⚠️ Все отчеты пустые, загрузка в базу не требуется.")
        return

    return pd.concat(all_data, ignore_index=True)
