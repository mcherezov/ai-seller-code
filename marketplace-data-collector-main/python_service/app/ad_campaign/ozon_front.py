import os
import time
import glob
import logging
from datetime import datetime
from typing import Optional
import pandas as pd

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver import Keys
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

    selenium_grid_url = os.getenv("SELENIUM_GRID_URL", "http://selenium:4444/wd/hub")
    try:
        driver = webdriver.Remote(command_executor=selenium_grid_url, options=chrome_options)

    except Exception as e:
        logger.error(f"Не удалось подключиться к Selenium Grid ({selenium_grid_url}). Проверьте, запущен ли он.",
                     exc_info=True)
        raise
    return driver


def _download_ad_campaign_report(
    driver: webdriver.Chrome,
    date_range: str,
    download_dir: str
) -> Optional[str]:
    """
    1) Открываем страницу
    2) Нажимаем 'Товар'
    3) Нажимаем 'Скачать в excel'
    4) Дожидаемся появления модального окна
    5) Находим поле ввода дат, вводим период
    6) Нажимаем 'Скачать'
    7) Ждём скачивания файла
    """
    cookies = driver.execute_cdp_cmd('Network.getAllCookies', {})
    logger.debug(f"Куки: {cookies}")

    url = "https://seller.ozon.ru/app/advertisement/product/overview"
    logger.info(f"Открываем страницу: {url}")
    driver.get(url)

    try:
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except TimeoutException:
        logger.error("❌ Страница не загрузилась за 20 секунд.")
        return None

    # # Добавляем задержку в 5 минут (300 секунд)
    # logger.info("Ожидание 5 минут перед кликом по кнопке...")
    # time.sleep(300)

    def save_debug_page(driver):
        """Сохраняет HTML, если не найдены элементы."""
        with open("debug_page.html", "w", encoding="utf-8") as file:
            file.write(driver.page_source)
        logger.error("❌ Сохранена копия страницы для анализа.")

    # === Клик по кнопке "Товар" ===
    try:
        # Ищем кнопку по тексту "Товар" или "Product" (на случай английской версии)
        button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(normalize-space(.), 'Товар') or contains(normalize-space(.), 'Product')]")
            )
        )
        button.click()
        logger.info("✅ Кликнули по кнопке 'Товар' (по тексту).")
    except TimeoutException:
        logger.error("❌ Не удалось найти кнопку 'Товар' по тексту.")
        save_debug_page(driver)
        return None

    # === Клик по кнопке "Скачать в Excel" ===
    try:
        # Ищем кнопку по тексту "Скачать" или "Download"
        download_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 "//button[contains(normalize-space(.), 'Скачать') or contains(normalize-space(.), 'Download')]")
            )
        )
        download_button.click()
        logger.info("✅ Кликнули по кнопке 'Скачать в Excel' (по тексту).")
    except TimeoutException:
        logger.error("❌ Не удалось найти кнопку 'Скачать в Excel' по тексту.")
        save_debug_page(driver)
        return None

    # === Ожидание появления модального окна ===
    try:
        date_input_xpath = "//*[@id='ods-window-target-container']//input[contains(@id, 'baseInput')]"
        date_input_css = "input[id^='baseInput']"
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, date_input_xpath)))
        date_input = driver.find_element(By.XPATH, date_input_xpath)
        date_input.clear()
        date_str = date_range.strftime('%d.%m.%Y')
        date_input.send_keys(f"{date_str} - {date_str}")
        logger.info(f"✅ Ввели период: {date_str} - {date_str}")
        time.sleep(2)

    except Exception as e:
        logger.error(f"Ошибка при вводе даты в модальном окне: {e}")
        return None

    # === Клик по кнопке "Скачать" в модальном окне (по тексту) ===
    try:
        # Ждем появления поля даты и переключаем фокус (как было)
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, date_input_xpath)))
        date_input = driver.find_element(By.XPATH, date_input_xpath)
        date_input.send_keys(Keys.TAB)
        time.sleep(2)

        # Поиск кнопки "Скачать" по тексту
        download_button_modal_xpath = (
            "//*[@id='ods-window-target-container']"
            "//button[contains(normalize-space(.), 'Сформировать') or contains(normalize-space(.), 'Generate')]"
        )

        modal_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, download_button_modal_xpath))
        )

        if not modal_btn.is_enabled():
            logger.warning("⚠️ Кнопка 'Скачать' еще не активна, ждем...")
            WebDriverWait(driver, 5).until(lambda d: modal_btn.is_enabled())

        modal_btn.click()
        logger.info("✅ Кликнули по кнопке 'Скачать' (по тексту).")

    except TimeoutException:
        logger.error("❌ Не удалось найти кнопку 'Скачать' в модальном окне по тексту.")
        save_debug_page(driver)
        return None


    except Exception as e:
        logger.error(f"❌ Ошибка при клике по кнопке 'Скачать': {e}")
        return None

    # === Ожидание скачивания файла ===
    start_time = time.time()
    downloaded_file = None

    while time.time() - start_time < 60:
        files = glob.glob(os.path.join(download_dir, "sku_statistics_*"))

        if files:
            latest_file = max(files, key=os.path.getctime)

            if latest_file.endswith(".crdownload"):
                logger.info("⚠️ Файл еще загружается, ждем...")
                time.sleep(1)
                continue

            downloaded_file = latest_file
            break
        else:
            time.sleep(1)

    if not downloaded_file:
        logger.error("❌ Файл не скачался за 60 секунд.")
        return None

    logger.info(f"✅ Файл успешно скачан: {downloaded_file}")
    return downloaded_file


def _process_excel_report(file_path: str) -> pd.DataFrame:
    """
    Пример обработки Excel (.xlsx).
    Здесь предполагаем, что нужный лист — первый, пропускаем первые 2 строки.
    """
    try:
        df = pd.read_excel(file_path, sheet_name=0, skiprows=1)
        logger.info(f"Прочитан Excel. Размер: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Ошибка чтения Excel-файла {file_path}: {e}")
        return pd.DataFrame()


def fetch_ozon_ad_campaign_statistics(chrome_profiles, date):
    """
    Скачивает отчеты о рекламных кампаниях для всех юридических лиц,
    обрабатывает их и возвращает объединенный DataFrame.

    Args:
        date (datetime): дата, за которую нужно получить данные (используется для формирования столбца "updated_at")

    Returns:
        pd.DataFrame или None: объединенный DataFrame с данными, или None, если данные не получены.
    """
    download_dir = "/home/seluser/Downloads"
    os.makedirs(download_dir, exist_ok=True)

    sheet_mapping = {"inter": "ИНТЕР", "ut": "АТ"}
    all_data = []

    for seller_legal, legal_entity in sheet_mapping.items():
        driver = None
        file_path = None

        try:
            driver = _setup_chrome_driver(seller_legal, chrome_profiles, download_dir)
            file_path = _download_ad_campaign_report(driver, date, download_dir)
            if file_path:
                logger.info(f"Отчет успешно скачан для {seller_legal}: {file_path}")

                df = _process_excel_report(file_path)
                if df.empty:
                    logger.warning(f"DataFrame пуст после чтения CSV для {seller_legal}.")
                else:
                    try:
                        df = df.rename(columns={
                            # Англоязычные названия (из API)
                            'Promotion type': 'Тип продвижения',
                            'Campaign ID': 'ID кампании',
                            'Expense, ₽, incl. VAT': 'Расход, ₽, с НДС',
                            'Advertising-to-Sales Ratio, percent': 'ДРР, percent',
                            'Advertising-to-Sales Ratio, %': 'ДРР, percent',
                            'Sales, ₽': 'Продажи, ₽',
                            'Orders, pcs': 'Заказы, шт',
                            'CTR, percent': 'CTR, percent',
                            'CTR, %': 'CTR, percent',
                            'Impressions': 'Показы',
                            'Clicks': 'Клики',
                            'Cost per order, ₽':  'Затраты на заказ, ₽',
                            'Average cost per click, ₽': 'Средняя стоимость клика, ₽',
                            'Carts': 'Корзины',
                            'Conversion to cart, percent': 'Конверсия в корзину, percent',
                            'Conversion to cart, %': 'Конверсия в корзину, percent',
                            'Product name in promotion': 'Название товара в продвижении',

                            # Русскоязычные названия с %
                            'ДРР, %': 'ДРР, percent',
                            'Конверсия в корзину, %': 'Конверсия в корзину, percent',
                        })

                        logger.info("✅ Переименование столбцов выполнено.")
                    except Exception as e:
                        logger.info(
                            "ℹ️ Переименование не потребовалось или не удалось. Возможно, названия уже соответствуют нужным.")
                    # Колонки, которые нужно конвертировать в float
                    columns_to_convert = [
                        'Расход, ₽, с НДС', 'ДРР, percent', 'Продажи, ₽', 'CTR, percent',
                        'Затраты на заказ, ₽', 'Средняя стоимость клика, ₽', 'Конверсия в корзину, percent'
                    ]

                    for col in columns_to_convert:
                        if col in df.columns:
                            original_non_null = df[col].notna().sum()
                            df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                            null_after = df[col].isna().sum()
                            if null_after > 0:
                                logger.warning(
                                    f"⚠️ В колонке '{col}' не удалось сконвертировать {null_after} из {original_non_null} значений.")

                    df["legalEntity"] = legal_entity
                    df["date"] = date.strftime("%Y-%m-%d")
                    all_data.append(df)

            else:
                logger.error(f"Отчет не был скачан для {seller_legal}.")
        except Exception as e:
            logger.error(f"Ошибка при обработке {seller_legal}: {e}", exc_info=True)
        finally:
            if driver:
                driver.quit()
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось удалить файл {file_path}: {e}")

    if not all_data:
        logger.warning("⚠️ Все отчеты пустые, загрузка в базу не требуется.")
        return None
    else:
        final_df = pd.concat(all_data, ignore_index=True)

        # 🔍 Лог столбцов перед вставкой
        logger.debug(f"📊 Столбцы итогового DataFrame перед вставкой в БД: {final_df.columns.tolist()}")

        # 🔍 Дополнительно: проверка на отсутствие нужных колонок
        expected_columns = {
            'SKU', 'Тип продвижения', 'ID кампании', 'Расход, ₽, с НДС', 'ДРР, percent',
            'Продажи, ₽', 'Заказы, шт', 'CTR, percent', 'Показы', 'Клики',
            'Затраты на заказ, ₽', 'Средняя стоимость клика, ₽', 'Корзины', 'Конверсия в корзину, percent',
            'legalEntity', 'date', 'Название товара в продвижении'
        }
        actual = set(final_df.columns)
        missing = expected_columns - actual
        if missing:
            logger.warning(f"⚠️ Отсутствуют ожидаемые столбцы: {missing}")

        return final_df


