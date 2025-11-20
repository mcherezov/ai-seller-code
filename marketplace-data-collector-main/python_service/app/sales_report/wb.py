import logging
import os
from time import sleep
from typing import Dict, Union
import uuid
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
import base64
import zipfile
import io
import pandas as pd
from datetime import datetime, date as date_type, timedelta
import json


def get_zip_link(driver):
    try:
        links = driver.find_elements(By.TAG_NAME, "a")
        for i in range(len(links)):
            try:
                fresh_link = driver.find_elements(By.TAG_NAME, "a")[i]
                href = fresh_link.get_attribute("href")
                if href and href.endswith(".zip"):
                    return href
            except StaleElementReferenceException:
                continue
    except Exception as e:
        logging.warning(f"⚠️ Ошибка получения zip-ссылки: {e}")
    return None



def fetch_wb_sales_report(marketplace_keys: Dict[str, str], entities_meta, date: Union[datetime, date_type]) -> pd.DataFrame:
    """
    Скачивает отчеты Wildberries для указанного юридического лица
    """
    all_data = pd.DataFrame()
    for seller_legal, profile_config in marketplace_keys["chrome_profiles"].items():
        driver = None
        log_prefix = f"Отчеты продаж wb. {seller_legal}. "
        download_url_part = marketplace_keys["sale_report"]["paths"][seller_legal]
        try:
            logging.debug(f"{log_prefix}Запуск обработки отчёта за {date.strftime('%d.%m.%Y')}")

            chrome_options = Options()
            chrome_options.add_argument(f"user-data-dir=/google_chrome_users/")
            chrome_options.add_argument(f"--profile-directory={profile_config['profile_directory']}")
            chrome_options.add_argument("--no-first-run")
            chrome_options.add_argument("--no-default-browser-check")
            # chrome_options.add_argument("--headless")
            prefs = {
                "download.default_directory": "/home/seluser/Downloads",
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            }
            chrome_options.add_experimental_option("prefs", prefs)

            logging.debug(f"{log_prefix}Создаём драйвер и открываем страницу...")
            driver = webdriver.Remote(
                f"http://{os.getenv('SELENIUM_SERVICE_NAME')}:4444/wd/hub",
                options=chrome_options
            )
            driver.get(marketplace_keys["sale_report"]["url"])
            sleep(5)

            formatted_date_from = date.strftime("%d.%m.%Y")
            formatted_date_to = date.strftime("%d.%m.%Y")
            formatted_date_for_comparison = (date - timedelta(days=1)).strftime("%d.%m.%Y")

            logging.debug(f"{log_prefix}Открыта страница отчётов. Ждём кнопку выбора отчёта...")

            WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="app-content-id"]/div[1]/div/div/div/div[3]/div/div[1]/div/div[1]/div[2]/div/div/button')
                )
            ).click()
            logging.debug(f"{log_prefix}Нажали кнопку выбора отчёта")

            sleep(5)

            logging.debug(f"{log_prefix}Устанавливаем даты: {formatted_date_from} — {formatted_date_to}")

            start_date_field = WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="startDate"]'))
            )
            start_date_field.clear()
            start_date_field.send_keys(formatted_date_from)
            start_date_field.send_keys(Keys.TAB)
            sleep(0.5)

            end_date_field = WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="endDate"]'))
            )
            end_date_field.clear()
            end_date_field.send_keys(formatted_date_to)
            end_date_field.send_keys(Keys.TAB)
            sleep(1)

            logging.debug(f"{log_prefix}Нажимаем 'Сохранить'")
            WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[.//text()[contains(., 'Save') or contains(., 'Сохранить')]]")
                )
            ).click()

            logging.debug(f"{log_prefix}Ожидаем загрузку результатов отчёта")
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "Reports-table__wrapper")]')
                                               )
            )
            logging.debug(f"{log_prefix}Таблица отчётов найдена")

            sleep(10)

            formatted_date_for_comparison = (date - timedelta(days=1)).strftime("%d.%m.%Y")

            # Проверка на кнопку "Load more"
            while True:
                try:
                    logging.debug(f"{log_prefix}Проверяем наличие новых строк перед нажатием 'Load more'...")

                    start_date_no_xpath = f'//*[@id="app-content-id"]/div[1]/div/div/div/div[3]/div/div/div[2]/div/div/div/div[2]/div[last()]/div[4]/span'
                    end_date_no_xpath = f'//*[@id="app-content-id"]/div[1]/div/div/div/div[3]/div/div/div[2]/div/div/div/div[2]/div[last()]/div[5]/span'

                    start_date_no_element = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, start_date_no_xpath))
                    )
                    end_date_no_element = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, end_date_no_xpath))
                    )

                    start_date_text = start_date_no_element.text.strip()
                    end_date_text = end_date_no_element.text.strip()

                    logging.debug(
                        f"{log_prefix}Последняя строка отчёта: start_date = {start_date_text}, end_date = {end_date_text}")

                    if start_date_text not in (formatted_date_for_comparison, formatted_date_from) \
                            or end_date_text not in (formatted_date_for_comparison, formatted_date_to):
                        logging.debug(
                            f"{log_prefix}❌ Последняя строка вне диапазона, прекращаем загрузку дополнительных строк.")
                        break

                    logging.debug(f"{log_prefix}✅ Даты совпадают, ищем кнопку 'Load more'...")

                    load_more_button = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, '//button[span[text()="Load more"]]'))
                    )
                    logging.debug(f"{log_prefix}Нажимаем 'Load more'")
                    load_more_button.click()
                    sleep(3)

                except StaleElementReferenceException:
                    logging.debug(f"{log_prefix}⚠ Страница обновилась, элемент устарел. Пробуем ещё раз...")
                    continue
                except TimeoutException:
                    logging.debug(f"{log_prefix}⏹ Кнопка 'Load more' больше не отображается — завершение.")
                    break

            # === Новый сбор отчётов с проверкой наличия кнопки и периода ===
            report_num_arr = []

            try:
                rows = driver.find_elements(By.CSS_SELECTOR, "div[class^='Reports-table-row__']")
                logging.debug(f"{log_prefix}Найдено строк отчётов: {len(rows)}")

                for row_index, row in enumerate(rows, start=1):
                    try:
                        logging.debug(f"{log_prefix}Пробуем обработать строку #{row_index}...")

                        # Пытаемся найти кнопку с номером отчета
                        try:
                            report_no_button = row.find_element(By.TAG_NAME, "button")
                            report_no = report_no_button.text.strip()
                        except:
                            logging.debug(f"{log_prefix}⏭ Строка #{row_index} без кнопки — пропуск")
                            continue

                        # Пытаемся найти корректный период
                        span_elements = row.find_elements(By.TAG_NAME, "span")
                        period_text = None
                        for span in span_elements:
                            text = span.text.strip()
                            if text.startswith("from "):
                                period_text = text
                                break

                        if not period_text:
                            logging.debug(f"{log_prefix}⏭ Строка #{row_index} без корректного периода — пропуск")
                            continue

                        logging.debug(f"{log_prefix}→ Строка {row_index}: report={report_no}, period='{period_text}'")

                        # Разбираем период
                        if "from" in period_text and "to" in period_text:
                            period_from, period_to = period_text.replace("from", "").split("to")
                            period_from = period_from.strip()
                            period_to = period_to.strip()
                        else:
                            logging.warning(f"{log_prefix}⚠ Неправильный формат периода: '{period_text}'")
                            continue

                        # Проверяем даты
                        if period_from not in (formatted_date_for_comparison, formatted_date_from) or \
                                period_to not in (formatted_date_for_comparison, formatted_date_to):
                            logging.debug(f"{log_prefix}❌ Пропущено: {period_from} — {period_to}")
                            continue

                        logging.debug(f"{log_prefix}✅ Добавлен отчёт: {report_no}")
                        report_num_arr.append(report_no)

                    except Exception as e:
                        logging.error(f"{log_prefix}❌ Ошибка обработки строки #{row_index}: {e}", exc_info=True)
                        continue

            except Exception as e:
                logging.error(f"{log_prefix}❌ Ошибка поиска строк отчёта: {e}", exc_info=True)
            WebDriverWait(driver, 20).until(lambda d: d.get_cookie("WBTokenV3") is not None)

            for report_id in report_num_arr:
                try:
                    url = f"https://seller.wildberries.ru/suppliers-mutual-settlements/reports-implementations/reports-daily/report/{report_id}?isGlobalBalance=false"
                    driver.get(url)

                    # Кнопка "Download Excel"
                    WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Download Excel')]"))
                    ).click()

                    host_download_path = "/home/seluser/Downloads"
                    files_before = set(os.listdir(host_download_path))

                    # Кликаем по "Download Excel"
                    WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Download Excel')]"))
                    ).click()

                    # Ждём появления нового zip-файла
                    WebDriverWait(driver, 60).until(
                        lambda _: any(
                            f.endswith(".zip") and f not in files_before
                            for f in os.listdir(host_download_path)
                        )
                    )

                    # Ищем новый файл
                    zip_filename = next(
                        f for f in os.listdir(host_download_path)
                        if f.endswith(".zip") and f not in files_before
                    )
                    full_path = os.path.join(host_download_path, zip_filename)
                    logging.debug(f"{log_prefix}✅ Найден zip-файл: {full_path}")

                    # Обработка архива
                    try:
                        with zipfile.ZipFile(full_path, "r") as z:
                            name = z.namelist()[0]
                            with z.open(name) as f:
                                df = pd.read_excel(f, engine='openpyxl').fillna("")
                                df.drop(['№'], axis=1, errors='ignore', inplace=True)
                                meta = entities_meta.get(seller_legal)
                                df["legal_entity"] = meta["display_name"] if meta else seller_legal
                                all_data = pd.concat([all_data, df], ignore_index=True)
                                logging.info(f"{log_prefix}✅ Загружен отчёт {report_id}, строк: {df.shape[0]}")

                    except Exception as e:
                        logging.error(f"{log_prefix}Ошибка обработки ZIP-файла: {e}", exc_info=True)

                except Exception as e:
                    logging.error(f"{log_prefix}Ошибка обработки отчёта {report_id}: {e}", exc_info=True)

        except:
            logging.error(f"{log_prefix}Ошибка при инициализации драйвера")
            continue
        finally:
            if driver:
                try:
                    driver.close()
                    driver.quit()
                except Exception as e:
                    logging.warning(f"{log_prefix}Не смог закрыть драйвер: {e}")

    # Очищаем все пустые значения в all_data
    all_data.replace("", pd.NA, inplace=True)

    if "Доплаты" not in all_data.columns:
        all_data["Доплаты"] = 0

    numeric_cols = ["Скидка по программе софинансирова", "Скидка Wibes, percent"]
    real_cols = [
        "Цена розничная", "Вайлдберриз реализовал Товар (Пр)", "Цена розничная с учетом согласова",
        "Размер снижения кВВ из-за акции, perc", "Скидка постоянного Покупателя (СП",
        "Размер кВВ, percent", "Размер  кВВ без НДС, percent Базовый",
        "Итоговый кВВ без НДС, percent", "Вознаграждение с продаж до вычета ",
        "Возмещение за выдачу и возврат тов", "Эквайринг/Комиссии за организацию",
        "Размер комиссии за эквайринг/Коми", "Вознаграждение Вайлдберриз (ВВ), б",
        "НДС с Вознаграждения Вайлдберриз", "К перечислению Продавцу за реализ",
        "Услуги по доставке товара покупат", "Возмещение издержек по перевозке/",
        "Хранение", "Удержания", "Платная приемка", "Фиксированный коэффициент склада ",
        "Корректировка Вознаграждения Вай", "Промокод, percent"
    ]
    bigint_cols = [
        "Номер поставки", "Код номенклатуры", "Кол-во", "Согласованный продуктовый дискон",
        "Итоговая согласованная скидка, perce", "Размер снижения кВВ из-за рейтинга",
        "Количество доставок", "Количество возврата", "Общая сумма штрафов",
        "Доплаты", "Номер сборочного задания", "ШК", "Сумма удержанная за начисленные б",
        "Компенсация скидки по программе л"
    ]

    all_numeric = numeric_cols + real_cols + bigint_cols

    for col in all_numeric:
        if col in all_data.columns:
            all_data[col] = pd.to_numeric(all_data[col], errors="coerce").fillna(0)

    for col in all_data.select_dtypes(include="object"):
        if col not in all_numeric:
            all_data[col] = all_data[col].fillna("")

    for col in all_data.columns:
        if all_data[col].dtype == "object" and (all_data[col] == "").any():
            all_data[col] = all_data[col].replace("", None)

    final_df = all_data.drop_duplicates()
    final_df["uuid"] = [str(uuid.uuid4()) for _ in range(len(final_df))]

    for i, row in final_df.iterrows():
        values = [row[col] for col in final_df.columns]
        if any(v == "" for v in values):
            logging.debug("❌ ВСТАВКА ПУСТОЙ СТРОКИ В VALUES")
            logging.debug(values)
            raise Exception("Остановлено: найдена пустая строка в values")

    final_df.columns = (
        final_df.columns
        .str.replace('\u00A0', ' ')
        .str.strip()
    )

    logging.debug("❓ Колонки ДО переименования: %s", final_df.columns.tolist())

    rename_map = {
        "Размер изменения кВВ из-за акции, %": "Размер снижения кВВ из-за акции, perc",
    }
    for actual, expected in rename_map.items():
        if actual in final_df.columns and expected not in final_df.columns:
            final_df.rename(columns={actual: expected}, inplace=True)
            logging.debug("✅ Переименован столбец: '%s' → '%s'", actual, expected)

    logging.debug("❓ Колонки ПОСЛЕ переименования: %s", final_df.columns.tolist())

    return final_df