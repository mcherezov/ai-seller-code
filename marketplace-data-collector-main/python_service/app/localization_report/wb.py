import glob
import logging
import os
from time import sleep
import time
from typing import Dict, Union
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import zipfile
import io
import pandas as pd
from datetime import datetime, date as date_type

logger = logging.getLogger(__name__)


def fetch_wb_localization_index_report(marketplace_keys: Dict[str, str], date: Union[datetime, date_type]) -> pd.DataFrame:
    all_data = pd.DataFrame()  # Создаем пустой DataFrame для сбора всех данных
    for seller_legal, profile_config in marketplace_keys["chrome_profiles"].items():
        try:
            chrome_options = Options()
            chrome_options.add_argument(f"user-data-dir=/google_chrome_users/")
            chrome_options.add_argument(f"--profile-directory={profile_config['profile_directory']}")

            chrome_options.add_argument("--no-first-run")
            chrome_options.add_argument("--no-default-browser-check")

            download_dir = "/home/seluser/Downloads"
            os.makedirs(download_dir, exist_ok=True)
            # Настройки загрузки файлов
            prefs = {
                "download.default_directory": download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "profile.default_content_settings.popups": 0,
            }
            chrome_options.add_experimental_option("prefs", prefs)

            driver = webdriver.Remote(f"http://{os.getenv('SELENIUM_SERVICE_NAME')}:4444/wd/hub", options=chrome_options)
            driver.execute_cdp_cmd("Page.setDownloadBehavior", {
                "behavior": "allow",
                "downloadPath": download_dir,
            })
            driver.get(marketplace_keys["localization_index"]["url"])
            # Для некоторых продавцов за некоторые даты почему-то 1 секунды не хватает, не уменьшать!
            sleep(5)

            formatted_date_from = date.strftime("%d.%m.%Y")
            formatted_date_to = date.strftime("%d.%m.%Y")

            WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="app-content-id"]/div[1]/div/div/div/div[4]/div[1]/div[1]/div[1]/span/div/div/div/button'))
            ).click()
            # Для некоторых продавцов за некоторые даты почему-то 1 секунды не хватает, не уменьшать!
            sleep(5)

            start_date_field = WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="startDate"]'))
            )
            start_date_field.clear()
            start_date_field.send_keys(formatted_date_from)

            end_date_field = WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="endDate"]'))
            )
            end_date_field.clear()
            end_date_field.send_keys(formatted_date_to)

            sleep(1)

            WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="Portal-@wildberries/lazy-calendar"]/div/div/form/div[3]/div[2]/button/span'))
            ).click()

            # Ожидание загрузки таблицы результатов
            WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="app-content-id"]/div[1]/div/div/div/div[5]/div/div[2]/div'))
            )
            sleep(10)

            # Прокручиваем страницу вниз, чтобы кнопка Export to Excel была видна
            driver.execute_script("window.scrollBy(0, window.innerHeight / 2);")

            # Нажимаем кнопку Export to Excel
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="app-content-id"]/div[1]/div/div/div/div[5]/div/div[3]/div[1]/div[2]/span/button'))
            ).click()

            # Нажимаем кнопку Generate File
            WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="Portal-modal"]/div[2]/div/div/div[2]/div/div[2]/button[1]/span'))
            ).click()

            # TODO (ivan): Добавить проверку на количество файлов, пропускать мерчанта если файлов больше 10

            for _ in range(10):
                # Нажимаем кнопку Downloads
                WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, '//*[@id="app-content-id"]/div[1]/div/div/div/div[5]/div/div[3]/div[1]/div[2]/div/div[1]/button'))
                ).click()

                try:
                    # Нажимаем кнопку скачивания последнего отчета
                    WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, '//*[@id="layers"]/div/div/div[1]/div[3]/div[2]/div[2]/span'))
                    ).click()
                    break
                except:
                    # Нажимаем на фон, чтобы убрать диалог скачивания
                    WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, '//*[@id="app-content-id"]/div[1]/div/div/div'))
                    ).click()

            logger.info("Ожидание загрузки файла...")
            start_time = time.time()
            while time.time() - start_time < 60:
                downloaded_files = glob.glob(os.path.join(download_dir, "Delivery report by region *.zip"))

                if downloaded_files:
                    downloaded_file = max(downloaded_files, key=os.path.getctime)
                    timestamp = time.strftime("%Y%m%d_%H%M%S")
                    new_file_path = os.path.join(download_dir, f"{seller_legal}_wb_localization_{timestamp}.zip")
                    os.rename(downloaded_file, new_file_path)
                    break

                time.sleep(1)
            else:
                logger.error("Файл не был загружен в течение отведенного времени, пехеходим к следующему продавцу.")
                continue

            try:
                with zipfile.ZipFile(new_file_path, 'r') as zip_ref:
                    # Ищем первый (или нужный) XLSX файл внутри архива
                    xlsx_files = [name for name in zip_ref.namelist() if name.endswith('.xlsx')]

                    if not xlsx_files:
                        logger.error(f"❌ В архиве {new_file_path} нет файлов .xlsx")
                        continue
                    
                    xlsx_filename = xlsx_files[0]  # Берём первый найденный

                    # Читаем XLSX файл в память
                    with zip_ref.open(xlsx_filename) as file:
                        xl = pd.ExcelFile(io.BytesIO(file.read()))
                if "Detailed data" not in xl.sheet_names:
                    logger.error(f"❌ В файле {new_file_path} нет вкладки 'Detailed data'. Пропускаем обработку.")
                    continue

                df = xl.parse(sheet_name="Detailed data", skiprows=1)

                # Приводим к корректным типам данных
                numeric_columns = ["Total orders, pcs", "Total orders for products local, pcs", "Total orders for products not local, pcs",
                                "Total orders for products not local, %", "Orders from WB warehouse local, pcs",
                                "Orders from WB warehouse not local, pcs", "WB warehouse orders not local, %",
                                "Marketplace orders local, pcs", "Marketplace orders not local, pcs", "Marketplace orders not local, %",
                                "Warehouse stocks, pcs", "Marketplace stocks, pcs"]
                df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)

                df["legalEntity"] = seller_legal

                if not df.empty:
                    all_data = pd.concat([all_data, df], ignore_index=True)
            except Exception as e:
                logger.error(f"❌ Ошибка обработки Excel-файла {new_file_path}: {e}", exc_info=True)

                continue
            finally:
                if new_file_path:
                    if os.path.exists(new_file_path):
                        try:
                            os.remove(new_file_path)
                        except OSError as e:
                            logger.warning(f"⚠️ Не удалось удалить файл {new_file_path}: {e}")
                    else:
                        logger.warning(f"⚠️ Файл {new_file_path} отсутствует, возможно, скачивание не произошло.")
                    new_file_path = None
            logging.info(f"Данные для юридического лица '{seller_legal}' успешно добавлены.")
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке профиля {seller_legal}: {e}", exc_info=True)
        finally:
            try:
                driver.close()
                driver.quit()
            except:
                pass
            continue

    all_data.fillna(value="", inplace=True)  # Заменяем NaN на пустые строки
    final_df = all_data.drop_duplicates()

    return final_df
