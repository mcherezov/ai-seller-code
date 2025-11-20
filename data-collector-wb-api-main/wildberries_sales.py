from time import sleep
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import base64
import zipfile
import io
import pandas as pd
from google_sheets_utils import add_data_to_sheet, add_data_to_sheet_by_dataframe_columns_count, add_data_to_sheet_without_headers_with_format
from datetime import datetime, timedelta
import json
from config_loader import load_config
from context import current_mode

CONFIG = load_config()


def process_wb_reports(date_from: str, date_to: str, seller_legal: str, spreadsheet_id: str):
    """
    Скачивает отчеты Wildberries для указанного юридического лица
    и добавляет данные в указанный Google Sheets документ.
    """
    chrome_options = Options()
    # Настройки профилей браузера из конфигурации
    profile_config = CONFIG["chrome_profiles"][seller_legal]
    chrome_options.add_argument(f"user-data-dir={profile_config['user_data_dir']}")
    chrome_options.add_argument(f"--profile-directory={profile_config['profile_directory']}")

    # Путь для скачивания отчетов
    download_url_part = CONFIG["wildberries"]["report_paths"][seller_legal]


    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--no-default-browser-check")

    # Настройка драйвера
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(CONFIG["wildberries"]["url"])
    sleep(1)

    # Ввод параметров (дат)

    # если делаем отчеты по дням, то надо вычесть 4 дня из даты начала (отчеты за 4 дня нужны)
    mode = current_mode.get()

    if mode == "days":
        # Преобразуем строку в объект datetime
        date_from = datetime.strptime(date_from, "%d.%m.%Y")

        # Вычитаем 4 дня
        date_from -= timedelta(days=4)

        # Преобразуем обратно в строку
        date_from = date_from.strftime("%d.%m.%Y")

    WebDriverWait(driver, 60).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="app-content-id"]/div[1]/div/div/div/div[3]/div/div/div[1]/div/div[1]/div[1]/div/div/div[2]/button'))
    ).click()
    sleep(1)

    start_date_field = WebDriverWait(driver, 60).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="startDate"]'))
    )
    start_date_field.clear()
    start_date_field.send_keys(date_from)

    end_date_field = WebDriverWait(driver, 60).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="endDate"]'))
    )
    end_date_field.clear()
    end_date_field.send_keys(date_to)

    WebDriverWait(driver, 60).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="Portal-@wildberries/lazy-calendar"]/div/div/form/div[3]/div[2]/button/span'))
    ).click()

    # Ожидание загрузки таблицы результатов
    WebDriverWait(driver, 3).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="app-content-id"]/div[1]/div/div/div/div[3]/div/div/div[2]'))
    )
    sleep(3)

    # Проверка на кнопку "Load more"
    while True:
        try:
            load_more_button = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, '//button[span[text()="Load more"]]'))
            )
            load_more_button.click()
            #sleep(3)
        except TimeoutException:
            print("Кнопка 'Load more' больше не отображается.")
            break

    # Сбор отчетов
    report_num_arr = []
    row_index = 1
    while True:
        try:
            report_no_xpath = f'//*[@id="app-content-id"]/div[1]/div/div/div/div[3]/div/div/div[2]/div/div/div/div[2]/div[{row_index}]/div[2]/span'
            report_no_element = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, report_no_xpath))
            )
            report_no = report_no_element.text
            #download_link = f'https://seller-services.wildberries.ru/ns/reports/{download_url_part}/api/v1/reports/{report_no}/details/archived-excel'
            report_num_arr.append(report_no)
            row_index += 1
        except TimeoutException:
            print("Все доступные номера отчетов для скачивания собраны.")
            break

    all_data = pd.DataFrame()  # Создаем пустой DataFrame для сбора всех данных

    def get_dataframe_from_page(driver):
        """Парсит страницу, извлекает base64, читает zip, достает xlsx и возвращает DataFrame."""
        page_content = driver.find_element(By.TAG_NAME, "body").text
        data = json.loads(page_content)
        file_content_base64 = data['data']['file']
        file_content = base64.b64decode(file_content_base64)

        with zipfile.ZipFile(io.BytesIO(file_content)) as zip_file:
            file_list = zip_file.namelist()
            #print("Содержимое ZIP-архива:", file_list)

            xlsx_name = file_list[0]
            #print(f"Имя файла в zip-архиве: {xlsx_name}")

            with zip_file.open(xlsx_name) as xlsx_file:
                # Считываем DataFrame напрямую из потока, без сохранения на диск
                df = pd.read_excel(xlsx_file, engine='openpyxl').fillna("")
                print(f"DataFrame успешно создан. Размеры: {df.shape}")
                #print("Первые 5 строк DataFrame:")
                #print(df.head())
                return df, xlsx_name

    for rep in report_num_arr:
        # Открываем ссылку в новой вкладке
        driver.execute_script("window.open(arguments[0], '_blank');",
                              f'https://seller-services.wildberries.ru/ns/reports/{download_url_part}/api/v1/reports/{rep}/details/archived-excel')
        driver.switch_to.window(driver.window_handles[-1])
        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        #print(f"Содержимое страницы:\n{driver.find_element(By.TAG_NAME, 'body').text[:500]}...")

        try:
            #print("JSON успешно распарсен.")
            df, xlsx_name = get_dataframe_from_page(driver)

            # Проверяем, не пуст ли DataFrame
            if not df.empty:
                all_data = pd.concat([all_data, df], ignore_index=True)
                print(f"Данные из файла '{xlsx_name}' добавлены в общий DataFrame.")
            else:
                #print(f"Файл '{xlsx_name}' пуст, попробуем другую ссылку.")

                # Меняем ссылку на другую
                new_link = f'https://seller-services.wildberries.ru/ns/reports/seller-wb-balance/api/v1/reports/{rep}/details/archived-excel'
                print(f"Изменённая ссылка: {new_link}")

                # Закрываем текущую вкладку
                driver.close()
                # Переключаемся на предыдущую вкладку
                driver.switch_to.window(driver.window_handles[-1])

                # Открываем новую вкладку с изменённой ссылкой
                driver.execute_script("window.open(arguments[0], '_blank');", new_link)
                driver.switch_to.window(driver.window_handles[-1])
                WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

                # Повторно пытаемся получить DataFrame
                df, xlsx_name = get_dataframe_from_page(driver)

                if not df.empty:
                    all_data = pd.concat([all_data, df], ignore_index=True)
                    #print(f"Данные из файла '{xlsx_name}' добавлены в общий DataFrame после изменения ссылки.")
                else:
                    print(f"Даже после смены ссылки файл '{xlsx_name}' остался пустым.")

        except json.JSONDecodeError:
            print("Не удалось распарсить содержимое страницы как JSON.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")
        except json.JSONDecodeError:
            print("Не удалось распарсить содержимое как JSON.")
        except zipfile.BadZipFile:
            print("Файл не является корректным zip-архивом.")
        except Exception as e:
            print(f"Ошибка при обработке файла: {e}")

        # Закрываем вкладку и возвращаемся к предыдущей
        driver.close()
        driver.switch_to.window(driver.window_handles[0])

    driver.quit()

    # Записываем собранные данные в файл

    all_data.fillna(value="", inplace=True)  # Заменяем NaN на пустые строки


    if mode == "days":
        # Преобразуем date_to в формат yyyy-mm-dd
        date_to_formatted = datetime.strptime(date_to, "%d.%m.%Y").strftime("%Y-%m-%d")

        # Создаём копию DataFrame с нужными строками
        filtered_data = all_data[all_data["Дата продажи"] == date_to_formatted].copy()

        if not all_data.empty:
            add_data_to_sheet_without_headers_with_format(spreadsheet_id, all_data, "SalesReport(ext) - 4 days")
            print("Продажи (4 дня) успешно добавлены в Google Sheets.")
        else:
            print("Продажи (4 дня): Нет данных для записи в Google Sheets.")

        if not filtered_data.empty:
            add_data_to_sheet_without_headers_with_format(spreadsheet_id, filtered_data, "SalesReport(ext)")
            print("Продажи: Все данные успешно добавлены в Google Sheets.")
        else:
            print("Продажи: Нет данных для записи в Google Sheets.")
    else:
        if not all_data.empty:
            add_data_to_sheet_without_headers_with_format(spreadsheet_id, all_data, "SalesReport(ext)")
            print("Все данные успешно добавлены в Google Sheets.")
        else:
            print("Нет данных для записи в Google Sheets.")

    print(f"Данные для юридического лица '{seller_legal}' успешно добавлены.")


if __name__ == "__main__":
    def test_process_wb_reports():
        try:
            process_wb_reports("14.01.2025", "14.01.2025", "ut", "16O51ACdKhs0HxAnu1AaVCvxguoYban2D8WEzL92toGY")
            print("Тест успешно завершен.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")


    test_process_wb_reports()

    # import shutil
    #
    # chromedriver_path = shutil.which("chromedriver")
    # print(chromedriver_path)