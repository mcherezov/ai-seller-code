import requests
import json
import pandas as pd
from urllib.parse import urlencode
from config_loader import load_config
import os
import logging
import sys
import time

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]  # Только вывод в консоль
)

CONFIG = load_config()
CLIENT_ID = CONFIG["microsoft"]["client_id"]
CLIENT_SECRET = CONFIG["microsoft"]["client_secret"]
TENANT_ID = "common"
REDIRECT_URI = "http://localhost"
SCOPES = "offline_access Files.ReadWrite.All"

TOKEN_FILE = "token.json"

# --- Функции работы с токенами ---
def save_token(tokens):
    """Сохраняет токены в файл."""
    with open(TOKEN_FILE, "w") as f:
        json.dump(tokens, f)
    logging.info("Токены сохранены в файл.")

def load_token():
    """Загружает токены из файла."""
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            return json.load(f)
    return None

def get_new_tokens():
    """Получает новый токен через браузерную авторизацию."""
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "response_mode": "query",
        "scope": SCOPES,
        "state": "12345"
    }
    auth_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/authorize?{urlencode(params)}"
    logging.info(f"Перейди по ссылке и авторизуйся для получения кода:\n{auth_url}")
    auth_code = input("Введи код авторизации из URL: ").strip()

    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    payload = {
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": auth_code,
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES
    }

    response = requests.post(url, data=payload)
    log_response(response, "Получение нового токена")
    response.raise_for_status()
    tokens = response.json()
    save_token(tokens)
    return tokens

def refresh_access_token(refresh_token):
    """Обновляет токен доступа с использованием refresh token."""
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    payload = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
        "scope": SCOPES
    }

    response = requests.post(url, data=payload)
    log_response(response, "Обновление access токена")
    response.raise_for_status()
    tokens = response.json()
    save_token(tokens)
    return tokens

def get_access_token():
    """Возвращает действующий access token."""
    tokens = load_token()
    if tokens and "refresh_token" in tokens:
        try:
            logging.info("Обновляем access token...")
            new_tokens = refresh_access_token(tokens["refresh_token"])
            return new_tokens["access_token"]
        except Exception as e:
            logging.error(f"Ошибка при обновлении токена: {e}")
    logging.info("Запрашиваем новый токен...")
    tokens = get_new_tokens()
    return tokens["access_token"]

# --- Логирование HTTP-ответов ---
def log_response(response, action_description):
    """Логирует статус и тело ответа от сервера."""
    logging.info(f"{action_description}: Статус-код {response.status_code}")
    # if response.content:
    #     logging.info(f"Ответ сервера: {response.text}")

# --- Функции для работы с файлами ---
def list_all_files_on_drive():
    """Получает список всех файлов на OneDrive пользователя."""
    access_token = get_access_token()
    url = "https://graph.microsoft.com/v1.0/me/drive/root/children"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers)
    log_response(response, "Получение списка файлов")
    response.raise_for_status()
    files = response.json().get("value", [])

    logging.info("\nСписок файлов на OneDrive:")
    for file in files:
        file_type = "Folder" if "folder" in file else "File"
        logging.info(f"Имя: {file['name']}, ID: {file['id']}, Тип: {file_type}")
    return files

def create_table(file_id, worksheet_id):
    """Создаёт таблицу на листе, если её нет."""
    access_token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/me/drive/items/{file_id}/workbook/worksheets/{worksheet_id}/tables/add"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    body = {"address": "A1:D1", "hasHeaders": True}

    response = requests.post(url, headers=headers, json=body)
    log_response(response, "Создание таблицы")
    response.raise_for_status()
    table = response.json()
    logging.info(f"Создана таблица с именем: {table['name']}, ID: {table['id']}")
    return table["id"]

def get_table_columns(file_id, worksheet_id, table_id):
    """Получает список заголовков столбцов таблицы."""
    access_token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/me/drive/items/{file_id}/workbook/worksheets/{worksheet_id}/tables/{table_id}/columns"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers)
    log_response(response, "Получение столбцов таблицы")
    response.raise_for_status()

    columns = response.json().get("value", [])
    column_names = [col["name"] for col in columns]
    logging.info(f"Столбцы таблицы: {column_names}")
    return column_names


def add_data_to_excel(file_id, worksheet_name, data):
    """
    Добавляет данные в Excel Online, корректируя размер данных под таблицу.
    Обрабатывает ошибки 429 (Too Many Requests) и 503 (Service Unavailable) с повтором запросов.
    """
    access_token = get_access_token()
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    def get_valid_access_token():
        """Обновляет access_token при 401 ошибке."""
        nonlocal access_token
        logging.info("Обновляем токен доступа из-за ошибки 401...")
        access_token = get_access_token()
        headers["Authorization"] = f"Bearer {access_token}"

    def wait_on_throttle(response, attempt):
        """Обрабатывает ошибки 429 и 503 с экспоненциальной задержкой."""
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            wait_time = int(retry_after)
        else:
            wait_time = min(2 ** attempt, 60)  # Экспоненциальная задержка с максимумом 60 секунд
        logging.warning(f"Ошибка {response.status_code}. Ждём {wait_time} секунд перед повтором...")
        time.sleep(wait_time)

    try:
        # Получаем ID листа
        url = f"https://graph.microsoft.com/v1.0/me/drive/items/{file_id}/workbook/worksheets"
        response = requests.get(url, headers=headers)
        log_response(response, "Получение ID листа")
        if response.status_code == 401:
            get_valid_access_token()
            response = requests.get(url, headers=headers)

        response.raise_for_status()
        worksheets = response.json().get("value", [])
        worksheet = next((sheet for sheet in worksheets if sheet["name"] == worksheet_name), None)
        if not worksheet:
            raise Exception(f"Лист '{worksheet_name}' не найден.")
        worksheet_id = worksheet["id"]

        # Получаем или создаём таблицу
        table_url = f"https://graph.microsoft.com/v1.0/me/drive/items/{file_id}/workbook/worksheets/{worksheet_id}/tables"
        response = requests.get(table_url, headers=headers)
        log_response(response, "Получение таблиц")
        if response.status_code == 401:
            get_valid_access_token()
            response = requests.get(table_url, headers=headers)

        response.raise_for_status()
        tables = response.json().get("value", [])

        if not tables:
            logging.info("Таблицы не найдены. Создаём новую таблицу...")
            table_id = create_table(file_id, worksheet_id)
        else:
            table_id = tables[0]["id"]
            logging.info(f"Используем существующую таблицу с ID: {table_id}")

        # Получаем структуру таблицы
        table_columns = get_table_columns(file_id, worksheet_id, table_id)
        num_columns = len(table_columns)

        # Корректируем данные
        if len(data.columns) != num_columns:
            logging.warning("Количество столбцов данных не совпадает с таблицей. Корректируем...")
            if len(data.columns) > num_columns:
                data = data.iloc[:, :num_columns]
            else:
                for i in range(num_columns - len(data.columns)):
                    data[f"Дополнительно_{i+1}"] = ""
        rows = data.fillna("").values.tolist()

        # Разбиваем данные на батчи по 1000 строк
        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            add_row_url = f"{table_url}/{table_id}/rows/add"
            body = {"values": batch}
            attempt = 1

            while True:
                response = requests.post(add_row_url, headers=headers, json=body)
                #log_response(response, "Добавление данных в таблицу")

                if response.status_code in (429, 503):  # Throttling или временная недоступность
                    wait_on_throttle(response, attempt)
                    attempt += 1
                elif response.status_code == 401:  # Неавторизованный доступ
                    get_valid_access_token()
                else:
                    response.raise_for_status()
                    break  # Запрос успешен

            logging.info(f"Добавлено {len(batch)} строк в таблицу.")

        logging.info("Все данные успешно добавлены в Excel!")

    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка HTTP при работе с Microsoft Excel API: {e}")
    except Exception as e:
        logging.error(f"Произошла ошибка: {e}")
        raise


if __name__ == "__main__":
    try:
        files = list_all_files_on_drive()
        FILE_NAME = "!Shared Сводная таблица.xlsx"
        file = next((f for f in files if f["name"] == FILE_NAME), None)
        if not file:
            raise Exception(f"Файл '{FILE_NAME}' не найден на диске.")

        file_id = file["id"]
        logging.info(f"ID выбранного файла: {file_id}")

        SHEET_NAME = "Sheet1"
        df = pd.DataFrame({
            "Дата": ["2024-06-01", "2024-06-02"],
            "Продажи": [100, 150],
            "Позиция": ["Первая", "Вторая"]
        })
        add_data_to_excel(file_id, SHEET_NAME, df)

    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")