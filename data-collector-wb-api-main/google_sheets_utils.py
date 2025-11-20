import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
from config_loader import load_config
from datetime import datetime
from gspread_dataframe import set_with_dataframe
import time
import functools
from gspread.exceptions import APIError
from microsoft_sheets_utils import add_data_to_excel

CONFIG = load_config()


def retry_on_quota_error(max_retries=10, delay=10):
    """
    Декоратор для повторных попыток выполнения функции в случае ошибок API (429, 5xx).

    :param max_retries: Максимальное количество повторных попыток.
    :param delay: Задержка между попытками в секундах.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            log_prefix = f"Запись в google sheet. "
            err_prefix = f"\033[91mWARNING: {log_prefix}\033[0m"

            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except APIError as e:
                    status_code = e.response.status_code
                    if status_code == 429 or (500 <= status_code < 600):  # Обрабатываем 429 и ошибки 5xx
                        print(
                            f"{err_prefix}Ошибка {status_code}: {e.response.reason}. Ожидание {delay} секунд перед повторной попыткой... (Попытка {attempt} из {max_retries})")
                        time.sleep(delay)
                    else:
                        print(f"{err_prefix}Ошибка {status_code}: {e.response.reason}. Прерываем выполнение.")
                        raise
                except Exception as e:
                    print(f"{err_prefix}Неизвестная ошибка: {str(e)}. Прерываем выполнение.")
                    raise
            raise Exception(
                f"{err_prefix}Превышено максимальное количество повторных попыток ({max_retries}) из-за ошибок.")

        return wrapper

    return decorator

def connect_to_google_sheets():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CONFIG["google_sheets"]["credentials_file"], scope)
    client = gspread.authorize(creds)
    return client

@retry_on_quota_error()
def create_spreadsheet_copy(sheet_name):
    client = connect_to_google_sheets()
    new_spreadsheet = client.copy(CONFIG["google_sheets"]["template_spreadsheet_id"], title=sheet_name)
    print("Копия создана:", f"https://docs.google.com/spreadsheets/d/{new_spreadsheet.id}/edit")
    return new_spreadsheet

@retry_on_quota_error()
def add_data_to_sheet(spreadsheet_id, data, sheet_name):
    client = connect_to_google_sheets()
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    data_list = data.values.tolist()
    sheet.insert_rows(data_list, row=2)

def add_data_to_sheet_by_dataframe_columns_count(spreadsheet_id, data, sheet_name):
    from gspread.utils import rowcol_to_a1
    client = connect_to_google_sheets()
    spreadsheet = client.open_by_key(spreadsheet_id)
    sheet = spreadsheet.worksheet(sheet_name)

    # Преобразуем DataFrame в список списков (строки)
    data_list = data.astype(str).values.tolist()
    rows_to_insert = len(data_list)
    # Количество столбцов в DataFrame
    columns_to_insert = data.shape[1]

    sheet_id = sheet.id

    # Формируем тело запроса с учётом нужного числа столбцов.
    # Используем endColumnIndex = columns_to_insert, чтобы не трогать столбцы "правее".
    body = {
        "requests": [
            {
                "insertRange": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": 1 + rows_to_insert,
                        "startColumnIndex": 0,
                        "endColumnIndex": columns_to_insert
                    },
                    "shiftDimension": "ROWS"
                }
            }
        ]
    }

    # Вызываем batch_update именно у Spreadsheet-объекта
    spreadsheet.batch_update(body)

    # Теперь заполним вставленные строки данными
    # Сформируем A1-диапазон с помощью rowcol_to_a1
    #  - Начальная ячейка: (row=2, col=1) => A2
    #  - Конечная ячейка: (row=1+rows_to_insert, col=columns_to_insert)
    #    например, если columns_to_insert=3, будет 'C(1+rows_to_insert)'
    start_cell = rowcol_to_a1(2, 1)  # A2
    end_cell = rowcol_to_a1(1 + rows_to_insert, columns_to_insert)
    update_range = f"{start_cell}:{end_cell}"

    sheet.update(update_range, data_list)

    print(f"Добавлено {rows_to_insert} строк в диапазон {update_range}.\n"
          "Все столбцы за пределами этих данных остались нетронутыми.")


@retry_on_quota_error()
def add_data_to_sheet_with_headers_and_format(spreadsheet_id, data, sheet_name, clear_range, start_col):
    """
    Добавляет данные в Google Sheets, с возможностью указания диапазона для очистки и столбца вставки.

    Аргументы:
    - spreadsheet_id: ID таблицы Google Sheets
    - data: DataFrame с данными для вставки
    - sheet_name: Название листа
    - clear_range: Диапазон для очистки
    - start_col: Столбец, с которого начинать вставку
    """
    client = connect_to_google_sheets()
    sh = client.open_by_key(spreadsheet_id)
    worksheet = sh.worksheet(sheet_name)

    rows = len(worksheet.get_all_values())
    clear_range_full = f"{clear_range}{rows}"
    worksheet.batch_clear([clear_range_full])

    data = data.copy()
    data.columns = [
        col.strftime("%d.%m.%Y") if isinstance(col, datetime) else col for col in data.columns
    ]

    for col in data.columns:
        if data[col].dtype == object:
            try:
                data[col] = (
                    data[col]
                    .astype(str)
                    .str.replace(' ', '')
                    .str.replace(' ', '')
                    .str.replace(',', '.')
                    .astype(float)
                )
            except ValueError:
                pass

    set_with_dataframe(
        worksheet,
        data,
        include_index=False,
        include_column_header=True,
        row=1,
        col=start_col
    )


@retry_on_quota_error()
def clear_range(spreadsheet_id, sheet_name, range_to_clear):
    """
    Очищает указанный диапазон на листе Google Sheets.

    :param spreadsheet_id: Идентификатор Google Sheets.
    :param sheet_name: Название вкладки для очистки.
    :param range_to_clear: Диапазон ячеек для очистки (например, 'A1:D10').
    """
    client = connect_to_google_sheets()
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

    # Очищаем указанный диапазон (без sheet_name в строке)
    sheet.batch_clear([range_to_clear])

    # print(f"Диапазон '{range_to_clear}' на листе '{sheet_name}' в Google Sheets '{spreadsheet_id}' успешно очищен.")


@retry_on_quota_error()
def add_data_to_sheet_without_headers_with_format(spreadsheet_id, data, sheet_name):
    client = connect_to_google_sheets()
    sh = client.open_by_key(spreadsheet_id)
    worksheet = sh.worksheet(sheet_name)

    data = data.copy()
    data.columns = [
        col.strftime("%d.%m.%Y") if isinstance(col, datetime) else col for col in data.columns
    ]

    for col in data.columns:
        try:
            data[col] = pd.to_datetime(
                data[col], format="%d.%m.%Y", dayfirst=True, errors='raise'
            )
            data[col] = data[col].dt.strftime("%d.%m.%Y")
        except (ValueError, TypeError):
            try:
                data[col] = (
                    data[col]
                    .astype(str)
                    .str.replace('\u00A0', '')
                    .str.replace(' ', '')
                    .str.replace(',', '.')
                    .astype(float)
                )
            except ValueError:
                pass

    str_list = list(filter(None, worksheet.col_values(1)))
    first_empty_row = len(str_list) + 1

    # Преобразуем DataFrame в JSON и выводим его в отформатированном виде
    # json_data = data.to_json(orient='records', force_ascii=False)  # Формат JSON для строк
    # parsed_json = json.loads(json_data)  # Десериализация для красивого вывода
    # print("Данные в формате JSON:")
    # print(json.dumps(parsed_json, indent=4, ensure_ascii=False))  # Красивый вывод

    data = data.fillna("")

    data_list = data.values.tolist()
    cell_range = f'A{first_empty_row}'

    worksheet.update(cell_range, data_list, value_input_option='USER_ENTERED')

    #print(f"Данные успешно добавлены в лист '{sheet_name}' начиная с строки {first_empty_row}.")

@retry_on_quota_error()
def insert_text_to_cell(spreadsheet_id, sheet_name, cell, text):
    client = connect_to_google_sheets()
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    sheet.update_acell(cell, text)

@retry_on_quota_error()
def prepare_and_insert_metadata(spreadsheet_id, sheet_name, legal_entity, date_from, date_to, marketplace):
    if legal_entity == "inter":
        entity_text = "ИНТЕР"
    elif legal_entity == "ut":
        entity_text = "АТ"
    elif legal_entity == "kravchik":
        entity_text = "КРАВЧИК"
    else:
        raise ValueError(f"Неизвестное юридическое лицо: {legal_entity}")

    insert_text_to_cell(spreadsheet_id, sheet_name, "B2", entity_text)

    date_from_dt = datetime.strptime(date_from, "%d.%m.%Y")
    date_to_dt = datetime.strptime(date_to, "%d.%m.%Y")
    if date_from_dt == date_to_dt:
        date_text = date_from_dt.strftime("%d.%m.%Y")
    else:
        date_text = f"{date_from_dt.strftime('%d.%m.%Y')}-{date_to_dt.strftime('%d.%m.%Y')}"
    insert_text_to_cell(spreadsheet_id, sheet_name, "E2", date_text)
    insert_text_to_cell(spreadsheet_id, sheet_name, "A2", marketplace)
    insert_text_to_cell(spreadsheet_id, sheet_name, "C2", f"{date_from_dt.strftime('%d.%m.%Y')}")
    insert_text_to_cell(spreadsheet_id, sheet_name, "D2", f"{date_to_dt.strftime('%d.%m.%Y')}")

@retry_on_quota_error()
def get_sheet_data(spreadsheet_id, sheet_name):
    client = connect_to_google_sheets()
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    data = sheet.get_all_values()

    if not data or len(data) < 2:
        raise Exception("Таблица пуста или данные отсутствуют.")

    df = pd.DataFrame(data[1:], columns=data[0])
    return df

@retry_on_quota_error()
def clear_sheet(spreadsheet_id, sheet_name):
    """
    Очищает все данные на указанном листе Google Sheets, кроме первой строки.

    :param spreadsheet_id: Идентификатор Google Sheets.
    :param sheet_name: Название вкладки для очистки.
    """
    client = connect_to_google_sheets()
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    # Получаем количество строк и столбцов
    rows = len(sheet.get_all_values())
    cols = len(sheet.row_values(1))
    if rows > 1:
        range_to_clear = f'A2:{chr(64 + cols)}{rows}'
        sheet.batch_clear([range_to_clear])
    #print(f"Лист '{sheet_name}' в Google Sheets '{spreadsheet_id}' был успешно очищен, кроме первой строки.")



@retry_on_quota_error()
def get_sheet_data_columns(spreadsheet_id, sheet_name, columns):
    """
    Получает данные из определённых столбцов Google Sheets.

    :param spreadsheet_id: Идентификатор Google Sheets.
    :param sheet_name: Название вкладки.
    :param columns: Список столбцов, которые нужно получить.
    :return: DataFrame с данными из указанных столбцов.
    """
    client = connect_to_google_sheets()
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    data = sheet.get_all_values()

    if not data or len(data) < 2:
        raise Exception("Таблица пуста или данные отсутствуют.")

    df = pd.DataFrame(data[1:], columns=data[0])
    return df[columns]

@retry_on_quota_error()
def append_data_between_spreadsheets(
    source_spreadsheet_id,
    source_sheet_name,
    source_range,
    target_spreadsheet_id,
    target_sheet_name
):
    client = connect_to_google_sheets()

    source_spreadsheet = client.open_by_key(source_spreadsheet_id)
    source_sheet = source_spreadsheet.worksheet(source_sheet_name)

    data_to_copy = source_sheet.get(source_range)

    if not data_to_copy:
        #print("Нет данных для копирования.")
        return

    target_spreadsheet = client.open_by_key(target_spreadsheet_id)
    target_sheet = target_spreadsheet.worksheet(target_sheet_name)

    target_sheet.append_rows(data_to_copy, value_input_option='USER_ENTERED')
    #print("Данные успешно скопированы и добавлены в целевую таблицу.")


@retry_on_quota_error()
def append_data_from_google_to_microsoft_excel(
        source_spreadsheet_id,
        source_sheet_name,
        source_range,
        target_file_id,
        target_sheet_name
):
    """
    Переносит данные из Google Sheets в Microsoft Excel.
    """
    client = connect_to_google_sheets()

    # Читаем данные из Google Sheets
    source_spreadsheet = client.open_by_key(source_spreadsheet_id)
    source_sheet = source_spreadsheet.worksheet(source_sheet_name)
    data_to_copy = source_sheet.get(source_range)

    if not data_to_copy:
        print("Нет данных для копирования.")
        return

    print(f"Получено {len(data_to_copy)} строк из Google Sheets.")

    # Проверка, есть ли хотя бы одна строка с заголовками и данные
    if len(data_to_copy) < 2:
        print("Недостаточно данных для создания DataFrame (нужны заголовки и хотя бы одна строка данных).")
        return

    # Преобразуем данные в DataFrame
    try:
        df = pd.DataFrame(data_to_copy[1:], columns=data_to_copy[0])  # Первая строка — заголовки
        print(f"Данные преобразованы в DataFrame: {len(df)} строк, {len(df.columns)} столбцов.")
    except Exception as e:
        print(f"Ошибка при создании DataFrame: {e}")
        return

    # Вывод первых нескольких строк для отладки
    print("Первые 5 строк DataFrame:")
    print(df.head())

    # Удаляем полностью пустые строки
    df.dropna(how="all", inplace=True)
    if df.empty:
        print("Все строки после очистки оказались пустыми. Операция прервана.")
        return
    print(f"Данные после удаления пустых строк: {len(df)} строк.")

    # Функция для определения и преобразования столбца
    def convert_column(column):
        """
        Преобразует столбец:
        - Заменяет запятые на точки.
        - Удаляет символы '%'.
        - Удаляет любые другие символы кроме цифр, точек и минусов.
        - Преобразует в float, если возможно.
        - Преобразует в datetime, если это дата.
        """
        original = df[column].copy()

        # Преобразуем все значения к строковому типу
        converted = original.astype(str).str.replace(',', '.', regex=False).str.strip()

        # Удаляем символы '%'
        converted = converted.str.replace('%', '', regex=False).str.strip()

        # Удаляем любые другие символы кроме цифр, точек и минусов
        converted = converted.str.replace(r'[^\d\.-]', '', regex=True)

        # Попытка преобразования в float
        numeric = pd.to_numeric(converted, errors='coerce')
        non_na_ratio_numeric = numeric.notna().mean()

        if non_na_ratio_numeric > 0.8:  # Если более 80% значений успешно преобразованы
            # Проверяем, содержат ли исходные данные символ '%'
            if original.astype(str).str.contains('%').any():
                numeric = numeric / 100  # Преобразуем проценты в дробные числа
            df[column] = numeric
            #print(f"Столбец '{column}' успешно преобразован в float.")
            return

        # Если не удалось преобразовать в float, пытаемся преобразовать в дату
        # Попытка определить, является ли столбец датой
        try:
            # Попытка распознать дату
            parsed_dates = pd.to_datetime(original, dayfirst=True, errors='coerce', format='%d.%m.%Y')
            non_na_ratio_dates = parsed_dates.notna().mean()
            if non_na_ratio_dates > 0.8:  # Если более 80% значений успешно распознаны как даты
                df[column] = parsed_dates.dt.strftime('%m/%d/%Y')  # Преобразуем в строку в нужном формате
                #print(f"Столбец '{column}' успешно преобразован в формат даты dd.mm.yyyy.")
                return
        except Exception as e:
            pass

        # Если не удалось ни преобразовать в float, ни в дату, оставляем как есть
        #print(f"Столбец '{column}' не является числовым или датой и оставлен как строковый.")

    # Обрабатываем каждый столбец
    for column in df.columns:
        convert_column(column)

    print("Преобразование чисел с запятыми и дат выполнено.")

    # Вывод типов данных для проверки
    #print("Типы данных столбцов после преобразования:")
    #print(df.dtypes)

    # Добавляем данные в Microsoft Excel
    try:
        add_data_to_excel(target_file_id, target_sheet_name, df)
        print("Данные успешно добавлены в Microsoft Excel.")
    except Exception as e:
        print(f"Ошибка при добавлении данных в Microsoft Excel: {e}")

@retry_on_quota_error()
def remove_rows_by_conditions(
    spreadsheet_id: str,
    sheet_name: str,
    date_str: str,
    marketplace: str,
    seller_legal: str
):
    """
    Удаляет все строки, где:
      - в столбце E (5-й) == e_value
      - в столбце F (6-й) == f_value
      - в столбце G (7-й) == date_str
    через batch_update -> DeleteDimensionRequest.


    Это физически удаляет строки, а не просто очищает их содержимое.
    """

    if seller_legal == "ut":
        seller_legal = "АТ"
    if seller_legal == "inter":
        seller_legal = "ИНТЕР"
    if seller_legal == "kravchik":
        seller_legal = "КРАВЧИК"

    client = connect_to_google_sheets()
    spreadsheet = client.open_by_key(spreadsheet_id)

    # Найдём sheetId по названию листа
    sheet_id = None
    sheets_metadata = spreadsheet.fetch_sheet_metadata().get("sheets", [])
    for s in sheets_metadata:
        if s["properties"]["title"] == sheet_name:
            sheet_id = s["properties"]["sheetId"]
            break

    if sheet_id is None:
        raise ValueError(f"Не найден лист с именем '{sheet_name}' в таблице {spreadsheet_id}")

    # Читаем все данные листа
    sheet = spreadsheet.worksheet(sheet_name)
    all_values = sheet.get_all_values()

    # Если лист пуст или там только заголовок
    if len(all_values) <= 1:
        print("Лист пуст или содержит только заголовок, удалять нечего.")
        return

    # Список индексов строк (в терминах API), которые хотим удалить
    # all_values[0] -- заголовок (API-индекс 0)
    # all_values[1] -- первая строка данных (API-индекс 1) и т.д.
    data_rows = all_values[1:]  # строки с данными
    rows_to_delete = []

    for i, row in enumerate(data_rows, start=1):
        # Проверяем, что есть хотя бы 7 столбцов (E=4, F=5, G=6)
        if len(row) >= 7:
            if row[4] == marketplace and row[5] == seller_legal and row[6] == date_str:
                rows_to_delete.append(i)  # i уже совпадает с 0-based индексом в Sheets API

    if not rows_to_delete:
        print("Нет строк, удовлетворяющих заданным условиям. Удалять нечего.")
        return

    # Чтобы при удалении строк сверху не смещались индексы нижних строк, удаляем «снизу вверх».
    # Для этого отсортируем индексы в убывающем порядке.
    rows_to_delete.sort(reverse=True)

    # Можно либо делать один запрос на каждую группу подряд идущих строк,
    # либо вообще на каждую строку отдельно. Предпочтительнее группировать.
    requests = []

    # Группируем идущие подряд строки в интервалы
    start_idx = None
    prev_idx = None

    for idx in rows_to_delete:
        if start_idx is None:
            # Начало нового диапазона
            start_idx = idx
            prev_idx = idx
        else:
            if idx == prev_idx - 1:
                # Продолжаем диапазон подряд
                prev_idx = idx
            else:
                # Закрываем предыдущий диапазон (prev_idx..start_idx)
                requests.append({
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": prev_idx,
                            "endIndex": start_idx + 1  # endIndex не включительно
                        }
                    }
                })
                # Начинаем новый
                start_idx = idx
                prev_idx = idx

    # Добавим последний (или единственный) диапазон
    requests.append({
        "deleteDimension": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "ROWS",
                "startIndex": prev_idx,
                "endIndex": start_idx + 1
            }
        }
    })

    # Отправляем все удаления одним batch_update
    body = {"requests": requests}
    spreadsheet.batch_update(body)

    print(
        f"Удалено {len(rows_to_delete)} строк, удовлетворяющих условиям: "
        f"E='{marketplace}', F='{seller_legal}', G='{date_str}'."
    )

if __name__ == "__main__":
    # # Обязательно надо копировать с заголовком таблицы! (указать в диапазоне)
    # SOURCE_SPREADSHEET_ID = "1tGdt-CYJIRzvCnj0aI_4AAeFLwrVkmmxcU-_tMKKVXM"  # ID Google Таблицы
    # SOURCE_SHEET_NAME = "toMicrosoft"  # Лист Google Таблицы
    # SOURCE_RANGE = "A1:BE"  # Диапазон данных
    #
    # TARGET_FILE_ID = "014T7RYLNSIF432ZA4ARCIW2KZXSNRKZU3"  # ID файла Microsoft Excel
    # TARGET_SHEET_NAME = "Исходные для недельного"  # Лист Microsoft Excel
    #
    # append_data_from_google_to_microsoft_excel(
    #     SOURCE_SPREADSHEET_ID,
    #     SOURCE_SHEET_NAME,
    #     SOURCE_RANGE,
    #     TARGET_FILE_ID,
    #     TARGET_SHEET_NAME
    # )

    spreadsheet_id = "1jfIG5kKw8nr1xvK4rY2nplcLjes2DaNit16wh7zdGnw"
    sheet_name = "Данные для сводной"
    target_date = "02.01.2025"
    target_value_e = "Wildberries"
    target_value_f = "КРАВЧИК"

    remove_rows_by_conditions(spreadsheet_id, sheet_name, target_date, target_value_e, target_value_f)