import pandas as pd
from google_sheets_utils import get_sheet_data, add_data_to_sheet_with_headers_and_format
from config_loader import load_config
from datetime import datetime

CONFIG = load_config()

def transfer_selfcost_data(date_from: str, date_to: str, spreadsheet_id: str):
    """
    Перенос данных между Google Sheets с фильтрацией по дате.

    :param date_from: Дата начала выборки в формате DD.MM.YYYY.
    :param date_to: Дата окончания выборки в формате DD.MM.YYYY.
    :param spreadsheet_id: Идентификатор целевой Google Sheets.
    """
    # Проверка и преобразование дат
    try:
        formatted_date_from = datetime.strptime(date_from, "%d.%m.%Y")
        formatted_date_to = datetime.strptime(date_to, "%d.%m.%Y")
    except ValueError as e:
        raise Exception(f"Неверный формат даты. Ожидается DD.MM.YYYY. {str(e)}")

    # Получаем ID исходной таблицы из конфига
    source_spreadsheet_id = CONFIG["google_sheets"]["selfcost_spreadsheet_id"]

    # Загружаем данные из исходной таблицы
    df = get_sheet_data(source_spreadsheet_id, "Себестоимость")  # Укажите имя вкладки

    # Проверяем, что таблица содержит данные
    if df.empty:
        raise Exception("Исходная таблица пуста.")

    # Создаём DataFrame с обязательными столбцами
    result_df = df.iloc[:, :15].copy()  # Первые три столбца всегда остаются

    # Фильтруем столбцы по дате из названия столбца
    for col in df.columns[15:]:  # Пропускаем первые три столбца
        try:
            col_name = col.strip()
            #print(f"Проверяем столбец '{col_name}'")

            # Пробуем распарсить название столбца как дату
            try:
                col_date = datetime.strptime(col_name, "%d.%m.%Y")
            except ValueError:
                #print(f"Пропускаем столбец '{col_name}', название столбца не является датой.")
                continue

            # Проверяем, входит ли дата в указанный диапазон
            if formatted_date_from <= col_date <= formatted_date_to:
                print(f"Столбец '{col_name}' добавляется, дата {col_date.strftime('%d.%m.%Y')} входит в диапазон.")
                result_df[col] = df[col]  # Добавляем столбец, если дата попадает в диапазон
            #else:
                #print(f"Столбец '{col_name}' не добавляется, дата {col_date.strftime('%d.%m.%Y')} вне диапазона.")
        except Exception as e:
            # Логируем ошибки при обработке столбца
            print(f"Ошибка при обработке столбца '{col_name}': {e}")
            continue

    # Добавляем данные в целевую таблицу, включая заголовки столбцов
    add_data_to_sheet_with_headers_and_format(spreadsheet_id, result_df, "SelfCost(ext)")
    print(f"Данные перенесены на вкладку SelfCost(ext) в таблице с ID: {spreadsheet_id}.")


if __name__ == "__main__":
    def test_transfer_selfcost_data():
        try:
            transfer_selfcost_data("01.12.2024", "05.12.2024", "1Gjx1UFXW0nZPUiDIrzNedBiJad1B__YQmTjH0uTSnws")
            print("Данные успешно перенесены.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    test_transfer_selfcost_data()