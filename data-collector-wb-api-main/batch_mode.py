from datetime import datetime, timedelta
import calendar
from app import generate_report_stream
from wildeberries_orders_7days_from_db import sync_wb_orders_7days_from_db
from context import current_mode

# -----------------------------------------------------------------------------
# Batch-функция /generate-report/
# -----------------------------------------------------------------------------
async def run_generate_report(date_from: str, date_to: str, target_file_id: str, target_list: str):
    """
    Batch-режим для произвольного диапазона, обрабатывающий все юридические лица.
    """
    print(f"\n=== Batch: generate-report ===")
    print(f"Параметры: date_from={date_from}, date_to={date_to}")

    legal_entities = ["ut", "inter", "kravchik"]
    # legal_entities = ["ut", "kravchik"]
    # legal_entities = ["inter"]
    for le in legal_entities:
        print(f"\n--- Начало обработки для {le} ---")
        async for line in generate_report_stream(date_from, date_to, le, target_file_id, target_list):
            print(line.strip())
        print(f"--- Завершена обработка для {le} ---")

    print("=== Завершено. ===\n")


# -----------------------------------------------------------------------------
# Batch-функция /generate-report-by-days/
# -----------------------------------------------------------------------------
async def run_generate_report_by_days(date_from: str, date_to: str, target_file_id: str, target_list: str):
    """
    Batch-режим для обработки по дням.
    """
    # задаем контекст для смещения даты начала получения отчетов по заказам на 4 дня раньше
    current_mode.set("days")

    print(f"\n=== Batch: generate-report-by-days ===")
    print(f"Параметры: date_from={date_from}, date_to={date_to}")

    date_from_obj = datetime.strptime(date_from, "%d.%m.%Y")
    date_to_obj = datetime.strptime(date_to, "%d.%m.%Y")

    all_dates = [
        date_from_obj + timedelta(days=i)
        for i in range((date_to_obj - date_from_obj).days + 1)
    ]
    legal_entities = ["ut", "inter", "kravchik"]
    # legal_entities = ["inter"]


    for single_date in all_dates:
        formatted_date = single_date.strftime("%d.%m.%Y")
        for le in legal_entities:
            print(f"\n--- Начало обработки для {formatted_date} и {le} ---")
            async for line in generate_report_stream(formatted_date, formatted_date, le, target_file_id, target_list):
                print(line.strip())
            print(f"--- Завершена обработка для {formatted_date} и {le} ---")

    print("\nВсе отчеты обработаны.\n")


# -----------------------------------------------------------------------------
# Batch-функция /generate-report-by-months/
# -----------------------------------------------------------------------------
async def run_generate_report_by_months(date_from: str, date_to: str, target_file_id: str, target_list: str):
    """
    Batch-режим для месячных интервалов.
    """
    print(f"\n=== Batch: generate-report-by-months ===")
    print(f"Параметры: date_from={date_from}, date_to={date_to}")

    date_from_obj = datetime.strptime(date_from, "%d.%m.%Y")
    date_to_obj = datetime.strptime(date_to, "%d.%m.%Y")

    def get_month_ranges(start_date, end_date):
        ranges = []
        current_date = start_date
        while current_date <= end_date:
            start_of_month = current_date
            _, last_day = calendar.monthrange(current_date.year, current_date.month)
            end_of_month = datetime(current_date.year, current_date.month, last_day)
            if end_of_month > end_date:
                end_of_month = end_date
            ranges.append((start_of_month, end_of_month))
            current_date = end_of_month + timedelta(days=1)
        return ranges

    month_ranges = get_month_ranges(date_from_obj, date_to_obj)
    #legal_entities = ["ut"]
    legal_entities = ["kravchik"]
    #legal_entities = ["inter"]
    for start_date, end_date in month_ranges:
        formatted_start_date = start_date.strftime("%d.%m.%Y")
        formatted_end_date = end_date.strftime("%d.%m.%Y")
        for le in legal_entities:
            print(f"\n--- Начало обработки для {formatted_start_date} - {formatted_end_date} и {le} ---")
            async for line in generate_report_stream(formatted_start_date, formatted_end_date, le, target_file_id, target_list):
                print(line.strip())
            print(f"--- Завершена обработка для {formatted_start_date} - {formatted_end_date} и {le} ---")

    print("\nВсе отчеты обработаны.\n")


# -----------------------------------------------------------------------------
# Batch-функция /generate-report-by-weeks/
# -----------------------------------------------------------------------------
async def run_generate_report_by_weeks(date_from: str, date_to: str, target_file_id: str, target_list: str):
    """
    Batch-режим для недельных интервалов.
    """
    print(f"\n=== Batch: generate-report-by-weeks ===")
    print(f"Параметры: date_from={date_from}, date_to={date_to}")

    date_from_obj = datetime.strptime(date_from, "%d.%m.%Y")
    date_to_obj = datetime.strptime(date_to, "%d.%m.%Y")

    def get_week_ranges(start_date, end_date):
        ranges = []
        monday_of_week = start_date - timedelta(days=start_date.weekday())
        sunday_of_week = monday_of_week + timedelta(days=6)
        first_interval_end = min(sunday_of_week, end_date)
        ranges.append((start_date, first_interval_end))
        current_start = first_interval_end + timedelta(days=1)
        while current_start <= end_date:
            monday = current_start - timedelta(days=current_start.weekday()) if current_start.weekday() != 0 else current_start
            sunday = monday + timedelta(days=6)
            interval_end = min(sunday, end_date)
            ranges.append((monday, interval_end))
            current_start = interval_end + timedelta(days=1)
        return ranges

    week_ranges = get_week_ranges(date_from_obj, date_to_obj)
    legal_entities = ["ut", "inter", "kravchik"]
    #legal_entities = ["kravchik"]
    for start_date, end_date in week_ranges:
        formatted_start_date = start_date.strftime("%d.%m.%Y")
        formatted_end_date = end_date.strftime("%d.%m.%Y")
        for le in legal_entities:
            print(f"\n--- Начало обработки для {formatted_start_date} - {formatted_end_date} и {le} ---")
            async for line in generate_report_stream(formatted_start_date, formatted_end_date, le, target_file_id, target_list):
                print(line.strip())
            print(f"--- Завершена обработка для {formatted_start_date} - {formatted_end_date} и {le} ---")

    print("\nВсе отчеты обработаны.\n")


# -------------------------------------------------------------------
# Обработка sync_wb_orders_7days_from_db
# -------------------------------------------------------------------
def run_sync_wb_orders(target_file_id, clear_range, start_col):
    """
    Запуск синхронизации заказов WB за 7 дней в Google Sheets.
    """
    print("Запускаем выгрузку WB заказов за 7 дней...")
    sync_wb_orders_7days_from_db(
        spreadsheet_id=target_file_id,
        clear_range=clear_range,
        start_col=start_col
    )
