import argparse
import sys
import asyncio

# Импорт batch-функций
from batch_mode import (
    run_generate_report,
    run_generate_report_by_days,
    run_generate_report_by_months,
    run_generate_report_by_weeks,
    run_sync_wb_orders
)

def main():
    """
    Основная точка входа для запуска batch-режимов.
    """
    parser = argparse.ArgumentParser(description="Скрипт для batch-генерации отчётов")
    parser.add_argument("--generate-report", action="store_true", help="Batch: /generate-report (весь диапазон).")
    parser.add_argument("--generate-report-by-days", action="store_true", help="Batch: /generate-report-by-days.")
    parser.add_argument("--generate-report-by-weeks", action="store_true", help="Batch: /generate-report-by-weeks.")
    parser.add_argument("--generate-report-by-months", action="store_true", help="Batch: /generate-report-by-months.")
    parser.add_argument("--sync-wb-orders-7days", action="store_true", help="Запуск выгрузки заказов WB за 7 дней.")

    parser.add_argument("--date-from", type=str, default=None, help="Начальная дата (дд.мм.гггг)")
    parser.add_argument("--date-to", type=str, default=None, help="Конечная дата (дд.мм.гггг)")
    parser.add_argument("--target_file_id", type=str, default=None, help="ID файла для загрузки результата")
    parser.add_argument("--target_list", type=str, default=None, help="Имя вкладки в файле с результатом")
    parser.add_argument("--clear_range", type=str, default=None, help="Интервал вставки данных в листе (напр. А1:N)")
    parser.add_argument("--start_col", type=int, default=None, help="Стартовый столбец вставки (напр. 1")

    args = parser.parse_args()

    if args.generate_report:
        if not args.date_from or not args.date_to:
            print("Ошибка: для --generate-report нужны --date-from и --date-to!")
            sys.exit(1)
        asyncio.run(run_generate_report(args.date_from, args.date_to, args.target_file_id, args.target_list))
        return

    if args.generate_report_by_days:
        if not args.date_from or not args.date_to:
            print("Ошибка: для --generate-report-by-days нужны --date-from и --date-to!")
            sys.exit(1)
        asyncio.run(run_generate_report_by_days(args.date_from, args.date_to, args.target_file_id, args.target_list))
        return

    if args.generate_report_by_weeks:
        if not args.date_from or not args.date_to:
            print("Ошибка: для --generate-report-by-weeks нужны --date-from и --date-to!")
            sys.exit(1)
        asyncio.run(run_generate_report_by_weeks(args.date_from, args.date_to, args.target_file_id, args.target_list))
        return

    if args.generate_report_by_months:
        if not args.date_from or not args.date_to:
            print("Ошибка: для --generate-report-by-months нужны --date-from и --date-to!")
            sys.exit(1)
        asyncio.run(run_generate_report_by_months(args.date_from, args.date_to, args.target_file_id, args.target_list))
        return

    if args.sync_wb_orders_7days:
        if not args.target_file_id:
            print("Ошибка: --sync-wb-orders-7days требует --target_file_id!")
            sys.exit(1)
        run_sync_wb_orders(args.target_file_id, args.clear_range, args.start_col)
        return

    parser.print_help()

if __name__ == "__main__":
    main()