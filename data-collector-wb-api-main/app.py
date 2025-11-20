import asyncio
from google_sheets_utils import create_spreadsheet_copy, prepare_and_insert_metadata, remove_rows_by_conditions
from google_sheets_utils import append_data_between_spreadsheets
from wildberries_sales_from_DB import process_wb_reports
from wildberries_orders import fetch_orders
from wildberries_paid_receiving import fetch_acceptance_report
from wildberries_supplies import fetch_supplies_report
from wildberries_commission import fetch_commission_data
from wildberries_ad_campaign import fetch_ad_campaigns
from wildberries_paid_storage import fetch_paid_storage
from wildberries_stocks import fetch_stocks
from selfcost import transfer_selfcost_data
from wildberries_orders_7days_before import fetch_orders_7_days
from wildberries_funnel import fetch_sales_funnel
from context import current_mode
from wildberries_orders_cleared_by_funnel import funnel_and_orders_pipeline


# --------------------------------------------------------------------------------------
# Функция generate_report_stream, которая генерирует поток данных для обработки отчетов
# --------------------------------------------------------------------------------------
async def generate_report_stream(date_from: str, date_to: str, legal_entity: str, target_file_id: str, target_list_name: str):
    """
    Генерация потоков сообщений для обработки отчетов.
    """
    yield "data: Стартуем обработку отчета\n\n"
    spreadsheet_urls = {}

    for seller_legal in [legal_entity]:
        yield f"data: Создаём копию шаблона для {seller_legal}\n\n"
        spreadsheet = create_spreadsheet_copy(f"WB report {seller_legal} from {date_from} to {date_to}")
        spreadsheet_id = spreadsheet.id

        yield f"data: Добавляем системные данные для {seller_legal}\n\n"
        prepare_and_insert_metadata(spreadsheet_id, "Config(ext)", seller_legal, date_from, date_to, "Wildberries")

        yield f"data: Выполняем параллельные задачи для {seller_legal}\n\n"
        await asyncio.gather(
            asyncio.to_thread(funnel_and_orders_pipeline, date_from, date_to, seller_legal, spreadsheet_id),
            asyncio.to_thread(fetch_acceptance_report, date_from, date_to, seller_legal, spreadsheet_id),
            asyncio.to_thread(fetch_supplies_report, date_from, date_to, seller_legal, spreadsheet_id),
            asyncio.to_thread(fetch_ad_campaigns, date_from, date_to, seller_legal, spreadsheet_id),
            asyncio.to_thread(fetch_commission_data, seller_legal, spreadsheet_id),
            asyncio.to_thread(fetch_paid_storage, date_from, date_to, seller_legal, spreadsheet_id),
            asyncio.to_thread(fetch_stocks, date_from, date_to, seller_legal, spreadsheet_id),
            asyncio.to_thread(transfer_selfcost_data, date_from, date_to, spreadsheet_id),
            asyncio.to_thread(fetch_orders_7_days, date_from, seller_legal, spreadsheet_id),
        )
        await asyncio.to_thread(process_wb_reports, date_from, date_to, seller_legal, spreadsheet_id)


        # если делается отчет по дням
        mode = current_mode.get()
        if mode == "days":
            remove_rows_by_conditions(target_file_id,target_list_name,date_from, "Wildberries", legal_entity)

        # Перенос данных в сводную таблицу
        append_data_between_spreadsheets(
            spreadsheet_id,
            "Данные для сводной",
            "A4:BG",
            target_file_id,
            target_list_name
        )

        spreadsheet_urls[seller_legal] = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
        yield f"data: Обработка для {seller_legal} завершена\n\n"

    yield f"data: Ссылки на файлы: {spreadsheet_urls}\n\n"