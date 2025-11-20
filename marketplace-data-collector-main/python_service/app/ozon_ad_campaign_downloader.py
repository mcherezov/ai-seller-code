from datetime import datetime, timedelta
import asyncio
import traceback
from fastapi import FastAPI, HTTPException
from ad_campaign.ozon import fetch_ozon_ad_campaign_statistics
from db import delete_previous_and_insert_new_postgres_table
from config_loader import load_config
from environment import setup_logger

setup_logger()
app = FastAPI()
processing = False  # Флаг обработки запроса

async def try_get_day_data(date):
    config = load_config()
    try_count = 0
    while try_count < 3:
        try:
            print(f"Дата {date}, попытка {try_count + 1}")
            df = fetch_ozon_ad_campaign_statistics(config['ozon_ad_compaign'], date)
            if df is not None and not df.empty:
                delete_previous_and_insert_new_postgres_table(df, 'ozon_ad_compaign')
                print(f"Успешно обновлены данные за {date} в таблице ozon_ad_campaign.")
                await asyncio.sleep(60 * 1)
                return True
            else:
                print(f"Нет данных за {date}, пропускаем.")
                try_count += 1
            await asyncio.sleep(60 * 1)
        except Exception as e:
            try_count += 1
            traceback.print_exc()
            print(f"Дата {date}, попытка {try_count}. Ошибка: {e}")
            await asyncio.sleep(60 * 10)
    return False

async def process_dates(dates):
    success_count = 0
    for date in dates:
        if await try_get_day_data(date):
            success_count += 1
    return success_count

def parse_dates(date_range: str):
    parts = date_range.split("-")
    try:
        if len(parts) == 1:
            return [datetime.strptime(parts[0].strip(), "%d.%m.%Y").date()]
        elif len(parts) == 2:
            start = datetime.strptime(parts[0].strip(), "%d.%m.%Y").date()
            end = datetime.strptime(parts[1].strip(), "%d.%m.%Y").date()
            return [start + timedelta(days=i) for i in range((end - start).days + 1)]
    except ValueError:
        return None

@app.get("/update-ozon-ad")
async def update_ozon_ad(date_range: str):
    global processing
    if processing:
        raise HTTPException(status_code=429, detail="Запрос уже выполняется, пожалуйста, подождите")
    
    dates = parse_dates(date_range)
    if dates is None:
        raise HTTPException(status_code=400, detail="Неверный формат даты. Используйте DD.MM.YYYY или DD.MM.YYYY-DD.MM.YYYY")
    
    processing = True
    try:
        success_count = await process_dates(dates)
        return {"message": f"Обновлены данные за {success_count} дней."}
    finally:
        processing = False
