import logging
from datetime import datetime
import traceback
import requests
import pytz
from typing import Dict, List
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from db import load_data
from environment import setup_logger
from main import send_telegram_message
from config_loader import load_config

logger = logging.getLogger(__name__)


def today_msk_datetime():
    moscow_tz = pytz.timezone('Europe/Moscow')
    return datetime.now(moscow_tz)


def validate_schedule_format(schedule: Dict, advert_id: str) -> None:
    """
    Проверяет формат расписания на корректность.
    
    Args:
        schedule: Расписание изменений цен
        advert_id: ID рекламной кампании
        
    Raises:
        ValueError: Если формат данных некорректный
    """
    # Проверяем наличие всех дней недели
    if not all(str(day) in schedule for day in range(7)):
        raise ValueError(
            f"Некорректный формат расписания для кампании {advert_id}: "
            f"отсутствуют некоторые дни недели"
        )
        
    # Проверяем минимум 2 изменения цены для каждого дня
    for day, day_prices in schedule.items():
        if len(day_prices) < 2:
            raise ValueError(
                f"Некорректный формат расписания для кампании {advert_id}: "
                f"день {day} содержит менее 2 изменений цены"
            )


def find_last_price_change(time_prices: Dict[str, int], current_hour: int, current_minute: int) -> tuple[int, int, int] | None:
    """
    Находит последнее прошедшее изменение цены для текущего времени.
    
    Args:
        time_prices: Словарь с изменениями цен для конкретного дня
        current_hour: Текущий час
        current_minute: Текущая минута
        
    Returns:
        Кортеж (час, минута, цена) или None, если не найдено прошедших изменений
    """
    last_price_change = None
    last_price = None
    
    for time_str, price_change in time_prices.items():
        hour, minute = map(int, time_str.split(':'))
        
        # Если текущее время совпадает с расписанием
        if hour == current_hour and minute == current_minute:
            return hour, minute, price_change
            
        # Проверяем, является ли это время последним прошедшим
        if (hour < current_hour or (hour == current_hour and minute <= current_minute)):
            if last_price_change is None or (
                hour > last_price_change[0] or 
                (hour == last_price_change[0] and minute > last_price_change[1])
            ):
                last_price_change = (hour, minute)
                last_price = price_change
                
    return (last_price_change[0], last_price_change[1], last_price) if last_price_change else None


def apply_current_price_changes(settings_df) -> None:
    """
    Применяет изменения цен для текущего времени.
    Если текущее время находится между запланированными изменениями,
    применяет последнее прошедшее изменение цены.
    Если нет прошедших изменений на текущий день, применяет последнюю цену с предыдущего дня.
    
    Args:
        settings_df: DataFrame с настройками автобиддера
        
    Raises:
        ValueError: Если для текущего дня нет расписания, настройки пустые,
                  или формат данных некорректный
    """
    if settings_df.empty:
        error_msg = "Empty settings are not allowed"
        logger.error(error_msg)
        raise ValueError(error_msg)
        
    # Проверяем формат данных для всех кампаний
    for _, setting in settings_df.iterrows():
        validate_schedule_format(setting['schedule'], setting['advert_id'])
        
    current_time = today_msk_datetime()
    current_weekday = str(current_time.weekday())
    current_hour = current_time.hour
    current_minute = current_time.minute
    
    for _, setting in settings_df.iterrows():
        advert_id = setting['advert_id']
        schedule = setting['schedule']
        nm_ids = setting['nm_ids']
        
        # Проверяем расписание для текущего дня
        time_prices = schedule[current_weekday]
        
        # Пытаемся найти подходящее изменение цены для текущего дня
        price_change = find_last_price_change(time_prices, current_hour, current_minute)
        
        if price_change:
            hour, minute, price = price_change
            logger.info(
                f"Applying price change for campaign {advert_id}: "
                f"time {hour:02d}:{minute:02d}, new price {price}"
            )
            change_campaign_price(
                advert_id=advert_id,
                price_change=price,
                nm_ids=nm_ids,
                legal_entity=setting['legal_entity']
            )
            return
            
        # Если не нашли прошедших изменений в текущем дне,
        # берем последнее изменение предыдущего дня
        prev_weekday = str((int(current_weekday) - 1) % 7)
        prev_day_prices = schedule[prev_weekday]
        
        if prev_day_prices:
            last_time = max(prev_day_prices.keys())
            last_price = prev_day_prices[last_time]
            logger.info(
                f"Applying last price from previous day for campaign {advert_id}: "
                f"last change was at {last_time}, new price {last_price}"
            )
            change_campaign_price(
                advert_id=advert_id,
                price_change=last_price,
                nm_ids=nm_ids,
                legal_entity=setting['legal_entity']
            )
            return


def handle_campaign_error(retry_state):
    """
    Обрабатывает ошибки операций с кампаниями после исчерпания всех попыток.
    
    Args:
        retry_state: Состояние повторных попыток от tenacity
    """
    advert_id = retry_state.kwargs['advert_id']
    price_change = retry_state.kwargs['price_change']
    error = retry_state.outcome.exception()
    
    logger.error(f"Failed to change campaign price after all retry attempts: {error}")
    send_telegram_message(f"❌ Failed to change price for campaign ID {advert_id} to {price_change}: {error}")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((Exception,)),
    before_sleep=lambda retry_state: logger.warning(
        f"Retrying price change operation after error: {retry_state.outcome.exception()}. "
        f"Attempt {retry_state.attempt_number}/3"
    ),
    after=handle_campaign_error
)
def change_campaign_price(advert_id: int, price_change: int, nm_ids: List[int], legal_entity: str):
    """
    Изменяет цену рекламной кампании через API Wildberries.
    
    Args:
        advert_id: ID рекламной кампании (целое число)
        price_change: Новая цена в копейках
        nm_ids: Список идентификаторов товаров (nm)
        legal_entity: Юридическое лицо (ИНТЕР, АТ или КРАВЧИК)
    """
    url = "https://advert-api.wildberries.ru/adv/v0/bids"
    
    # Загружаем конфиг и получаем API ключ
    config = load_config()
    api_key = config['wb_autobidder_keys'].get(legal_entity.lower())
    
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }
    
    # Создаем список nm_bids для каждого nm_id
    nm_bids = [
        {"nm": nm_id, "bid": price_change}
        for nm_id in nm_ids
    ]

    payload = {
        "bids": [
            {
                "advert_id": advert_id,
                "nm_bids": nm_bids
            }
        ]
    }
    
    response = requests.patch(url, headers=headers, json=payload)
    
    if response.status_code != 204:
        raise Exception(f"Ошибка изменения цены кампании {advert_id}: {response.status_code}, {response.text}")


def get_settings_hash(settings_df) -> str:
    """
    Создает хеш настроек для сравнения.
    
    Args:
        settings_df: DataFrame с настройками автобиддера
        
    Returns:
        str: Хеш настроек
    """
    if settings_df.empty:
        return ""
    return settings_df.to_json(orient='records')


def update_scheduler_jobs(scheduler: BlockingScheduler, settings_df) -> None:
    """
    Обновляет задачи в планировщике на основе новых настроек.
    
    Args:
        scheduler: Планировщик задач
        settings_df: DataFrame с настройками автобиддера
    """
    # Удаляем все существующие задачи
    scheduler.remove_all_jobs()
    
    if settings_df.empty:
        logger.warning("No autobidder settings found in database")
        return
    
    # Проверяем формат данных для всех кампаний
    for _, setting in settings_df.iterrows():
        validate_schedule_format(setting['schedule'], setting['advert_id'])
    
    # Применяем изменения цен для текущего времени
    apply_current_price_changes(settings_df)
    
    # Для каждой настройки создаем задачи на основе расписания
    for _, setting in settings_df.iterrows():
        advert_id = setting['advert_id']
        schedule = setting['schedule']
        nm_ids = setting['nm_ids']
        
        # Для каждого дня недели создаем задачи
        for weekday, time_prices in schedule.items():
            for time_str, price_change in time_prices.items():
                # Разбираем время на часы и минуты
                hour, minute = map(int, time_str.split(':'))
                
                # Create trigger with explicit timezone handling
                # CronTrigger will use the scheduler's timezone (Europe/Moscow)
                # The weekday parameter is 0-6 where 0 is Monday
                trigger = CronTrigger(
                    day_of_week=int(weekday),  # Convert string to int for proper weekday handling
                    hour=hour,
                    minute=minute,
                    timezone='Europe/Moscow'  # Explicitly set timezone for the trigger
                )
                
                # Add job to scheduler with timezone-aware trigger
                job_id = f'price_change_{advert_id}_{weekday}_{time_str}'
                
                scheduler.add_job(
                    change_campaign_price,
                    trigger=trigger,
                    kwargs={
                        'advert_id': advert_id,
                        'price_change': price_change,
                        'nm_ids': nm_ids,
                        'legal_entity': setting['legal_entity']
                    },
                    id=job_id,
                    replace_existing=True
                )
                
                logger.info(
                    f"Scheduled price change for campaign {advert_id} on weekday {weekday}: "
                    f"time {time_str}, new price {price_change}"
                )


def check_settings_changes(scheduler: BlockingScheduler) -> None:
    """
    Проверяет изменения в настройках автобиддера и обновляет планировщик при необходимости.
    
    Args:
        scheduler: Планировщик задач
    """
    try:
        # Загружаем текущие настройки
        current_settings = load_data('wb_autobidder')
        current_hash = get_settings_hash(current_settings)
        
        # Получаем сохраненный хеш из атрибутов планировщика
        saved_hash = getattr(scheduler, '_settings_hash', None)
        
        # Если хеш изменился или его нет, обновляем задачи
        if saved_hash != current_hash:
            logger.info("Autobidder settings changed, updating scheduler jobs")
            update_scheduler_jobs(scheduler, current_settings)
            scheduler._settings_hash = current_hash
            send_telegram_message("🔄 Autobidder settings updated, scheduler jobs refreshed")
            
    except Exception as e:
        traceback.print_exc()
        logger.error(f"Error checking settings changes: {e}")
        send_telegram_message(f"❌ Error checking autobidder settings: {e}")


def schedule_campaigns():
    """
    Планирует задачи для управления рекламными кампаниями на основе настроек из БД.
    """
    scheduler = BlockingScheduler(timezone='Europe/Moscow')
    
    # Добавляем задачу проверки изменений настроек каждую минуту
    scheduler.add_job(
        check_settings_changes,
        'interval',
        minutes=1,
        args=[scheduler],
        id='check_settings_changes',
        replace_existing=True
    )
    
    # Инициализируем начальные настройки
    check_settings_changes(scheduler)
    
    return scheduler


def main():
    """
    Основной цикл работы автобиддера.
    Использует APScheduler для управления задачами.
    """
    try:
        logger.info("Starting autobidder service")
        scheduler = schedule_campaigns()
        scheduler.start()
            
    except Exception as e:
        logger.error(f"Error in autobidder main loop: {e}")
        send_telegram_message(f"❌ Error in autobidder: {e}")
        raise


if __name__ == '__main__':
    setup_logger()
    main()
