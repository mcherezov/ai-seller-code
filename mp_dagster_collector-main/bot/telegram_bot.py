import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from telegram.ext import Application, CommandHandler
from telegram import Bot
from typing import List
import re
import requests

log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_dir / 'telegram_bot.log')]
)
logger = logging.getLogger(__name__)

CONFIG_DIR = Path(r'C:\Users\Luxury PC\Documents\ako\config')
logger.info(f"Using config directory: {CONFIG_DIR}")


def load_config():
    env_path = CONFIG_DIR / '.env'
    if not env_path.exists():
        logger.error(f"Файл .env не найден по пути {env_path}")
        return {}
    load_dotenv(dotenv_path=env_path)
    cert_path = CONFIG_DIR / 'CA.pem'
    if not cert_path.exists():
        logger.error(f"SSL-сертификат не найден по пути {cert_path}")
    config = {
        'db_host': os.getenv('DEST_DB_HOST'),
        'db_port': os.getenv('DEST_DB_PORT', '5432'),
        'db_name': os.getenv('DEST_DB_NAME'),
        'db_user': os.getenv('DEST_DB_USER'),
        'db_password': os.getenv('DEST_DB_PASSWORD'),
        'db_sslmode': os.getenv('DEST_DB_SSLMODE'),
        'db_sslrootcert': str(cert_path) if cert_path.exists() else None,
        'telegram_token': os.getenv('TELEGRAM_BOT_TOKEN'),
        'chat_id': os.getenv('TELEGRAM_CHAT_ID')
    }
    missing_params = [key for key, value in config.items() if not value and key != 'db_sslrootcert']
    if missing_params:
        logger.error(f"Отсутствуют параметры в .env: {', '.join(missing_params)}")
        return {}
    logger.debug(f"Загружена конфигурация: {config}")
    return config


def escape_markdown(text: str) -> str:
    """Экранирует спецсимволы Markdown v1"""
    if not isinstance(text, str):
        text = str(text)
    return re.sub(r'([_*\[\]()~`>#+-=|{}.!])', r'\\\1', text)


def get_optimization_results(optimization_date: str = None):
    """Извлекает рекомендации из таблицы algo.cluster_optimization_results."""
    try:
        config = load_config()
        if not config:
            logger.error("Невозможно продолжить из-за некорректной конфигурации")
            return None

        conn_string = (
            f"postgresql://{config['db_user']}:{config['db_password']}@"
            f"{config['db_host']}:{config['db_port']}/{config['db_name']}?sslmode={config['db_sslmode']}"
            f"{'&sslrootcert=' + config['db_sslrootcert'] if config.get('db_sslrootcert') else ''}"
        )
        engine = create_engine(conn_string)

        query = """
                SELECT campaign_id, product_id, cluster_name, avg_cpc, total_clicks, total_sum, 
                       status, recommendation, max_cpc, optimization_date
                FROM algo.cluster_optimization_results
            """
        params = {}
        conditions = []
        if optimization_date:
            conditions.append("optimization_date = :optimization_date")
            params['optimization_date'] = optimization_date
        else:
            conditions.append("optimization_date = CURRENT_DATE")
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY campaign_id, cluster_name"

        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)
            if df.empty:
                logger.warning(f"Рекомендации за дату {optimization_date or 'текущую'} отсутствуют")
                return None

        logger.info(f"Извлечено {len(df)} рекомендаций из базы данных")
        return df

    except Exception as e:
        logger.error(f"Ошибка при извлечении рекомендаций: {str(e)}")
        return None


def format_recommendations(df: pd.DataFrame, max_length: int = 4096) -> List[str]:
    """Форматирует DataFrame рекомендаций в список текстовых сообщений, разделяя по длине."""
    if df is None or df.empty:
        return ["Рекомендации отсутствуют."]

    messages = []
    current_message = "📊 *Рекомендации по оптимизации кластеров*\n\n"
    current_message += f"Дата оптимизации: {escape_markdown(df['optimization_date'].iloc[0].strftime('%Y-%m-%d'))}\n"
    current_message += f"ID кампании: {escape_markdown(df['campaign_id'].iloc[0])}\n"
    current_message += f"ID товара: {escape_markdown(df['product_id'].iloc[0])}\n"
    current_message += f"Максимальный CPC: {df['max_cpc'].iloc[0]:.3f} ₽\n"
    current_message += f"Всего кластеров: {len(df)}\n"
    current_message += f"Валидных кластеров: {len(df[df['status'] == 'Оставить'])}\n"
    current_message += f"Исключено кластеров: {len(df[df['status'] == 'Исключить'])}\n\n"
    current_message += "*Детали по кластерам:*\n"

    for _, row in df.iterrows():
        cluster_text = (
            f"🔹 *Кластер*: {escape_markdown(row['cluster_name'])}\n"
            f"   Средний CPC: {row['avg_cpc']:.3f} ₽\n" if pd.notna(row['avg_cpc']) else "   Средний CPC: NaN\n"
        )
        cluster_text += (
            f"   Клики: {escape_markdown(row['total_clicks'])}\n"
            f"   Затраты: {row['total_sum']:.2f} ₽\n"
            f"   Статус: {escape_markdown(row['status'])}\n"
            f"   Рекомендация: {escape_markdown(row['recommendation'])}\n\n"
        )

        if len(current_message + cluster_text) > max_length:
            messages.append(current_message)
            current_message = "📊 *Продолжение рекомендаций по кластерам*\n\n" + cluster_text
        else:
            current_message += cluster_text

    if current_message:
        messages.append(current_message)

    return messages


def send_telegram_notification(config: dict, message: str):
    """Отправляет уведомление в Telegram."""
    try:
        token = config.get('telegram_token')
        chat_id = config.get('chat_id')

        if not token or not chat_id:
            logger.error("Отсутствуют параметры Telegram (token или chat_id)")
            return False

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'Markdown'
        }

        response = requests.post(url, data=payload)
        if response.status_code == 200:
            logger.info("Уведомление успешно отправлено в Telegram")
            return True
        else:
            logger.error(f"Ошибка отправки уведомления: {response.text}")
            return False

    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления в Telegram: {str(e)}")
        return False


async def start(update, context):
    await update.message.reply_text("Бот запущен и готов к работе!")


async def recommend(update, context):
    try:
        config = load_config()
        if str(update.message.chat_id) != config['chat_id']:
            await update.message.reply_text("Извините, этот бот работает только в определенном чате.")
            logger.warning(f"Несанкционированный доступ из chat_id={update.message.chat_id}")
            return

        optimization_date = context.args[0] if context.args else None
        if optimization_date:
            try:
                datetime.strptime(optimization_date, '%Y-%m-%d')
            except ValueError:
                await update.message.reply_text(
                    "Неверный формат даты. Используйте YYYY-MM-DD, например, /recommend 2025-06-23")
                logger.error(f"Неверный формат даты: {optimization_date}")
                return

        recommendations_df = get_optimization_results(optimization_date)
        if recommendations_df is None or recommendations_df.empty:
            await update.message.reply_text(
                f"Рекомендации за дату {optimization_date or 'текущую'} не найдены."
            )
            return

        messages = format_recommendations(recommendations_df)
        for message in messages:
            await update.message.reply_text(message, parse_mode='Markdown')

        logger.info(f"Отправлены рекомендации за {optimization_date or 'текущую дату'} в chat_id={config['chat_id']}")

        notification_message = "Оптимизация завершена. Рекомендации отправлены."
        send_telegram_notification(config, notification_message)

    except Exception as e:
        logger.error(f"Ошибка при выполнении команды /recommend: {str(e)}")
        await update.message.reply_text("Произошла ошибка при получении рекомендаций. Попробуйте позже.")


def main():
    config = load_config()
    if not config:
        logger.error("Не удалось загрузить конфигурацию, бот не запущен")
        return

    application = Application.builder().token(config['telegram_token']).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("recommend", recommend))

    logger.info("Запуск Telegram-бота")
    application.run_polling(allowed_updates=["message"])


if __name__ == "__main__":
    main()