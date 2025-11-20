import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import dotenv_values
import os
import requests
from ucb_bandit_cpm import UCBBanditCPM
from api.wb_api import WildberriesAPI
from api.wb_api_actions import WildberriesAPIactions
from sql.database_manager import UCBBanditDatabase

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_dir / 'bandit.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class SessionState:
    def __init__(self):
        self.log_messages = []

st = SessionState()

def load_config():
    """Загружает конфигурацию из .env файла (Telegram, Wildberries API и базу данных)."""
    possible_env_paths = [
        Path(os.getenv("AKO_CONFIG_PATH", "")) / '.env' if os.getenv("AKO_CONFIG_PATH") else None,
        Path(__file__).parent / 'config' / '.env',
        Path.home() / 'Documents' / 'ako' / 'config' / '.env',
        Path(__file__).parent.parent / 'config' / '.env',
    ]

    logger.info(f"Проверяемые пути для файла .env: {', '.join(str(p) for p in possible_env_paths if p)}")

    env_path = None
    for path in [p for p in possible_env_paths if p]:
        if path.exists():
            env_path = path
            logger.info(f"Файл .env найден по пути: {path}")
            break
        else:
            logger.info(f"Файл .env не найден по пути: {path}")

    if not env_path:
        error_msg = f"Файл .env не найден в путях: {', '.join(str(p) for p in possible_env_paths if p)}"
        logger.error(error_msg)
        st.log_messages.append(error_msg)
        return {}

    env_config = dotenv_values(env_path)
    logger.info(f"Файл .env успешно загружен из {env_path}")

    config = {
        'telegram_bot_token': env_config.get('TELEGRAM_BOT_TOKEN'),
        'telegram_chat_id': env_config.get('TELEGRAM_CHAT_ID_TEST'),
        'wb_api_token': env_config.get('WB_API_TOKEN_YULIA'),

        'db_config': {
         
        }
    }

    if config['db_config']['sslmode'] == 'verify-full' and config['db_config']['sslrootcert'] != 'system':
        cert_path = Path(config['db_config']['sslrootcert'])
        if not cert_path.exists():
            error_msg = f"Файл сертификата SSL не найден по пути: {cert_path}"
            logger.error(error_msg)
            st.log_messages.append(error_msg)
            config['db_config']['sslmode'] = 'disable'
            logger.warning("SSL отключен из-за отсутствия сертификата")

    missing_params = [key for key, value in config.items() if not value and key != 'db_config']
    missing_db_params = [key for key, value in config['db_config'].items() if
                        not value and key not in ['sslmode', 'sslrootcert']]
    if missing_params or missing_db_params:
        error_msg = f"Отсутствуют параметры в .env: {', '.join(missing_params + [f'db_config.{k}' for k in missing_db_params])}"
        logger.error(error_msg)
        st.log_messages.append(error_msg)
        return {}

    logger.debug(f"Загружена конфигурация: {config}")
    return config


def send_telegram_message(message: str, bot_token: str, chat_id: str):
    """Отправка сообщения в Telegram."""
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': message,
        }
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            logger.info("Уведомление успешно отправлено в Telegram")
            st.log_messages.append("Уведомление успешно отправлено в Telegram")
        else:
            error_msg = f"Ошибка отправки в Telegram: {response.status_code} - {response.text}"
            logger.error(error_msg)
            st.log_messages.append(error_msg)
    except Exception as e:
        error_msg = f"Ошибка при отправке уведомления в Telegram: {str(e)}"
        logger.error(error_msg)
        st.log_messages.append(error_msg)

def get_config_with_retry(wb_api, max_attempts=5, initial_delay=1):
    """Получение конфигурации с повторными попытками при ошибке 429."""
    attempt = 0
    delay = initial_delay
    while attempt < max_attempts:
        try:
            config_data = wb_api.get_config()
            logger.info(f"Получено {len(config_data.get('config', []))} конфигурационных параметров и {len(config_data.get('categories', []))} категорий")
            return config_data
        except Exception as e:
            if '429' in str(e):
                logger.warning(f"Ошибка 429: Too Many Requests. Повтор через {delay} секунд...")
                time.sleep(delay)
                attempt += 1
                delay *= 2
            else:
                logger.error(f"Ошибка при получении конфигурации: {str(e)}")
                raise
    logger.error(f"Не удалось выполнить запрос после {max_attempts} попыток: https://advert-api.wildberries.ru/adv/v0/config")
    return None

def run_ucb_bandit():
    """Запускает UCBBanditCPM для всех активных кампаний, учитывая минимальные ставки CPM."""
    try:
        config = load_config()
        if not config:
            raise ValueError("Не удалось загрузить конфигурацию")

        db = UCBBanditDatabase(config['db_config'])
        wb_api_config = WildberriesAPI(config.get('wb_api_token'))
        wb_api_actions = WildberriesAPIactions(config.get('wb_api_token'), logger)

        campaign_data = db.get_active_campaign_ids()
        logger.info(
            f"Найдено {len(campaign_data)} активных кампаний: {[data['campaign_id'] for data in campaign_data]}")

        if not campaign_data:
            logger.warning("Активные кампании не найдены")
            st.log_messages.append("Активные кампании не найдены")
            message = (
                f"❌ Нет активных кампаний для оптимизации\n"
                f"Проверьте таблицу core.algo_states в базе данных\n"
                f"📜 Логи: {'; '.join(st.log_messages[-3:])}"
            )
            bot_token = config.get('telegram_bot_token')
            chat_id = config.get('telegram_chat_id')
            if bot_token and chat_id:
                send_telegram_message(message, bot_token, chat_id)
            return

        bot_token = config.get('telegram_bot_token')
        chat_id = config.get('telegram_chat_id')
        wb_api_token = config.get('wb_api_token')

        config_data = get_config_with_retry(wb_api_config)
        if not config_data or 'config' not in config_data:
            logger.warning("Не удалось получить конфигурационные данные, используются значения по умолчанию")
            config_data = {'config': [
                {'name': 'cpm_min_booster', 'value': '125.0'},
                {'name': 'cpm_min_search_catalog', 'value': '250.0'}
            ]}

        for data in campaign_data:
            company_id = str(data['campaign_id'])
            nm = int(data['product_id'])
            logger.info(f"Начало обработки кампании {company_id} с product_id {nm}")

            try:
                campaign_info = wb_api_config.get_campaigns_info([int(company_id)])
                if not campaign_info or not isinstance(campaign_info, list) or len(campaign_info) == 0:
                    logger.warning(f"Не удалось получить информацию о кампании {company_id}: {campaign_info}")
                    campaign_type = None
                    min_bid = None # Change
                    campaign_active = False
                else:
                    campaign_type = campaign_info[0].get('type')
                    min_bid = None #
                    for item in config_data['config']:
                        if item['name'] == ('cpm_min_search_catalog' if campaign_type == 9 else 'cpm_min_booster'):
                            min_bid = float(item['value'])
                            break
                    logger.info(f"Получена минимальная ставка CPM для кампании {company_id}: {min_bid}")
                    campaign_active = True

                bandit = UCBBanditCPM(advert_id=company_id, min_bid=min_bid)

                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=7)

                recommendation = bandit.recommend_action(
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat()
                )

                logger.info(f"Рекомендация для кампании {company_id}: {recommendation}")

                message_lines = [
                    f"ℹ️ Оптимизация UCB Bandit для кампании {company_id}",
                    f"ID Кампании: {company_id}",
                    f"ID Товара: {nm}",
                ]

                recommended_cpm = None
                if recommendation and isinstance(recommendation, dict):
                    current_cpm = recommendation.get('current_cpm', 0.0)
                    recommended_cpm = recommendation.get('recommended_cpm', 0.0)
                    message_lines.append(f"Текущий CPM: {current_cpm:.2f} ₽")
                    message_lines.append(f"Рекомендация: Установите CPM равный {recommended_cpm:.2f} ₽")
                else:
                    logger.error(f"Рекомендация пуста для кампании {company_id}")
                    st.log_messages.append(f"Рекомендация пуста для кампании {company_id}")
                    message_lines.append("Не удалось получить рекомендацию")
                    campaign_active = False

                if wb_api_token and recommendation and isinstance(recommendation,
                                                                  dict) and recommended_cpm > 0 and campaign_active:
                    bids = [
                        {
                            "advert_id": int(company_id),
                            "nm_bids": [
                                {
                                    "nm": int(nm),
                                    "bid": int(round(recommended_cpm))
                                }
                            ]
                        }
                    ]
                    result = wb_api_actions.set_bids(bids)
                    if result:
                        logger.info(f"Ставка успешно обновлена для кампании {company_id}: {bids}")
                        message_lines.append(f"✅ Ставка обновлена: Новая ставка CPM: {recommended_cpm:.2f} ₽")
                        bandit.current_cpm = recommended_cpm
                    else:
                        logger.error(f"Не удалось обновить ставку для кампании {company_id}")
                        message_lines = [
                            f"❌ Ошибка оптимизации UCB Bandit для кампании {company_id}",
                            f"ID Кампании: {company_id}",
                            f"ID Товара: {nm}",
                        ]
                        message_lines.append(f"Возможно, проблема с токеном API Wildberries")
                        message_lines.append(f"📜 Логи: {'; '.join(st.log_messages[-3:])}")
                    time.sleep(1)
                else:
                    reason = []
                    if not wb_api_token:
                        reason.append("отсутствует wb_api_token")
                    if not recommendation or not isinstance(recommendation, dict):
                        reason.append("рекомендация пуста или некорректна")
                    if recommended_cpm <= 0:
                        reason.append("рекомендованный CPM <= 0")
                    if not campaign_active:
                        reason.append("кампания неактивна (API вернул 204 или пустые данные)")
                    logger.warning(f"Не удалось обновить ставку для кампании {company_id}: {', '.join(reason)}")
                    message_lines = [
                        f"❌ Ошибка оптимизации UCB Bandit для кампании {company_id}",
                        f"ID Кампании: {company_id}",
                        f"ID Товара: {nm}",
                    ]
                    message_lines.append(f"Возможно, проблема с токеном API Wildberries")
                    message_lines.append(f"📜 Логи: {'; '.join(st.log_messages[-3:])}")

                if bot_token and chat_id:
                    send_telegram_message("\n".join(message_lines), bot_token, chat_id)
                else:
                    logger.warning(f"Не удалось отправить уведомление в Telegram для кампании {company_id}: отсутствуют bot_token или chat_id")
                    st.log_messages.append(f"Не удалось отправить уведомление в Telegram для кампании {company_id}: отсутствуют bot_token или chat_id")

            except Exception as e:
                logger.error(f"Ошибка обработки кампании {company_id}: {str(e)}")
                st.log_messages.append(f"Ошибка обработки кампании {company_id}: {str(e)}")
                message_lines = [
                    f"❌ Ошибка оптимизации UCB Bandit для кампании {company_id}",
                    f"ID Кампании: {company_id}",
                    f"ID Товара: {nm}",
                    f"Ошибка: {str(e)}",
                    f"Возможно, проблема с токеном API Wildberries",
                    f"📜 Логи: {'; '.join(st.log_messages[-3:])}"
                ]
                if bot_token and chat_id:
                    send_telegram_message("\n".join(message_lines), bot_token, chat_id)
                time.sleep(1)
                continue

        logger.info("Обработка всех кампаний завершена")

    except ValueError as e:
        logger.error(f"Ошибка получения активных кампаний или конфигурации: {str(e)}")
        st.log_messages.append(f"Ошибка получения активных кампаний или конфигурации: {str(e)}")
        config = load_config()
        bot_token = config.get('telegram_bot_token')
        chat_id = config.get('telegram_chat_id')
        message = (
            f"❌ Ошибка оптимизации UCB Bandit\n"
            f"Ошибка: {str(e)}\n"
            f"📜 Логи: {'; '.join(st.log_messages[-3:])}"
        )
        if bot_token and chat_id:
            send_telegram_message(message, bot_token, chat_id)
    except Exception as e:
        logger.error(f"Произошла ошибка: {str(e)}")
        st.log_messages.append(f"Произошла ошибка: {str(e)}")
        config = load_config()
        bot_token = config.get('telegram_bot_token')
        chat_id = config.get('telegram_chat_id')
        message = (
            f"❌ Ошибка оптимизации UCB Bandit\n"
            f"Ошибка: {str(e)}\n"
            f"Возможно, проблема с токеном API Wildberries",
            f"📜 Логи: {'; '.join(st.log_messages[-3:])}"
        )
        if bot_token and chat_id:
            send_telegram_message(message, bot_token, chat_id)

if __name__ == "__main__":
    run_ucb_bandit()
