import requests
import time
import logging
from functools import wraps
from threading import Lock

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

RATE_LIMIT_SECONDS = 60

# Хранение времени последнего запроса для каждого seller
_last_request_time = {}
_lock = Lock()


def rate_limited(seller: str):
    """Декоратор для ограничения частоты запросов"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with _lock:
                now = time.time()
                last_time = _last_request_time.get(seller, 0)
                elapsed = now - last_time

                if elapsed < RATE_LIMIT_SECONDS:
                    wait_time = RATE_LIMIT_SECONDS - elapsed
                    logger.debug(f"⏳ Rate limit для {seller}. Ждем {wait_time:.1f} секунд...")
                    time.sleep(wait_time)

                _last_request_time[seller] = time.time()

            return func(*args, **kwargs)
        return wrapper
    return decorator


def retry_on_failure(max_retries=5, retry_delay=60):
    """Декоратор для повторов в случае ошибок"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (requests.exceptions.RequestException, Exception) as e:
                    logger.warning(f"Попытка {attempt}/{max_retries} неудачна: {e}")
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                    else:
                        raise
        return wrapper
    return decorator


@retry_on_failure()
def send_get_request(url: str, headers: dict, params: dict = None, seller: str = "default") -> dict:
    """GET-запрос с лимитом"""
    @rate_limited(seller)
    def inner_get():
        response = requests.get(url, headers=headers, params=params)
        logger.debug(f"GET {url} - {response.status_code}")
        if response.status_code == 200:
            return response.json()
        raise Exception(f"Ошибка GET-запроса: {response.status_code} | {response.text}")

    return inner_get()


@retry_on_failure()
def send_post_request(url: str, headers: dict, json: dict = None, seller: str = "default") -> dict:
    """POST-запрос с лимитом"""
    @rate_limited(seller)
    def inner_post():
        response = requests.post(url, headers=headers, json=json)
        logger.debug(f"POST {url} - {response.status_code}")
        if response.status_code == 200:
            return response.json()
        raise Exception(f"Ошибка POST-запроса: {response.status_code} | {response.text}")

    return inner_post()
