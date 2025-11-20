import logging
import time

from requests import RequestException


def request_with_retries(requests_method, url,
                         max_retries=5, retry_delay=60, additional_429_delay=120, retry_statuses=(429, 500, 502, 503, 504, 404),
                         err_prefix="", **kwargs):
    status_codes = []
    for attempt in range(max_retries):
        try:
            response = requests_method(url, **kwargs)

            if response.status_code in retry_statuses:
                logging.debug(f"{err_prefix}Ошибка {response.status_code}. "
                      f"Попытка {attempt + 1} из {max_retries}. Ожидание {retry_delay} секунд...")
                status_codes.append(response.status_code)
                time.sleep(retry_delay)
                if response.status_code == 429:
                    time.sleep(additional_429_delay)
            else:
                return response

        except RequestException as e:
            logging.debug(f"{err_prefix}Ошибка сети: {e}. Попытка {attempt + 1} из {max_retries}. Ожидание {retry_delay} секунд...")
            time.sleep(retry_delay)

    raise Exception(f"{err_prefix}Запрос не выполнен после {max_retries} попыток, status_codes={status_codes}.")
