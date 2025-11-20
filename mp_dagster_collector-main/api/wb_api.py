import requests
from datetime import datetime, timedelta
from typing import Dict, Optional
import logging
import time

logger = logging.getLogger(__name__)

class WildberriesAPI:
    def __init__(self, token):
        self.base_url = "https://advert-api.wildberries.ru/adv"
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self.last_config_request_time = None

    def make_request(self, endpoint, method="GET", params=None, data=None, retries=3, backoff_factor=1):
        """
        Выполнить запрос с повторными попытками при ошибке 429.

        Args:
            endpoint: Конечная точка API.
            method: HTTP-метод (GET, POST и т.д.).
            params: Параметры запроса.
            data: Тело запроса.
            retries: Количество повторных попыток при ошибке 429.
            backoff_factor: Множитель задержки для экспоненциального backoff.
        """
        url = f"{self.base_url}{endpoint}"
        for attempt in range(retries):
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=self.headers,
                    params=params,
                    json=data
                )
                print(f"[{method}] {url}")
                print("Status code:", response.status_code)
                print("Raw response:", response.text[:300])
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    sleep_time = backoff_factor * (2 ** attempt)
                    logger.warning(f"Ошибка 429: Too Many Requests. Повтор через {sleep_time} секунд...")
                    time.sleep(sleep_time)
                    continue
                elif response.status_code == 401:
                    logger.error("Ошибка 401: Пользователь не авторизован")
                    return None
                print(f"HTTP Error: {e}")
                print(f"Response content: {response.text}")
                return None
            except requests.exceptions.RequestException as e:
                print(f"Request Error: {e}")
                return None
            except ValueError as e:
                print(f"JSON parse error: {e}")
                print("Raw response that failed to parse:", response.text)
                return None
        logger.error(f"Не удалось выполнить запрос после {retries} попыток: {url}")
        return None

    def get_config(self) -> Optional[Dict]:
        """
        Получить конфигурационные значения для рекламных кампаний.

        Returns:
            Словарь с конфигурацией и категориями или None при ошибке.
            Пример структуры ответа:
            {
                "categories": [{}],
                "config": [
                    {
                        "description": "Минимальный бюджет кампании",
                        "name": "budget_min",
                        "value": "1000"
                    },
                    ...
                ]
            }
        """
        if self.last_config_request_time:
            time_since_last = (datetime.now() - self.last_config_request_time).total_seconds()
            if time_since_last < 60:
                logger.warning(f"Слишком частые запросы. Подождите {60 - time_since_last:.1f} секунд")
                time.sleep(60 - time_since_last)

        endpoint = "/v0/config"
        try:
            response = self.make_request(endpoint, method="GET")
            self.last_config_request_time = datetime.now()

            if response is None:
                logger.error("Не удалось получить конфигурационные значения")
                return None

            if not isinstance(response, dict):
                logger.error(f"Некорректный формат ответа: {response}")
                return None

            categories = response.get("categories", [])
            config = response.get("config", [])

            if not config:
                logger.warning("Конфигурационные данные отсутствуют в ответе")
                return None

            logger.info(f"Получено {len(config)} конфигурационных параметров и {len(categories)} категорий")
            return response

        except Exception as e:
            logger.error(f"Ошибка при получении конфигурации: {e}")
            return None

    def get_campaigns_count(self):
        endpoint = "/v1/promotion/count"
        response = self.make_request(endpoint, method="GET")
        if not response:
            return 0, []
        adverts = response.get("adverts", [])
        all_advert_ids = []
        total_count = 0
        for group in adverts:
            total_count += group.get("count", 0)
            advert_list = group.get("advert_list", [])
            all_advert_ids.extend([item.get("advertId") for item in advert_list if "advertId" in item])
        return total_count, all_advert_ids

    def get_campaigns_info(self, advert_ids: list[int]) -> Optional[list]:
        """
        Получить информацию о кампаниях по списку ID.

        Args:
            advert_ids: Список ID кампаний.

        Returns:
            Список информации о кампаниях или None, если запрос не удался.
        """
        if not advert_ids:
            logger.warning("Список ID пуст")
            return None

        max_ids_per_request = 50
        all_campaigns = []

        for i in range(0, len(advert_ids), max_ids_per_request):
            batch_ids = advert_ids[i:i + max_ids_per_request]
            logger.debug(f"Запрос информации для {len(batch_ids)} кампаний: {batch_ids}")
            endpoint = "/v1/promotion/adverts"
            response = self.make_request(endpoint, method="POST", data=batch_ids)

            if response is None:
                logger.warning(f"Не удалось получить данные для пакета кампаний: {batch_ids}")
                continue

            if isinstance(response, list):
                all_campaigns.extend(response)
            else:
                logger.warning(f"Некорректный формат ответа для пакета кампаний: {response}")

            time.sleep(1)

        if not all_campaigns:
            logger.error("Нет данных по рекламным объявлениям для всех пакетов")
            return None

        logger.info(f"Получена информация о {len(all_campaigns)} кампаниях")
        return all_campaigns

    def get_campaign_stats(self, advert_id: int, start_date: str, end_date: str) -> Optional[list]:
        """
        Получить статистику кампании за указанный период.

        Args:
            advert_id: ID кампании.
            start_date: Начальная дата в формате YYYY-MM-DD.
            end_date: Конечная дата в формате YYYY-MM-DD.

        Returns:
            Список статистики или None, если запрос не удался.
            Ответ айди компании NM_ID (Супер вложенный /days/aps/NM/NM_ID)
        """
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            if start > end:
                logger.warning(f"Некорректный диапазон дат: {start_date} > {end_date}")
                return None
            if end > datetime.now():
                logger.warning(f"Конечная дата {end_date} в будущем, корректируем")
                end = datetime.now().date()
                end_date = end.isoformat()

            endpoint = "/v2/fullstats"
            data = [{"advertId": advert_id, "interval": {"begin": start_date, "end": end_date}}]
            response = self.make_request(endpoint, method="POST", data=data)

            if response is None:
                logger.warning(f"Не удалось получить статистику для кампании {advert_id}")
                return None

            if isinstance(response, list) and response:
                logger.info(f"Получена статистика для кампании {advert_id}")
                return response[0].get("stat", [])

            logger.warning(f"Нет статистики для кампании {advert_id} за {start_date}–{end_date}, пробуем 7 дней")
            end = datetime.now().date() - timedelta(days=1)
            start = end - timedelta(days=6)
            data = [{"advertId": advert_id, "interval": {"begin": start.isoformat(), "end": end.isoformat()}}]
            response = self.make_request(endpoint, method="POST", data=data)

            if isinstance(response, list) and response:
                logger.info(f"Получена статистика для кампании {advert_id} за 7 дней")
                return response[0].get("stat", [])

            logger.warning(f"Нет статистики для кампании {advert_id} даже за 7 дней")
            return None

        except Exception as e:
            logger.error(f"Ошибка при получении статистики кампании {advert_id}: {e}")
            return None

    def get_search_stat_words(self, campaign_id: int):
        info = self.get_campaigns_info([campaign_id])
        if not info or not isinstance(info, list):
            print(f"Не удалось получить информацию о кампании {campaign_id}")
            return None

        campaign = info[0]
        if campaign.get("type") != 9:
            print(f"Кампания {campaign_id} не является поисковой (type != 9)")
            return None

        endpoint = "/v1/stat/words"
        params = {"id": campaign_id}
        response = self.make_request(endpoint, method="GET", params=params)

        if response is None:
            print(f"Нет данных по ключевым словам для кампании {campaign_id}")
            return None

        keywords = response.get("words", {}).get("keywords", [])
        if not keywords:
            print(f"Ключевые слова не найдены в кампании {campaign_id}")
            return []

        print(f"Найдено ключевых слов: {len(keywords)}")
        return keywords

    def get_keyword_clusters(self, campaign_id: int, excluded: list[str] = None) -> list[dict]:
        """
        Получить кластеры ключевых фраз для кампании.
        """
        url = f"{self.base_url}/v2/auto/stat-words?id={campaign_id}"
        payload = {"excluded": excluded or []}
        response = requests.request("GET", url, headers=self.headers, json=payload)
        logger.debug(f"[GET] {url}\nStatus code: {response.status_code}\nResponse: {response.text}")

        if response.status_code == 200:
            try:
                clusters = response.json().get("clusters", [])
                logger.debug(f"Clusters received: {clusters}")
                return clusters
            except Exception as e:
                logger.error(f"Ошибка при разборе JSON: {e}")
                return []
        else:
            logger.warning(f"Не удалось получить кластеры: {response.status_code}, {response.text}")
            return []

    def get_stats_keywords(self, advert_id: int, start_date: str, end_date: str) -> list[dict]:
        """
        Получает статистику по ключевым фразам за указанный период, разбивая на недельные интервалы.

        Args:
            advert_id: ID рекламной кампании.
            start_date: Начальная дата в формате 'YYYY-MM-DD'.
            end_date: Конечная дата в формате 'YYYY-MM-DD'.

        Returns:
            Список словарей со статистикой по ключевым фразам.
        """
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d').date()
            end = datetime.strptime(end_date, '%Y-%m-%d').date()
        except ValueError as e:
            logger.error(f"Ошибка формата даты: {e}")
            return []

        if start > end:
            logger.error("start_date должна быть раньше end_date")
            return []

        max_days_per_request = 7
        result = []
        current_start = start

        while current_start <= end:
            current_end = min(current_start + timedelta(days=max_days_per_request - 1), end)

            params = {
                "advert_id": advert_id,
                "from": current_start.isoformat(),
                "to": current_end.isoformat()
            }

            logger.debug(f"Запрос статистики для {advert_id}: {current_start} - {current_end}")
            data = self.make_request("/v0/stats/keywords", method="GET", params=params)

            if not data or "keywords" not in data:
                logger.error(f"Не удалось получить данные для кампании {advert_id}. Ответ API: {data}")
            else:
                for block in data["keywords"]:
                    date_str = block.get("date")
                    for stat in block.get("stats", []):
                        result.append({
                            "date": date_str,
                            "keyword": stat.get("keyword"),
                            "views": stat.get("views"),
                            "clicks": stat.get("clicks"),
                            "ctr": stat.get("ctr"),
                            "sum": stat.get("sum"),
                        })

            time.sleep(1)
            current_start = current_end + timedelta(days=1)

        if not result:
            logger.warning(
                f"Нет данных по ключевым фразам для кампании {advert_id} за период {start_date} - {end_date}")

        return result

    def print_keyword_stats(self, stats: list[dict]):
        """
        Выводит статистику по ключевым словам в табличном формате.
        """
        if not stats:
            print("Нет данных по ключевым фразам за запрошенный период.")
            return

        print(f"{'Дата':<12} {'Ключевая фраза':<25} {'Просм.':>6} {'Кл.':>4} {'CTR':>6} {'Сумма':>8}")
        print("-" * 65)
        for rec in stats:
            shows = rec.get('shows', 0)
            clicks = rec.get('clicks', 0)
            ctr = rec.get('ctr', 0.0)
            spend = rec.get('sum', 0.0)
            print(f"{rec['date']:<12} {rec['keyword'][:25]:<25} "
                  f"{shows:>6} {clicks:>4} {ctr:>5.2f}% {spend:>8.2f}₽")