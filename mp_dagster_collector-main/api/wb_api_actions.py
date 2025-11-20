import requests
import logging
from typing import List, Dict, Any, Optional
import time
import re

class WildberriesAPIactions:
    BASE_URL = "https://advert-api.wildberries.ru/adv"

    def __init__(self, token: str, logger: logging.Logger, use_bearer: bool = True):
        """
        Инициализирует класс для работы с API Wildberries.

        Args:
            token (str): Токен для авторизации в API.
            logger (logging.Logger): Логгер для записи событий и ошибок.
            use_bearer (bool): Использовать ли префикс 'Bearer' в заголовке Authorization.
        """
        self.logger = logger
        self.token = token.strip() if token else ""
        if not self.token:
            self.logger.error("Токен API пустой или отсутствует")
            raise ValueError("Токен API не предоставлен")
        self.use_bearer = use_bearer
        self.headers = {
            "Authorization": f"Bearer {self.token}" if use_bearer else self.token,
            "Content-Type": "application/json"
        }
        self.logger.debug(f"Инициализация WildberriesAPIactions с токеном (первые 10 символов): {self.token[:10]}...")
        self.logger.debug(f"Заголовок Authorization: {self.headers['Authorization']}")

    def make_request(self, endpoint: str, method: str = "GET", params: Optional[Dict[str, str]] = None,
                     data: Optional[Dict[str, Any]] = None, retries: int = 3, backoff_factor: float = 1.0) -> Optional[Any]:
        url = f"{self.BASE_URL}{endpoint}"
        self.logger.debug(f"[{method}] Request to {url} with params {params} and data {data}")

        for attempt in range(retries):
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=self.headers,
                    params=params,
                    json=data,
                    timeout=30
                )
                self.logger.info(f"[{method}] {url} | Status: {response.status_code}")

                if response.status_code == 204:
                    return None

                response.raise_for_status()

                if response.text:
                    try:
                        return response.json()
                    except ValueError as e:
                        self.logger.error(f"JSON parse error: {e} | Response: {response.text[:500]}")
                        return {"errors": f"JSON parse error: {e}"}
                return {}

            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    sleep_time = backoff_factor * (2 ** attempt)
                    self.logger.warning(f"Rate limit exceeded. Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    continue
                elif response.status_code == 401:
                    self.logger.error(f"Unauthorized: Check token. | Response: {response.text}")
                    return {"errors": response.text}
                elif response.status_code == 400:
                    self.logger.error(f"Bad request: {response.text}")
                    return {"errors": response.text}
                else:
                    self.logger.error(f"HTTP error: {e} | Response: {response.text[:500]}")
                    return {"errors": response.text}

            except requests.exceptions.Timeout:
                self.logger.error(f"Request timeout: {url}")
                return {"errors": "Request timeout"}

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request error: {e} | URL: {url}")
                return {"errors": str(e)}

        self.logger.error(f"Failed to execute request after {retries} attempts: {url}")
        return {"errors": f"Failed after {retries} attempts"}

    def set_fixed_phrases(self, campaign_id: int, fixed_phrases: List[str], retries: int = 3, backoff_factor: float = 0.5) -> Optional[List[str]]:
        """
        Устанавливает или удаляет фиксированные фразы для кампании Аукцион.

        Args:
            campaign_id (int): ID кампании.
            fixed_phrases (List[str]): Список фиксированных фраз (пустой список для удаления всех фраз). Максимум 100 фраз.
            retries (int): Количество попыток при ошибке (по умолчанию 3).
            backoff_factor (float): Множитель задержки при ретраях (по умолчанию 0.5).

        Returns:
            Optional[List[str]]: Список установленных фраз при успехе (код 200), None при ошибке.
        """
        if not isinstance(campaign_id, int):
            self.logger.error("Campaign ID must be an integer.")
            return None

        if len(fixed_phrases) > 100:
            self.logger.error(f"Exceeded limit of 100 fixed phrases (got {len(fixed_phrases)}).")
            return None

        endpoint = "/v1/search/set-plus"
        params = {"id": str(campaign_id)}
        data = {"pluse": fixed_phrases}

        response = self.make_request(
            endpoint=endpoint,
            method="POST",
            params=params,
            data=data,
            retries=retries,
            backoff_factor=backoff_factor
        )

        if isinstance(response, list):
            self.logger.info(f"Fixed phrases set successfully for campaign {campaign_id}: {response}")
            return response

        self.logger.error(f"Failed to set fixed phrases for campaign {campaign_id}: {response}")
        return None

    def set_excluded_phrases(self, campaign_id: int, excluded_phrases: List[str]) -> Optional[Dict]:
        """
        Устанавливает или удаляет минус-фразы для автоматической кампании.

        Args:
            campaign_id (int): ID кампании.
            excluded_phrases (List[str]): Список минус-фраз (пустой список для удаления всех фраз).

        Returns:
            Optional[Dict]: Ответ API или None в случае ошибки.
        """
        if not isinstance(campaign_id, int):
            self.logger.error("Campaign ID must be an integer.")
            return None

        if len(excluded_phrases) > 1000:
            self.logger.error("Exceeded limit of 1000 minus phrases.")
            return None

        endpoint = "/v1/auto/set-excluded"
        params = {"id": str(campaign_id)}
        data = {"excluded": excluded_phrases}

        response = self.make_request(
            endpoint=endpoint,
            method="POST",
            params=params,
            data=data
        )

        if response is None or (isinstance(response, dict) and 'errors' not in response):
            self.logger.info(f"Minus phrases updated successfully for campaign {campaign_id}.")
            return {}

        self.logger.error(f"Failed to set/remove minus phrases for campaign {campaign_id}: {response.get('errors', response)}")
        return None

    def set_search_excluded_phrases(self, campaign_id: int, excluded_phrases: List[str], retries: int = 3, backoff_factor: float = 0.5) -> bool:
        """
        Устанавливает или удаляет минус-фразы для кампании Аукцион в поиске.

        Args:
            campaign_id (int): ID кампании.
            excluded_phrases (List[str]): Список минус-фраз (пустой список для удаления всех фраз). Максимум 1000 фраз.
            retries (int): Количество попыток при ошибке (по умолчанию 3).
            backoff_factor (float): Множитель задержки при ретраях (по умолчанию 0.5).

        Returns:
            bool: True в случае успеха (код 200 или 204), False в случае ошибки.
        """
        if not isinstance(campaign_id, int):
            self.logger.error("Campaign ID must be an integer.")
            return False

        if len(excluded_phrases) > 1000:
            self.logger.error(f"Exceeded limit of 1000 minus phrases (got {len(excluded_phrases)}).")
            return False

        endpoint = "/v1/search/set-excluded"
        params = {"id": str(campaign_id)}
        data = {"excluded": excluded_phrases}

        response = self.make_request(
            endpoint=endpoint,
            method="POST",
            params=params,
            data=data,
            retries=retries,
            backoff_factor=backoff_factor
        )

        if response is None or isinstance(response, list) or (isinstance(response, dict) and 'errors' not in response):
            self.logger.info(f"Search minus phrases updated successfully for campaign {campaign_id}: {excluded_phrases}")
            return True

        self.logger.error(f"Failed to set/remove search minus phrases for campaign {campaign_id}: {response.get('errors', response)}")
        return False

    def set_bids(self, bids: List[Dict[str, Any]], bandit: Optional['UCBBanditLogic'] = None, retries: int = 3, backoff_factor: float = 1.0) -> bool:
        """
        Изменяет ставки карточек товаров по артикулам WB в автоматических кампаниях и Аукционе.
        url: https://dev.wildberries.ru/openapi/promotion#tag/Upravlenie-kampaniyami/paths/~1adv~1v0~1bids/patch

        Если ставка ниже минимально допустимой, метод извлекает минимальное значение (`min`) из ответа API,
        повторяет запрос с скорректированной ставкой и обновляет min_bid в бандите, если он передан.

        Args:
            bids (List[Dict[str, Any]]): Список объектов с данными о ставках.
                Каждый объект должен содержать advert_id и nm_bids (список с nm и bid).
                Пример: [{"advert_id": 6348555, "nm_bids": [{"nm": 3462354, "bid": 500}]}]
            bandit (Optional[UCBBanditLogic]): Объект бандита для обновления min_bid (если передан).
            retries (int): Количество попыток при ошибке (по умолчанию 3).
            backoff_factor (float): Множитель задержки при ретраях (по умолчанию 1.0).

        Returns:
            bool: True в случае успеха (204), False в случае ошибки.
        """
        if not bids or len(bids) > 20:
            self.logger.error(f"Invalid bids: empty or exceeds 20 items limit (got {len(bids)}).")
            return False

        for bid in bids:
            if not isinstance(bid.get("advert_id"), int) or not bid.get("nm_bids"):
                self.logger.error(f"Invalid bid format: {bid}")
                return False
            for nm_bid in bid.get("nm_bids", []):
                if not isinstance(nm_bid.get("nm"), int) or not isinstance(nm_bid.get("bid"), int):
                    self.logger.error(f"Invalid nm_bid format: {nm_bid}")
                    return False

        endpoint = "/v0/bids"
        data = {"bids": bids}
        max_correction_attempts = 1

        for correction_attempt in range(max_correction_attempts + 1):
            response = self.make_request(
                endpoint=endpoint,
                method="PATCH",
                data=data,
                retries=retries,
                backoff_factor=backoff_factor
            )

            if response is None:
                self.logger.info(f"Bids updated successfully for {len(bids)} campaigns")
                return True

            if isinstance(response, dict) and 'errors' in response:
                updated = False
                for error in response.get("errors", []):
                    detail = error.get("detail", "")
                    field = error.get("field", "")
                    min_bid_match = re.search(r"min: (\d+)", detail)
                    if min_bid_match:
                        min_bid = int(min_bid_match.group(1))
                        if bandit and hasattr(bandit.config, 'min_bid'):
                            bandit.update_min_bid(min_bid)
                        indices = re.findall(r"\[(\d+)\]", field)
                        if len(indices) == 2:
                            bid_idx, nm_bid_idx = map(int, indices)
                            try:
                                old_bid = data["bids"][bid_idx]["nm_bids"][nm_bid_idx]["bid"]
                                data["bids"][bid_idx]["nm_bids"][nm_bid_idx]["bid"] = min_bid
                                self.logger.info(
                                    f"Corrected bid for nm {data['bids'][bid_idx]['nm_bids'][nm_bid_idx]['nm']} "
                                    f"from {old_bid} to {min_bid} in advert_id {data['bids'][bid_idx]['advert_id']}"
                                )
                                updated = True
                            except (IndexError, KeyError) as e:
                                self.logger.error(f"Failed to correct bid: invalid indices in field {field} | Error: {e}")
                                return False
                        else:
                            self.logger.error(f"Invalid field format in error: {field}")
                            return False
                    else:
                        self.logger.error(f"Failed to set bids: {detail}")
                        return False

                if updated:
                    time.sleep(0.2)
                    continue

            self.logger.error(f"Failed to set bids: {response.get('errors', response)}")
            return False

        self.logger.error(f"Failed to set bids after {max_correction_attempts} correction attempts")
        return False
