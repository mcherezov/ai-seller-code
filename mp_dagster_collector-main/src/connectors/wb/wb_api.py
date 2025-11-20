import os
import re
import json
from typing import Any, Dict, List, Optional, Union, Tuple
import asyncio
import aiohttp
from aiohttp import ClientResponseError
from datetime import datetime
from zoneinfo import ZoneInfo
from src.connectors.wb.utils import retry_download

# Новые базовые URL для API Wildberries
COMMON_API_URL     = "https://common-api.wildberries.ru/api/v1"
ANALYTICS_BASE_URL = "https://seller-analytics-api.wildberries.ru"
ANALYTICS_V1_URL   = f"{ANALYTICS_BASE_URL}/api/v1"
ANALYTICS_V2_URL   = f"{ANALYTICS_BASE_URL}/api/v2"
STATISTICS_API_URL = "https://statistics-api.wildberries.ru/api/v1"
ADVERT_API_URL     = "https://advert-api.wildberries.ru"
CONTENT_API_URL = "https://content-api.wildberries.ru"


class WildberriesAsyncClient:
    """
    Асинхронный клиент для Wildberries API на aiohttp.
    Использует новые корневые URL для разных категорий эндпоинтов.
    Берёт токен из WB_API_TOKEN (или принимает в __init__).

    Пример:
        async with WildberriesAsyncClient() as client:
            orders = await client.fetch_orders()
    """

    def __init__(self, token: Optional[str] = None, token_id: Optional[int] = None):
        self.token = token or os.getenv("WB_API_TOKEN")
        if not self.token:
            raise ValueError("Не задан WB_API_TOKEN")

        self.token_id = token_id
        self._session: Optional[aiohttp.ClientSession] = None


    async def __aenter__(self) -> "WildberriesAsyncClient":
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        self._session = aiohttp.ClientSession(headers=headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    async def fetch_orders(
            self,
            date_from: Optional[str] = None,
            flag: int = 0,
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        """
        GET /supplier/orders — заказы поставщика.
        Параметры:
          - dateFrom: строка в формате YYYY-MM-DDTHH:MM:SS (MSK, без таймзоны; 'T' сохраняется, 'Z'/оффсет удаляются)
          - flag: 0 — фильтр по last_change_date; 1 — фильтр по дате создания
        Возвращает кортеж: (данные, заголовки)
        """
        assert self._session, "Use 'async with' to initialize session"
        url = f"{STATISTICS_API_URL}/supplier/orders"
        params: Dict[str, Any] = {}

        def _to_msk_iso_no_tz(df: Optional[str]) -> str:
            """
            Нормализует входную дату/дату-время к 'YYYY-MM-DDTHH:MM:SS' в MSK без таймзоны.
            Принимает варианты:
              - 'YYYY-MM-DD'
              - 'YYYY-MM-DDTHH:MM:SS'
              - с 'Z' или с оффсетом (например, '+03:00')
              - None → текущее время MSK
            """
            if not df:
                dt = datetime.now(ZoneInfo("Europe/Moscow"))
                return dt.strftime("%Y-%m-%dT%H:%M:%S")

            s = df.strip()
            try:
                if s.endswith("Z"):
                    # Преобразуем 'Z' → UTC, затем в MSK и убираем tzinfo
                    dt = datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(ZoneInfo("Europe/Moscow")).replace(
                        tzinfo=None)
                elif re.search(r"[+-]\d{2}:\d{2}$", s):
                    # Вход с оффсетом → переведём в MSK и уберём tzinfo
                    dt = datetime.fromisoformat(s).astimezone(ZoneInfo("Europe/Moscow")).replace(tzinfo=None)
                else:
                    # Считаем, что это уже MSK без таймзоны: дата или дата-время
                    dt = datetime.fromisoformat(s if "T" in s else f"{s}T00:00:00")
            except Exception:
                # На всякий случай: попробуем как 'YYYY-MM-DD'
                dt = datetime.fromisoformat(s.split("T")[0]).replace(hour=0, minute=0, second=0, microsecond=0)

            return dt.strftime("%Y-%m-%dT%H:%M:%S")

        params["dateFrom"] = _to_msk_iso_no_tz(date_from)
        params["flag"] = int(flag)

        async with self._session.get(url, params=params, timeout=60) as resp:
            resp.raise_for_status()
            return await resp.json(), dict(resp.headers)

    async def fetch_commission(self, locale: str = "ru") -> Tuple[Dict[str, Any], Dict[str, str]]:
        """
        GET /tariffs/commission — данные комиссий по категориям.
        Параметры: только locale (по умолчанию: ru).
        Возвращает кортеж: (данные, заголовки)
        """
        assert self._session
        url = f"{COMMON_API_URL}/tariffs/commission"
        params = {"locale": locale}

        async with self._session.get(url, params=params, timeout=60) as resp:
            resp.raise_for_status()
            return await resp.json(), dict(resp.headers)

    async def fetch_ad_config(self) -> Tuple[Dict[str, Any], Dict[str, str]]:
        """
        GET /adv/v0/config — конфигурации рекламных кампаний (advert)
        Возвращает кортеж: (данные, заголовки)
        """
        assert self._session
        url = f"{ADVERT_API_URL}/adv/v0/config"
        async with self._session.get(url, timeout=60) as resp:
            resp.raise_for_status()
            return await resp.json(), dict(resp.headers)

    async def fetch_sales_funnel(
            self,
            payload: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        """
        POST /api/v2/nm-report/detail — агрегированная воронка по NM.

        Параметры в payload (минимально достаточные, см. доку WB):
          - timezone: "Europe/Moscow"
          - period: { "begin": "YYYY-MM-DD HH:MM:SS", "end": "YYYY-MM-DD HH:MM:SS" }
          - page: <int>  # номер страницы, начинается с 1

        Метод сам выполняет пагинацию по флагу `data.isNextPage` и
        возвращает кортеж:
            ({"data": {"cards": [...], "isNextPage": false, "pages": N}}, last_headers)

        Где `cards` — конкатенация карточек со всех страниц,
        `last_headers` — заголовки последнего успешного ответа WB.
        """
        assert payload is not None and isinstance(payload, dict), "payload must be dict"

        base_payload = dict(payload)
        start_page = base_payload.get("page", 1)
        try:
            start_page = int(start_page)
        except Exception:
            start_page = 1
        base_payload.pop("page", None)  # дальше будем подставлять сами

        url = f"{getattr(self, 'ANALYTICS_V2_URL', 'https://seller-analytics-api.wildberries.ru')}/api/v2/nm-report/detail"

        all_cards = []
        page = max(start_page, 1)
        pages_done = 0
        last_headers: Dict[str, str] = {}


        SLEEP_BETWEEN_PAGES_SEC = 0.35

        async def _do_request(req_payload: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, str]]:
            """Унифицированный запрос: пытаемся использовать self.request(...), иначе aiohttp-сессию напрямую."""
            # Новый клиент (wb_api_v2): есть универсальный self.request
            if hasattr(self, "request"):
                r = await self.request("POST", url, json_body=req_payload)
                body_text = getattr(r, "body_text", None)
                body_bytes = getattr(r, "body_bytes", None)
                if body_text is None and body_bytes:
                    try:
                        body_text = body_bytes.decode("utf-8", "ignore")
                    except Exception:
                        body_text = None
                try:
                    data = json.loads(body_text) if isinstance(body_text, str) else {}
                except Exception:
                    data = {}
                headers = dict(getattr(r, "headers", {}) or {})
                # Добавим статус в заголовки, чтобы верхний уровень мог его увидеть
                status = getattr(r, "status", None)
                if status is not None:
                    headers.setdefault("status", str(status))
                return data, headers

            assert getattr(self, "_session", None), "aiohttp session is not initialized"
            headers = {}

            async with self._session.post(url, json=req_payload, headers=headers, timeout=120) as resp:
                resp.raise_for_status()
                data = await resp.json()
                hdrs = dict(resp.headers)
                hdrs.setdefault("status", str(resp.status))
                return data, hdrs

        # ── цикл пагинации ─────────────────────────────────────────────────────────
        while True:
            req_payload = dict(base_payload)
            req_payload["page"] = page

            data, hdrs = await _do_request(req_payload)
            last_headers = hdrs or {}
            pages_done += 1

            # Извлекаем карточки и флаг «есть следующая страница»
            d = data if isinstance(data, dict) else {}
            data_block = d.get("data") or {}
            cards = data_block.get("cards") or []
            is_next = bool(data_block.get("isNextPage", False))

            if isinstance(cards, list) and cards:
                all_cards.extend(cards)

            if not is_next:
                break

            page += 1
            await asyncio.sleep(SLEEP_BETWEEN_PAGES_SEC)

        agg = {
            "data": {
                "cards": all_cards,
                "isNextPage": False,
                "pages": pages_done,
            }
        }
        if last_headers is None:
            last_headers = {}
        last_headers = dict(last_headers)
        last_headers.setdefault("x-aggregated-pages", str(pages_done))

        return agg, last_headers

    async def request_stocks_report(self, params: Dict[str, Any]) -> Dict[str, Any]:
        assert self._session
        url = f"{ANALYTICS_V1_URL}/warehouse_remains"

        def _normalize_params(params: Dict[str, Any]) -> Dict[str, str]:
            return {k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in params.items()}

        normalized_params = _normalize_params(params)  # 🔁 конвертируем bool → "true"/"false"
        async with self._session.get(url, params=normalized_params, timeout=120) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_stocks_report_status(
        self,
        task_id: Union[int, str],
    ) -> Dict[str, Any]:
        """
        GET /warehouse_remains/tasks/{task_id}/status (analytics v1)
        """
        assert self._session
        url = f"{ANALYTICS_V1_URL}/warehouse_remains/tasks/{task_id}/status"
        async with self._session.get(url, timeout=60) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def download_stocks_report(
            self,
            task_id: Union[int, str],
    ) -> Tuple[bytes, Dict[str, str]]:
        """
        GET /warehouse_remains/tasks/{task_id}/download (analytics v1)
        Возвращает кортеж: (сырые байты, заголовки)
        """
        assert self._session
        url = f"{ANALYTICS_V1_URL}/warehouse_remains/tasks/{task_id}/download"
        return await retry_download(self._session, url)

    async def fetch_paid_storage(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        GET /paid_storage — платное хранение (analytics v1)
        """
        assert self._session
        url = f"{ANALYTICS_V1_URL}/paid_storage"
        params: Dict[str, Any] = {}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to
        async with self._session.get(url, params=params, timeout=60) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_paid_storage_status(
        self,
        task_id: Union[int, str],
    ) -> Dict[str, Any]:
        """
        GET /paid_storage/tasks/{task_id}/status (analytics v1)
        """
        assert self._session
        url = f"{ANALYTICS_V1_URL}/paid_storage/tasks/{task_id}/status"
        async with self._session.get(url, timeout=60) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def download_paid_storage(
            self,
            task_id: Union[int, str],
    ) -> Tuple[bytes, Dict[str, str]]:
        """
        GET /paid_storage/tasks/{task_id}/download (analytics v1)
        Возвращает кортеж: (сырые байты, заголовки)
        """
        assert self._session
        url = f"{ANALYTICS_V1_URL}/paid_storage/tasks/{task_id}/download"
        return await retry_download(self._session, url)

    async def fetch_paid_acceptions(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        GET /acceptance_report — платная приёмка (analytics v1)
        """
        assert self._session
        url = f"{ANALYTICS_V1_URL}/acceptance_report"
        params: Dict[str, Any] = {}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to
        async with self._session.get(url, params=params, timeout=60) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_paid_acceptions_status(
        self,
        task_id: Union[int, str],
    ) -> Dict[str, Any]:
        """
        GET /acceptance_report/tasks/{task_id}/status (analytics v1)
        """
        assert self._session
        url = f"{ANALYTICS_V1_URL}/acceptance_report/tasks/{task_id}/status"
        async with self._session.get(url, timeout=60) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def download_paid_acceptions(
            self,
            task_id: Union[int, str],
    ) -> Tuple[bytes, Dict[str, str]]:
        """
        GET /acceptance_report/tasks/{task_id}/download (analytics v1)
        Возвращает кортеж: (сырые байты, заголовки)
        """
        assert self._session
        url = f"{ANALYTICS_V1_URL}/acceptance_report/tasks/{task_id}/download"
        return await retry_download(self._session, url)

    async def fetch_suppliers(
            self,
            date_from: Optional[str] = None,
            date_to: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
        """
        GET /supplier/incomes — доходы поставщика
        Возвращает кортеж: (данные, заголовки)
        """
        assert self._session
        url = f"{STATISTICS_API_URL}/supplier/incomes"
        params = {}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to

        try:
            async with self._session.get(url, params=params, timeout=60) as resp:
                resp.raise_for_status()
                return await resp.json(), dict(resp.headers)
        except ClientResponseError as e:
            if e.status == 404:
                return [], {}
            raise

    async def get_advert_ids(
            self,
            date_from: str,
            date_to: str
    ) -> List[int]:
        """
        GET /adv/v1/promotion/count — список рекламных кампаний
        """
        assert self._session
        url = f"{ADVERT_API_URL}/adv/v1/promotion/count"
        params = {
            "dateFrom": date_from,
            "dateTo": date_to
        }
        async with self._session.get(url, params=params, timeout=60) as resp:
            resp.raise_for_status()
            data = await resp.json()
            ids = [
                advert["advertId"]
                for group in data.get("adverts", [])
                for advert in group.get("advert_list", [])
                if "advertId" in advert
            ]
            return ids


    async def get_advert_list(self) -> Tuple[Dict[str, Any], Dict[str, str]]:
        """
        GET /adv/v1/promotion/count — списки всех РК, сгруппированные по типу и статусу,
        с датой последнего изменения кампаний.
        Возвращает кортеж: (полный JSON-ответ, заголовки ответа).
        """
        assert self._session
        url = f"{ADVERT_API_URL}/adv/v1/promotion/count"
        async with self._session.get(url, timeout=60) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data, dict(resp.headers)


    async def get_advert_list_depr(
        self,
        date_from: str,
        date_to: str
    ) -> List[Dict[str, Any]]:
        """
        GET /adv/v1/promotion/count — полный список рекламных кампаний
        за период, вместе со статусами.
        """
        assert self._session
        url = f"{ADVERT_API_URL}/adv/v1/promotion/count"
        params = {"dateFrom": date_from, "dateTo": date_to}
        async with self._session.get(url, params=params, timeout=60) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return [
                {
                    "advertId": advert["advertId"],
                    "status": group["status"],
                    "changeTime": advert.get("changeTime"),
                }
                for group in data.get("adverts", [])
                for advert in group.get("advert_list", [])
                if "advertId" in advert
            ]

    async def fetch_ad_stats(self, payload: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
        """
        POST /adv/v2/fullstats — статистика по рекламе (advert).
        Возвращает: (список записей, заголовки)
        """
        assert self._session
        url = f"{ADVERT_API_URL}/adv/v2/fullstats"

        async with self._session.post(url, json=payload, timeout=120) as resp:
            body = await resp.text()
            headers = dict(resp.headers)
            headers["status"] = str(resp.status)

            if resp.status >= 400:
                err_text = body
                try:
                    j = json.loads(body)
                    if isinstance(j, dict) and isinstance(j.get("error"), str):
                        err_text = j["error"]
                except Exception:
                    pass

                if "there are no companies with correct intervals" in err_text.lower():
                    headers["wb-error"] = err_text
                    return [], headers

                raise aiohttp.ClientResponseError(
                    request_info=resp.request_info,
                    history=resp.history,
                    status=resp.status,
                    message=f"{resp.reason}. body={body[:2000]}",
                    headers=resp.headers,
                )

            try:
                data = json.loads(body)
            except Exception as e:
                raise RuntimeError(f"WB returned non-JSON body on success: {body[:500]}") from e

            return data, headers


    async def fetch_ad_info(self, payload: List[int]) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
        """
        POST /adv/v1/promotion/adverts — инфо по объявлениям (advert)
        Ограничение: не более 50 ID за раз.
        Возвращает кортеж: (объединённый список записей, заголовки последнего запроса)
        """
        assert self._session
        url = f"{ADVERT_API_URL}/adv/v1/promotion/adverts"
        all_data = []
        last_headers = {}

        chunks = [payload[i:i + 50] for i in range(0, len(payload), 50)]
        for chunk in chunks:
            for attempt in range(3):  # retry loop
                try:
                    async with self._session.post(url, json=chunk, timeout=120) as resp:
                        resp.raise_for_status()
                        data = await resp.json()
                        all_data.extend(data)
                        last_headers = dict(resp.headers)
                        break  # break out of retry loop if success
                except aiohttp.ClientResponseError as e:
                    if e.status == 429 and attempt < 2:
                        await asyncio.sleep(2 ** attempt)  # exponential backoff
                        continue
                    raise
            await asyncio.sleep(0.4)  # пауза между chunk-запросами

        return all_data, last_headers

    async def fetch_clusters_batch(self, advert_ids: List[int]) -> List[Tuple[Dict[str, Any], Dict[str, str]]]:
        """
        Обходит список advert_ids, вызывает GET /adv/v2/auto/stat-words по каждому ID.
        Возвращает список кортежей: [(ответ по кампании, заголовки), ...]
        """
        assert self._session
        url = f"{ADVERT_API_URL}/adv/v2/auto/stat-words"
        results = []

        for advert_id in advert_ids:
            params = {"id": advert_id}
            try:
                async with self._session.get(url, params=params, timeout=60) as resp:
                    resp.raise_for_status()
                    headers = dict(resp.headers)

                    try:
                        data = await resp.json()
                    except Exception:
                        print(f"Невалидный JSON для advert_id={advert_id}, текст: {await resp.text()}")
                        data = {}

                    if not isinstance(data, dict):
                        print(f"Не dict в ответе для advert_id={advert_id}: {data}")
                        data = {}

                    results.append(({"advertId": advert_id, **data}, headers))
            except ClientResponseError as e:
                if e.status == 404:
                    results.append(({"advertId": advert_id, "excluded": [], "clusters": []}, {}))
                else:
                    raise

            await asyncio.sleep(0.3)  # лимит 4 запроса/сек

        return results

    async def fetch_keywords_batch(
            self,
            advert_ids: List[int],
            date_from: str,
            date_to: str
    ) -> List[Tuple[Dict[str, Any], Dict[str, str]]]:
        """
        Обходит список advert_ids, вызывает GET /adv/v0/stats/keywords для каждого.
        Возвращает список кортежей: [(ответ, заголовки), ...]
        """
        assert self._session
        url = f"{ADVERT_API_URL}/adv/v0/stats/keywords"
        results = []

        for advert_id in advert_ids:
            params = {
                "advert_id": advert_id,
                "from": date_from,
                "to": date_to
            }

            try:
                async with self._session.get(url, params=params, timeout=60) as resp:
                    headers = dict(resp.headers)
                    try:
                        resp.raise_for_status()
                        data = await resp.json()
                        results.append(({"advertId": advert_id, **data}, headers))
                    except ClientResponseError as e:
                        if e.status in (400, 404):
                            results.append(({"advertId": advert_id, "keywords": []}, headers))
                            if e.status == 400:
                                print(
                                    f"[fetch_keywords_batch] 400 Bad Request для advert_id={advert_id}, вероятно архивирована или неактивна.")
                        else:
                            raise
            except Exception as e:
                print(f"[fetch_keywords_batch] Необработанная ошибка advert_id={advert_id}: {e}")
                raise

            await asyncio.sleep(0.3)  # соблюдение лимита 4 req/sec

        return results

    async def fetch_sku_cards_all(self, with_photo: int = -1, limit: int = 100) -> Tuple[
        List[Dict[str, Any]], Dict[str, str]]:
        """
        POST /content/v2/get/cards/list — Получение всех карточек с постраничной загрузкой.
        Возвращает (список карточек, заголовки последнего ответа)
        """
        assert self._session
        url = f"{CONTENT_API_URL}/content/v2/get/cards/list"
        cursor = {}
        all_cards = []
        last_headers = {}

        while True:
            payload = {
                "settings": {
                    "cursor": {
                        "limit": limit,
                        **cursor
                    },
                    "filter": {
                        "withPhoto": with_photo
                    }
                }
            }

            async with self._session.post(url, json=payload, timeout=120) as resp:
                resp.raise_for_status()
                data = await resp.json()
                last_headers = dict(resp.headers)

            cards = data.get("cards", [])
            if not cards:
                break

            all_cards.extend(cards)

            cursor_data = data.get("cursor", {})
            if cursor_data.get("total", 0) < limit:
                break

            cursor = {
                "updatedAt": cursor_data.get("updatedAt"),
                "nmID": cursor_data.get("nmID")
            }

            await asyncio.sleep(0.3)

        return all_cards, last_headers

    async def fetch_product_search_texts(
        self,
        payload: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        """
        POST /api/v2/search-report/product/search-texts — поиск текстов по товару (analytics v2).
        Параметры payload:
          - nmId           (int)   — идентификатор товара
          - topOrderBy     (str)   — одно из: openToCart, openCard, addToCart, orders, cartToOrder
          - orderBy.field  (str)   — поле сортировки, например: openToCart
          - orderBy.mode   (str)   — 'asc' или 'desc'
          (при необходимости можно добавить другие опции из спецификации)
        Возвращает кортеж: (данные JSON, заголовки ответа)
        """
        assert self._session, "Use 'async with' to initialize session"
        url = f"{ANALYTICS_V2_URL}/search-report/product/search-texts"
        async with self._session.post(url, json=payload, timeout=120) as resp:
            resp.raise_for_status()
            return await resp.json(), dict(resp.headers)
