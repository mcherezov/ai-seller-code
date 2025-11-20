import os
import time
from typing import Any, Dict, List, Optional, Tuple
import asyncio
import aiohttp
from aiohttp import ClientResponseError


class OzonAsyncClient:
    """
    Асинхронный клиент для Ozon API на aiohttp.
    Поддерживает:
      - Авторизацию для Performance API (client credentials)
      - Методы для работы с рекламой, комиссиями, заказами, продажами и воронкой продаж.
    """

    # Performance API (Ozon Performance)
    _PERF_BASE = "https://api-performance.ozon.ru/api/client"
    _PERF_TOKEN = f"{_PERF_BASE}/token"
    _PERF_EXPENSE = f"{_PERF_BASE}/statistics/expense"
    _PERF_STATS = f"{_PERF_BASE}/statistics"
    _PERF_REPORT = f"{_PERF_BASE}/statistics/report"

    # Marketplace API (Ozon Seller)
    _MP_BASE = "https://api-seller.ozon.ru"
    _PRICE_INFO = f"{_MP_BASE}/v5/product/info/prices"
    _FINANCE = f"{_MP_BASE}/v3/finance/transaction/list"
    _ANALYTICS = f"{_MP_BASE}/v1/analytics/data"
    _REPORT_CREATE = f"{_MP_BASE}/v1/report/postings/create"
    _REPORT_INFO = f"{_MP_BASE}/v1/report/info"

    def __init__(
        self,
        client_id: str,
        api_key: str,
        client_secret: Optional[str] = None,
        performance_scopes: Optional[List[str]] = None,
    ):
        self.client_id = client_id
        self.api_key = api_key
        self.client_secret = client_secret
        self._perf_token: Optional[str] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "OzonAsyncClient":
        headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self._session = aiohttp.ClientSession(headers=headers)
        # если есть client_secret — сразу авторизуемся для Performance API
        if self.client_secret:
            await self._authorize_performance()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    async def _authorize_performance(self) -> None:
        assert self._session, "Session not initialized"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials"
        }
        async with self._session.post(self._PERF_TOKEN, json=payload, timeout=30) as resp:
            resp.raise_for_status()
            token_data = await resp.json()
        self._perf_token = token_data.get("access_token")
        # добавим заголовок для Performance
        self._session._default_headers["Authorization"] = f"Bearer {self._perf_token}"

    async def fetch_ad_campaign_expense(
        self,
        date_from: str,
        date_to: str
    ) -> Tuple[bytes, Dict[str, str]]:
        """
        GET /statistics/expense — CSV статистика рекламных кампаний
        date_from/ date_to в формате YYYY-MM-DD
        """
        assert self._session, "Use 'async with' to initialize session"
        params = {"dateFrom": date_from, "dateTo": date_to}
        async with self._session.get(self._PERF_EXPENSE, params=params, timeout=60) as resp:
            resp.raise_for_status()
            return await resp.read(), dict(resp.headers)

    async def fetch_commission(
            self,
            visibility: str = "ALL",
            limit: int = 1000,
            cursor: str = ""
    ) -> Tuple[Dict[str, Any], Dict[str, str], int]:
        """
        POST /v5/product/info/prices — комиссия по товарам
        Пагинация через cursor
        """
        assert self._session, "Use 'async with' to initialize session"
        payload = {"filter": {"visibility": visibility}, "cursor": cursor, "limit": limit}
        async with self._session.post(self._PRICE_INFO, json=payload, timeout=30) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
            return data, dict(resp.headers), resp.status

    async def fetch_sales(
            self,
            date_from: str,
            date_to: str,
            page: int = 1,
            page_size: int = 1000
    ) -> Tuple[Dict[str, Any], Dict[str, str], int]:
        """
        POST /v3/finance/transaction/list — список финансовых операций
        date_from/date_to в ISO
        """
        assert self._session, "Use 'async with' to initialize session"
        payload = {
            "filter": {"date": {"from": date_from, "to": date_to}},
            "page": page,
            "page_size": page_size
        }
        async with self._session.post(self._FINANCE, json=payload, timeout=60) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
            return data, dict(resp.headers), resp.status


    async def fetch_sales_funnel(
            self,
            date_from: str,
            date_to: str,
            metrics: List[str],
            dimension: List[str],
            limit: int = 1000,
            offset: int = 0
    ) -> tuple[Dict[str, Any], Dict[str, str], int]:
        assert self._session, "Use 'async with' to initialize session"
        payload = {
            "date_from": date_from,
            "date_to": date_to,
            "metrics": metrics,
            "dimension": dimension,
            "limit": limit,
            "offset": offset
        }
        async with self._session.post(self._ANALYTICS, json=payload, timeout=60) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data, dict(resp.headers), resp.status

    async def create_orders_report(
            self,
            date_from: str,
            date_to: str,
            delivery_schema: str = "fbo"
    ) -> str:
        """
        POST /v1/report/postings/create — создаёт отчёт заказов
        Возвращает code (этапы create/status мы не сохраняем в бронзу)
        """
        assert self._session, "Use 'async with' to initialize session"
        payload = {
            "filter": {
                "processed_at_from": date_from,
                "processed_at_to": date_to,
                "delivery_schema": [delivery_schema]
            },
            "language": "DEFAULT"
        }
        async with self._session.post(self._REPORT_CREATE, json=payload, timeout=60) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
        return (data.get("result") or {}).get("code", "")

    async def get_report_status(
            self,
            code: str
    ) -> Tuple[Dict[str, Any], Dict[str, str], int]:
        """
        POST /v1/report/info — проверяет статус отчёта
        Возвращает (полный JSON, headers, http_status)
        (в JSON при готовности должен быть file_url)
        """
        assert self._session, "Use 'async with' to initialize session"
        async with self._session.post(self._REPORT_INFO, json={"code": code}, timeout=60) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
            return data, dict(resp.headers), resp.status

    async def download_orders_report(
            self,
            file_url: str
    ) -> Tuple[bytes, Dict[str, str], int]:
        """
        GET URL скачивания отчёта
        Возвращает (байты файла, headers, http_status)
        """
        assert self._session, "Use 'async with' to initialize session"
        async with self._session.get(file_url, timeout=120) as resp:
            resp.raise_for_status()
            return await resp.read(), dict(resp.headers), resp.status

    async def fetch_orders(
        self,
        date_from: str,
        date_to: str,
        schemas: List[str] = ["fbo", "fbs"]
    ) -> List[bytes]:
        """
        Полный цикл создания, ожидания и скачивания отчётов для списка схем.
        """
        results = []
        for schema in schemas:
            code = await self.create_orders_report(date_from, date_to, schema)
            # Poll until ready
            for _ in range(60):
                status = await self.get_report_status(code)
                if status == "success":
                    break
                await asyncio.sleep(30)
            # Получаем URL из info
            info = await self._session.post(self._REPORT_INFO, json={"code": code})
            info.raise_for_status()
            file_url = (await info.json()).get("result", {}).get("file")
            data = await self.download_orders_report(file_url)
            results.append(data)
        return results
