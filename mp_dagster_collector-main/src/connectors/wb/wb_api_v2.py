from __future__ import annotations

import os
import re
import json
import base64
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import aiohttp
from aiohttp import ClientResponseError
from urllib.parse import urlparse, parse_qsl
from zoneinfo import ZoneInfo

# === Проектные утилиты времени (совместимы с бронзовым протоколом) ===
# parse_http_date и MSK уже есть в кодовой базе (timeutils.py)
try:
    from dagster_conf.lib.timeutils import parse_http_date, MSK  # prod-путь
except Exception:  # pragma: no cover - локальный фолбэк
    from email.utils import parsedate_to_datetime
    MSK = ZoneInfo("Europe/Moscow")

    def parse_http_date(value: str | None, fallback: datetime) -> datetime:
        if not value:
            return fallback
        try:
            dt = parsedate_to_datetime(value)
            return dt if dt.tzinfo else dt.replace(tzinfo=MSK)
        except Exception:
            return fallback


# === Базовые URL Wildberries ===
COMMON_API_URL        = "https://common-api.wildberries.ru/api/v1"
ANALYTICS_BASE_URL    = "https://seller-analytics-api.wildberries.ru"
ANALYTICS_V1_URL      = f"{ANALYTICS_BASE_URL}/api/v1"
ANALYTICS_V2_URL      = f"{ANALYTICS_BASE_URL}/api/v2"
STATISTICS_API_URL    = "https://statistics-api.wildberries.ru/api/v1"
STATISTICS_API_V2_URL = "https://statistics-api.wildberries.ru/api/v5"
ADVERT_API_URL        = "https://advert-api.wildberries.ru"
CONTENT_API_URL       = "https://content-api.wildberries.ru"


# === Единый контейнер ответа — удобно для бронзы/нормалайзеров ===
@dataclass
class WBResponse:
    status: int
    headers: Dict[str, str]
    response_dttm: datetime
    received_at: datetime
    body_text: str | None = None    # только для текст/JSON
    body_bytes: bytes | None = None # только для бинарника
    # АУДИТ отправленного запроса: как он реально ушёл на сеть
    # { "method": "GET|POST", "url": str, "query": {..}, "json_body": Any, "headers": {...}, ... }
    request: Optional[Dict[str, Any]] = None


def _is_text_like(headers: Dict[str, str]) -> bool:
    ct = (headers.get("Content-Type") or headers.get("content-type") or "").lower()
    if not ct:
        return False
    return (
        "application/json" in ct
        or ct.startswith("text/")
        or "charset=" in ct
    )


class WildberriesAsyncClient:
    """
    Универсальный асинхронный клиент WB с единым транспортом, ретраями, rate-limit
    и вспомогательными сценариями (пагинация, батчи, submit/poll/download).

    Использование:
        async with WildberriesAsyncClient() as wb:
            r = await wb.fetch_orders(date_from="2025-09-01T00:00:00")
            print(r.status, r.response_dttm, (r.body_text or "")[:200])
    """

    # ------------------------- сервисные хелперы -------------------------
    @staticmethod
    def _sanitize_token(tok: Optional[str]) -> str:
        """
        Нормализует значение токена:
        - убирает обрамляющие кавычки/пробелы/переводы строк
        - срезает случайный префикс 'Bearer ' / 'JWT ' если он уже есть
        """
        s = (tok or "").strip().strip('"').strip("'").strip()
        # если прислали уже с 'Bearer ', уберём, чтобы не было 'Bearer Bearer ...'
        s = re.sub(r"^(?:Bearer|JWT)\s+", "", s, flags=re.IGNORECASE)
        return s

    @staticmethod
    def _now_msk() -> datetime:
        return datetime.now(MSK)

    @staticmethod
    def _merge_queries(base: Dict[str, Any] | None, extra: Dict[str, Any] | None) -> Dict[str, Any]:
        """Сливает query для аудита. Списки склеиваем, строки не трогаем."""
        out: Dict[str, Any] = dict(base or {})
        for k, v in (extra or {}).items():
            if v is None:
                continue
            if k not in out:
                out[k] = v
                continue
            # Если оба — строки и одинаковые, оставляем.
            if isinstance(out[k], str) and isinstance(v, str):
                if out[k] == v:
                    continue
                # Разные строки → собираем в уникальный список
                out[k] = sorted({out[k], v})
                continue
            # Если список + список
            if isinstance(out[k], list) and isinstance(v, list):
                out[k] = list(dict.fromkeys(out[k] + v))
                continue
            # Если список + скаляр
            if isinstance(out[k], list):
                if v not in out[k]:
                    out[k].append(v)
                continue
            # Если скаляр + список
            if isinstance(v, list):
                merged = [out[k]]
                for it in v:
                    if it not in merged:
                        merged.append(it)
                out[k] = merged
                continue
            # Иначе — перезаписываем (последний победил)
            out[k] = v
        return out

    @staticmethod
    def _audit_make(method: str, url: str, *, params: Dict[str, Any] | None = None,
                    json_body: Any | None = None, headers: Dict[str, str] | None = None) -> Dict[str, Any]:
        return {
            "method": method.upper(),
            "url": url,
            "query": dict(params or {}),
            "json_body": json_body if json_body is not None else None,
            "headers": dict(headers or {}),
        }

    @staticmethod
    def _audit_merge_many(base_url: str, method: str, audits: List[Dict[str, Any]], *,
                          marker: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """Собирает единый audit для серии одинаковых вызовов одного URL.
        Правила:
          - method / url ставим общие
          - query сливаем по ключам (_merge_queries)
          - json_body: если одинаковый — один; если разные — список
          - добавляем служебные пометки (batched=True, chunks=.. или pagination='all')
        """
        q_acc: Dict[str, Any] = {}
        body_acc: Any = None
        heterogeneous_body = False
        for a in audits:
            q_acc = WildberriesAsyncClient._merge_queries(q_acc, a.get("query") or {})
            jb = a.get("json_body", None)
            if jb is None:
                continue
            if body_acc is None:
                body_acc = jb
            else:
                if body_acc != jb:
                    # делаем список уникальных тел
                    heterogeneous_body = True
        if heterogeneous_body:
            bodies = []
            for a in audits:
                jb = a.get("json_body", None)
                if jb is not None and jb not in bodies:
                    bodies.append(jb)
            body_acc = bodies

        out = {
            "method": method.upper(),
            "url": base_url,
            "query": q_acc,
            "json_body": body_acc,
            "batched": True,
        }
        if marker:
            out.update(marker)
        return out

    def __init__(self, token: Optional[str] = None, *, token_id: Optional[int] = None,
                 rps: int = 5, timeout: int = 120, retries: int = 3, backoff: float = 0.5):
        raw = token or os.getenv("WB_API_TOKEN")
        if not raw:
            raise ValueError("Не задан WB_API_TOKEN")
        self.token = self._sanitize_token(raw)
        self.token_id = token_id
        self._session: Optional[aiohttp.ClientSession] = None
        self._sem = asyncio.Semaphore(max(1, int(rps)))
        self._timeout = timeout
        self._retries = max(1, int(retries))
        self._backoff = max(0.0, float(backoff))

    async def __aenter__(self) -> "WildberriesAsyncClient":
        print(f"WB token starts with: {self.token[:5]}*** (len={len(self.token)})")
        self._session = aiohttp.ClientSession(
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.token}",  # уже очищенный токен
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    # ------------------------- утилиты параметров -------------------------
    @staticmethod
    def _to_msk_iso_no_tz(dt_like: Optional[str | datetime]) -> str:
        """
        Нормализует вход к 'YYYY-MM-DDTHH:MM:SS' в МСК без tz.
        Принимает:
          - 'YYYY-MM-DD'
          - 'YYYY-MM-DDTHH:MM:SS'
          - ISO с 'Z' или '+/-HH:MM'
          - datetime (с/без tzinfo)
          - None → now@MSK
        """
        if dt_like is None:
            dt = datetime.now(MSK)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        if isinstance(dt_like, datetime):
            dt = dt_like if dt_like.tzinfo else dt_like.replace(tzinfo=MSK)
            dt = dt.astimezone(MSK).replace(tzinfo=None)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")

        s = str(dt_like).strip()
        try:
            if s.endswith("Z"):
                dt = datetime.fromisoformat(s[:-1] + "+00:00").astimezone(MSK).replace(tzinfo=None)
            elif re.search(r"[+-]\d{2}:\d{2}$", s):
                dt = datetime.fromisoformat(s).astimezone(MSK).replace(tzinfo=None)
            else:
                dt = datetime.fromisoformat(s if "T" in s else f"{s}T00:00:00")
        except Exception:
            # fallback: только дата
            dt = datetime.fromisoformat(s.split("T")[0]).replace(hour=0, minute=0, second=0, microsecond=0)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def _json_message(text: str) -> str:
        try:
            j = json.loads(text)
            return j.get("error") or j.get("message") or text
        except Exception:
            return text

    # ----------------------- Универсальный транспорт ----------------------
    async def request(
        self,
        method: str,
        url: str,
        *,
        params: Dict[str, Any] | None = None,
        json_body: Any | None = None,
        data: Any | None = None,
        headers: Dict[str, str] | None = None,
        token_override: str | None = None,
    ) -> WBResponse:
        assert self._session, "Use 'async with' to initialize session"

        method_u = method.upper()
        hdrs: Dict[str, str] = {}
        if token_override:
            hdrs["Authorization"] = f"Bearer {self._sanitize_token(token_override)}"
        if headers:
            hdrs.update(headers)

        eff_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        eff_headers.update(hdrs)

        audit_request = self._audit_make(method_u, url, params=params, json_body=json_body, headers=eff_headers)

        for attempt in range(self._retries):
            async with self._sem:
                try:
                    async with self._session.request(
                        method_u,
                        url,
                        params=params,
                        json=json_body,
                        data=data,
                        headers=hdrs,
                        timeout=self._timeout,
                    ) as resp:
                        raw = await resp.read()
                        status = resp.status
                        h = dict(resp.headers)
                        now = self._now_msk()
                        resp_dt = parse_http_date(h.get("Date"), fallback=now)

                        if status >= 400:
                            # Попробуем вытащить сообщение как текст
                            try:
                                t = raw.decode("utf-8", errors="ignore")
                                msg = self._json_message(t)
                            except Exception:
                                msg = resp.reason or "HTTP error"
                            raise ClientResponseError(
                                request_info=resp.request_info,
                                history=resp.history,
                                status=status,
                                message=f"{resp.reason}: {msg}",
                                headers=resp.headers,
                            )

                        # Успех: различаем текст/JSON и бинарник
                        if _is_text_like(h):
                            try:
                                body_text = raw.decode("utf-8")
                            except Exception:
                                body_text = raw.decode("latin-1", errors="ignore")
                            return WBResponse(status, h, resp_dt, now, body_text=body_text, body_bytes=None, request=audit_request)
                        else:
                            return WBResponse(status, h, resp_dt, now, body_text=None, body_bytes=raw, request=audit_request)

                except ClientResponseError:
                    raise
                except Exception:
                    if attempt < self._retries - 1:
                        await asyncio.sleep(self._backoff * (2 ** attempt))
                        continue
                    raise

    # ---------------- Пагинация страницами (ленивая) ----------------
    async def paginate_pages(
        self,
        call: Callable[[int], "WBResponse" | asyncio.Future],
        *,
        start: int = 1,
        max_pages: Optional[int] = None,
        pause_sec: float = 0.0,
    ) -> Iterable[WBResponse]:
        page = start
        while True:
            r = await call(page)
            yield r
            page += 1
            if max_pages and (page - start) >= max_pages:
                break
            if pause_sec:
                await asyncio.sleep(pause_sec)

    # ---------------- Батч‑POST (склейка в единый JSON‑массив) ----------------
    async def post_in_chunks(
        self,
        url: str,
        items: List[Any],
        *,
        chunk_size: int = 50,
        pause_sec: float = 0.4,
        token_override: Optional[str] = None,
    ) -> WBResponse:
        all_items: List[Any] = []
        last_meta: Optional[WBResponse] = None
        audits: List[Dict[str, Any]] = []

        for i in range(0, len(items), chunk_size):
            chunk = items[i : i + chunk_size]
            r = await self.request("POST", url, json_body=chunk, token_override=token_override)
            last_meta = r
            audits.append(r.request or self._audit_make("POST", url, json_body=chunk))
            try:
                part = json.loads(r.body_text) if r.body_text else []
            except Exception:
                part = []
            if isinstance(part, list):
                all_items.extend(part)
            if pause_sec:
                await asyncio.sleep(pause_sec)

        body = json.dumps(all_items, ensure_ascii=False)
        now = self._now_msk()
        merged_audit = self._audit_merge_many(url, "POST", audits, marker={"chunk_size": chunk_size})
        return WBResponse(
            status=(last_meta.status if last_meta else 200),
            headers=(last_meta.headers if last_meta else {}),
            response_dttm=(last_meta.response_dttm if last_meta else now),
            received_at=(last_meta.received_at if last_meta else now),
            body_text=body,
            request=merged_audit,
        )

    # ---------------- Склейка нескольких WBResponse в один ----------------
    async def merge_responses(
        self,
        response_1: Optional[WBResponse],
        response_2: WBResponse,
    ) -> WBResponse:
        """
            Вспомогательная функция: объединение двух объектов WBResponse в один.
            Все атрибуты берутся из второго WBResponse, кроме body_text, который склеивается из обоих.
        """
        # Если первый WBResponse = None, то возвращаем второй без изменений.
        # чтобы было проще использовать текущю функцию (merge_responses) в цикле, без if-else.
        if not response_1: return response_2

        # @TODO: удалить HARDCODE
        if response_1.status != 200: 
            raise ValueError(f"response_1 status is not 200:: {response_1.status}")
        if response_2.status != 200: 
            raise ValueError(f"response_2 status is not 200:: {response_2.status}")
        
        def get_list(obj) -> List[Any]:
            if type(obj) is list: return obj
            else: return [obj]
        
        response_body_1 = get_list(json.loads(response_1.body_text)) if response_1.body_text else []
        response_body_2 = get_list(json.loads(response_2.body_text)) if response_2.body_text else []
        response_body = response_body_1 + response_body_2

        return WBResponse(
            status=response_2.status,
            headers=response_2.headers,
            request=response_2.request,
            response_dttm=response_2.response_dttm,
            received_at=response_2.received_at,
            body_text=json.dumps(response_body, ensure_ascii=False),
        )

    # ---------------- Poll сценарий (submit→status) ----------------
    async def poll_until_done(
        self,
        status_call: Callable[[], "WBResponse" | asyncio.Future],
        *,
        status_path: Callable[[str], str],
        done_values: set[str],
        error_values: set[str],
        interval_sec: float = 10.0,
        timeout_sec: float = 1800.0,
    ) -> WBResponse:
        start = self._now_msk()
        audits: List[Dict[str, Any]] = []
        last: Optional[WBResponse] = None
        while True:
            r = await status_call()
            last = r
            if r.request:
                audits.append(r.request)
            try:
                status = status_path(r.body_text)
            except Exception:
                status = ""
            if status in done_values:
                # Склеим audit статусов в один
                merged = self._audit_merge_many(last.request["url"] if last and last.request else "", "GET", audits,
                                                marker={"polling": True, "interval_sec": interval_sec})
                return WBResponse(last.status, last.headers, last.response_dttm, last.received_at, last.body_text, request=merged)
            if status in error_values:
                raise RuntimeError(f"WB task failed with status={status}")
            if (self._now_msk() - start).total_seconds() > timeout_sec:
                raise TimeoutError("WB task polling timeout")
            await asyncio.sleep(interval_sec)

    # --------------- Вспомогательная загрузка бинарника → base64 ---------------
    async def _download_bytes_as_b64(self, url: str, *, token_override: Optional[str] = None) -> WBResponse:
        assert self._session
        hdrs = {"Authorization": f"Bearer {token_override}"} if token_override else None

        # Слепок запроса для аудита (query парсим из URL)
        parsed = urlparse(url)
        audit_request = self._audit_make("GET", url, params=dict(parse_qsl(parsed.query, keep_blank_values=True)),
                                         headers=hdrs)

        for attempt in range(self._retries):
            async with self._sem:
                try:
                    async with self._session.get(url, headers=hdrs, timeout=self._timeout) as resp:
                        data = await resp.read()
                        status = resp.status
                        h = dict(resp.headers)
                        now = self._now_msk()
                        resp_dt = parse_http_date(h.get("Date"), fallback=now)
                        if status >= 400:
                            msg = f"{resp.reason}"
                            if status in (429, 500, 502, 503, 504) and attempt < self._retries - 1:
                                await asyncio.sleep(self._backoff * (2 ** attempt))
                                continue
                            raise ClientResponseError(
                                request_info=resp.request_info,
                                history=resp.history,
                                status=status,
                                message=msg,
                                headers=resp.headers,
                            )
                        b64 = base64.b64encode(data).decode("ascii")
                        h = {**h, "x-body-encoding": "base64"}
                        return WBResponse(
                            status=status,
                            headers=h,
                            response_dttm=resp_dt,
                            received_at=now,
                            body_text=b64,
                            request=audit_request,
                        )
                except ClientResponseError:
                    raise
                except Exception:
                    if attempt < self._retries - 1:
                        await asyncio.sleep(self._backoff * (2 ** attempt))
                        continue
                    raise

    # =====================================================================
    # ТОНКИЕ ОБЁРТКИ ДЛЯ КОНКРЕТНЫХ ЭНДПОИНТОВ (без дублирования логики сети)
    # =====================================================================

    # ---------- Statistics API ----------
    async def fetch_orders(
        self,
        *,
        date_from: Optional[str | datetime] = None,
        flag: int = 0,
        token_override: Optional[str] = None,
    ) -> WBResponse:
        """GET /supplier/orders"""
        url = f"{STATISTICS_API_URL}/supplier/orders"
        params = {
            "dateFrom": self._to_msk_iso_no_tz(date_from),
            "flag": int(flag),
        }
        return await self.request("GET", url, params=params, token_override=token_override)

    async def fetch_suppliers(
        self,
        *,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        token_override: Optional[str] = None,
    ) -> WBResponse:
        """GET /supplier/incomes"""
        url = f"{STATISTICS_API_URL}/supplier/incomes"
        params: Dict[str, Any] = {}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to
        try:
            return await self.request("GET", url, params=params, token_override=token_override)
        except ClientResponseError as e:
            if e.status == 404:
                # Возвращаем пустой JSON-массив как строку + audit запроса
                now = self._now_msk()
                audit = self._audit_make("GET", url, params=params)
                return WBResponse(
                    status=404,
                    headers={"warning": "not found"},
                    response_dttm=now,
                    received_at=now,
                    body_text="[]",
                    request=audit,
                )
            raise

    # ---------- Common API ----------
    async def fetch_commission(self, *, locale: str = "ru", token_override: Optional[str] = None) -> WBResponse:
        """GET /tariffs/commission"""
        url = f"{COMMON_API_URL}/tariffs/commission"
        params = {"locale": locale}
        return await self.request("GET", url, params=params, token_override=token_override)

    # ---------- Advert API ----------
    async def fetch_ad_config(self, *, token_override: Optional[str] = None) -> WBResponse:
        """GET /adv/v0/config"""
        url = f"{ADVERT_API_URL}/adv/v0/config"
        return await self.request("GET", url, token_override=token_override)

    async def get_advert_list(self, *, token_override: Optional[str] = None) -> WBResponse:
        """GET /adv/v1/promotion/count (полный JSON ответа)"""
        url = f"{ADVERT_API_URL}/adv/v1/promotion/count"
        return await self.request("GET", url, token_override=token_override)

    async def get_advert_ids(self, *, date_from: str, date_to: str, token_override: Optional[str] = None) -> WBResponse:
        """GET /adv/v1/promotion/count → массив advertId"""
        url = f"{ADVERT_API_URL}/adv/v1/promotion/count"
        params = {"dateFrom": date_from, "dateTo": date_to}
        r = await self.request("GET", url, params=params, token_override=token_override)
        try:
            data = json.loads(r.body_text) if r.body_text else {}
        except Exception:
            data = {}
        ids = [
            adv.get("advertId")
            for group in (data.get("adverts") or [])
            for adv in (group.get("advert_list") or [])
            if isinstance(adv, dict) and "advertId" in adv
        ]
        body = json.dumps(ids, ensure_ascii=False)
        audit = self._audit_make("GET", url, params=params)
        return WBResponse(r.status, r.headers, r.response_dttm, r.received_at, body, request=audit)

    async def fetch_ad_info(self, advert_ids: List[int], *, token_override: Optional[str] = None) -> WBResponse:
        """POST /adv/v1/promotion/adverts (батчами по 50)"""
        url = f"{ADVERT_API_URL}/adv/v1/promotion/adverts"
        return await self.post_in_chunks(url, advert_ids, chunk_size=50, pause_sec=0.4, token_override=token_override)

    async def fetch_ad_stats(
        self,
        ids: List[int] | List[str],
        *,
        begin_date: str,
        end_date: str,
        token_override: Optional[str] = None,
    ) -> WBResponse:
        """
        GET /adv/v3/fullstats?ids=...&beginDate=YYYY-MM-DD&endDate=YYYY-MM-DD
        - Батчим ids по 100.
        - 429: ждём по X-Ratelimit-Retry/Reset/Retry-After и повторяем тот же чанк.
        - 400 Invalid payload (invalid ... id: N): вырезаем N из чанка и повторяем.
        - 400 "no stats" сообщения: пропускаем чанк.
        - Склеиваем ответ в один JSON-массив + объединённый audit запроса.
        """
        url = f"{ADVERT_API_URL}/adv/v3/fullstats"

        def _hdr_int(hdrs: dict | None, *names: str, default: int | None = None) -> int | None:
            hdrs = hdrs or {}
            for n in names:
                v = hdrs.get(n)
                if v is None:
                    continue
                try:
                    return int(str(v))
                except Exception:
                    continue
            return default

        def _is_no_stats(msg: str) -> bool:
            m = (msg or "").lower()
            return (
                "there are no companies with correct intervals" in m
                or "there are no statistics for this advertising period" in m
                or "no statistics for this advertising period" in m
            )

        ids_list = [int(x) for x in (ids or [])]

        all_items: List[dict] = []
        last: Optional[WBResponse] = None
        audits: List[Dict[str, Any]] = []

        for i in range(0, len(ids_list), 100):
            chunk = ids_list[i:i + 100]

            # Повторяем запрос для чанка, пока не получим успех или чанк не опустеет
            while chunk:
                try:
                    params = {
                        "beginDate": begin_date,
                        "endDate": end_date,
                        "ids": ",".join(str(x) for x in chunk),
                    }
                    r = await self.request("GET", url, params=params, token_override=token_override)
                    last = r
                    audits.append(r.request or self._audit_make("GET", url, params=params))

                    try:
                        part = json.loads(r.body_text) if r.body_text else []
                    except Exception:
                        part = []

                    if isinstance(part, list):
                        all_items.extend(part)

                    # После удачного ответа ждём по rate-limit, если он есть
                    delay = _hdr_int(r.headers, "X-Ratelimit-Reset", "X-Ratelimit-Retry", "Retry-After", default=0)
                    await asyncio.sleep(delay if delay and delay > 0 else 0.3)
                    break  # успех: следующий чанк

                except ClientResponseError as e:
                    # 429 — глобальный лимит
                    if e.status == 429:
                        delay = _hdr_int(getattr(e, "headers", None),
                                         "X-Ratelimit-Retry", "Retry-After", "X-Ratelimit-Reset",
                                         default=45)
                        await asyncio.sleep(max(delay or 45, 1))
                        continue

                    # 400 Invalid payload — вырезаем проблемные advert id
                    if e.status == 400:
                        msg = getattr(e, "message", "") or ""

                        # "no stats" — пропускаем чанк без ретраев/ошибок
                        if _is_no_stats(msg):
                            chunk = []
                            break

                        bad_ids = {int(x) for x in re.findall(r"invalid .*? id:\s*(\d+)", msg, flags=re.IGNORECASE)}
                        if bad_ids:
                            chunk = [x for x in chunk if x not in bad_ids]
                            if not chunk:
                                break
                            await asyncio.sleep(60)
                            continue
                    raise

        body = json.dumps(all_items, ensure_ascii=False)
        now = self._now_msk()
        merged = self._audit_merge_many(url, "GET", audits, marker={"chunk_size": 100})
        q = merged.get("query") or {}
        ids_val = q.get("ids")
        if isinstance(ids_val, list):
            try:
                split: List[str] = []
                for it in ids_val:
                    split.extend([s for s in str(it).split(",") if s])
                uniq = list(dict.fromkeys(split))
                q["ids"] = ",".join(uniq)
            except Exception:
                pass
        merged["query"] = q

        return WBResponse(
            status=(last.status if last else 200),
            headers=(last.headers if last else {}),
            response_dttm=(last.response_dttm if last else now),
            received_at=(last.received_at if last else now),
            body_text=body,
            request=merged,
        )

    async def fetch_clusters_batch(self, advert_ids: List[int], *, token_override: Optional[str] = None) -> WBResponse:
        """GET /adv/v2/auto/stat-words по каждому id → единый JSON‑массив"""
        url = f"{ADVERT_API_URL}/adv/v2/auto/stat-words"
        out: List[Dict[str, Any]] = []
        last: Optional[WBResponse] = None
        audits: List[Dict[str, Any]] = []
        for advert_id in advert_ids:
            try:
                params = {"id": advert_id}
                r = await self.request("GET", url, params=params, token_override=token_override)
                last = r
                audits.append(r.request or self._audit_make("GET", url, params=params))
                try:
                    data = json.loads(r.body_text) if r.body_text else {}
                except Exception:
                    data = {}
                if not isinstance(data, dict):
                    data = {}
                out.append({"advertId": advert_id, **data})
            except ClientResponseError as e:
                if e.status == 404:
                    out.append({"advertId": advert_id, "excluded": [], "clusters": []})
                else:
                    raise
            await asyncio.sleep(0.3)  # 4 req/s
        body = json.dumps(out, ensure_ascii=False)
        now = self._now_msk()
        merged = self._audit_merge_many(url, "GET", audits)
        # Сожмём id в строку, как это выглядело бы в одном запросе (для аудита)
        q = merged.get("query") or {}
        q["id"] = ",".join(str(x) for x in advert_ids)
        merged["query"] = q
        return WBResponse(
            status=(last.status if last else 200),
            headers=(last.headers if last else {}),
            response_dttm=(last.response_dttm if last else now),
            received_at=(last.received_at if last else now),
            body_text=body,
            request=merged,
        )

    async def fetch_keywords_batch(
        self,
        advert_ids: List[int],
        *,
        date_from: str,
        date_to: str,
        token_override: Optional[str] = None,
    ) -> WBResponse:
        """GET /adv/v0/stats/keywords по каждому id → единый JSON‑массив"""
        url = f"{ADVERT_API_URL}/adv/v0/stats/keywords"
        out: List[Dict[str, Any]] = []
        last: Optional[WBResponse] = None
        audits: List[Dict[str, Any]] = []
        for advert_id in advert_ids:
            try:
                params = {"advert_id": advert_id, "from": date_from, "to": date_to}
                r = await self.request("GET", url, params=params, token_override=token_override)
                last = r
                audits.append(r.request or self._audit_make("GET", url, params=params))
                try:
                    data = json.loads(r.body_text) if r.body_text else {}
                except Exception:
                    data = {}
                if not isinstance(data, dict):
                    data = {}
                out.append({"advertId": advert_id, **data})
            except ClientResponseError as e:
                # 400/404 → пустой список по рекламе (совместимо с легаси)
                if e.status in (400, 404):
                    headers = {"warning": f"{e.status}"}
                    now = self._now_msk()
                    last = WBResponse(e.status, headers, now, now, body_text=json.dumps({"keywords": []}),
                                      request=self._audit_make("GET", url, params={"advert_id": advert_id, "from": date_from, "to": date_to}))
                    out.append({"advertId": advert_id, "keywords": []})
                else:
                    raise
            await asyncio.sleep(0.3)
        body = json.dumps(out, ensure_ascii=False)
        now = self._now_msk()
        merged = self._audit_merge_many(url, "GET", audits)
        q = merged.get("query") or {}
        q["advert_id"] = ",".join(str(x) for x in advert_ids)
        q["from"] = date_from
        q["to"] = date_to
        merged["query"] = q
        return WBResponse(
            status=(last.status if last else 200),
            headers=(last.headers if last else {}),
            response_dttm=(last.response_dttm if last else now),
            received_at=(last.received_at if last else now),
            body_text=body,
            request=merged,
        )

    # ---------- Content API ----------
    async def fetch_sku_cards_all(
        self,
        *,
        with_photo: int = -1,
        limit: int = 100,
        token_override: Optional[str] = None,
    ) -> WBResponse:
        """
        POST /content/v2/get/cards/list — курсорная пагинация, собираем все карточки.
        Возвращаем WBResponse с body_text = JSON-массив всех cards.
        """
        url = f"{CONTENT_API_URL}/content/v2/get/cards/list"
        cursor: Dict[str, Any] = {}
        all_cards: List[Dict[str, Any]] = []
        last: Optional[WBResponse] = None
        audits: List[Dict[str, Any]] = []

        while True:
            payload = {
                "settings": {
                    "cursor": {"limit": limit, **cursor},
                    "filter": {"withPhoto": with_photo},
                }
            }
            r = await self.request("POST", url, json_body=payload, token_override=token_override)
            last = r
            audits.append(r.request or self._audit_make("POST", url, json_body=payload))
            try:
                data = json.loads(r.body_text) if r.body_text else {}
            except Exception:
                data = {}
            cards = data.get("cards") or []
            if not cards:
                break
            all_cards.extend(cards)

            cur = data.get("cursor") or {}
            if (cur.get("total") or 0) < limit:
                break
            cursor = {"updatedAt": cur.get("updatedAt"), "nmID": cur.get("nmID")}
            await asyncio.sleep(0.3)

        body = json.dumps(all_cards, ensure_ascii=False)
        now = self._now_msk()
        merged = self._audit_merge_many(url, "POST", audits, marker={"pagination": "all"})
        merged["json_body"] = {"settings": {"cursor": {"limit": limit}, "filter": {"withPhoto": with_photo}}, "pagination": "all"}
        return WBResponse(
            status=(last.status if last else 200),
            headers=(last.headers if last else {}),
            response_dttm=(last.response_dttm if last else now),
            received_at=(last.received_at if last else now),
            body_text=body,
            request=merged,
        )

    # ---------- Analytics V2 ----------
    async def fetch_sales_funnel(self, payload: Dict[str, Any], *, token_override: Optional[str] = None) -> WBResponse:
        """POST /nm-report/detail — агрегированная воронка (analytics v2)"""
        url = f"{ANALYTICS_V2_URL}/nm-report/detail"
        return await self.request("POST", url, json_body=payload, token_override=token_override)

    async def fetch_product_search_texts(self, payload: Dict[str, Any], *, token_override: Optional[str] = None) -> WBResponse:
        """
            POST /search-report/product/search-texts
            Метод формирует топ поисковых запросов по товару.
            Параметры выбора поисковых запросов:
            limit — количество запросов, максимум 30 (для тарифа Продвинутый — 100)
            topOrderBy — способ выбора топа запросов
            Параметры includeSubstitutedSKUs и includeSearchTexts не могут одновременно иметь значение false.
        """
        url = f"{ANALYTICS_V2_URL}/search-report/product/search-texts"

        print(f'payload:\n{payload}')
        # Получаем список nmId, по которым нужно собрать статистики поисковой воронки
        
        # Лимит на количество nmId в запросе = 50, поэтому делаем запросы в цикле
        # for nm_ids_50 in nm_ids_list:

        merged_responses = None
        for top_order_by in ["orders", "openCard", "addToCart"]:
            # Топ запросов, по которым больше всего:
            # * openCard — перешли в карточку
            # * addToCart — добавили в корзину
            # * orders — заказали товаров
            # * openToCart — конверсия в корзину
            # * cartToOrder — конверсия в заказ

            payload.update({"topOrderBy": top_order_by})
            print(f'Отправляем запрос с topOrderBy="{top_order_by}"')
            new_response = await self.request("POST", url, json_body=payload, token_override=token_override)
            merged_responses = await self.merge_responses(merged_responses, new_response)

            # Ждем 20 секунд, чтобы не получить от сервера отбой "429: too many requests"
            await asyncio.sleep(20)

        return merged_responses


    # ---------- Analytics V1 — отчёты с submit→status→download ----------
    # Складские остатки
    async def request_stocks_report(self, params: Dict[str, Any], *, token_override: Optional[str] = None) -> WBResponse:
        """GET /warehouse_remains — старт задачи (возвращает taskId в JSON)"""
        url = f"{ANALYTICS_V1_URL}/warehouse_remains"
        # конвертируем bool → 'true'/'false' как в легаси
        norm = {k: (str(v).lower() if isinstance(v, bool) else str(v)) for k, v in params.items()}
        return await self.request("GET", url, params=norm, token_override=token_override)

    async def get_stocks_report_status(self, task_id: Union[int, str], *, token_override: Optional[str] = None) -> WBResponse:
        """GET /warehouse_remains/tasks/{task_id}/status"""
        url = f"{ANALYTICS_V1_URL}/warehouse_remains/tasks/{task_id}/status"
        return await self.request("GET", url, token_override=token_override)

    async def download_stocks_report(self, task_id: Union[int, str], *, token_override: Optional[str] = None) -> WBResponse:
        """GET /warehouse_remains/tasks/{task_id}/download → base64 в body_text"""
        url = f"{ANALYTICS_V1_URL}/warehouse_remains/tasks/{task_id}/download"
        return await self.request("GET", url, token_override=token_override)

    # Платное хранение
    async def fetch_paid_storage(self, *, date_from: Optional[str] = None, date_to: Optional[str] = None, token_override: Optional[str] = None) -> WBResponse:
        """GET /paid_storage — старт задачи"""
        url = f"{ANALYTICS_V1_URL}/paid_storage"
        params: Dict[str, Any] = {}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to
        return await self.request("GET", url, params=params, token_override=token_override)

    async def get_paid_storage_status(self, task_id: Union[int, str], *, token_override: Optional[str] = None) -> WBResponse:
        """GET /paid_storage/tasks/{task_id}/status"""
        url = f"{ANALYTICS_V1_URL}/paid_storage/tasks/{task_id}/status"
        return await self.request("GET", url, token_override=token_override)

    async def download_paid_storage(self, task_id: Union[int, str], *, token_override: Optional[str] = None) -> WBResponse:
        """GET /paid_storage/tasks/{task_id}/download → base64 в body_text"""
        url = f"{ANALYTICS_V1_URL}/paid_storage/tasks/{task_id}/download"
        return await self.request("GET", url, token_override=token_override)

    # Платная приёмка
    async def fetch_paid_acceptions(self, *, date_from: Optional[str] = None, date_to: Optional[str] = None, token_override: Optional[str] = None) -> WBResponse:
        """GET /acceptance_report — старт задачи"""
        url = f"{ANALYTICS_V1_URL}/acceptance_report"
        params: Dict[str, Any] = {}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to
        return await self.request("GET", url, params=params, token_override=token_override)

    async def get_paid_acceptions_status(self, task_id: Union[int, str], *, token_override: Optional[str] = None) -> WBResponse:
        """GET /acceptance_report/tasks/{task_id}/status"""
        url = f"{ANALYTICS_V1_URL}/acceptance_report/tasks/{task_id}/status"
        return await self.request("GET", url, token_override=token_override)

    async def download_paid_acceptions(self, task_id: Union[int, str], *, token_override: Optional[str] = None) -> WBResponse:
        """GET /acceptance_report/tasks/{task_id}/download → base64 в body_text"""
        url = f"{ANALYTICS_V1_URL}/acceptance_report/tasks/{task_id}/download"
        return await self.request("GET", url, token_override=token_override)

    async def fetch_report_detail_by_period(
        self,
        *,
        date_from: str | datetime,
        date_to: str | datetime,
        token_override: Optional[str] = None,
    ) -> WBResponse:
        """
        GET /supplier/reportDetailByPeriod — постраничная загрузка финансового отчёта.
        Логика:
          • стартуем с rrdid=0;
          • после каждого ответа берём максимальный rrd_id из полученных строк
            и подставляем его в следующий запрос (параметр rrdid);
          • продолжаем, пока не придёт пустой массив [];
          • возвращаем единый JSON-массив во `body_text` и объединённый audit.

        Параметры date_from/date_to приводим к 'YYYY-MM-DD' (МСК).
        """
        url = f"{STATISTICS_API_V2_URL}/supplier/reportDetailByPeriod"

        # Приведём к YYYY-MM-DD (МСК)
        def _to_date_str(x: str | datetime) -> str:
            if isinstance(x, datetime):
                # берём дату в МСК
                try:
                    from zoneinfo import ZoneInfo
                    d = x.astimezone(ZoneInfo("Europe/Moscow")).date()
                except Exception:
                    d = x.date()
                return d.isoformat()
            s = str(x).strip()
            return s[:10]

        params: Dict[str, Any] = {
            "dateFrom": _to_date_str(date_from),
            "dateTo": _to_date_str(date_to),
            "rrdid": 0,
        }

        all_rows: List[Dict[str, Any]] = []
        last: Optional[WBResponse] = None
        audits: List[Dict[str, Any]] = []
        rrdid_seq: List[int] = [0]

        def _hdr_int(hdrs: Dict[str, str] | None, *names: str, default: int | None = None) -> int | None:
            hdrs = hdrs or {}
            for n in names:
                v = hdrs.get(n)
                if v is None:
                    continue
                try:
                    return int(str(v))
                except Exception:
                    pass
            return default

        while True:
            r = await self.request("GET", url, params=params, token_override=token_override)
            last = r
            audits.append(r.request or self._audit_make("GET", url, params=params))

            try:
                chunk = json.loads(r.body_text) if r.body_text else []
            except Exception:
                chunk = []

            if not isinstance(chunk, list):
                if isinstance(chunk, dict):
                    for k in ("data", "result", "items", "rows"):
                        v = chunk.get(k)
                        if isinstance(v, list):
                            chunk = v
                            break
                    else:
                        chunk = []
                else:
                    chunk = []

            if not chunk:
                break

            all_rows.extend(chunk)

            next_rrd: Optional[int] = None
            for row in chunk:
                rid = row.get("rrd_id") or row.get("rrdId") or row.get("rrdid")
                try:
                    if rid is not None:
                        rid = int(rid)
                        next_rrd = rid if (next_rrd is None or rid > next_rrd) else next_rrd
                except Exception:
                    continue

            if next_rrd is None:
                break

            params["rrdid"] = next_rrd
            rrdid_seq.append(next_rrd)

            delay = _hdr_int(r.headers, "X-Ratelimit-Retry", "X-Ratelimit-Reset", "Retry-After", default=0)
            if delay and delay > 0:
                await asyncio.sleep(delay)
            else:
                await asyncio.sleep(0.2)

        body = json.dumps(all_rows, ensure_ascii=False)
        now = self._now_msk()
        merged = self._audit_merge_many(url, "GET", audits, marker={"pagination": "rrdid"})
        q = merged.get("query") or {}
        q["rrdid_start"] = rrdid_seq[0]
        q["rrdid_end"] = rrdid_seq[-1] if rrdid_seq else 0
        merged["query"] = q
        return WBResponse(
            status=(last.status if last else 200),
            headers=(last.headers if last else {}),
            response_dttm=(last.response_dttm if last else now),
            received_at=(last.received_at if last else now),
            body_text=body,
            request=merged,
        )

    async def fetch_supplier_sales(
        self,
        *,
        date_from: str | datetime | None = None,
        flag: int = 0,
        token_override: str | None = None,
    ) -> WBResponse:
        """
        GET /api/v1/supplier/sales — продажи и возвраты.
        Параметры:
          - dateFrom (RFC3339, МСК): обязательный.
          - flag (0|1): 0 — инкремент по lastChangeDate; 1 — все за дату dateFrom (время игнорится).
        См. офиц. доки.  # Sales. lastChangeDate, flag semantics.
        """
        url = f"{STATISTICS_API_URL}/supplier/sales"
        params = {
            "dateFrom": self._to_msk_iso_no_tz(date_from),
            "flag": int(flag),
        }
        return await self.request("GET", url, params=params, token_override=token_override)

    async def fetch_supplier_stocks(
        self,
        *,
        date_from: str | datetime | None = None,
        token_override: str | None = None,
    ) -> WBResponse:
        """
        GET /api/v1/supplier/stocks — остатки в реальном времени.
        Параметры:
          - dateFrom (RFC3339, МСК): обязательный.
        Примечание: история остатков не хранится; используйте «раннюю» дату, чтобы получить текущие остатки.
        См. офиц. доки.  # Stocks. real-time, no history.
        """
        url = f"{STATISTICS_API_URL}/supplier/stocks"
        params = {
            "dateFrom": self._to_msk_iso_no_tz(date_from),
        }
        return await self.request("GET", url, params=params, token_override=token_override)

    # --- PUBLIC (no-auth) WWW search: exactmatch ---

    async def fetch_www_text_search_page_public(
            self,
            *,
            query: str,
            page: int = 1,
            dest: int = -1257786,
            app_type: int = 32,
            lang: str = "ru",
            curr: str = "rub",
            resultset: str = "catalog",
            sort: str = "popular",
            uclusters: int = 0,
            uiv: int = 0,
            base_url: str = "https://search.wb.ru/exactmatch/ru/common/v14/search",
            token_override: str | None = None,  # фабрика всё равно передаёт; игнорируем
    ) -> "WBResponse":
        """
        Публичный WWW-поиск WB: 1 страница БЕЗ Authorization.
        Семантика/параметры ровно как у fetch_www_text_search_page.
        """
        import aiohttp, asyncio, json

        params = {
            "query": query,
            "page": int(page),
            "dest": dest,
            "appType": app_type,
            "lang": lang,
            "curr": curr,
            "resultset": resultset,
            "sort": sort,
            "uclusters": uclusters,
            "uiv": uiv,
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9",
            "Origin": "https://www.wildberries.ru",
            "Referer": "https://www.wildberries.ru/",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        status = 0
        body_text = ""
        resp_headers = {}
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.get(base_url, params=params, headers=headers) as resp:
                    status = resp.status
                    body_text = await resp.text()
                    resp_headers = dict(resp.headers or {})
                    # быстрый фолбэк на v4, если сервер дал 5xx
                    if status >= 500:
                        fallback = "https://search.wb.ru/exactmatch/ru/common/v4/search"
                        async with sess.get(fallback, params=params, headers=headers) as r2:
                            if r2.status < 500:
                                status = r2.status
                                body_text = await r2.text()
                                resp_headers = dict(r2.headers or {})
        except Exception:
            # оставим status=0; бронза зафиксирует и ретрайнёт по политике
            pass

        audit = self._audit_make("GET", base_url, params=params, headers=headers)
        now = self._now_msk()
        return WBResponse(
            status=status,
            headers=resp_headers,
            response_dttm=now,
            received_at=now,
            body_text=body_text,
            request=audit,
        )

    async def fetch_www_text_search_pages_public(
            self,
            *,
            query: str,
            pages: int | list[int] = 3,
            dest: int = -1257786,
            app_type: int = 32,
            lang: str = "ru",
            curr: str = "rub",
            resultset: str = "catalog",
            sort: str = "popular",
            uclusters: int = 0,
            uiv: int = 0,
            pause_sec: float = 0.2,
            base_url: str = "https://search.wb.ru/exactmatch/ru/common/v14/search",
            token_override: str | None = None,
    ) -> "WBResponse":
        """
        Агрегатор нескольких страниц БЕЗ Authorization.
        Совместим по формату с fetch_www_text_search_pages.
        """
        import json, asyncio
        if isinstance(pages, int):
            page_list = list(range(1, int(pages) + 1))
        else:
            page_list = sorted({int(p) for p in pages})

        results, audits, last = [], [], None
        for p in page_list:
            r = await self.fetch_www_text_search_page_public(
                query=query, page=p, dest=dest, app_type=app_type,
                lang=lang, curr=curr, resultset=resultset, sort=sort,
                uclusters=uclusters, uiv=uiv, base_url=base_url,
                token_override=token_override,
            )
            last = r
            audits.append(r.request or self._audit_make("GET", base_url, params={"query": query, "page": p}))
            try:
                payload = json.loads(r.body_text) if r.body_text else None
            except Exception:
                payload = r.body_text

            results.append({
                "page": p,
                "params": (r.request or {}).get("query") or {"query": query, "page": p},
                "status": r.status,
                "headers": r.headers,
                "response_dttm": r.response_dttm.isoformat(),
                "payload": payload,
            })
            if pause_sec:
                await asyncio.sleep(pause_sec)

        merged = self._audit_merge_many(base_url, "GET", audits, marker={"batched": True, "pages": page_list})
        body = json.dumps({"results": results, "count": len(results)}, ensure_ascii=False)
        now = self._now_msk()
        return WBResponse(
            status=(last.status if last else 200),
            headers=(last.headers if last else {}),
            response_dttm=(last.response_dttm if last else now),
            received_at=(last.received_at if last else now),
            body_text=body,
            request=merged,
        )
