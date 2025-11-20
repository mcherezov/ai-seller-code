from __future__ import annotations
"""
WbNormUtils — общий набор утилит для нормализации ответов WB.

Цели:
- Убрать дублирование из нормализаторов (DRY).
- Сохранить простоту интерфейса (KISS) и обратную совместимость с текущим кодом.

Основные группы хелперов:
1) Извлечение JSON из payload и получение "списка записей" из типичных обёрток.
2) Безопасные конвертеры типов (int/float/str/bool/date/datetime).
3) Работа со временем и таймзонами (ISO, Z/UTC → MSK, начало часа/дня).
4) Утилиты для pandas/xlsx (ZIP→XLSX, типизация колонок, добавление meta).

Все функции статичны, поэтому класс удобно импортировать как неймспейс:
    from wb_norm_utils import WbNormUtils as U
    data = U.json_from_payload(payload)

NB: В хелперах есть "мягкие" фолбэки, чтобы не падать на неожиданных формах
ответа, но при необходимости конкретный нормализатор может ужесточить проверку.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence
import json
import re

try:
    from dagster_conf.lib.normalizer_base import Normalizer
except Exception:
    class Normalizer:
        def normalize(self, payload: Any, *, meta: Optional[Dict[str, Any]] = None, partition_ctx: Optional[Dict[str, Any]] = None) -> Iterable[Dict[str, Any]]:
            raise NotImplementedError

try:
    from zoneinfo import ZoneInfo
    MSK = ZoneInfo("Europe/Moscow")
    UTC = ZoneInfo("UTC")
except Exception:
    MSK = None
    UTC = timezone.utc


# @dataclass
# class WbNormUtils:
#     """Коллекция статических утилит.
#
#     Не требует инстанциирования: используйте WbNormUtils.<method>(...).
#     Введён как класс для удобного неймспейса и возможности расширения.
#     """
#
#     # ------------------------------------------------------------------
#     # 1) Извлечение JSON / списка записей
#     # ------------------------------------------------------------------
#     @staticmethod
#     def json_from_payload(payload: Any, *, body_keys: Sequence[str] = ("response_body", "body", "response")) -> Any:
#         """Достаёт JSON-представление из разных форм payload.
#
#         Поддерживаемые формы:
#           - str / bytes (JSON-текст или base64 и т.п.)
#           - dict с ключом из body_keys, либо сам JSON-словарь/список
#           - объект с атрибутом .response_body
#
#         Возвращает распарсенный JSON (dict/list/scalar) либо исходное значение,
#         если парсинг невозможен.
#         """
#         raw = payload
#         # dict с телом
#         if isinstance(raw, dict):
#             for k in body_keys:
#                 if k in raw:
#                     raw = raw[k]
#                     break
#         else:
#             # объект бронзы с атрибутом response_body
#             if not isinstance(raw, (str, bytes, bytearray, dict, list)):
#                 raw = getattr(raw, "response_body", raw)
#
#         # bytes → str
#         if isinstance(raw, (bytes, bytearray)):
#             try:
#                 raw = raw.decode("utf-8")
#             except Exception:
#                 raw = raw.decode("latin-1", errors="ignore")
#
#         # str → json
#         if isinstance(raw, str):
#             try:
#                 return json.loads(raw)
#             except Exception:
#                 return raw  # вернём как есть — пусть вызывающий решит
#         return raw
#
#     @staticmethod
#     def extract_list(obj: Any, *, prefer_keys: Sequence[str] = ("data", "result", "items", "rows", "cards", "payload")) -> List[Any]:
#         """Пытается получить список из типичных обёрток WB.
#         - Если obj — список, вернуть как есть.
#         - Если obj — словарь, поискать первый list по prefer_keys.
#         - Если не найдено — вернуть [] либо [obj] для непустых скаляров.
#         """
#         if isinstance(obj, list):
#             return obj
#         if isinstance(obj, dict):
#             for k in prefer_keys:
#                 v = obj.get(k)
#                 if isinstance(v, list):
#                     return v
#             # иногда {'data': {'cards': [...]}} → один уровень глубже
#             for v in obj.values():
#                 if isinstance(v, dict):
#                     for k in prefer_keys:
#                         vv = v.get(k)
#                         if isinstance(vv, list):
#                             return vv
#             return []
#         return []
#
#     # ------------------------------------------------------------------
#     # 2) Безопасные конвертеры типов
#     # ------------------------------------------------------------------
#     @staticmethod
#     def to_int(x: Any, *, default: Optional[int] = None) -> Optional[int]:
#         try:
#             return int(x) if x not in (None, "") else default
#         except Exception:
#             return default
#
#     @staticmethod
#     def to_float(x: Any, *, default: Optional[float] = None) -> Optional[float]:
#         try:
#             return float(x) if x not in (None, "") else default
#         except Exception:
#             return default
#
#     @staticmethod
#     def to_str(x: Any, *, default: Optional[str] = None, strip: bool = False) -> Optional[str]:
#         if x is None:
#             return default
#         s = str(x)
#         return s.strip() if strip else s
#
#     @staticmethod
#     def to_bool(x: Any, *, truthy: set = frozenset({"1", "true", "t", "yes", "y"})) -> Optional[bool]:
#         if x in (None, ""):
#             return None
#         if isinstance(x, bool):
#             return x
#         s = str(x).strip().lower()
#         if s in truthy:
#             return True
#         if s in {"0", "false", "f", "no", "n"}:
#             return False
#         return None
#
#     # Дата/время ----------------------------------------------------------------
#     @staticmethod
#     def parse_iso_datetime(x: Any) -> Optional[datetime]:
#         """Парсит ISO‑дату/время. Поддерживает:
#         - суффикс 'Z'
#         - произвольные пробелы в времени/таймзоне ("22: 51: 28", "+03: 00")
#         - date → преобразует в datetime @ 00:00
#         - уже готовый datetime → вернуть как есть
#         """
#         if x in (None, ""):
#             return None
#         if isinstance(x, datetime):
#             return x
#         if isinstance(x, date):
#             return datetime(x.year, x.month, x.day)
#         s = str(x).strip()
#         s = s.replace("Z", "+00:00")
#         s = re.sub(r"\s*:\s*", ":", s)  # убрать лишние пробелы вокруг ':'
#         # если дата без 'T' — добавим полночь
#         if "T" not in s and re.match(r"^\d{4}-\d{2}-\d{2}$", s):
#             s = f"{s}T00:00:00"
#         try:
#             return datetime.fromisoformat(s)
#         except Exception:
#             return None
#
#     @staticmethod
#     def parse_iso_date(x: Any) -> Optional[date]:
#         """Парсит YYYY-MM-DD или ISO‑строку в date. Поддержка date/datetime входов."""
#         if x in (None, ""):
#             return None
#         if isinstance(x, date) and not isinstance(x, datetime):
#             return x
#         if isinstance(x, datetime):
#             return x.date()
#         s = str(x).strip().replace("Z", "+00:00")
#         # сначала пробуем чистую дату
#         m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
#         if m:
#             try:
#                 return date.fromisoformat(m.group(1))
#             except Exception:
#                 pass
#         dt = WbNormUtils.parse_iso_datetime(s)
#         return dt.date() if dt else None
#
#     @staticmethod
#     def to_msk(dt_like: Any) -> Optional[datetime]:
#         """Переводит дату/время в MSK (tz-aware). Наивные считаем UTC.
#         Возвращает tz-aware datetime в МСК.
#         """
#         dt = WbNormUtils.parse_iso_datetime(dt_like)
#         if dt is None:
#             return None
#         # если наивный — считаем UTC
#         if dt.tzinfo is None:
#             dt = dt.replace(tzinfo=UTC)
#         if MSK:
#             return dt.astimezone(MSK)
#         # фолбэк: вернуть как есть
#         return dt
#
#     @staticmethod
#     def day_start(dt_like: Any, *, tz: Optional[timezone | ZoneInfo] = None) -> Optional[datetime]:
#         dt = WbNormUtils.parse_iso_datetime(dt_like)
#         if dt is None:
#             return None
#         if tz is not None:
#             if dt.tzinfo is None:
#                 dt = dt.replace(tzinfo=tz)
#             dt = dt.astimezone(tz)
#         return dt.replace(hour=0, minute=0, second=0, microsecond=0)
#
#     @staticmethod
#     def hour_start(dt_like: Any, *, tz: Optional[timezone | ZoneInfo] = None) -> Optional[datetime]:
#         dt = WbNormUtils.parse_iso_datetime(dt_like)
#         if dt is None:
#             return None
#         if tz is not None:
#             if dt.tzinfo is None:
#                 dt = dt.replace(tzinfo=tz)
#             dt = dt.astimezone(tz)
#         return dt.replace(minute=0, second=0, microsecond=0)
#
#     # ------------------------------------------------------------------
#     # 3) Часто переиспользуемая бизнес-логика выбора дат
#     # ------------------------------------------------------------------
#     @staticmethod
#     def pick_anchor_datetime(*, run_dttm: Optional[datetime], scheduled_iso: Optional[str], available: Sequence[datetime]) -> Optional[datetime]:
#         """Выбирает "якорь" времени по приоритету:
#         1) scheduled_iso (ISO, приводится к MSK)
#         2) run_dttm (приводится к MSK)
#         3) max(available)
#         """
#         if not available:
#             return run_dttm
#         def _norm(dt: Optional[datetime]) -> Optional[datetime]:
#             if dt is None:
#                 return None
#             if MSK:
#                 if dt.tzinfo is None:
#                     dt = dt.replace(tzinfo=UTC)
#                 return dt.astimezone(MSK)
#             return dt
#         if scheduled_iso:
#             sdt = WbNormUtils.to_msk(scheduled_iso)
#             if sdt:
#                 return sdt
#         if run_dttm:
#             return _norm(run_dttm)
#         return max(available)
#
#     @staticmethod
#     def choose_keyword_stats_dt(*, anchor: datetime, available: Sequence[datetime], time_grain: str = "1d") -> Optional[datetime]:
#         """Логика выбора среза по статистике ключей.
#         - 1h: берём максимум среди доступных за anchor.date()
#         - 1d: берём (anchor.date() - 1 день), иначе max(available)
#         Входные даты должны быть уже в MSK.
#         """
#         if not available:
#             return None
#         time_grain = (time_grain or "1d").lower()
#         if time_grain == "1h":
#             same_day = [dt for dt in available if dt.date() == anchor.date()]
#             return max(same_day) if same_day else max(available)
#         else:
#             target_day = anchor.date() - timedelta(days=1)
#             same_day = [dt for dt in available if dt.date() == target_day]
#             return same_day[0] if same_day else max(available)
#
#     # ------------------------------------------------------------------
#     # 4) Pandas / XLSX (фин-отчёты, ZIP→XLSX)
#     # ------------------------------------------------------------------
#     @staticmethod
#     def xlsx_from_bronze(payload: Any) -> "pd.DataFrame":  # type: ignore[name-defined]
#         """bronze → response_body(JSON) → base64 ZIP → XLSX → pandas.DataFrame.
#         Возвращает DataFrame с оригинальными заголовками WB.
#         Если в payload нет файла — вернёт DataFrame из JSON (list|dict → rows).
#         """
#         import base64, io, zipfile, pandas as pd
#         raw = payload
#         if isinstance(raw, dict) and "response_body" in raw:
#             raw = raw["response_body"]
#         if isinstance(raw, (bytes, bytearray)):
#             raw = raw.decode("utf-8", "ignore")
#         try:
#             data = json.loads(raw) if isinstance(raw, str) else raw
#         except Exception:
#             data = raw
#         # обычный путь: в JSON есть ключ "file" с base64 ZIP
#         b64 = None
#         if isinstance(data, dict):
#             b64 = data.get("file") or (data.get("data") or {}).get("file")
#         if not b64:
#             import pandas as pd
#             return pd.DataFrame(data if isinstance(data, list) else ([data] if data else []))
#         zbytes = base64.b64decode(b64)
#         with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
#             xlsx_name = next((n for n in zf.namelist() if n.lower().endswith(".xlsx")), None)
#             if not xlsx_name:
#                 raise ValueError("ZIP не содержит .xlsx")
#             with zf.open(xlsx_name) as f:
#                 df = pd.read_excel(f)
#         return df
#
#     # — безопасные преобразования колонок ---------------------------------
#     @staticmethod
#     def df_safe_date(df: "pd.DataFrame", col: str) -> None:  # type: ignore[name-defined]
#         import pandas as pd
#         if col in df.columns:
#             s = pd.to_datetime(df[col], errors="coerce")
#             df[col] = s.dt.date
#
#     @staticmethod
#     def df_safe_float(df: "pd.DataFrame", col: str) -> None:  # type: ignore[name-defined]
#         import pandas as pd
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
#
#     @staticmethod
#     def df_safe_int(df: "pd.DataFrame", col: str) -> None:  # type: ignore[name-defined]
#         import pandas as pd
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
#
#     @staticmethod
#     def df_ensure_cols(df: "pd.DataFrame", cols: Sequence[str]) -> "pd.DataFrame":  # type: ignore[name-defined]
#         for c in cols:
#             if c not in df.columns:
#                 df[c] = None
#         return df
#
#     @staticmethod
#     def df_with_meta(df: "pd.DataFrame", meta: Dict[str, Any], keys: Sequence[str] = ("business_dttm", "company_id", "request_uuid", "response_dttm")) -> "pd.DataFrame":  # type: ignore[name-defined]
#         for k in keys:
#             if k not in df.columns:
#                 df[k] = meta.get(k)
#         return df
#
#     # ------------------------------------------------------------------
#     # 5) Частые паттерны для нормализаторов
#     # ------------------------------------------------------------------
#     @staticmethod
#     def payload_to_cards(payload: Any) -> List[Dict[str, Any]]:
#         """Специализация для /nm-report/detail: вытаскивает список cards."""
#         data = WbNormUtils.json_from_payload(payload)
#         if isinstance(data, dict):
#             return ((data.get("data") or {}).get("cards") or [])
#         if isinstance(data, list):
#             return data
#         return []
#
#     @staticmethod
#     def resp_dttm_fallback(resp_dttm: Optional[datetime]) -> datetime:
#         """Фолбэк для response_dttm: текущее время (UTC→MSK)."""
#         now = datetime.now(UTC)
#         return now.astimezone(MSK) if MSK else now
#
#     @staticmethod
#     def out_date_from_run_or_resp(*, run_dttm: Optional[datetime], resp_dttm: Optional[datetime]) -> Optional[date]:
#         """Для ежедневных отчётов: дата = run_dttm.date() или resp_dttm.date()."""
#         if isinstance(run_dttm, datetime):
#             return run_dttm.date()
#         if isinstance(resp_dttm, datetime):
#             return resp_dttm.date()
#         return None
#
#     def _to_int(x):
#         try:
#             return int(x) if x is not None and x != "" else None
#         except Exception:
#             return None
#
#     def _to_float(x):
#         try:
#             return float(x) if x is not None and x != "" else None
#         except Exception:
#             return None
#
#     def _to_str(x):
#         if x is None:
#             return None
#         return str(x)
#
#     def _to_date(x) -> Optional[date]:
#         if x is None or x == "":
#             return None
#         if isinstance(x, date) and not isinstance(x, datetime):
#             return x
#         if isinstance(x, datetime):
#             return x.date()
#         # строки вида "YYYY-MM-DD"
#         try:
#             return datetime.strptime(str(x), "%Y-%m-%d").date()
#         except Exception:
#             # любые ISO/другие — последняя попытка
#             try:
#                 return datetime.fromisoformat(str(x)).date()
#             except Exception:
#                 return None
#
#     def _ensure_list(payload: Any) -> List[Dict[str, Any]]:
#         """Пытаемся достать «список записей» из типичных форм: {}, {'data': [...]}, {'result': [...]}, и т.п."""
#         if isinstance(payload, list):
#             return payload
#         if isinstance(payload, dict):
#             for key in ("data", "result", "items", "rows"):
#                 val = payload.get(key)
#                 if isinstance(val, list):
#                     return val
#             # иногда прилетает {'data': {'cards': [...]}} и т.п. — один уровень глубже
#             for v in payload.values():
#                 if isinstance(v, dict):
#                     for key in ("data", "result", "items", "rows"):
#                         val = v.get(key)
#                         if isinstance(val, list):
#                             return val
#         return []

#
# @dataclass
# class WbUniversalNormalizer(Normalizer):
#     """
#     Универсальный нормализатор-реестр.
#     target: логическое имя серебра/ассета, по которому выбирается метод нормализации.
#     Примеры:
#       - "nm_report_detail__buyouts_percent_1d"
#       - "nm_report_detail__sales_funnels_1d"
#       - "paid_storage_1d"
#       - "stats_keywords"
#     """
#     target: str
#
#     # single shared utils instance
#     _u: WbNormUtils = WbNormUtils()
#
#     # ---------------------------------------------------
#     # Публичный вход: совместим с normalize_wrapper фабрики
#     # ---------------------------------------------------
#     def normalize(
#         self,
#         payload: Any,
#         *,
#         meta: Optional[Dict[str, Any]] = None,
#         partition_ctx: Optional[Dict[str, Any]] = None,
#     ) -> Iterable[Dict[str, Any]]:
#         if meta is None and partition_ctx:
#             meta = {
#                 "business_dttm": str(partition_ctx.get("business_dttm")),
#                 "company_id": partition_ctx.get("company_id"),
#             }
#
#         fn_name = f"norm__{self.target}"
#         fn = getattr(self, fn_name, None)
#         if not fn:
#             raise RuntimeError(f"Нормализатор для '{self.target}' не реализован (ожидал метод {fn_name}())")
#         return fn(payload, meta or {})
#
#     # ---------------------------------------------------
#     # nm_report_detail — buyouts_percent_1d
#     # ---------------------------------------------------
#     def norm__wb_buyouts_percent_1d(self, payload, meta=None):
#         """
#         Извлекает buyoutsPercent по NM из bronze.response_body (nm-report/detail)
#         → записи для silver.wb_buyouts_percent_1d
#         Ожидаемые meta: request_uuid, response_dttm, company_id
#         """
#         u = self._u
#         meta = meta or {}
#         req_uuid = meta.get("request_uuid")
#         resp_dttm = meta.get("response_dttm")
#         company_id = meta.get("company_id")
#
#         data = u.json_from_payload(payload)
#         cards = u.extract_cards(data)
#
#         out = []
#         for item in cards:
#             sel = (item or {}).get("statistics", {}).get("selectedPeriod", {}) or {}
#             begin_dt = u.parse_iso_ts(sel.get("begin"))
#             if begin_dt is None:
#                 continue
#             biz_dt = u.day_start(begin_dt)
#             out.append({
#                 "request_uuid": req_uuid,
#                 "response_dttm": resp_dttm,
#                 "company_id": company_id,
#                 "date": begin_dt,
#                 "business_dttm": biz_dt,
#                 "nm_id": item.get("nmID"),
#                 "buyouts_percent": (sel.get("conversions", {}) or {}).get("buyoutsPercent") or 0.0,
#             })
#         return out
#
#     # ---------------------------------------------------
#     # nm_report_detail — sales_funnels_1d
#     # ---------------------------------------------------
#     def norm__wb_sales_funnels_1d(self, payload, meta=None):
#         """
#         Извлекает воронку продаж по NM из bronze.response_body (nm-report/detail)
#         → записи для silver.wb_sales_funnels_1d
#         Ожидаемые meta: request_uuid, response_dttm, company_id
#         """
#         u = self._u
#         meta = meta or {}
#         req_uuid = meta.get("request_uuid")
#         resp_dttm = meta.get("response_dttm")
#         company_id = meta.get("company_id")
#
#         data = u.json_from_payload(payload)
#         cards = u.extract_cards(data)
#
#         I, F, S = u.to_int, u.to_float, u.to_str
#         out = []
#         for item in cards:
#             sel = (item or {}).get("statistics", {}).get("selectedPeriod", {}) or {}
#             begin_dt = u.parse_iso_ts(sel.get("begin"))
#             if begin_dt is None:
#                 continue
#             obj = (item.get("object") or {})
#             stocks = (item.get("stocks") or {})
#             out.append({
#                 "request_uuid": req_uuid,
#                 "response_dttm": resp_dttm,
#                 "company_id": company_id,
#                 "business_dttm": u.day_start(begin_dt),
#                 "date": begin_dt.date(),
#                 "nm_id": item.get("nmID"),
#                 "supplier_article": S(item.get("vendorCode")) or "",
#                 "brand": S(item.get("brandName")) or "",
#                 "subject_id": I(obj.get("id")) or 0,
#                 "subject_name": S(obj.get("name")) or "",
#                 "open_card_count": I(sel.get("openCardCount")) or 0,
#                 "add_to_cart_count": I(sel.get("addToCartCount")) or 0,
#                 "orders_count": I(sel.get("ordersCount")) or 0,
#                 "orders_sum": F(sel.get("ordersSumRub")) or 0.0,
#                 "buyouts_count": I(sel.get("buyoutsCount")) or 0,
#                 "buyouts_sum": F(sel.get("buyoutsSumRub")) or 0.0,
#                 "cancel_count": I(sel.get("cancelCount")) or 0,
#                 "cancel_sum": F(sel.get("cancelSumRub")) or 0.0,
#                 "avg_price": F(sel.get("avgPriceRub")) or 0.0,
#                 "stocks_mp": F(stocks.get("stocksMp")),
#                 "stocks_wb": F(stocks.get("stocksWb")),
#             })
#         return out
#
#     # ---------------------------------------------------
#     # paid_storage_1d (WB платное хранение)
#     # ---------------------------------------------------
#     def norm__paid_storage_1d(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         req_uuid = meta.get("request_uuid")
#         resp_dttm = meta.get("response_dttm")
#         company_id = meta.get("company_id")
#         business_dttm = meta.get("business_dttm")
#
#         data = u.json_from_payload(payload)
#         items = u.ensure_list(data)
#
#         I, F, D = u.to_int, u.to_float, u.to_date
#         out = []
#         for row in items or []:
#             rec_date = D(row.get("date")) or (D(business_dttm) if business_dttm else None)
#             out.append({
#                 "request_uuid": req_uuid,
#                 "response_dttm": resp_dttm,
#                 "company_id": company_id,
#                 "business_dttm": business_dttm,
#                 "date": rec_date,
#                 "office_id": I(row.get("officeId")),
#                 "income_id": I(row.get("giId")),
#                 "barcode": row.get("barcode"),
#                 "calc_type": row.get("calcType"),
#                 "pallet_place_code": I(row.get("palletPlaceCode")),
#                 "warehouse_name": row.get("warehouse"),
#                 "warehouse_coef": F(row.get("warehouseCoef")),
#                 "log_warehouse_coef": F(row.get("logWarehouseCoef")),
#                 "chrt_id": I(row.get("chrtId")),
#                 "tech_size": row.get("size"),
#                 "supplier_article": row.get("vendorCode"),
#                 "nm_id": I(row.get("nmId")),
#                 "volume": F(row.get("volume")),
#                 "warehouse_price": F(row.get("warehousePrice")),
#                 "barcodes_count": I(row.get("barcodesCount")),
#                 "pallet_count": F(row.get("palletCount")),
#                 "loyalty_discount": F(row.get("loyaltyDiscount")),
#                 "subject": row.get("subject"),
#                 "brand": row.get("brand"),
#                 "original_date": D(row.get("originalDate")),
#                 "tariff_fix_date": D(row.get("tariffFixDate")),
#                 "tariff_lower_date": D(row.get("tariffLowerDate")),
#             })
#         return out
#
#     # ---------------------------------------------------
#     # stats_keywords → wb_adv_keyword_stats_{1d,1h}
#     # ---------------------------------------------------
#     def norm__wb_adv_keyword_stats(self, payload, meta=None):
#         from datetime import timedelta
#         u = self._u
#         meta = meta or {}
#         req_uuid = meta.get("request_uuid")
#         resp_dttm = meta.get("response_dttm")
#         company_id = meta.get("company_id")
#         run_dttm = meta.get("run_dttm")
#         time_grain = (meta.get("time_grain") or "1d").lower()
#         advert_id = (meta.get("request_parameters") or {}).get("advert_id") or meta.get("advert_id")
#
#         data = u.json_from_payload(payload)
#         if isinstance(data, dict):
#             data = data.get("keywords", [])
#         if not isinstance(data, list):
#             return []
#
#         # доступные метки времени
#         available = []
#         for d in data:
#             dt = u.parse_iso_ts((d or {}).get("date"), to_tz=u.tz.MSK)
#             if dt:
#                 available.append(dt)
#         if not available:
#             return []
#
#         # якорь
#         scheduled_iso = meta.get("scheduled_dttm") or meta.get("run_schedule_dttm")
#         anchor = u.parse_iso_ts(scheduled_iso, to_tz=u.tz.MSK) if scheduled_iso else (u.to_tz(run_dttm, u.tz.MSK) if run_dttm else max(available))
#
#         if time_grain == "1h":
#             same_day = [dt for dt in available if dt.date() == anchor.date()]
#             stat_dt = max(same_day) if same_day else max(available)
#         else:
#             stat_dt = max([dt for dt in available if dt.date() == anchor.date() - timedelta(days=1)] or available)
#
#         kw_day = next((d for d in data if u.to_tz(u.parse_iso_ts((d or {}).get("date")), u.tz.MSK) == stat_dt), None) or max(
#             data, key=lambda x: u.parse_iso_ts((x or {}).get("date")) or u.DATETIME_MIN
#         )
#
#         inserted_at = meta.get("inserted_at") or run_dttm or resp_dttm
#         stats_list = (kw_day or {}).get("stats") or []
#
#         out = []
#         for s in stats_list:
#             out.append({
#                 "company_id": company_id,
#                 "request_uuid": req_uuid,
#                 "advert_id": advert_id,
#                 "date": u.to_date(stat_dt),
#                 "keyword": (s.get("keyword") or "").strip(),
#                 "views": u.to_int(s.get("views"), default=0) or 0,
#                 "clicks": u.to_int(s.get("clicks"), default=0) or 0,
#                 "cost": u.to_float(s.get("sum"), default=0.0) or 0.0,
#                 "inserted_at": inserted_at,
#             })
#         return out
#
#     # ---------------------------------------------------
#     # wb_paid_acceptances_1d
#     # ---------------------------------------------------
#     def norm__wb_paid_acceptances_1d(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         req_uuid = meta.get("request_uuid")
#         resp_dttm = meta.get("response_dttm")
#         company_id = meta.get("company_id")
#
#         data = u.json_from_payload(payload)
#         rows = u.ensure_list(data)
#         I, F, D = u.to_int, u.to_float, u.to_date
#
#         out = []
#         for row in rows:
#             nm_id_raw = (row or {}).get("nmID")
#             if nm_id_raw is None:
#                 nm_id_raw = (row or {}).get("nmId")
#             income_id = I((row or {}).get("incomeId"))
#             nm_id = I(nm_id_raw)
#             if income_id is None or nm_id is None:
#                 continue
#             out.append({
#                 "request_uuid": req_uuid,
#                 "response_dttm": resp_dttm,
#                 "company_id": company_id,
#                 "quantity": I(row.get("count")) or 0,
#                 "gi_create_date": D(row.get("giCreateDate")),
#                 "shk_create_date": D(row.get("shkCreateDate")),
#                 "income_id": income_id,
#                 "nm_id": nm_id,
#                 "total_cost": F(row.get("total")) or 0.0,
#                 "subject_name": (row.get("subjectName") or row.get("subject") or ""),
#             })
#         return out
#
#     # ---------------------------------------------------
#     # wb_adv_promotions_1h
#     # ---------------------------------------------------
#     def norm__wb_adv_promotions_1h(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         req_uuid = meta.get("request_uuid")
#         resp_dttm = meta.get("response_dttm")
#         company_id = meta.get("company_id")
#         business_dt = meta.get("business_dttm")
#
#         data = u.json_from_payload(payload) or {}
#         groups = (data.get("adverts") or []) if isinstance(data, dict) else []
#
#         out = []
#         for group in groups or []:
#             g_status = group.get("status")
#             g_type = group.get("type")
#             for adv in (group.get("advert_list") or []):
#                 advert_id = u.to_int(adv.get("advertId"))
#                 if advert_id is None:
#                     continue
#                 out.append({
#                     "request_uuid": req_uuid,
#                     "response_dttm": resp_dttm,
#                     "company_id": company_id,
#                     "business_dttm": business_dt,
#                     "advert_id": advert_id,
#                     "advert_status": adv.get("status", g_status),
#                     "advert_type": adv.get("type", g_type),
#                     "change_time": u.parse_iso_ts(adv.get("changeTime")),
#                 })
#         return out
#
#     # ---------------------------------------------------
#     # wb_adv_campaigns
#     # ---------------------------------------------------
#     def norm__wb_adv_campaigns(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         req_uuid = meta.get("request_uuid")
#         business_dttm = meta.get("business_dttm") or meta.get("run_dttm")
#         receive_dttm = meta.get("response_dttm")
#
#         data = u.json_from_payload(payload) or {}
#         groups = data.get("adverts") or data.get("items") or []
#         if not isinstance(groups, list):
#             groups = []
#
#         out = []
#         for g in groups:
#             if not isinstance(g, dict):
#                 continue
#             g_type = u.to_int(g.get("type"))
#             g_status = u.to_int(g.get("status"))
#             for a in (g.get("advert_list") or []):
#                 if not isinstance(a, dict):
#                     continue
#                 out.append({
#                     "business_dttm": business_dttm,
#                     "request_uuid": req_uuid,
#                     "receive_dttm": receive_dttm,
#                     "advert_id": u.to_int(a.get("advertId")),
#                     "advert_name": a.get("name"),
#                     "create_time": u.parse_iso_ts(a.get("createTime"), relax_spaces=True),
#                     "change_time": u.parse_iso_ts(a.get("changeTime"), relax_spaces=True),
#                     "start_time": u.parse_iso_ts(a.get("startTime"), relax_spaces=True),
#                     "end_time": u.parse_iso_ts(a.get("endTime"), relax_spaces=True),
#                     "advert_status": u.to_int(a.get("status")) if a.get("status") is not None else g_status,
#                     "advert_type": u.to_int(a.get("type")) if a.get("type") is not None else g_type,
#                     "payment_type": a.get("paymentType"),
#                     "daily_budget": u.to_int(a.get("dailyBudget")),
#                     "search_pluse_state": a.get("searchPluseState"),
#                 })
#         return [r for r in out if r.get("advert_id") is not None]
#
#     # ---------------------------------------------------
#     # wb_adv_product_rates
#     # ---------------------------------------------------
#     def norm__wb_adv_product_rates(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         company_id = meta.get("company_id")
#         req_uuid = meta.get("request_uuid")
#         run_dttm = meta.get("run_dttm")
#         inserted_at = meta.get("inserted_at") or run_dttm
#
#         data = u.json_from_payload(payload)
#         if isinstance(data, dict):
#             data = data.get("adverts") or data.get("items") or data.get("result") or []
#         if not isinstance(data, list):
#             data = []
#
#         I, F = u.to_int, u.to_float
#         out = []
#         for entry in data:
#             if not isinstance(entry, dict):
#                 continue
#             advert_id = I(entry.get("advertId"))
#             campaign_type = I(entry.get("type"))
#             status = I(entry.get("status"))
#
#             params_list: List[Dict[str, Any]] = []
#             if campaign_type == 8:  # авто-кампании
#                 auto = entry.get("autoParams") or {}
#                 base = {
#                     "advert_status": status,
#                     "advert_type": campaign_type,
#                     "cpm_first": F(auto.get("cpm")),
#                     "active_carousel": (auto.get("active") or {}).get("carousel"),
#                     "active_recom": (auto.get("active") or {}).get("recom"),
#                     "active_booster": (auto.get("active") or {}).get("booster"),
#                     "subject_id": I((auto.get("subject") or {}).get("id")),
#                     "cpm_catalog": None,
#                     "cpm_search": None,
#                 }
#                 for m in (auto.get("nmCPM") or []):
#                     params_list.append({**base, "nm_id": I((m or {}).get("nm")), "cpm_current": F((m or {}).get("cpm"))})
#
#             elif campaign_type == 9:  # аукционы
#                 for ap in (entry.get("auction_multibids") or []):
#                     params_list.append({
#                         "nm_id": I((ap or {}).get("nm")),
#                         "cpm_current": F((ap or {}).get("bid")),
#                         "cpm_catalog": None,
#                         "cpm_search": None,
#                         "cpm_first": None,
#                         "advert_status": status,
#                         "advert_type": campaign_type,
#                         "active_carousel": None,
#                         "active_recom": None,
#                         "active_booster": None,
#                         "subject_id": None,
#                     })
#                 for up in (entry.get("unitedParams") or []):
#                     base = {
#                         "advert_status": status,
#                         "advert_type": campaign_type,
#                         "cpm_catalog": F((up or {}).get("catalogCPM")),
#                         "cpm_search": F((up or {}).get("searchCPM")),
#                         "cpm_first": None,
#                         "active_carousel": None,
#                         "active_recom": None,
#                         "active_booster": None,
#                         "subject_id": I(((up or {}).get("subject") or {}).get("id")),
#                     }
#                     nms = (up or {}).get("nms") or []
#                     if isinstance(nms, dict):
#                         nms = [nms]
#                     for nm in nms:
#                         nm_id = I(nm if isinstance(nm, (int, str)) else (nm or {}).get("nm"))
#                         params_list.append({**base, "nm_id": nm_id, "cpm_current": None})
#             else:
#                 for ap in (entry.get("params") or []):
#                     price = F((ap or {}).get("price"))
#                     subject_id = I((ap or {}).get("subjectId"))
#                     nms_list = (ap or {}).get("nms") or []
#                     if isinstance(nms_list, dict):
#                         nms_list = [nms_list]
#                     for nm_entry in nms_list:
#                         nm_id = I(nm_entry if isinstance(nm_entry, (int, str)) else (nm_entry or {}).get("nm"))
#                         params_list.append({
#                             "nm_id": nm_id,
#                             "cpm_current": price,
#                             "cpm_catalog": None,
#                             "cpm_search": None,
#                             "cpm_first": None,
#                             "advert_status": status,
#                             "advert_type": campaign_type,
#                             "active_carousel": None,
#                             "active_recom": None,
#                             "active_booster": None,
#                             "subject_id": subject_id,
#                         })
#
#             for p in params_list:
#                 out.append({
#                     "company_id": company_id,
#                     "request_uuid": req_uuid,
#                     "inserted_at": inserted_at,
#                     "run_dttm": run_dttm,
#                     "advert_id": advert_id,
#                     **p,
#                 })
#         return out
#
#     # ---------------------------------------------------
#     # wb_adv_product_stats_1d (fullstats v3)
#     # ---------------------------------------------------
#     def norm__wb_adv_product_stats_1d(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         company_id = meta.get("company_id")
#         request_uuid = meta.get("request_uuid")
#         business_dttm = meta.get("business_dttm")
#         response_dttm = meta.get("response_dttm") or meta.get("receive_dttm")
#
#         data = u.json_from_payload(payload)
#         if isinstance(data, dict):
#             data = data.get("result") or data.get("items") or data.get("adverts") or []
#         if not isinstance(data, list):
#             return []
#
#         I, F = u.to_int, u.to_float
#         out = []
#         for entry in data:
#             if not isinstance(entry, dict):
#                 continue
#             advert_id = I(entry.get("advertId"))
#             if advert_id is None:
#                 continue
#             for day in (entry.get("days") or []):
#                 dd = u.parse_iso_ts((day or {}).get("date"), to_tz=u.tz.MSK)
#                 if not dd:
#                     continue
#                 date = u.day_start(dd)
#                 for app in (day.get("apps") or []):
#                     app_type = I(app.get("appType") or app.get("app_type"))
#                     nms = app.get("nms")
#                     if not isinstance(nms, list):
#                         nms = app.get("nm") or []
#                     for nm in nms:
#                         nm_id = I((nm or {}).get("nmId"))
#                         if nm_id is None:
#                             continue
#                         out.append({
#                             "company_id": company_id,
#                             "request_uuid": request_uuid,
#                             "advert_id": advert_id,
#                             "app_type": app_type,
#                             "nm_id": nm_id,
#                             "request_dttm": u.parse_iso_ts(response_dttm) if isinstance(response_dttm, str) else response_dttm,
#                             "business_dttm": business_dttm,
#                             "response_dttm": response_dttm,
#                             "date": date,
#                             "views": I(nm.get("views")),
#                             "clicks": I(nm.get("clicks")),
#                             "cost": F(nm.get("sum")),
#                             "carts": I(nm.get("atbs")),
#                             "orders": I(nm.get("orders")),
#                             "items": I(nm.get("shks")),
#                             "revenue": F(nm.get("sum_price")),
#                             "canceled": I(nm.get("canceled")),
#                         })
#         return out
#
#     # ---------------------------------------------------
#     # wb_adv_product_positions {1d,1h}
#     # ---------------------------------------------------
#     def norm__wb_adv_product_positions(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         company_id = meta.get("company_id")
#         request_uuid = meta.get("request_uuid")
#         response_dttm = meta.get("response_dttm")
#         time_grain = (meta.get("time_grain") or "1d").lower()
#         biz_anchor = meta.get("business_dttm")
#
#         data = u.json_from_payload(payload)
#         if isinstance(data, dict):
#             data = data.get("result") or data.get("items") or data.get("adverts") or []
#         if not isinstance(data, list):
#             return []
#
#         I = u.to_int
#         out = []
#         for entry in data or []:
#             if not isinstance(entry, dict):
#                 continue
#             advert_id = I(entry.get("advertId"))
#             if advert_id is None:
#                 continue
#             for bst in (entry.get("boosterStats") or []):
#                 bst_date = u.parse_iso_ts((bst or {}).get("date"), to_tz=u.tz.MSK)
#                 if not bst_date:
#                     continue
#                 if time_grain == "1h":
#                     if not biz_anchor:
#                         continue
#                     bucket = u.hour_bucket_start(bst_date)
#                     if bucket != biz_anchor:
#                         continue
#                     business_dttm = biz_anchor
#                 else:
#                     business_dttm = u.day_start(bst_date)
#                 out.append({
#                     "company_id": company_id,
#                     "request_uuid": request_uuid,
#                     "advert_id": advert_id,
#                     "nm_id": I(bst.get("nm")),
#                     "datetime": bst_date,
#                     "avg_position": I(bst.get("avg_position")),
#                     "business_dttm": business_dttm,
#                     "response_dttm": response_dttm,
#                 })
#         return out
#
#     # ---------------------------------------------------
#     # auto/stat-words → wb_adv_keyword_clusters_1d
#     # ---------------------------------------------------
#     def norm__auto_stat_words(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         rp = meta.get("request_parameters") or {}
#         advert_id = rp.get("advert_id", meta.get("advert_id"))
#
#         data = u.json_from_payload(payload) or {}
#         clusters = data.get("clusters") or []
#         excluded = data.get("excluded") or []
#
#         out = []
#         for c in clusters:
#             if not isinstance(c, dict):
#                 continue
#             cluster_name = c.get("cluster")
#             cluster_views = c.get("count")
#             for kw in (c.get("keywords") or []):
#                 out.append({
#                     "run_dttm": meta.get("run_dttm"),
#                     "advert_id": advert_id,
#                     "keyword_cluster": cluster_name,
#                     "keyword_cluster_views": u.to_int(cluster_views, default=0) or 0,
#                     "keyword": str(kw) if kw is not None else "",
#                     "is_excluded": False,
#                 })
#         for ex in excluded:
#             out.append({
#                 "run_dttm": meta.get("run_dttm"),
#                 "advert_id": advert_id,
#                 "keyword_cluster": str(ex) if ex is not None else "",
#                 "keyword_cluster_views": 0,
#                 "keyword": "",
#                 "is_excluded": True,
#             })
#         return out
#
#     # ---------------------------------------------------
#     # /cards/list → wb_mp_skus_1d
#     # ---------------------------------------------------
#     def norm__wb_mp_skus_1d(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         req_uuid = meta.get("request_uuid")
#         resp_dttm = meta.get("response_dttm")
#         company_id = meta.get("company_id")
#
#         data = u.json_from_payload(payload)
#         if isinstance(data, dict):
#             for k in ("data", "result", "items", "cards"):
#                 v = data.get(k)
#                 if isinstance(v, list):
#                     data = v
#                     break
#             else:
#                 data = [data]
#         elif not isinstance(data, list):
#             data = []
#
#         I, F, S = u.to_int, u.to_float, u.to_str
#         out = []
#         for item in data:
#             if not isinstance(item, dict):
#                 continue
#             sizes = item.get("sizes") or []
#             first_size = sizes[0] if sizes else {}
#             skus = first_size.get("skus") or []
#             barcode = skus[0] if skus else None
#             dims = item.get("dimensions") or {}
#             out.append({
#                 "request_uuid": req_uuid,
#                 "response_dttm": resp_dttm,
#                 "company_id": company_id,
#                 "nm_id": I(item.get("nmID")),
#                 "imt_id": I(item.get("imtID")),
#                 "subject_id": I(item.get("subjectID")),
#                 "subject_name": S(item.get("subjectName")) or "",
#                 "supplier_article": S(item.get("vendorCode")) or "",
#                 "brand": S(item.get("brand")) or "",
#                 "title": S(item.get("title")) or "",
#                 "description": S(item.get("description")) or "",
#                 "length": F(dims.get("length")),
#                 "width": F(dims.get("width")),
#                 "height": F(dims.get("height")),
#                 "weight": F(dims.get("weightBrutto")),
#                 "chrt_id": I(first_size.get("chrtID")),
#                 "tech_size": S(first_size.get("techSize")),
#                 "barcode": S(barcode),
#             })
#         return out
#
#     # ---------------------------------------------------
#     # /supplier/incomes → wb_supplier_incomes_1d
#     # ---------------------------------------------------
#     def norm__wb_supplier_incomes_1d(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         req_uuid = meta.get("request_uuid")
#         resp_dttm = meta.get("response_dttm")
#         company_id = meta.get("company_id")
#
#         data = u.json_from_payload(payload)
#         rows = u.ensure_list(data)
#
#         I, F, DT, D = u.to_int, u.to_float, u.to_datetime, u.to_date
#         out = []
#         for item in rows:
#             if not isinstance(item, dict):
#                 continue
#             rec = {
#                 "request_uuid": req_uuid,
#                 "response_dttm": resp_dttm,
#                 "company_id": company_id,
#                 "income_id": I(item.get("incomeId")),
#                 "number": item.get("number"),
#                 "date": DT(item.get("date")),
#                 "last_change_date": DT(item.get("lastChangeDate")),
#                 "supplier_article": (item.get("supplierArticle") or ""),
#                 "tech_size": item.get("techSize"),
#                 "barcode": item.get("barcode"),
#                 "quantity": I(item.get("quantity"), default=0) or 0,
#                 "total_price": F(item.get("totalPrice"), default=0.0) or 0.0,
#                 "date_close": DT(item.get("dateClose")),
#                 "warehouse_name": item.get("warehouseName"),
#                 "status": item.get("status"),
#                 "nm_id": I(item.get("nmId")),
#             }
#             if rec["income_id"] is None or (rec["barcode"] in (None, "")):
#                 continue
#             out.append(rec)
#         return out
#
#     # ---------------------------------------------------
#     # /supplier/orders → wb_order_items_1d
#     # ---------------------------------------------------
#     def norm__wb_order_items_1d(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         req_uuid = meta.get("request_uuid")
#         resp_dttm = meta.get("response_dttm")
#         company_id = meta.get("company_id")
#
#         data = u.json_from_payload(payload)
#         rows = u.ensure_list(data)
#
#         # вычислим business_dttm (MSK)
#         business_dt = meta.get("business_dttm")
#         if business_dt is None:
#             sched_iso = meta.get("run_schedule_dttm") or meta.get("scheduled_dttm")
#             anchor = u.parse_iso_ts(sched_iso, to_tz=u.tz.MSK) if isinstance(sched_iso, str) else u.to_tz(resp_dttm, u.tz.MSK) or u.now_msk()
#             business_dt = u.day_start(anchor - u.timedelta(days=1)) if anchor else None
#         if resp_dttm is None:
#             resp_dttm = u.now_msk()
#
#         I, F, S = u.to_int, u.to_float, u.to_str
#         out = []
#         for item in rows:
#             if not isinstance(item, dict):
#                 continue
#             dt = u.to_msk(item.get("date"))
#             last_dt = u.to_msk(item.get("lastChangeDate"))
#             cancel_dt = u.to_msk(item.get("cancelDate"))
#             if last_dt is None:
#                 continue
#             out.append({
#                 "request_uuid": req_uuid,
#                 "response_dttm": resp_dttm,
#                 "business_dttm": business_dt,
#                 "g_number": S(item.get("gNumber")),
#                 "sr_id": item.get("srid"),
#                 "date": dt,
#                 "last_change_date": last_dt,
#                 "warehouse_name": S(item.get("warehouseName")),
#                 "warehouse_type": S(item.get("warehouseType")),
#                 "country_name": S(item.get("countryName")),
#                 "oblast_okrug_name": S(item.get("oblastOkrugName")),
#                 "region_name": S(item.get("regionName")),
#                 "supplier_article": S(item.get("supplierArticle")),
#                 "nm_id": item.get("nmId"),
#                 "barcode": S(item.get("barcode")),
#                 "category": S(item.get("category")),
#                 "subject": S(item.get("subject")),
#                 "brand": S(item.get("brand")),
#                 "tech_size": S(item.get("techSize")),
#                 "income_id": I(item.get("incomeID")),
#                 "total_price": F(item.get("totalPrice")),
#                 "discount_percent": F(item.get("discountPercent")),
#                 "spp": I(item.get("spp")),
#                 "finished_price": F(item.get("finishedPrice")),
#                 "price_with_discount": F(item.get("priceWithDisc")),
#                 "is_cancel": bool(item.get("isCancel", False)),
#                 "cancel_date": cancel_dt,
#                 "sticker": S(item.get("sticker")),
#                 "company_id": company_id,
#             })
#         return out
#
#     # ---------------------------------------------------
#     # /tariffs/commission → wb_commission_1d
#     # ---------------------------------------------------
#     def norm__wb_commission_1d(self, payload, meta=None):
#         from decimal import Decimal
#         u = self._u
#         meta = meta or {}
#         req_uuid = meta.get("request_uuid")
#         resp_dttm = meta.get("response_dttm")
#         business_dttm = meta.get("business_dttm") or resp_dttm
#
#         data = u.json_from_payload(payload)
#         items = []
#         if isinstance(data, dict):
#             items = data.get("report") or data.get("commission") or data.get("items") or []
#         if isinstance(items, dict):
#             items = [items]
#         if not isinstance(items, list):
#             items = []
#
#         I, S = u.to_int, (lambda v: "" if v is None else str(v))
#         D4 = u.decimal_4
#         out = []
#         for it in items:
#             if not isinstance(it, dict):
#                 continue
#             out.append({
#                 "request_uuid": req_uuid,
#                 "response_dttm": resp_dttm,
#                 "business_dttm": business_dttm,
#                 "kgvp_booking": D4(it.get("kgvpBooking")),
#                 "kgvp_marketplace": D4(it.get("kgvpMarketplace")),
#                 "kgvp_pickup": D4(it.get("kgvpPickup")),
#                 "kgvp_supplier": D4(it.get("kgvpSupplier")),
#                 "kgvp_supplier_express": D4(it.get("kgvpSupplierExpress")),
#                 "kgvp_paid_storage": D4(it.get("paidStorageKgvp")),
#                 "category_id": I(it.get("parentID")),
#                 "category_name": S(it.get("parentName")),
#                 "subject_id": I(it.get("subjectID")),
#                 "subject_name": S(it.get("subjectName")),
#             })
#         return out
#
#     # ---------------------------------------------------
#     # /warehouse-remains → wb_stocks_1d
#     # ---------------------------------------------------
#     def norm__wb_stocks_1d(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         req_uuid = meta.get("request_uuid")
#         resp_dttm = meta.get("response_dttm")
#         company_id = meta.get("company_id")
#         run_dttm = meta.get("run_dttm")
#
#         data = u.json_from_payload(payload)
#         rows = u.ensure_list(data)
#
#         I, F, S = u.to_int, u.to_float, u.to_str
#         # дата для silver — день запуска бронзы/resp_dttm
#         out_date = run_dttm.date() if u.is_date_like(run_dttm) else (resp_dttm.date() if u.is_datetime(resp_dttm) else None)
#
#         out = []
#         for row in rows:
#             if not isinstance(row, dict):
#                 continue
#             warehouses = row.get("warehouses") or []
#             if not isinstance(warehouses, list):
#                 warehouses = []
#             in_way_to_client = None
#             in_way_from_client = None
#             total = None
#             for w in warehouses:
#                 name = (S((w or {}).get("warehouseName")) or "")
#                 norm = name.replace(" ", "").lower()
#                 qty = I((w or {}).get("quantity"))
#                 if norm == "впутидополучателей":
#                     in_way_to_client = qty
#                 elif norm == "впутивозвратынаскладwb":
#                     in_way_from_client = qty
#                 elif norm == "всегонаходитсянаскладах":
#                     total = qty
#             common = {
#                 "request_uuid": req_uuid,
#                 "response_dttm": resp_dttm,
#                 "company_id": company_id,
#                 "date": out_date,
#                 "supplier_article": S(row.get("vendorCode")) or "",
#                 "nm_id": I(row.get("nmId")),
#                 "barcode": S(row.get("barcode")),
#                 "tech_size": S(row.get("techSize")),
#                 "volume": F(row.get("volume")),
#                 "in_way_to_client": in_way_to_client,
#                 "in_way_from_client": in_way_from_client,
#                 "total": total,
#             }
#             for w in warehouses:
#                 name = S((w or {}).get("warehouseName")) or ""
#                 norm = name.replace(" ", "").lower()
#                 if norm in {"впутидополучателей", "впутивозвратынаскладwb", "всегонаходитсянаскладах"}:
#                     continue
#                 out.append({**common, "warehouse_name": name, "quantity": I((w or {}).get("quantity"))})
#         return out
#
#     # ---------------------------------------------------
#     # /exactmatch/.../search → wb_www_text_search_1d
#     # ---------------------------------------------------
#     def norm__wb_www_text_search_1d(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         req_uuid = meta.get("request_uuid")
#         resp_dttm = meta.get("response_dttm")
#         business_dttm = meta.get("business_dttm")
#         company_id = meta.get("company_id")
#
#         params = meta.get("request_parameters") or {}
#         if not params and isinstance(payload, dict):
#             rp = payload.get("request_parameters")
#             params = u.json_loads_safely(rp) if isinstance(rp, str) else (rp if isinstance(rp, dict) else {})
#
#         data = u.json_from_payload(payload)
#         md = (data.get("metadata") or {}) if isinstance(data, dict) else {}
#         sr = (md.get("search_result") or {})
#
#         I, F, S = u.to_int, u.to_float, u.to_str
#         keyword = params.get("query")
#         app_type = I(params.get("appType"))
#         lang = S(params.get("lang")) or ""
#         cur = S(params.get("curr")) or ""
#         resultset = S(params.get("resultset")) or ""
#         page = I(params.get("page"), default=1) or 1
#
#         catalog_type = S(md.get("catalog_type")) or ""
#         catalog_value = S(md.get("catalog_value")) or ""
#         normquery = md.get("normquery")
#         search_result_name = S(sr.get("name") or md.get("name")) or ""
#         search_result_rmi = S(sr.get("rmi") or md.get("rmi")) or ""
#         search_result_title = S(sr.get("title") or md.get("title")) or ""
#         search_result_rs = I(sr.get("rs") or md.get("rs"))
#         search_result_qv = S(sr.get("qv") or md.get("qv")) or ""
#
#         products = data.get("products") or [] if isinstance(data, dict) else []
#         if not isinstance(products, list):
#             products = []
#
#         out = []
#         for idx, prod in enumerate(products, start=1):
#             nm_id_val = prod.get("id")
#             if nm_id_val in (None, "", 0):
#                 continue
#             colors = prod.get("colors") or []
#             color0 = colors[0] if colors else {}
#             sizes = prod.get("sizes") or []
#             size0 = sizes[0] if sizes else {}
#             log = prod.get("log") or {}
#             pmeta = prod.get("meta") or {}
#             price0 = size0.get("price") or {}
#
#             out.append({
#                 "company_id": I(company_id),
#                 "request_uuid": req_uuid,
#                 "response_dttm": resp_dttm,
#                 "business_dttm": business_dttm,
#                 "keyword": keyword,
#                 "date": resp_dttm,
#                 "nm_position": idx,
#                 "app_type": app_type,
#                 "lang": lang,
#                 "cur": cur,
#                 "resultset": resultset,
#                 "page": page,
#                 "catalog_type": catalog_type,
#                 "catalog_value": catalog_value,
#                 "normquery": normquery,
#                 "search_result_name": search_result_name,
#                 "search_result_rmi": search_result_rmi,
#                 "search_result_title": search_result_title,
#                 "search_result_rs": search_result_rs,
#                 "search_result_qv": search_result_qv,
#                 "nm_id": I(prod.get("id")),
#                 "product_time_1": I(prod.get("time1")),
#                 "product_time_2": I(prod.get("time2")),
#                 "product_wh": I(prod.get("wh")),
#                 "product_d_type": I(prod.get("dtype")),
#                 "product_dist": I(prod.get("dist")),
#                 "product_root": I(prod.get("root")),
#                 "product_kind_id": I(prod.get("kindId")),
#                 "product_brand": S(prod.get("brand")),
#                 "product_brand_id": I(prod.get("brandId")),
#                 "product_site_brand_id": I(prod.get("siteBrandId")),
#                 "product_colors_id": I(color0.get("id")) if color0 else None,
#                 "product_colors_name": color0.get("name") if color0 else None,
#                 "product_subject_id": I(prod.get("subjectId")),
#                 "product_subject_parent_id": I(prod.get("subjectParentId")),
#                 "product_name": S(prod.get("name")),
#                 "product_entity": S(prod.get("entity")),
#                 "product_match_id": I(prod.get("matchId")),
#                 "product_supplier": S(prod.get("supplier")),
#                 "product_supplier_id": I(prod.get("supplierId")),
#                 "product_supplier_rating": F(prod.get("supplierRating")),
#                 "product_supplier_flags": I(prod.get("supplierFlags")),
#                 "product_pics": I(prod.get("pics")),
#                 "product_rating": I(prod.get("rating")),
#                 "product_review_rating": F(prod.get("reviewRating")),
#                 "product_nm_review_rating": F(prod.get("nmReviewRating")),
#                 "product_feedbacks": I(prod.get("feedbacks")),
#                 "product_nm_feedbacks": I(prod.get("nmFeedbacks")),
#                 "product_panel_promo_id": I(prod.get("panelPromoId")) if prod.get("panelPromoId") is not None else None,
#                 "product_volume": I(prod.get("volume")),
#                 "product_view_flags": I(prod.get("viewFlags")),
#                 "product_sizes_name": S(size0.get("name")),
#                 "product_sizes_orig_name": S(size0.get("origName")),
#                 "product_sizes_rank": I(size0.get("rank")),
#                 "product_sizes_option_id": I(size0.get("optionId")),
#                 "product_sizes_wh": I(size0.get("wh")),
#                 "product_sizes_time_1": I(size0.get("time1")),
#                 "product_sizes_time_2": I(size0.get("time2")),
#                 "product_sizes_d_type": I(size0.get("dtype")),
#                 "product_sizes_price_basic": I(price0.get("basic")),
#                 "product_sizes_price_product": I(price0.get("product")),
#                 "product_sizes_price_logistics": I(price0.get("logistics")),
#                 "product_sizes_price_return": I(price0.get("return")),
#                 "product_sizes_sale_conditions": I(size0.get("saleConditions")),
#                 "product_sizes_payload": S(size0.get("payload")),
#                 "product_total_quantity": I(prod.get("totalQuantity")) if prod.get("totalQuantity") is not None else None,
#                 "product_log_cpm": I(log.get("cpm")) if log else None,
#                 "product_log_promotion": I(log.get("promotion")) if log else None,
#                 "product_log_promo_position": I(log.get("promoPosition")) if log else None,
#                 "product_log_position": I(log.get("position")) if log else None,
#                 "product_log_advert_id": I(log.get("advertId")) if log else None,
#                 "product_log_tp": S(log.get("tp")) if log else None,
#                 "product_logs": prod.get("logs"),
#                 "product_meta_tokens": u.dumps_json((pmeta.get("tokens", []))),
#                 "product_meta_preset_id": I(pmeta.get("presetId")),
#             })
#         return out
#
#     # ---------------------------------------------------
#     # wb_fin_reports_1w (JSON → rows, типизация дат)
#     # ---------------------------------------------------
#     def norm__wb_fin_reports_1w(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         data = u.json_from_payload(payload)
#         rows = u.ensure_list(data)
#
#         OPTIONAL = [
#             "bonus_type_name", "suppliercontract_code", "rebill_logistic_org",
#             "kiz", "sticker_id", "ppvz_office_name", "ppvz_supplier_name",
#             "ppvz_inn", "declaration_number", "trbx_id",
#         ]
#         DATE_FIELDS = {
#             "date_from", "date_to", "create_dt", "fix_tariff_date_from",
#             "fix_tariff_date_to", "rr_dt",
#         }
#         TSTZ_FIELDS = {"order_dt", "sale_dt"}
#
#         out = []
#         for row in rows:
#             if not isinstance(row, dict):
#                 continue
#             rec = dict(row)
#             for k, v in list(rec.items()):
#                 if isinstance(v, str) and v.strip() == "":
#                     rec[k] = None
#             for k in OPTIONAL:
#                 rec.setdefault(k, None)
#             for k in DATE_FIELDS:
#                 if k in rec:
#                     rec[k] = u.to_date(rec[k])
#             for k in TSTZ_FIELDS:
#                 if k in rec:
#                     rec[k] = u.to_datetime(rec[k], prefer_tz=u.tz.MSK)
#             if not rec.get("bonus_type_name"):
#                 rec["bonus_type_name"] = " "
#             if not rec.get("srid"):
#                 rec["srid"] = " "
#             rec.update({
#                 "request_uuid": meta.get("request_uuid"),
#                 "response_dttm": meta.get("response_dttm"),
#                 "company_id": meta.get("company_id"),
#                 "business_dttm": meta.get("business_dttm"),
#             })
#             out.append(rec)
#         return out
#
#     # ===================== WB: FIN REPORT 1D (ZIP→XLSX) =====================
#
#     def norm__wb_fin_adjustments_product_1d(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         df = u.xlsx_from_bronze(payload)
#         if df is None or df.empty:
#             return []
#         reason_col = "Обоснование для оплаты"
#         reasons = {"Удержание", "Штраф", "Добровольная компенсация при возврате"}
#         if reason_col in df.columns:
#             df = df[df[reason_col].isin(reasons)].copy()
#         if df.empty:
#             return []
#         rename_map = {
#             "Номер поставки": "income_id",
#             "Предмет": "subject",
#             "Код номенклатуры": "nomenclature_code",
#             "Бренд": "brand_name",
#             "Артикул поставщика": "suplier_article",
#             "Название": "name",
#             "Размер": "tech_size",
#             "Баркод": "barcode",
#             "Тип документа": "doc_type_name",
#             "Обоснование для оплаты": "supplier_oper_name",
#             "Дата заказа покупателем": "order_date",
#             "Дата продажи": "sale_date",
#             "Общая сумма штрафов": "penalty",
#             "Виды логистики, штрафов и доплат": "bonus_type_name",
#             "Номер офиса": "office_id",
#             "Склад": "warehouse_name",
#             "Страна": "country",
#             "Тип коробов": "box_type",
#             "ШК": "shk_id",
#             "Srid": "srid",
#             "К перечислению Продавцу за реализованный Товар": "seller_payout",
#             "Корректировка Вознаграждения Вайлдберриз (ВВ)": "additional_payment",
#             "Сумма удержанная за начисленные баллы программы лояльности": "cashback_amount",
#             "Компенсация скидки по программе лояльности": "cashback_discount",
#         }
#         df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
#         for c in ("income_id", "nomenclature_code", "office_id", "shk_id"):
#             u.safe_int_col(df, c)
#         for c in ("order_date", "sale_date"):
#             u.safe_date_col(df, c)
#         for c in ("penalty", "seller_payout", "additional_payment", "cashback_amount", "cashback_discount"):
#             u.safe_float_col(df, c)
#         df = u.with_meta(df, meta)
#         target_cols = [
#             "business_dttm", "company_id", "request_uuid", "response_dttm",
#             "income_id", "subject", "nomenclature_code", "brand_name", "suplier_article",
#             "name", "tech_size", "barcode", "doc_type_name", "supplier_oper_name",
#             "order_date", "sale_date", "penalty", "bonus_type_name", "office_id",
#             "warehouse_name", "country", "box_type", "shk_id", "srid",
#             "seller_payout", "additional_payment", "cashback_amount", "cashback_discount",
#         ]
#         df = u.ensure_cols(df, target_cols)
#         return df[target_cols].to_dict(orient="records")
#
#     def norm__wb_fin_adjustments_general_1d(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         df = u.xlsx_from_bronze(payload)
#         if df is None or df.empty:
#             return []
#         reason_col = "Обоснование для оплаты"
#         reasons = {"Удержание", "Штраф", "Добровольная компенсация при возврате"}
#         if reason_col in df.columns:
#             df = df[df[reason_col].isin(reasons)].copy()
#         if df.empty:
#             return []
#         rename_map = {
#             "Номер поставки": "income_id",
#             "Предмет": "subject",
#             "Код номенклатуры": "nm_id",
#             "Бренд": "brand_name",
#             "Артикул поставщика": "suplier_article",
#             "Название": "name",
#             "Размер": "tech_size",
#             "Баркод": "barcode",
#             "Тип документа": "doc_type_name",
#             "Обоснование для оплаты": "supplier_oper_name",
#             "Дата заказа покупателем": "order_date",
#             "Дата продажи": "sale_date",
#             "Общая сумма штрафов": "penalty",
#             "Виды логистики, штрафов и доплат": "bonus_type_name",
#             "Номер офиса": "office_id",
#             "Склад": "warehouse_name",
#             "Страна": "country",
#             "Тип коробов": "box_type",
#             "ШК": "shk_id",
#             "Srid": "srid",
#             "Удержания": "deduction",
#         }
#         df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
#         for c in ("income_id", "nm_id", "office_id", "shk_id"):
#             u.safe_int_col(df, c)
#         for c in ("order_date", "sale_date"):
#             u.safe_date_col(df, c)
#         for c in ("penalty", "deduction"):
#             u.safe_float_col(df, c)
#         df = u.with_meta(df, meta)
#         target_cols = [
#             "business_dttm", "company_id", "request_uuid", "response_dttm",
#             "income_id", "subject", "nm_id", "brand_name", "suplier_article",
#             "name", "tech_size", "barcode", "doc_type_name", "supplier_oper_name",
#             "order_date", "sale_date", "penalty", "bonus_type_name", "office_id",
#             "warehouse_name", "country", "box_type", "shk_id", "srid", "deduction",
#         ]
#         df = u.ensure_cols(df, target_cols)
#         return df[target_cols].to_dict(orient="records")
#
#     def norm__wb_sales_1d(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         df = u.xlsx_from_bronze(payload)
#         if df is None or df.empty:
#             return []
#         reason_col = "Обоснование для оплаты"
#         reasons = {"Продажа", "Возврат"}
#         if reason_col in df.columns:
#             df = df[df[reason_col].isin(reasons)].copy()
#         if df.empty:
#             return []
#         rename_map = {
#             "Номер поставки": "income_id",
#             "Предмет": "subject",
#             "Код номенклатуры": "nm_id",
#             "Бренд": "brand_name",
#             "Артикул поставщика": "supplier_article",
#             "Название": "name",
#             "Размер": "tech_size",
#             "Баркод": "barcode",
#             "Обоснование для оплаты": "payment_reason",
#             "Дата заказа покупателем": "order_date",
#             "Дата продажи": "sale_date",
#             "Кол-во": "quantity",
#             "Цена розничная": "price",
#             "Вайлдберриз реализовал Товар (Пр)": "wb_realization_price",
#             "Скидка постоянного Покупателя (СПП), %": "spp",
#             "Размер кВВ, %": "commision_percent",
#             "Цена розничная с учетом согласованной скидки": "price_with_discount",
#             "Эквайринг/Комиссии за организацию платежей": "acquiring_amount",
#             "Размер комиссии за эквайринг/Комиссии за организацию платежей, %": "acquiring_percent",
#             "Наименование банка-эквайера": "acquiring_bank",
#             "Склад": "warehouse_name",
#             "Страна": "country",
#             "Srid": "srid",
#             "К перечислению Продавцу за реализованный Товар": "seller_payout",
#         }
#         df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
#         for c in ("income_id", "nm_id", "quantity"):
#             u.safe_int_col(df, c)
#         for c in ("order_date", "sale_date"):
#             u.safe_date_col(df, c)
#         for c in ("price", "wb_realization_price", "spp", "commision_percent",
#                    "price_with_discount", "acquiring_amount", "acquiring_percent", "seller_payout"):
#             u.safe_float_col(df, c)
#         df = u.with_meta(df, meta)
#         target_cols = [
#             "business_dttm",
#             "company_id", "request_uuid", "response_dttm",
#             "income_id", "subject", "nm_id", "brand_name", "supplier_article",
#             "name", "tech_size", "barcode", "payment_reason",
#             "order_date", "sale_date", "quantity", "price", "wb_realization_price",
#             "spp", "commision_percent", "price_with_discount",
#             "acquiring_amount", "acquiring_percent", "acquiring_bank",
#             "warehouse_name", "country", "seller_payout", "srid",
#         ]
#         df = u.ensure_cols(df, target_cols)
#         return df[target_cols].to_dict(orient="records")
#
#     def norm__wb_logistics_1d(self, payload, meta=None):
#         u = self._u
#         meta = meta or {}
#         df = u.xlsx_from_bronze(payload)
#         if df is None or df.empty:
#             return []
#         reason_col = "Обоснование для оплаты"
#         if reason_col in df.columns:
#             df = df[df[reason_col] == "Логистика"].copy()
#         if df.empty:
#             return []
#         rename_map = {
#             "Номер поставки": "income_id",
#             "Код номенклатуры": "nm_id",
#             "Бренд": "brand_name",
#             "Артикул поставщика": "supplier_article",
#             "Название": "name",
#             "Размер": "tech_size",
#             "Баркод": "barcode",
#             "Обоснование для оплаты": "payment_reason",
#             "Дата заказа покупателем": "order_date",
#             "Дата продажи": "sale_date",
#             "Кол-во": "delivery_quantity",
#             "Услуги по доставке товара покупателю": "logistic_cost",
#             "Виды логистики, штрафов и корректировок ВВ": "logistics_type",
#             "Склад": "warehouse_name",
#             "Страна": "country",
#             "Srid": "srid",
#         }
#         df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
#         for c in ("income_id", "nm_id", "delivery_quantity"):
#             u.safe_int_col(df, c)
#         for c in ("order_date", "sale_date"):
#             u.safe_date_col(df, c)
#         for c in ("logistic_cost",):
#             u.safe_float_col(df, c)
#         df = u.with_meta(df, meta)
#         target_cols = [
#             "business_dttm", "company_id", "request_uuid", "response_dttm",
#             "income_id", "nm_id", "brand_name", "supplier_article", "name",
#             "tech_size", "barcode", "payment_reason", "order_date", "sale_date",
#             "delivery_quantity", "logistic_cost", "logistics_type",
#             "warehouse_name", "country", "srid",
#         ]
#         df = u.ensure_cols(df, target_cols)
#         return df[target_cols].to_dict(orient="records")




def _to_int(x):
    try:
        return int(x) if x is not None and x != "" else None
    except Exception:
        return None

def _to_float(x):
    try:
        return float(x) if x is not None and x != "" else None
    except Exception:
        return None

def _to_str(x):
    if x is None:
        return None
    return str(x)

def _to_date(x) -> Optional[date]:
    if x is None or x == "":
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    if isinstance(x, datetime):
        return x.date()
    # строки вида "YYYY-MM-DD"
    try:
        return datetime.strptime(str(x), "%Y-%m-%d").date()
    except Exception:
        # любые ISO/другие — последняя попытка
        try:
            return datetime.fromisoformat(str(x)).date()
        except Exception:
            return None

def _ensure_list(payload: Any) -> List[Dict[str, Any]]:
    """Пытаемся достать «список записей» из типичных форм: {}, {'data': [...]}, {'result': [...]}, и т.п."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "result", "items", "rows"):
            val = payload.get(key)
            if isinstance(val, list):
                return val
        # иногда прилетает {'data': {'cards': [...]}} и т.п. — один уровень глубже
        for v in payload.values():
            if isinstance(v, dict):
                for key in ("data", "result", "items", "rows"):
                    val = v.get(key)
                    if isinstance(val, list):
                        return val
    return []

@dataclass
class WbUniversalNormalizer(Normalizer):
    """
    Универсальный нормализатор-реестр.
    target: логическое имя серебра/ассета, по которому выбирается метод нормализации.
    Примеры:
      - "nm_report_detail__buyouts_percent_1d"
      - "nm_report_detail__sales_funnels_1d"
      - "paid_storage_1d"
      - "stats_keywords"
    """
    target: str

    # ---------------------------------------------------
    # Публичный вход: совместим с normalize_wrapper фабрики
    # ---------------------------------------------------
    def normalize(
        self,
        payload: Any,
        *,
        meta: Optional[Dict[str, Any]] = None,
        partition_ctx: Optional[Dict[str, Any]] = None,
    ) -> Iterable[Dict[str, Any]]:
        # Для обратной совместимости: если нормализатор зовут как callable(body, partition_ctx=...)
        if meta is None and partition_ctx:
            meta = {
                "business_dttm": str(partition_ctx.get("business_dttm")),
                "company_id": partition_ctx.get("company_id"),
            }

        fn_name = f"norm__{self.target}"
        fn = getattr(self, fn_name, None)
        if not fn:
            raise RuntimeError(f"Нормализатор для '{self.target}' не реализован (ожидал метод {fn_name}())")
        return fn(payload, meta or {})

    # ---------------------------------------------------
    # nm_report_detail — buyouts_percent_1d
    # config.yml дал нам маппинг: nmId -> nm_id, buyoutsPercent -> buyouts_percent,
    # + date = partition.business_dttm@MSK (день)
    # ---------------------------------------------------
    def norm__wb_buyouts_percent_1d(self, payload, meta=None):
        """
        Извлекает buyoutsPercent по NM из bronze.response_body (nm-report/detail)
        → записи для silver.wb_buyouts_percent_1d

        Ожидаемые meta: request_uuid, response_dttm, company_id
        """
        import json
        from datetime import datetime, timezone

        meta = meta or {}
        req_uuid = meta.get("request_uuid")
        resp_dttm = meta.get("response_dttm")
        company_id = meta.get("company_id")

        # 1) Достаём JSON
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        # 2) Берём список cards
        cards = []
        if isinstance(data, dict):
            cards = ((data.get("data") or {}).get("cards") or [])
        elif isinstance(data, list):
            cards = data  # маловероятно, но поддержим
        else:
            cards = []

        def _parse_iso(s):
            if not s:
                return None
            s = str(s).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(s)
            except Exception:
                return None

        out = []
        for item in cards:
            sel = (item or {}).get("statistics", {}).get("selectedPeriod", {}) or {}
            begin_dt = _parse_iso(sel.get("begin"))
            if begin_dt is None:
                continue
            # NB: business_dttm — полночь даты begin (локаль нам не важна на этапе нормализации)
            biz_dt = begin_dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=begin_dt.tzinfo)
            rec = {
                "request_uuid": req_uuid,
                "response_dttm": resp_dttm,
                "company_id": company_id,
                "date": begin_dt,  # Silver хранит date/datetime — отдадим исходный момент
                "business_dttm": biz_dt,
                "nm_id": item.get("nmID"),
                "buyouts_percent": (sel.get("conversions", {}) or {}).get("buyoutsPercent") or 0.0,
            }
            out.append(rec)

        return out


    # ---------------------------------------------------
    def norm__wb_sales_funnels_1d(self, payload, meta=None):
        """
        Извлекает воронку продаж по NM из bronze.response_body (nm-report/detail)
        → записи для silver.wb_sales_funnels_1d

        Ожидаемые meta: request_uuid, response_dttm, company_id
        """
        import json
        from datetime import datetime

        meta = meta or {}
        req_uuid = meta.get("request_uuid")
        resp_dttm = meta.get("response_dttm")
        company_id = meta.get("company_id")

        # 1) Достаём JSON
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        # 2) Берём список cards
        cards = []
        if isinstance(data, dict):
            cards = ((data.get("data") or {}).get("cards") or [])
        elif isinstance(data, list):
            cards = data
        else:
            cards = []

        def _i(v):
            try:
                return int(v) if v not in (None, "",) else 0
            except Exception:
                return 0

        def _f(v):
            try:
                return float(v) if v not in (None, "",) else 0.0
            except Exception:
                return 0.0

        def _parse_iso(s):
            if not s:
                return None
            s = str(s).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(s)
            except Exception:
                return None

        out = []
        for item in cards:
            sel = (item or {}).get("statistics", {}).get("selectedPeriod", {}) or {}
            begin_dt = _parse_iso(sel.get("begin"))
            if begin_dt is None:
                continue
            item_date = begin_dt.date()
            business_dttm = begin_dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=begin_dt.tzinfo)

            obj = (item.get("object") or {})
            stocks = (item.get("stocks") or {})

            rec = {
                "request_uuid": req_uuid,
                "response_dttm": resp_dttm,
                "company_id": company_id,
                "business_dttm": business_dttm,
                "date": item_date,
                "nm_id": item.get("nmID"),
                "supplier_article": item.get("vendorCode") or "",
                "brand": item.get("brandName") or "",
                "subject_id": _i(obj.get("id")),
                "subject_name": obj.get("name") or "",
                "open_card_count": _i(sel.get("openCardCount")),
                "add_to_cart_count": _i(sel.get("addToCartCount")),
                "orders_count": _i(sel.get("ordersCount")),
                "orders_sum": _f(sel.get("ordersSumRub")),
                "buyouts_count": _i(sel.get("buyoutsCount")),
                "buyouts_sum": _f(sel.get("buyoutsSumRub")),
                "cancel_count": _i(sel.get("cancelCount")),
                "cancel_sum": _f(sel.get("cancelSumRub")),
                "avg_price": _f(sel.get("avgPriceRub")),
                "stocks_mp": _f(stocks.get("stocksMp")),
                "stocks_wb": _f(stocks.get("stocksWb")),
            }
            out.append(rec)

        return out



    # ---------------------------------------------------
    def norm__search_product_stats(self, payload, meta=None):
        """
        Переносит данные из bronze в silver:
        wb_product_search_texts_1d -> wb_search_product_stats_1d

        Ожидаемые meta: business_dttm, company_id, request_uuid, response_dttm
        """
        import json

        meta = meta or {}
        business_dttm = meta.get("business_dttm")
        company_id = meta.get("company_id")
        request_uuid = meta.get("request_uuid")
        response_dttm = meta.get("response_dttm")

        def _i(v):
            try:
                return int(v) if v not in (None, "",) else 0
            except Exception:
                return 0

        def _f(v):
            try:
                return float(v) if v not in (None, "",) else 0.0
            except Exception:
                return 0.0

        # Преобразуем сырой payload в Python-объект data
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw
        
        print(f'Кол-во склеенных респонсов: {len(data)}')

        # Список элементов, которые превратятся в строки silver-таблицы
        rows = []

        # Множество уникальных элементов для поиска дубликатов по ключу (business_dttm, nm_id, search_term)
        unique_items = set()

        # Для каждого JSONа в массиве
        for json_line in data:
            items = json_line.get("data").get("items") or [{}]
        
            print(f'Кол-во элементов в очередном запросе: {len(items)}')

            for item in items:
                nm_id = _i(item.get("nmId"))
                search_term = item.get("text")

                # Если в исходном payload есть дубликат по указанному ключу, то пропускаем его,
                # иначе получили бы ошибку UniqueViolationError при запсиси в silver-таблицу
                key = (business_dttm, nm_id, search_term)
                if key in unique_items:
                    continue
                unique_items.add(key)

                rows.append({
                    "company_id": company_id,
                    "business_dttm": business_dttm,
                    "request_uuid": request_uuid,
                    "response_dttm": response_dttm,
                    "nm_id": nm_id,
                    "supplier_article": item.get("vendorCode"),
                    "search_term": search_term,
                    "subject_name": item.get("subjectName"),
                    "brand_name": item.get("brandName"),
                    "supplier_article": item.get("vendorCode"),
                    "product_name": item.get("name"),
                    "is_card_rated": item.get("isCardRated"),
                    "rating": _i(item.get("rating")),
                    "feedback_rating": _i(item.get("feedbackRating")),
                    "price_min": _i(item.get("price", {}).get("minPrice")),
                    "price_max": _i(item.get("price", {}).get("maxPrice")),
                    "frequency": _i(item.get("frequency", {}).get("current")),
                    "week_frequency": _i(item.get("weekFrequency")),
                    "median_position": _i(item.get("medianPosition", {}).get("current")),
                    "avg_position": _i(item.get("avgPosition", {}).get("current")),
                    "open_card": _i(item.get("openCard", {}).get("current")),
                    "add_to_cart": _i(item.get("addToCart", {}).get("current")),
                    "open_to_cart": _f(item.get("openToCart", {}).get("current")),
                    "orders": _i(item.get("orders", {}).get("current")),
                    "cart_to_order": _f(item.get("cartToOrder", {}).get("current")),
                    "visibility": _i(item.get("visibility", {}).get("current")),
                })

        return rows


    # ---------------------------------------------------
    # paid_storage_1d (WB платное хранение) — по вашим правилам column_lineage
    # ---------------------------------------------------
    def norm__paid_storage_1d(self, payload, meta=None):
        """
        Нормализация bronze.response_body отчёта платного хранения:
        → список записей для silver.paid_storage_1d
        Ожидаемые ключи meta: request_uuid, response_dttm, company_id, (optional) business_dttm
        """
        import json
        from datetime import datetime, date

        meta = meta or {}

        # 1) Достаём JSON из payload (строка/bytes/объект с полем response_body/словарь)
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray)):
            if isinstance(raw, dict):
                raw = raw.get("response_body") or raw.get("body") or raw.get("response") or raw
            else:
                raw = getattr(raw, "response_body", raw)

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")

        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            # если это уже распарсенный объект
            data = raw

        # 2) Превращаем в список
        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in ("data", "result", "items", "payload"):
                v = data.get(key)
                if isinstance(v, list):
                    items = v
                    break
            else:
                items = [data] if data else []
        else:
            items = []

        def _safe_date(x):
            if not x:
                return None
            if isinstance(x, date):
                return x
            if isinstance(x, datetime):
                return x.date()
            try:
                # WB отдаёт "YYYY-MM-DD"
                return datetime.strptime(str(x), "%Y-%m-%d").date()
            except Exception:
                return None

        # 3) Маппинг под silver.paid_storage_1d
        req_uuid = meta.get("request_uuid")
        resp_dttm = meta.get("response_dttm")
        company_id = meta.get("company_id")
        business_dttm = meta.get("business_dttm")

        out = []
        for row in items or []:
            # Числовые поля осторожно приводим
            def _i(v):
                try:
                    return int(v) if v not in (None, "",) else None
                except Exception:
                    return None

            def _f(v):
                try:
                    return float(v) if v not in (None, "",) else None
                except Exception:
                    return None

            rec_date = _safe_date(row.get("date")) or (_safe_date(business_dttm) if business_dttm else None)

            out.append({
                "date": rec_date,
                "office_id": _i(row.get("officeId")),
                "income_id": _i(row.get("giId")),
                "barcode": row.get("barcode"),
                "calc_type": row.get("calcType"),
                "pallet_place_code": _i(row.get("palletPlaceCode")),

                "warehouse_name": row.get("warehouse"),
                "warehouse_coef": _f(row.get("warehouseCoef")),
                "log_warehouse_coef": _f(row.get("logWarehouseCoef")),
                "chrt_id": _i(row.get("chrtId")),
                "tech_size": row.get("size"),
                "supplier_article": row.get("vendorCode"),
                "nm_id": _i(row.get("nmId")),
                "volume": _f(row.get("volume")),
                "warehouse_price": _f(row.get("warehousePrice")),
                "barcodes_count": _i(row.get("barcodesCount")),
                "pallet_count": _f(row.get("palletCount")),
                "loyalty_discount": _f(row.get("loyaltyDiscount")),
                "subject": row.get("subject"),
                "brand": row.get("brand"),
                "original_date": _safe_date(row.get("originalDate")),
                "tariff_fix_date": _safe_date(row.get("tariffFixDate")),
                "tariff_lower_date": _safe_date(row.get("tariffLowerDate")),
            })

        return out

    # ---------------------------------------------------
    # stats_keywords — вложенный JSON:
    # { "keywords": [ { "date": "...", "stats": [ {clicks, ctr, keyword, sum, views}, ... ] }, ... ] }
    # Выравниваем «каждый stats-элемент» и протягиваем родительский date.
    # ---------------------------------------------------
    def norm__wb_adv_keyword_stats(self, payload, meta=None):
        """
        Универсальная нормализация для /adv/.../stats/keywords.

        Поддерживаемые форматы bronze.response_body:
          (A) dict: {"keywords": [...]} — один advert_id (старая схема);
          (B) list: [{"advertId": <id>, "keywords": [...]}, ...] — батч по нескольким РК (новая схема);
          (C) list: [{"date": "...", "stats": [...]}, ...] — сразу дневные записи (старая бронза).

        Возвращает список строк для silver:
          business_dttm (timestamptz, MSK), date (date, NOT NULL),
          company_id, campaign_id, keyword,
          views, clicks, cost, ctr,
          request_uuid, response_dttm, inserted_at.
        """
        import json
        from datetime import datetime, timedelta, date as date_type
        try:
            from zoneinfo import ZoneInfo
            MSK = ZoneInfo("Europe/Moscow")
        except Exception:
            MSK = None

        meta = meta or {}
        # --- служебные метаданные ---
        req_uuid = meta.get("request_uuid")
        resp_dttm = meta.get("response_dttm")
        company_id = meta.get("company_id")
        business_dttm = meta.get("business_dttm")  # уже в MSK на стороне пайплайна
        run_dttm = meta.get("run_dttm")
        sched_dttm = meta.get("scheduled_dttm") or meta.get("run_schedule_dttm")
        inserted_at = meta.get("inserted_at") or run_dttm or resp_dttm
        time_grain = (meta.get("time_grain") or "1d").lower()
        req_params = meta.get("request_parameters") or {}
        advert_hint = req_params.get("advert_id") or meta.get("advert_id")

        # ---- утилиты ----
        def _parse_dt_or_date(s: str | None) -> datetime | None:
            if not s:
                return None
            x = str(s).strip()
            if x.endswith("Z"):
                x = x[:-1] + "+00:00"
            # datetime → ok; иначе попробуем YYYY-MM-DD как datetime на полночь
            try:
                return datetime.fromisoformat(x)
            except Exception:
                try:
                    return datetime.strptime(x, "%Y-%m-%d")
                except Exception:
                    return None

        def _as_date(x) -> date_type | None:
            if not x:
                return None
            if isinstance(x, date_type) and not isinstance(x, datetime):
                return x
            if isinstance(x, datetime):
                return x.date()
            try:
                return datetime.fromisoformat(str(x)).date()
            except Exception:
                try:
                    return datetime.strptime(str(x), "%Y-%m-%d").date()
                except Exception:
                    return None

        # ---- parse JSON payload ----
        raw = payload
        if isinstance(raw, (bytes, bytearray)):
            try:
                raw = raw.decode("utf-8", "ignore")
            except Exception:
                raw = str(raw)
        if isinstance(raw, str):
            try:
                data = json.loads(raw)
            except Exception:
                data = None
        elif isinstance(raw, (dict, list)):
            data = raw
        else:
            data = None
        if data is None:
            return []

        # ---- вычисляем бизнес-день (MSK) ----
        biz_d = None
        if sched_dttm:
            sched = _parse_dt_or_date(sched_dttm)
            if isinstance(sched, datetime) and MSK:
                try:
                    if sched.tzinfo is None:
                        from datetime import timezone as std_tz
                        sched = sched.replace(tzinfo=std_tz.utc)
                    sched = sched.astimezone(MSK)
                except Exception:
                    pass
            if isinstance(sched, datetime):
                # для дневных ассетов бизнес-дата = дата расписания - 1 день
                biz_d = (sched.date() - timedelta(days=1)) if time_grain == "1d" else sched.date()

        if not biz_d:
            base = run_dttm or resp_dttm
            if isinstance(base, str):
                base = _parse_dt_or_date(base)
            if isinstance(base, datetime) and MSK:
                try:
                    base = base.astimezone(MSK)
                except Exception:
                    pass
            if isinstance(base, datetime):
                biz_d = base.date()

        # ---- унифицируем к {campaign_id: [ {date, stats:[...]} ]} ----
        batched_by_campaign: dict[int | str | None, list] = {}

        # (A) один advert_id
        if isinstance(data, dict) and "keywords" in data:
            batched_by_campaign[advert_hint] = data.get("keywords", [])

        # (B) батч по нескольким РК
        elif isinstance(data, list) and data and isinstance(data[0], dict) and (
                "advertId" in data[0] or "advert_id" in data[0]):
            for block in data:
                if not isinstance(block, dict):
                    continue
                adv_id = block.get("advertId") or block.get("advert_id")
                kw_list = block.get("keywords") or []
                batched_by_campaign[adv_id] = kw_list

        # (C) сразу список дневных записей
        elif isinstance(data, list) and (not data or (isinstance(data[0], dict) and "date" in (data[0] or {}))):
            batched_by_campaign[advert_hint] = data

        else:
            return []

        # ---- собираем доступные даты, выбираем target_date ----
        all_dt = []
        for kw_list in batched_by_campaign.values():
            for d in (kw_list or []):
                dt = _parse_dt_or_date((d or {}).get("date"))
                if isinstance(dt, datetime) and MSK:
                    try:
                        dt = dt.astimezone(MSK)
                    except Exception:
                        pass
                if dt:
                    all_dt.append(dt)
        if not all_dt:
            return []

        target_date = None
        if biz_d and any(dt.date() == biz_d for dt in all_dt):
            target_date = biz_d
        else:
            target_date = max(dt.date() for dt in all_dt)

        # ---- сборка строк ----
        out = []
        for adv_id, kw_list in batched_by_campaign.items():
            if not kw_list:
                continue

            # берём дневной блок на target_date
            day_block = None
            for d in kw_list:
                dt = _parse_dt_or_date((d or {}).get("date"))
                if isinstance(dt, datetime) and MSK:
                    try:
                        dt = dt.astimezone(MSK)
                    except Exception:
                        pass
                if isinstance(dt, datetime) and dt.date() == target_date:
                    day_block = d
                    break
            if not day_block:
                continue

            for s in (day_block.get("stats") or []):
                # приведения типов + дефолты (NOT NULL в silver)
                try:
                    views = int(s.get("views") or 0)
                except Exception:
                    views = 0
                try:
                    clicks = int(s.get("clicks") or 0)
                except Exception:
                    clicks = 0
                try:
                    cost = float(s.get("sum") if s.get("sum") is not None else s.get("cost") or 0.0)
                except Exception:
                    cost = 0.0
                try:
                    ctr = float(s.get("ctr") or 0.0)
                except Exception:
                    ctr = 0.0

                out.append({
                    "business_dttm": business_dttm,
                    "date": _as_date(target_date),
                    "company_id": company_id,
                    "advert_id": adv_id,
                    "keyword": (s.get("keyword") or "").strip(),
                    "views": views,
                    "clicks": clicks,
                    "cost": cost,
                    "ctr": ctr,
                    "request_uuid": req_uuid,
                    "response_dttm": resp_dttm,
                    "inserted_at": inserted_at,
                })

        return out

    # ---------------------------------------------------
    # Ниже — болванки под другие ассеты (дописать по их легаси-логике):
    # ---------------------------------------------------
    def norm__wb_paid_acceptances_1d(self, payload, meta=None):
        """
        Нормализует bronze.response_body платной приёмки → строки для silver.wb_paid_acceptances_1d.

        Ожидаемые meta:
          - request_uuid: UUID запроса
          - response_dttm: datetime ответа API
          - company_id: int

        Формат источника (на уровне корня обычно список словарей), поля элементов:
          - incomeId: int (обязательно)
          - nmId: int (обязательно)
          - count: int → quantity
          - total: float → total_price
          - giCreateDate: "YYYY-MM-DD" → gi_create_date (date)
          - shkCreateDate: "YYYY-MM-DD" → date (date)
        """
        import json
        from datetime import datetime, date

        meta = meta or {}
        req_uuid = meta.get("request_uuid")
        resp_dttm = meta.get("response_dttm")
        company_id = meta.get("company_id")

        # --- Извлечь/распарсить payload ---
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        # Поддержка разных обёрток: чаще всего это уже список, но подстрахуемся
        if isinstance(data, dict):
            # Поищем список в типичных ключах
            for k in ("data", "result", "items", "rows", "payload"):
                v = data.get(k)
                if isinstance(v, list):
                    data = v
                    break
            else:
                data = [data]
        elif not isinstance(data, list):
            data = []

        def _i(v):
            try:
                return int(v) if v not in (None, "",) else None
            except Exception:
                return None

        def _f(v):
            try:
                return float(v) if v not in (None, "",) else None
            except Exception:
                return None

        def _d(s):
            """Безопасный парс даты 'YYYY-MM-DD' в date."""
            if not s:
                return None
            if isinstance(s, date) and not isinstance(s, datetime):
                return s
            if isinstance(s, datetime):
                return s.date()
            s = str(s).strip()
            try:
                return datetime.strptime(s, "%Y-%m-%d").date()
            except Exception:
                return None

        out = []
        for row in data:
            # аккуратно берём nm_id: в ответах встречается nmID и nmId
            nm_id_raw = (row or {}).get("nmID")
            if nm_id_raw is None:
                nm_id_raw = (row or {}).get("nmId")

            income_id = _i((row or {}).get("incomeId"))
            nm_id = _i(nm_id_raw)

            if income_id is None or nm_id is None:
                # Эти поля составляют ключ в upsert — пропускаем некорректные строки
                continue

            rec = {
                "request_uuid": req_uuid,
                "response_dttm": resp_dttm,
                "company_id": company_id,

                "quantity": _i(row.get("count")) or 0,
                "gi_create_date": _d(row.get("giCreateDate")),
                "shk_create_date": _d(row.get("shkCreateDate")),  # ← раньше было "date"
                "income_id": income_id,
                "nm_id": nm_id,
                "total_cost": _f(row.get("total")) or 0.0,  # ← раньше было "total_price"
                "subject_name": (row.get("subjectName") or row.get("subject") or ""),  # NOT NULL защита
            }
            out.append(rec)

        return out
    def norm__wb_adv_promotions(self, payload, meta=None):
        """
        Нормализация bronze.response_body для /adv/v1/promotion/count
        → записи для silver.wb_adv_promotions_*

        Ожидаемые meta:
          - request_uuid: UUID запроса
          - response_dttm: datetime ответа API
          - company_id: int
          - business_dttm: таймстэмп (МСК)
        """
        import json
        from datetime import datetime

        meta = meta or {}
        req_uuid = meta.get("request_uuid")
        resp_dttm = meta.get("response_dttm")
        company_id = meta.get("company_id")
        business_dt = meta.get("business_dttm")

        # --- Достаём JSON из payload ---
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        # --- Универсальный доступ к списку групп ---
        if not isinstance(data, dict):
            data = {}

        groups = (data.get("adverts") or [])  # [{status, type, advert_list: [{advertId, changeTime, type?}, ...]}, ...]

        def _i(v):
            try:
                return int(v) if v not in (None, "",) else None
            except Exception:
                return None

        def _parse_iso(s: str):
            if not s:
                return None
            s = str(s).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(s)
            except Exception:
                return None

        out = []
        for group in groups or []:
            g_status = group.get("status")
            g_type = group.get("type")
            for adv in (group.get("advert_list") or []):
                advert_id = _i(adv.get("advertId"))
                if advert_id is None:
                    continue
                rec = {
                    "request_uuid": req_uuid,
                    "response_dttm": resp_dttm,
                    "company_id": company_id,
                    "business_dttm": business_dt,

                    "advert_id": advert_id,
                    "advert_status": adv.get("status", g_status),
                    "advert_type": adv.get("type", g_type),
                    "change_time": _parse_iso(adv.get("changeTime")),
                }
                out.append(rec)

        return out

    def norm__wb_adv_campaigns(self, payload, meta=None):
        """
        /adv/v1/promotion/adverts → строки для silver.wb_adv_campaigns_{1d,1h}.
        Ожидаемые meta:
          - request_uuid: str
          - business_dttm: datetime (для PK серебра)
          - response_dttm: datetime (как receive_dttm)
          - company_id: int (обычно подставляет фабрика)
        """
        import json, re
        from datetime import datetime

        meta = meta or {}
        req_uuid = meta.get("request_uuid")
        business_dttm = meta.get("business_dttm") or meta.get("run_dttm")
        receive_dttm = meta.get("response_dttm")

        # --- достаём JSON ---
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw
        if not isinstance(data, dict):
            data = {}

        groups = data.get("adverts") or data.get("items") or []
        if not isinstance(groups, list):
            groups = []

        def _parse_ts(s):
            if not s:
                return None
            s = re.sub(r"\s+", "", str(s))  # убираем пробелы "22: 51: 28" → "22:51:28", "+03: 00" → "+03:00"
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(s)
            except Exception:
                return None

        def _i(v):
            try:
                return int(v) if v not in (None, "") else None
            except Exception:
                return None

        out = []
        for g in groups:
            if not isinstance(g, dict):
                continue
            g_type = _i(g.get("type"))
            g_status = _i(g.get("status"))
            lst = g.get("advert_list") or []
            if not isinstance(lst, list):
                lst = []
            for a in lst:
                if not isinstance(a, dict):
                    continue
                out.append({
                    # ключевые поля серебра
                    "business_dttm": business_dttm,
                    "request_uuid": req_uuid,
                    "receive_dttm": receive_dttm,

                    "advert_id": _i(a.get("advertId")),
                    "advert_name": a.get("name"),
                    "create_time": _parse_ts(a.get("createTime")),
                    "change_time": _parse_ts(a.get("changeTime")),
                    "start_time": _parse_ts(a.get("startTime")),
                    "end_time": _parse_ts(a.get("endTime")),

                    # из группы и/или из элемента
                    "advert_status": _i(a.get("status")) if a.get("status") is not None else g_status,
                    "advert_type": _i(a.get("type")) if a.get("type") is not None else g_type,

                    "payment_type": a.get("paymentType"),
                    "daily_budget": _i(a.get("dailyBudget")),
                    "search_pluse_state": a.get("searchPluseState"),
                })

        # уберём пустые (без advert_id), чтобы не ловить NOT NULL
        out = [r for r in out if r.get("advert_id") is not None]
        return out

    def norm__wb_adv_product_rates(self, payload, meta=None):
        """
        Нормализует ставки/параметры по товарам из /adv/v1/promotion/adverts → строки для silver.wb_adv_product_rates_{1d,1h}.

        Ожидаемые meta:
          - company_id:   int
          - request_uuid: UUID запроса (str)
          - run_dttm:     datetime запуска (используется в PK/UK серебра)
          - inserted_at:  datetime вставки (если не задан — берём run_dttm)

        Выходные поля (ожидаемые серебром):
          company_id, request_uuid, inserted_at, run_dttm, advert_id,
          nm_id, cpm_current, cpm_catalog, cpm_search, cpm_first,
          advert_status, advert_type,
          active_carousel, active_recom, active_booster,
          subject_id
        """
        import json

        meta = meta or {}
        company_id = meta.get("company_id")
        req_uuid = meta.get("request_uuid")
        run_dttm = meta.get("run_dttm")
        inserted_at = meta.get("inserted_at") or run_dttm

        # --- достаём JSON из payload ---
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        if isinstance(data, dict):
            data = data.get("adverts") or data.get("items") or data.get("result") or []
        if not isinstance(data, list):
            data = []

        def _i(v, default=None):
            try:
                return int(v) if v not in (None, "",) else default
            except Exception:
                return default

        def _f(v, default=None):
            try:
                return float(v) if v not in (None, "",) else default
            except Exception:
                return default

        out = []
        for entry in data:
            if not isinstance(entry, dict):
                continue

            advert_id = _i(entry.get("advertId"))
            campaign_type = _i(entry.get("type"))
            status = _i(entry.get("status"))

            params_list = []

            # ---- type == 8 (авто-кампании) ----
            if campaign_type == 8:
                auto = entry.get("autoParams") or {}
                base = {
                    "advert_status": status,
                    "advert_type": campaign_type,
                    "cpm_first": _f(auto.get("cpm")),
                    "active_carousel": (auto.get("active") or {}).get("carousel"),
                    "active_recom": (auto.get("active") or {}).get("recom"),
                    "active_booster": (auto.get("active") or {}).get("booster"),
                    "subject_id": _i((auto.get("subject") or {}).get("id")),
                    "cpm_catalog": None,
                    "cpm_search": None,
                }
                for m in (auto.get("nmCPM") or []):
                    params_list.append({
                        **base,
                        "nm_id": _i((m or {}).get("nm")),
                        "cpm_current": _f((m or {}).get("cpm")),
                    })

            # ---- type == 9 (аукционы) ----
            elif campaign_type == 9:
                for ap in (entry.get("auction_multibids") or []):
                    params_list.append({
                        "nm_id": _i((ap or {}).get("nm")),
                        "cpm_current": _f((ap or {}).get("bid")),
                        "cpm_catalog": None,
                        "cpm_search": None,
                        "cpm_first": None,
                        "advert_status": status,
                        "advert_type": campaign_type,
                        "active_carousel": None,
                        "active_recom": None,
                        "active_booster": None,
                        "subject_id": None,
                    })
                for up in (entry.get("unitedParams") or []):
                    base = {
                        "advert_status": status,
                        "advert_type": campaign_type,
                        "cpm_catalog": _f((up or {}).get("catalogCPM")),
                        "cpm_search": _f((up or {}).get("searchCPM")),
                        "cpm_first": None,
                        "active_carousel": None,
                        "active_recom": None,
                        "active_booster": None,
                        "subject_id": _i(((up or {}).get("subject") or {}).get("id")),
                    }
                    nms = (up or {}).get("nms") or []
                    # бывает и скаляр/словарь — унифицируем в список
                    if isinstance(nms, dict):
                        nms = [nms]
                    for nm in nms:
                        params_list.append(
                            {**base, "nm_id": _i(nm if isinstance(nm, (int, str)) else (nm or {}).get("nm"))})

            # ---- прочие типы ----
            else:
                for ap in (entry.get("params") or []):
                    price = _f((ap or {}).get("price"))
                    subject_id = _i((ap or {}).get("subjectId"))
                    nms_list = (ap or {}).get("nms") or []
                    if isinstance(nms_list, dict):
                        nms_list = [nms_list]
                    for nm_entry in nms_list:
                        nm_id = _i(nm_entry if isinstance(nm_entry, (int, str)) else (nm_entry or {}).get("nm"))
                        params_list.append({
                            "nm_id": nm_id,
                            "cpm_current": price,
                            "cpm_catalog": None,
                            "cpm_search": None,
                            "cpm_first": None,
                            "advert_status": status,
                            "advert_type": campaign_type,
                            "active_carousel": None,
                            "active_recom": None,
                            "active_booster": None,
                            "subject_id": subject_id,
                        })

            # финальная сборка строк
            for p in params_list:
                out.append({
                    "company_id": company_id,
                    "request_uuid": req_uuid,
                    "inserted_at": inserted_at,
                    "run_dttm": run_dttm,
                    "advert_id": advert_id,
                    **p,
                })

        return out


    def norm__wb_adv_product_stats_1d(self, payload, meta=None):
        """
        fullstats (v3) → строки для silver.wb_adv_product_stats_1d
        Требуемые meta: company_id, request_uuid, response_dttm (строка или datetime).
        """
        import json
        from datetime import datetime
        try:
            from zoneinfo import ZoneInfo
            MSK = ZoneInfo("Europe/Moscow")
        except Exception:
            MSK = None

        meta = meta or {}
        company_id   = meta.get("company_id")
        request_uuid = meta.get("request_uuid")
        business_dttm = meta.get('business_dttm')
        response_dttm = meta.get("response_dttm") or meta.get("receive_dttm")

        # --- извлекаем JSON ---
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        # API возвращает список кампаний; поддержим обёртку-словарь
        if isinstance(data, dict):
            data = data.get("result") or data.get("items") or data.get("adverts") or []
        if not isinstance(data, list):
            return []

        # --- хелперы ---
        def _i(v, d=0):
            try:
                return int(v) if v not in (None, "",) else d
            except Exception:
                return d
        def _f(v, d=0.0):
            try:
                return float(v) if v not in (None, "",) else d
            except Exception:
                return d
        def _parse_iso_any(s):
            if not s:
                return None
            ss = str(s).strip()
            if ss.endswith("Z"):
                ss = ss[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(ss)
            except Exception:
                return None
            if MSK:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                dt = dt.astimezone(MSK)
            return dt
        def _to_msk_day_start(dt):
            if not dt:
                return None
            if dt.tzinfo is None and MSK:
                dt = dt.replace(tzinfo=MSK)
            if MSK:
                dt = dt.astimezone(MSK)
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)

        # если мета передана строкой — приведём
        if isinstance(response_dttm, str):
            response_dttm = _parse_iso_any(response_dttm)

        out = []

        # ---- только 1d-ветка ----
        for entry in data:
            if not isinstance(entry, dict):
                continue
            advert_id = _i(entry.get("advertId"), None)
            if advert_id is None:
                continue

            for day in (entry.get("days") or []):
                dd = _parse_iso_any((day or {}).get("date"))
                if not dd:
                    continue
                date = _to_msk_day_start(dd)
                for app in (day.get("apps") or []):
                    app_type = _i(app.get("appType") or app.get("app_type"))
                    # поддержка обеих форм: nms / nm
                    nms = app.get("nms")
                    if not isinstance(nms, list):
                        nms = app.get("nm") or []
                    for nm in nms:
                        nm_id = _i((nm or {}).get("nmId"), None)
                        if nm_id is None:
                            continue
                        out.append({
                            "company_id": company_id,
                            "request_uuid": request_uuid,
                            "advert_id": advert_id,
                            "app_type": app_type,
                            "nm_id": nm_id,
                            "request_dttm": response_dttm,
                            "business_dttm": business_dttm,
                            "response_dttm": response_dttm,
                            "date": date,
                            "views":   _i(nm.get("views")),
                            "clicks":  _i(nm.get("clicks")),
                            "cost":    _f(nm.get("sum")),
                            "carts":   _i(nm.get("atbs")),
                            "orders":  _i(nm.get("orders")),
                            "items":   _i(nm.get("shks")),
                            "revenue": _f(nm.get("sum_price")),
                            "canceled": _i(nm.get("canceled")),
                        })

        return out

    def norm__wb_adv_product_positions(self, payload, meta=None):
        """
        Нормализует boosterStats из fullstats → строки для silver.wb_adv_product_positions_{1d,1h}.

        Ожидаемые meta:
          - company_id:   int
          - request_uuid: UUID запроса (str)
          - response_dttm: datetime ответа API (tz-aware)
          - time_grain:   "1d" | "1h"  (по умолчанию "1d")
          - business_dttm: datetime (tz-aware, MSK) — обязателен для "1h" (начало целевого часа)
        """
        import json
        from datetime import datetime, timedelta
        try:
            from zoneinfo import ZoneInfo
            MSK = ZoneInfo("Europe/Moscow")
        except Exception:
            MSK = None

        meta = meta or {}
        company_id = meta.get("company_id")
        request_uuid = meta.get("request_uuid")
        response_dttm = meta.get("response_dttm")
        time_grain = (meta.get("time_grain") or "1d").lower()
        biz_anchor = meta.get("business_dttm")  # начало часа для 1h (МСК)

        # --- извлекаем JSON ---
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        if isinstance(data, dict):
            data = data.get("result") or data.get("items") or data.get("adverts") or []
        if not isinstance(data, list):
            return []

        def _parse_iso_any(s):
            if not s:
                return None
            ss = str(s).strip()
            if ss.endswith("Z"):
                ss = ss[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(ss)
            except Exception:
                return None
            if MSK:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                dt = dt.astimezone(MSK)
            return dt

        def _hour_bucket_start(dt_msk: datetime) -> datetime:
            return dt_msk.replace(minute=0, second=0, microsecond=0) - timedelta(hours=0)

        def _i(v, default=None):
            try:
                return int(v) if v not in (None, "",) else default
            except Exception:
                return default

        out = []

        for entry in data or []:
            if not isinstance(entry, dict):
                continue
            advert_id = _i(entry.get("advertId"))
            if advert_id is None:
                continue

            for bst in (entry.get("boosterStats") or []):
                bst_date = _parse_iso_any((bst or {}).get("date"))
                if not bst_date:
                    continue

                if time_grain == "1h":
                    if not biz_anchor:
                        continue
                    # WB часто отдаёт конец окна; нормализуем к началу часа и сравниваем с якорём
                    bucket = _hour_bucket_start(bst_date)
                    if bucket != biz_anchor:
                        continue
                    business_dttm = biz_anchor
                else:
                    # DAILY: business_dttm — начало суток
                    business_dttm = bst_date.replace(hour=0, minute=0, second=0, microsecond=0)

                out.append({
                    "company_id": company_id,
                    "request_uuid": request_uuid,
                    "advert_id": advert_id,
                    "nm_id": _i(bst.get("nm")),
                    "datetime": bst_date,
                    "avg_position": _i(bst.get("avg_position")),
                    "business_dttm": business_dttm,
                    "response_dttm": response_dttm,
                })

        return out

    def norm__auto_stat_words(self, payload, meta=None):
        """
        Извлекает кластеры ключевых слов и исключённые слова из bronze.response_body
        (/adv/v2/auto/stat-words) → строки для silver.wb_adv_keyword_clusters_1d.

        Ожидаемые meta:
          - request_uuid: UUID запроса (если передаётся снаружи — будет перезаписан фабрикой)
          - company_id:   int (добивается фабрикой)
          - run_dttm:     datetime запуска (если есть — проставим в записи)
          - request_parameters: {"advert_id": int}  ИЛИ meta["advert_id"]
        """
        import json

        meta = meta or {}
        # advert_id берём как в бронзе: из request_parameters, либо прямым полем
        rp = meta.get("request_parameters") or {}
        advert_id = rp.get("advert_id", meta.get("advert_id"))

        # payload может прийти строкой — аккуратно парсим
        raw = payload
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = {}

        data = raw if isinstance(raw, dict) else {}

        clusters = data.get("clusters") or []
        excluded = data.get("excluded") or []

        out = []

        # Кластеры: по каждому слову из keywords отдаём отдельную строку
        for c in clusters:
            if not isinstance(c, dict):
                continue
            cluster_name = c.get("cluster")
            cluster_views = c.get("count")
            kws = c.get("keywords") or []
            # keywords может быть списком строк — нормализуем
            for kw in kws:
                out.append({
                    "run_dttm": meta.get("run_dttm"),  # есть в легаси-серебре
                    "advert_id": advert_id,
                    "keyword_cluster": cluster_name,
                    "keyword_cluster_views": int(cluster_views) if isinstance(cluster_views, (int, float, str)) and str(
                        cluster_views).strip() != "" else 0,
                    "keyword": str(kw) if kw is not None else "",
                    "is_excluded": False,
                })

        # Исключённые слова: одна строка на элемент excluded, keyword пустой
        for ex in excluded:
            out.append({
                "run_dttm": meta.get("run_dttm"),
                "advert_id": advert_id,
                "keyword_cluster": str(ex) if ex is not None else "",
                "keyword_cluster_views": 0,
                "keyword": "",
                "is_excluded": True,
            })

        return out

    def norm__wb_mp_skus_1d(self, payload, meta=None):
        """
        Карточки SKU из bronze.response_body (/cards/list) → строки для silver.wb_mp_skus_1d.
        Ожидаемые meta: request_uuid, response_dttm, company_id.
        """
        import json

        meta = meta or {}
        req_uuid    = meta.get("request_uuid")
        resp_dttm   = meta.get("response_dttm")
        company_id  = meta.get("company_id")

        # --- извлекаем JSON ---
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        # В бронзе лежит именно список карточек; подстрахуемся на случай обёртки
        if isinstance(data, dict):
            for k in ("data", "result", "items", "cards"):
                v = data.get(k)
                if isinstance(v, list):
                    data = v
                    break
            else:
                data = [data]
        elif not isinstance(data, list):
            data = []

        def _i(v, default=None):
            try:
                return int(v) if v not in (None, "",) else default
            except Exception:
                return default

        def _f(v, default=None):
            try:
                return float(v) if v not in (None, "",) else default
            except Exception:
                return default

        def _s(v):
            return str(v) if v is not None else None

        out = []
        for item in data:
            if not isinstance(item, dict):
                continue

            sizes = item.get("sizes") or []
            first_size = sizes[0] if sizes else {}
            skus = first_size.get("skus") or []
            barcode = skus[0] if skus else None

            dims = item.get("dimensions") or {}

            out.append({
                "request_uuid":      req_uuid,
                "response_dttm":     resp_dttm,
                "company_id":        company_id,

                "nm_id":             _i(item.get("nmID")),
                "imt_id":            _i(item.get("imtID")),
                "subject_id":        _i(item.get("subjectID")),
                "subject_name":      _s(item.get("subjectName")) or "",
                "supplier_article":  _s(item.get("vendorCode")) or "",
                "brand":             _s(item.get("brand")) or "",
                "title":             _s(item.get("title")) or "",
                "description":       _s(item.get("description")) or "",

                "length":            _f(dims.get("length")),
                "width":             _f(dims.get("width")),
                "height":            _f(dims.get("height")),
                "weight":            _f(dims.get("weightBrutto")),

                "chrt_id":           _i(first_size.get("chrtID")),
                "tech_size":         _s(first_size.get("techSize")),
                "barcode":           _s(barcode),
            })

        return out

    def norm__wb_supplier_incomes_1d(self, payload, meta=None):
        """
        /supplier/incomes → строки для silver.norm__wb_supplier_incomes_1d.

        Ожидаемые meta:
          - request_uuid: UUID запроса
          - response_dttm: datetime ответа API
          - company_id: int

        Поля результата (как в ассете серебра):
          request_uuid, response_dttm, company_id,
          income_id, date, last_change_date, supplier_article, tech_size, barcode,
          quantity, total_price, date_close, warehouse_name, status, nm_id
        """
        import json
        from datetime import datetime, date

        meta = meta or {}
        req_uuid    = meta.get("request_uuid")
        resp_dttm   = meta.get("response_dttm")
        company_id  = meta.get("company_id")

        # --- извлекаем JSON из payload/bronze.response_body ---
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        # ожидаем список; поддержим типичные обёртки
        if isinstance(data, dict):
            for k in ("data", "result", "items", "rows", "payload"):
                v = data.get(k)
                if isinstance(v, list):
                    data = v
                    break
            else:
                data = [data]
        elif not isinstance(data, list):
            data = []

        # --- безопасные конверторы ---
        def _i(v, default=None):
            try:
                return int(v) if v not in (None, "") else default
            except Exception:
                return default

        def _f(v, default=None):
            try:
                return float(v) if v not in (None, "") else default
            except Exception:
                return default

        def _d(s):
            """Дата 'YYYY-MM-DD' или ISO → date"""
            if not s:
                return None
            if isinstance(s, date) and not isinstance(s, datetime):
                return s
            if isinstance(s, datetime):
                return s.date()
            ss = str(s).strip()
            # пробуем сначала чистую дату
            try:
                return datetime.strptime(ss, "%Y-%m-%d").date()
            except Exception:
                pass
            # затем ISO c временем/таймзоной
            try:
                if ss.endswith("Z"):
                    ss = ss[:-1] + "+00:00"
                return datetime.fromisoformat(ss).date()
            except Exception:
                return None

        def _dt(s):
            """ISO datetime (поддержка Z) → datetime | None"""
            if not s:
                return None
            if isinstance(s, datetime):
                return s
            ss = str(s).strip()
            try:
                if ss.endswith("Z"):
                    ss = ss[:-1] + "+00:00"
                return datetime.fromisoformat(ss)
            except Exception:
                return None

        out = []
        for item in data:
            if not isinstance(item, dict):
                continue

            rec = {
                "request_uuid": req_uuid,
                "response_dttm": resp_dttm,
                "company_id": company_id,

                "income_id": _i(item.get("incomeId")),
                "number": item.get("number"),
                "date": _dt(item.get("date")),
                "last_change_date": _dt(item.get("lastChangeDate")),
                "supplier_article": (item.get("supplierArticle") or ""),
                "tech_size": item.get("techSize"),
                "barcode": item.get("barcode"),
                "quantity": _i(item.get("quantity"), 0),
                "total_price": _f(item.get("totalPrice"), 0.0),
                "date_close": _dt(item.get("dateClose")),
                "warehouse_name": item.get("warehouseName"),
                "status": item.get("status"),
                "nm_id": _i(item.get("nmId")),
            }

            # ключевые поля: (income_id, barcode)
            if rec["income_id"] is None or rec["barcode"] in (None, ""):
                continue

            out.append(rec)

        return out

    def norm__wb_order_items_1d(self, payload, meta=None):
        """
        /supplier/orders → строки для silver.wb_order_items_1d.

        Ожидаемые meta:
          - request_uuid: UUID запроса
          - response_dttm: datetime ответа API (tz-aware)
          - business_dttm: datetime бизнес-суток (МСК, начало дня) — опционально
          - company_id: int
          - (опционально) run_schedule_dttm | scheduled_dttm — ISO-строка планового запуска (для фолбэка business_dttm)
        """
        import json
        from datetime import datetime, timedelta, timezone as std_timezone
        try:
            from zoneinfo import ZoneInfo
            MSK = ZoneInfo("Europe/Moscow")
            UTC = ZoneInfo("UTC")
        except Exception:
            MSK = None
            UTC = None

        meta = meta or {}
        req_uuid    = meta.get("request_uuid")
        resp_dttm   = meta.get("response_dttm")
        company_id  = meta.get("company_id")

        # ---- извлечь и распарсить payload ----
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        # WB отдаёт список; поддержим обёртки-словарь
        if isinstance(data, dict):
            for k in ("data", "result", "items", "rows", "payload"):
                v = data.get(k)
                if isinstance(v, list):
                    data = v
                    break
            else:
                data = [data]
        elif not isinstance(data, list):
            data = []

        # ---- хелперы ----
        def _to_msk(s):
            """ISO 'YYYY-MM-DDTHH:MM:SS[Z|+hh:mm]' → tz-aware MSK; пустые и '0001-01-01...' → None."""
            if not s:
                return None
            ss = str(s).strip()
            if ss.startswith("0001-01-01"):
                return None
            try:
                if ss.endswith("Z"):
                    ss = ss[:-1] + "+00:00"
                dt = datetime.fromisoformat(ss)
            except Exception:
                return None
            if MSK:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC or std_timezone.utc)
                return dt.astimezone(MSK)
            return dt  # без tz-инфры вернём как есть

        def _i(v, default=0):
            try:
                return int(v) if v not in (None, "",) else default
            except Exception:
                return default

        def _f(v, default=0.0):
            try:
                return float(v) if v not in (None, "",) else default
            except Exception:
                return default

        def _s(v, default=""):
            return str(v) if v not in (None,) else default

        # ---- вычислим business_dttm (МСК) ----
        business_dt = meta.get("business_dttm")
        if business_dt is None:
            sched_iso = meta.get("run_schedule_dttm") or meta.get("scheduled_dttm")
            if isinstance(sched_iso, str):
                try:
                    si = sched_iso.strip()
                    if si.endswith("Z"):
                        si = si[:-1] + "+00:00"
                    sched = datetime.fromisoformat(si)
                    if MSK:
                        if sched.tzinfo is None:
                            sched = sched.replace(tzinfo=UTC or std_timezone.utc)
                        sched = sched.astimezone(MSK)
                except Exception:
                    sched = None
            else:
                sched = None

            anchor = sched or resp_dttm or (datetime.now(std_timezone.utc) if UTC is None else datetime.now(UTC))
            if MSK and anchor is not None:
                if getattr(anchor, "tzinfo", None) is None:
                    anchor = anchor.replace(tzinfo=UTC or std_timezone.utc)
                anchor = anchor.astimezone(MSK)
                business_dt = (anchor - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                business_dt = None  # если TZ нет — оставим пустым

        # resp_dttm фолбэк — "сейчас МСК"
        if resp_dttm is None:
            now_msk = (datetime.now(std_timezone.utc) if UTC is None else datetime.now(UTC))
            resp_dttm = now_msk.astimezone(MSK) if MSK else now_msk

        out = []
        for item in data:
            if not isinstance(item, dict):
                continue

            dt        = _to_msk(item.get("date"))
            last_dt   = _to_msk(item.get("lastChangeDate"))
            cancel_dt = _to_msk(item.get("cancelDate"))

            # silver пропускает записи без last_change_date
            if last_dt is None:
                continue

            out.append({
                "request_uuid":         req_uuid,
                "response_dttm":        resp_dttm,
                "business_dttm":        business_dt,

                "g_number":             _s(item.get("gNumber")),
                "sr_id":                item.get("srid"),
                "date":                 dt,
                "last_change_date":     last_dt,
                "warehouse_name":       _s(item.get("warehouseName")),
                "warehouse_type":       _s(item.get("warehouseType")),
                "country_name":         _s(item.get("countryName")),
                "oblast_okrug_name":    _s(item.get("oblastOkrugName")),
                "region_name":          _s(item.get("regionName")),
                "supplier_article":     _s(item.get("supplierArticle")),
                "nm_id":                item.get("nmId"),
                "barcode":              _s(item.get("barcode")),
                "category":             _s(item.get("category")),
                "subject":              _s(item.get("subject")),
                "brand":                _s(item.get("brand")),
                "tech_size":            _s(item.get("techSize")),
                "income_id":            _i(item.get("incomeID")),
                "total_price":          _f(item.get("totalPrice")),
                "discount_percent":     _f(item.get("discountPercent")),
                "spp":                  _i(item.get("spp")),
                "finished_price":       _f(item.get("finishedPrice")),
                "price_with_discount":  _f(item.get("priceWithDisc")),
                "is_cancel":            bool(item.get("isCancel", False)),
                "cancel_date":          cancel_dt,
                "sticker":              _s(item.get("sticker")),
                "company_id":           company_id,
            })

        return out

    def norm__wb_commission_1d(self, payload, meta=None):
        """
        /tariffs/commission → строки для silver.wb_commission_1d.

        Ожидаемые meta:
          - request_uuid: UUID запроса
          - response_dttm: datetime ответа API (→ receive_dttm)
          - business_dttm: дата отчёта (если не передана — берём response_dttm)
          - company_id: int (может подставляться фабрикой)
        """
        import json
        from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

        meta = meta or {}
        req_uuid = meta.get("request_uuid")
        resp_dttm = meta.get("response_dttm")
        business_dttm = meta.get("business_dttm") or resp_dttm

        # --- вытаскиваем JSON из payload/bronze.response_body ---
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        items = []
        if isinstance(data, dict):
            items = data.get("report") or data.get("commission") or data.get("items") or []
        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            items = []

        # --- хелперы типов ---
        def _i(v):
            try:
                return int(v) if v not in (None, "") else None
            except Exception:
                return None

        def _d4(v):
            if v in (None, ""):
                return None
            try:
                d = Decimal(str(v))
                return d.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            except (InvalidOperation, ValueError, TypeError):
                return None

        def _s(v):
            return "" if v is None else str(v)

        out = []
        for it in items:
            if not isinstance(it, dict):
                continue

            rec = {
                "request_uuid": req_uuid,
                "response_dttm": resp_dttm,
                "business_dttm": business_dttm,

                "kgvp_booking": _d4(it.get("kgvpBooking")),
                "kgvp_marketplace": _d4(it.get("kgvpMarketplace")),
                "kgvp_pickup": _d4(it.get("kgvpPickup")),
                "kgvp_supplier": _d4(it.get("kgvpSupplier")),
                "kgvp_supplier_express": _d4(it.get("kgvpSupplierExpress")),
                "kgvp_paid_storage": _d4(it.get("paidStorageKgvp")),

                "category_id": _i(it.get("parentID")),
                "category_name": _s(it.get("parentName")),
                "subject_id": _i(it.get("subjectID")),
                "subject_name": _s(it.get("subjectName")),
            }
            out.append(rec)

        return out

    def norm__wb_stocks_1d(self, payload, meta=None):
        """
        Остатки по складам из bronze.response_body (/warehouse-remains report)
        → строки для silver.wb_stocks_1d.

        Ожидаемые meta: request_uuid, response_dttm, company_id, run_dttm
        """
        import json
        from datetime import datetime, date

        meta = meta or {}
        req_uuid    = meta.get("request_uuid")
        resp_dttm   = meta.get("response_dttm")
        company_id  = meta.get("company_id")
        run_dttm    = meta.get("run_dttm")  # в ассете дата = bronze.run_dttm.date()

        # --- извлекаем JSON из payload/bronze.response_body ---
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        # ожидаем список карточек остатков; поддержим типичные обёртки
        if isinstance(data, dict):
            for k in ("data", "result", "items", "rows", "payload"):
                v = data.get(k)
                if isinstance(v, list):
                    data = v
                    break
            else:
                data = [data]
        elif not isinstance(data, list):
            data = []

        def _i(v, default=None):
            try:
                return int(v) if v not in (None, "",) else default
            except Exception:
                return default

        def _f(v, default=None):
            try:
                return float(v) if v not in (None, "",) else default
            except Exception:
                return default

        def _s(v, default=None):
            return str(v) if v is not None else default

        # дата для silver — как в ассете: день запуска бронзы
        out_date = None
        if isinstance(run_dttm, datetime):
            out_date = run_dttm.date()
        elif isinstance(run_dttm, date):
            out_date = run_dttm
        elif isinstance(resp_dttm, datetime):
            out_date = resp_dttm.date()

        out = []
        for row in data:
            if not isinstance(row, dict):
                continue

            warehouses = row.get("warehouses") or []
            if not isinstance(warehouses, list):
                warehouses = []

            # агрегаты по спец-строкам (имена без пробелов, в нижнем регистре)
            in_way_to_client = None
            in_way_from_client = None
            total = None
            for w in warehouses:
                name = _s((w or {}).get("warehouseName"), "") or ""
                norm = name.replace(" ", "").lower()
                qty = _i((w or {}).get("quantity"))
                if norm == "впутидополучателей":
                    in_way_to_client = qty
                elif norm == "впутивозвратынаскладwb":
                    in_way_from_client = qty
                elif norm == "всегонаходитсянаскладах":
                    total = qty

            common = {
                "request_uuid":     req_uuid,
                "response_dttm":    resp_dttm,
                "company_id":       company_id,
                "date":             out_date,
                "supplier_article": _s(row.get("vendorCode"), ""),
                "nm_id":            _i(row.get("nmId")),
                "barcode":          _s(row.get("barcode")),
                "tech_size":        _s(row.get("techSize")),
                "volume":           _f(row.get("volume")),
                "in_way_to_client": in_way_to_client,
                "in_way_from_client": in_way_from_client,
                "total":            total,
            }

            # строки по обычным складам (исключаем агрегаты)
            for w in warehouses:
                name = _s((w or {}).get("warehouseName"), "")
                norm = (name or "").replace(" ", "").lower()
                if norm in {"впутидополучателей", "впутивозвратынаскладwb", "всегонаходитсянаскладах"}:
                    continue
                rec = {
                    **common,
                    "warehouse_name": name,
                    "quantity": _i((w or {}).get("quantity")),
                }
                out.append(rec)

        return out

    def norm__wb_www_text_search_1d(self, payload, meta=None):
        """
        /exactmatch/.../search → строки для silver.wb_www_text_search_1d.
        meta ожидает: request_uuid, response_dttm, business_dttm, company_id, request_parameters (optional).
        """
        import json

        meta = meta or {}
        req_uuid = meta.get("request_uuid")
        resp_dttm = meta.get("response_dttm")
        business_dttm = meta.get("business_dttm")
        company_id = meta.get("company_id")

        # --- параметры запроса ---
        params = meta.get("request_parameters") or {}
        if not params and isinstance(payload, dict):
            rp = payload.get("request_parameters")
            if isinstance(rp, str):
                try:
                    params = json.loads(rp)
                except Exception:
                    params = {}
            elif isinstance(rp, dict):
                params = rp

        def _i(v, default=None):
            try:
                return int(v) if v not in (None, "",) else default
            except Exception:
                return default

        def _f(v, default=None):
            try:
                return float(v) if v not in (None, "",) else default
            except Exception:
                return default

        def _s(v, default=""):
            return str(v) if v is not None else default

        def _parse_json(raw):
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", "ignore")
            if isinstance(raw, str):
                try:
                    return json.loads(raw)
                except Exception:
                    return {}
            return raw if isinstance(raw, dict) else {}

        # --- извлекаем JSON из payload/bronze.response_body ---
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, dict):
            body = raw.get("response_body")
            data = _parse_json(body if body is not None else raw)
        else:
            data = _parse_json(raw)

        # у разных версий WB данные лежат либо на верхнем уровне, либо под "data"
        data_root = data.get("data") if isinstance(data.get("data"), dict) else data
        md = (data_root.get("metadata") or {})
        # search_result может быть списком — берём первый элемент
        sr = md.get("search_result")
        if isinstance(sr, list):
            sr0 = (sr[0] if sr else {}) or {}
        elif isinstance(sr, dict):
            sr0 = sr
        else:
            sr0 = {}

        keyword = (
                (md.get("original") or "").strip()
                or (params.get("query") if isinstance(params, dict) else None)
                or (md.get("normquery") or "").strip()
                or (md.get("title") or "").strip()
                or (md.get("name") or "").strip()
        )

        if not keyword:
            keyword = (sr0.get("title") or sr0.get("name") or "").strip()

        app_type = _i(params.get("appType"))
        lang = _s(params.get("lang"))
        cur = _s(params.get("curr") or params.get("cur"))
        resultset = _s(params.get("resultset"))
        page = _i(params.get("page"), 1)

        catalog_type = _s(md.get("catalog_type"))
        catalog_value = _s(md.get("catalog_value"))
        normquery = md.get("normquery")

        search_result_name = _s(sr0.get("name") or md.get("name"))
        search_result_rmi = _s(sr0.get("rmi") or md.get("rmi"))
        search_result_title = _s(sr0.get("title") or md.get("title"))
        search_result_rs = _i(sr0.get("rs") or md.get("rs"))
        search_result_qv = _s(sr0.get("qv") or md.get("qv"))

        # продукты: top-level или под data.products
        products = data_root.get("products") or []
        if not isinstance(products, list):
            products = []

        out = []
        for idx, prod in enumerate(products, start=1):
            product_id_val = prod.get("id")
            if product_id_val in (None, "", 0):
                continue

            colors = prod.get("colors") or []
            color0 = colors[0] if colors else {}
            sizes = prod.get("sizes") or []
            size0 = sizes[0] if sizes else {}
            log = prod.get("log") or {}
            pmeta = prod.get("meta") or {}
            price0 = size0.get("price") or {}

            def _pick(d, *names):
                for n in names:
                    if n in d and d.get(n) not in (None, ""):
                        return d.get(n)
                return None

            rec = {
                # стандартные поля silver
                "company_id": _i(company_id),
                "request_uuid": req_uuid,
                "response_dttm": resp_dttm,
                "business_dttm": business_dttm,

                # ключ + дата/позиция
                "keyword": keyword,
                "date": resp_dttm,  # timestamptz
                "nm_position": idx,

                # параметры запроса
                "app_type": app_type,
                "lang": lang,
                "cur": cur,
                "resultset": resultset,
                "page": page,

                # metadata.search_result / metadata
                "catalog_type": catalog_type,
                "catalog_value": catalog_value,
                "normquery": normquery,
                "search_result_name": search_result_name,
                "search_result_rmi": search_result_rmi,
                "search_result_title": search_result_title,
                "search_result_rs": search_result_rs,
                "search_result_qv": search_result_qv,

                # товар (nm_id и прочие) — с fallback’ами названий
                "product_id": _i(prod.get("id")),
                "product_time_1": _i(_pick(prod, "time1", "time_1")),
                "product_time_2": _i(_pick(prod, "time2", "time_2")),
                "product_wh": _i(_pick(prod, "wh")),
                "product_d_type": _i(_pick(prod, "dtype", "d_type")),
                "product_dist": _i(_pick(prod, "dist")),
                "product_root": _i(_pick(prod, "root")),
                "product_kind_id": _i(_pick(prod, "kindId", "kind_id")),
                "product_brand": _s(prod.get("brand")),
                "product_brand_id": _i(_pick(prod, "brandId", "brand_id")),
                "product_site_brand_id": _i(_pick(prod, "siteBrandId", "site_brand_id")),
                "product_colors_id": _i(_pick(color0, "id")) if color0 else None,
                "product_colors_name": color0.get("name") if color0 else None,
                "product_subject_id": _i(_pick(prod, "subjectId", "subject_id")),
                "product_subject_parent_id": _i(_pick(prod, "subjectParentId", "subject_parent_id")),
                "product_name": _s(prod.get("name")),
                "product_entity": _s(prod.get("entity")),
                "product_match_id": _i(_pick(prod, "matchId", "match_id")),
                "product_supplier": _s(prod.get("supplier")),
                "product_supplier_id": _i(_pick(prod, "supplierId", "supplier_id")),
                "product_supplier_rating": _f(_pick(prod, "supplierRating", "supplier_rating")),
                "product_supplier_flags": _i(_pick(prod, "supplierFlags", "supplier_flags")),
                "product_pics": _i(_pick(prod, "pics")),
                "product_rating": _i(_pick(prod, "rating")),
                "product_review_rating": _f(_pick(prod, "reviewRating", "review_rating")),
                "product_nm_review_rating": _f(_pick(prod, "nmReviewRating", "nm_review_rating")),
                "product_feedbacks": _i(_pick(prod, "feedbacks")),
                "product_nm_feedbacks": _i(_pick(prod, "nmFeedbacks", "nm_feedbacks")),
                "product_panel_promo_id": _i(_pick(prod, "panelPromoId", "panel_promo_id")),
                "product_volume": _i(_pick(prod, "volume")),
                "product_view_flags": _i(_pick(prod, "viewFlags", "view_flags")),

                # размеры (первый элемент)
                "product_sizes_name": _s(size0.get("name")),
                "product_sizes_orig_name": _s(_pick(size0, "origName", "orig_name")),
                "product_sizes_rank": _i(_pick(size0, "rank")),
                "product_sizes_option_id": _i(_pick(size0, "optionId", "option_id")),
                "product_sizes_wh": _i(_pick(size0, "wh")),
                "product_sizes_time_1": _i(_pick(size0, "time1", "time_1")),
                "product_sizes_time_2": _i(_pick(size0, "time2", "time_2")),
                "product_sizes_d_type": _i(_pick(size0, "dtype", "d_type")),
                "product_sizes_price_basic": _i(_pick(price0, "basic")),
                "product_sizes_price_product": _i(_pick(price0, "product")),
                "product_sizes_price_logistics": _i(_pick(price0, "logistics")),
                "product_sizes_price_return": _i(_pick(price0, "return")),
                "product_sizes_sale_conditions": _i(_pick(size0, "saleConditions", "sale_conditions")),
                "product_sizes_payload": _s(size0.get("payload")),

                "product_total_quantity": _i(_pick(prod, "totalQuantity", "total_quantity")),

                # лог промо
                "product_log_cpm": _i(_pick(log, "cpm")),
                "product_log_promotion": _i(_pick(log, "promotion")),
                "product_log_promo_position": _i(_pick(log, "promoPosition", "promo_position")),
                "product_log_position": _i(_pick(log, "position")),
                "product_log_advert_id": _i(_pick(log, "advertId", "advert_id")),
                "product_log_tp": _s(_pick(log, "tp")),
                "product_logs": prod.get("logs"),

                # meta внутри продукта
                "product_meta_tokens": _s(json.dumps(pmeta.get("tokens", []), ensure_ascii=False)),
                "product_meta_preset_id": _i(_pick(pmeta, "presetId", "preset_id")),
            }

            out.append(rec)

        return out


    def norm__wb_fin_reports_1w(self, payload, meta=None):
        import json, re
        from datetime import datetime, date
        try:
            from zoneinfo import ZoneInfo
            MSK = ZoneInfo("Europe/Moscow")
        except Exception:
            MSK = None

        def _to_dt(val):
            if val is None or (isinstance(val, str) and val.strip() == ""):
                return None
            if isinstance(val, datetime):
                return val if val.tzinfo else (val.replace(tzinfo=MSK) if MSK else val)
            if isinstance(val, date):
                return datetime.combine(val, datetime.min.time(), tzinfo=MSK) if MSK else datetime.combine(val,
                                                                                                           datetime.min.time())
            if isinstance(val, str):
                s = val.strip().replace("Z", "+00:00")
                if "T" not in s and " " in s:
                    s = s.replace(" ", "T", 1)
                s = re.sub(r'\s*:\s*', ':', s)
                try:
                    dt = datetime.fromisoformat(s)
                except Exception:
                    return None
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=MSK) if MSK else dt
                return dt
            return None

        def _to_date(val):
            if val is None or (isinstance(val, str) and val.strip() == ""):
                return None
            if isinstance(val, date) and not isinstance(val, datetime):
                return val
            if isinstance(val, datetime):
                return val.date()
            if isinstance(val, str):
                s = val.strip()
                m = re.match(r'^(\d{4}-\d{2}-\d{2})', s)
                if m:
                    try:
                        from datetime import date as _date
                        return _date.fromisoformat(m.group(1))
                    except Exception:
                        pass
                dt = _to_dt(s)
                return dt.date() if dt else None
            return None

        meta = meta or {}
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        # гарантируем список
        if isinstance(data, dict):
            for k in ("data", "result", "items", "rows"):
                v = data.get(k)
                if isinstance(v, list):
                    data = v
                    break
            else:
                data = [data]
        elif not isinstance(data, list):
            data = []

        OPTIONAL = [
            "bonus_type_name", "suppliercontract_code", "rebill_logistic_org",
            "kiz", "sticker_id", "ppvz_office_name", "ppvz_supplier_name",
            "ppvz_inn", "declaration_number", "trbx_id",
        ]
        DATE_FIELDS = {
            "date_from", "date_to", "create_dt", "fix_tariff_date_from",
            "fix_tariff_date_to", "rr_dt",
        }
        TSTZ_FIELDS = {"order_dt", "sale_dt"}

        out = []
        for row in data:
            if not isinstance(row, dict):
                continue
            rec = dict(row)

            for k, v in list(rec.items()):
                if isinstance(v, str) and v.strip() == "":
                    rec[k] = None

            for k in OPTIONAL:
                rec.setdefault(k, None)

            for k in DATE_FIELDS:
                if k in rec:
                    rec[k] = _to_date(rec[k])
            for k in TSTZ_FIELDS:
                if k in rec:
                    rec[k] = _to_dt(rec[k])

            v = rec.get("bonus_type_name")
            if v is None or (isinstance(v, str) and v.strip() == ""):
                rec["bonus_type_name"] = " "

            v = rec.get("srid")
            if v is None or (isinstance(v, str) and v.strip() == ""):
                rec["srid"] = " "

            rec.update({
                "request_uuid": meta.get("request_uuid"),
                "response_dttm": meta.get("response_dttm"),
                "company_id": meta.get("company_id"),
                "business_dttm": meta.get("business_dttm"),
            })

            out.append(rec)

        return out

    # ===================== WB: FIN REPORT 1D (ZIP→XLSX) =====================

    @staticmethod
    def _wb__xlsx_from_bronze(payload):
        """
        bronze → response_body (JSON) → base64 ZIP → XLSX → pandas.DataFrame
        Возвращает DataFrame с оригинальными (русскими) заголовками WB.
        """
        import json, base64, io, zipfile, pandas as pd

        raw = payload
        # допускаем, что нам передали запись бронзы целиком
        if isinstance(raw, dict) and "response_body" in raw:
            raw = raw["response_body"]

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        # JSON с ключом "file"
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        # WB обычно кладёт base64 в поле "file"
        b64 = None
        if isinstance(data, dict):
            b64 = data.get("file") or (data.get("data") or {}).get("file")
        if not b64:
            # иногда приходит уже распакованный JSON/список — на такой случай просто делаем DataFrame
            import pandas as pd
            return pd.DataFrame(data if isinstance(data, list) else ([data] if data else []))

        zbytes = base64.b64decode(b64)
        with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
            # берём первый .xlsx
            xlsx_name = next((n for n in zf.namelist() if n.lower().endswith(".xlsx")), None)
            if not xlsx_name:
                raise ValueError("ZIP не содержит .xlsx")
            with zf.open(xlsx_name) as f:
                df = pd.read_excel(f)  # WB шлёт нормальные заголовки
        return df

    @staticmethod
    def _wb__safe_date_col(df, col):
        import pandas as pd
        if col not in df.columns:
            return
        s = pd.to_datetime(df[col], errors="coerce")
        df[col] = s.dt.date

    @staticmethod
    def _wb__safe_float_col(df, col):
        import pandas as pd, numpy as np
        if col not in df.columns:
            return
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    @staticmethod
    def _wb__safe_int_col(df, col):
        import pandas as pd
        if col not in df.columns:
            return
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    @staticmethod
    def _wb__ensure_cols(df, cols):
        for c in cols:
            if c not in df.columns:
                df[c] = None
        return df

    def _wb__with_meta(self, df, meta):
        """
        Добавляем служебные поля из meta: business_dttm, company_id, request_uuid, response_dttm
        """
        for k in ("business_dttm", "company_id", "request_uuid", "response_dttm"):
            if k not in df.columns:
                df[k] = meta.get(k)
        return df

    # -------------------- ADJUSTMENTS: BY PRODUCT --------------------
    def norm__wb_fin_adjustments_product_1d(self, payload, meta=None):
        """
        → silver.wb_fin_adjustments_product_1d
        Фильтр по "Обоснование для оплаты" ∈ {"Удержание","Штраф","Добровольная компенсация при возврате"}.
        """
        import pandas as pd
        meta = meta or {}
        df = self._wb__xlsx_from_bronze(payload)
        if df is None or df.empty:
            return []

        reason_col = "Обоснование для оплаты"
        reasons = {"Удержание", "Штраф", "Добровольная компенсация при возврате"}
        if reason_col in df.columns:
            df = df[df[reason_col].isin(reasons)].copy()
        if df.empty:
            return []

        rename_map = {
            "Номер поставки": "income_id",
            "Предмет": "subject",
            "Код номенклатуры": "nomenclature_code",
            "Бренд": "brand_name",
            # в твоей схеме именно suplier_article
            "Артикул поставщика": "suplier_article",
            "Название": "name",
            "Размер": "tech_size",
            "Баркод": "barcode",
            "Тип документа": "doc_type_name",
            "Обоснование для оплаты": "supplier_oper_name",
            "Дата заказа покупателем": "order_date",
            "Дата продажи": "sale_date",
            "Общая сумма штрафов": "penalty",
            "Виды логистики, штрафов и доплат": "bonus_type_name",
            "Номер офиса": "office_id",
            "Склад": "warehouse_name",
            "Страна": "country",
            "Тип коробов": "box_type",
            "ШК": "shk_id",
            "Srid": "srid",
            "К перечислению Продавцу за реализованный Товар": "seller_payout",
            "Корректировка Вознаграждения Вайлдберриз (ВВ)": "additional_payment",
            "Сумма удержанная за начисленные баллы программы лояльности": "cashback_amount",
            "Компенсация скидки по программе лояльности": "cashback_discount",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        # типизация чисел/дат
        for c in ("income_id", "nomenclature_code", "office_id", "shk_id"):
            self._wb__safe_int_col(df, c)
        for c in ("order_date", "sale_date"):
            self._wb__safe_date_col(df, c)
        for c in ("penalty", "seller_payout", "additional_payment", "cashback_amount", "cashback_discount"):
            self._wb__safe_float_col(df, c)

        # ВСЁ, что varchar → в строку (без 'nan'/'None')
        def _as_str(col):
            if col in df.columns:
                s = df[col].astype("object")
                s = s.where(pd.notna(s), None)
                df[col] = s.map(lambda x: str(x) if x is not None else None)

        for c in (
                "barcode", "srid", "suplier_article", "tech_size",
                "name", "brand_name", "subject", "warehouse_name", "country",
                "doc_type_name", "supplier_oper_name", "bonus_type_name", "box_type"
        ):
            _as_str(c)

        # служебные поля
        df = self._wb__with_meta(df, meta)

        target_cols = [
            "business_dttm", "company_id", "request_uuid", "response_dttm",
            "income_id", "subject", "nomenclature_code", "brand_name", "suplier_article",
            "name", "tech_size", "barcode", "doc_type_name", "supplier_oper_name",
            "order_date", "sale_date", "penalty", "bonus_type_name", "office_id",
            "warehouse_name", "country", "box_type", "shk_id", "srid",
            "seller_payout", "additional_payment", "cashback_amount", "cashback_discount",
        ]
        df = self._wb__ensure_cols(df, target_cols)
        return df[target_cols].to_dict(orient="records")

    # -------------------- ADJUSTMENTS: GENERAL --------------------
    def norm__wb_fin_adjustments_general_1d(self, payload, meta=None):
        """
        → silver.wb_fin_adjustments_general_1d
        Фильтр по "Обоснование для оплаты" ∈ {"Удержание","Штраф","Добровольная компенсация при возврате"}.
        """
        import pandas as pd
        meta = meta or {}
        df = self._wb__xlsx_from_bronze(payload)
        if df is None or df.empty:
            return []

        reason_col = "Обоснование для оплаты"
        reasons = {"Удержание", "Штраф", "Добровольная компенсация при возврате"}
        if reason_col in df.columns:
            df = df[df[reason_col].isin(reasons)].copy()
        if df.empty:
            return []

        rename_map = {
            "Номер поставки": "income_id",
            "Предмет": "subject",
            "Код номенклатуры": "nm_id",
            "Бренд": "brand_name",
            # в этой таблице тоже suplier_article
            "Артикул поставщика": "suplier_article",
            "Название": "name",
            "Размер": "tech_size",
            "Баркод": "barcode",
            "Тип документа": "doc_type_name",
            "Обоснование для оплаты": "supplier_oper_name",
            "Дата заказа покупателем": "order_date",
            "Дата продажи": "sale_date",
            "Общая сумма штрафов": "penalty",
            "Виды логистики, штрафов и доплат": "bonus_type_name",
            "Номер офиса": "office_id",
            "Склад": "warehouse_name",
            "Страна": "country",
            "Тип коробов": "box_type",
            "ШК": "shk_id",
            "Srid": "srid",
            "Удержания": "deduction",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        for c in ("income_id", "nm_id", "office_id", "shk_id"):
            self._wb__safe_int_col(df, c)
        for c in ("order_date", "sale_date"):
            self._wb__safe_date_col(df, c)
        for c in ("penalty", "deduction"):
            self._wb__safe_float_col(df, c)

        def _as_str(col):
            if col in df.columns:
                s = df[col].astype("object")
                s = s.where(pd.notna(s), None)
                df[col] = s.map(lambda x: str(x) if x is not None else None)

        for c in (
                "barcode", "srid", "suplier_article", "tech_size",
                "name", "brand_name", "subject", "warehouse_name", "country",
                "doc_type_name", "supplier_oper_name", "bonus_type_name", "box_type"
        ):
            _as_str(c)

        df = self._wb__with_meta(df, meta)

        target_cols = [
            "business_dttm", "company_id", "request_uuid", "response_dttm",
            "income_id", "subject", "nm_id", "brand_name", "suplier_article",
            "name", "tech_size", "barcode", "doc_type_name", "supplier_oper_name",
            "order_date", "sale_date", "penalty", "bonus_type_name", "office_id",
            "warehouse_name", "country", "box_type", "shk_id", "srid", "deduction",
        ]
        df = self._wb__ensure_cols(df, target_cols)
        return df[target_cols].to_dict(orient="records")

    # -------------------- SALES --------------------
    def norm__wb_sales_1d(self, payload, meta=None):
        """
        → silver.wb_sales_1d
        Фильтр по "Обоснование для оплаты" ∈ {"Продажа","Возврат"}.
        """
        import pandas as pd
        meta = meta or {}
        df = self._wb__xlsx_from_bronze(payload)
        if df is None or df.empty:
            return []

        reason_col = "Обоснование для оплаты"
        reasons = {"Продажа", "Возврат"}
        if reason_col in df.columns:
            df = df[df[reason_col].isin(reasons)].copy()
        if df.empty:
            return []

        rename_map = {
            "Номер поставки": "income_id",
            "Предмет": "subject",
            "Код номенклатуры": "nm_id",
            "Бренд": "brand_name",
            "Артикул поставщика": "supplier_article",
            "Название": "name",
            "Размер": "tech_size",
            "Баркод": "barcode",
            "Обоснование для оплаты": "payment_reason",
            "Дата заказа покупателем": "order_date",
            "Дата продажи": "sale_date",
            "Кол-во": "quantity",
            "Цена розничная": "price",
            "Вайлдберриз реализовал Товар (Пр)": "wb_realization_price",
            "Скидка постоянного Покупателя (СПП), %": "spp",
            # в схеме — commision_percent (одна m)
            "Размер кВВ, %": "commision_percent",
            "Цена розничная с учетом согласованной скидки": "price_with_discount",
            "Эквайринг/Комиссии за организацию платежей": "acquiring_amount",
            "Размер комиссии за эквайринг/Комиссии за организацию платежей, %": "acquiring_percent",
            "Наименование банка-эквайера": "acquiring_bank",
            "Склад": "warehouse_name",
            "Страна": "country",
            "Srid": "srid",
            "К перечислению Продавцу за реализованный Товар": "seller_payout",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        for c in ("income_id", "nm_id", "quantity"):
            self._wb__safe_int_col(df, c)
        for c in ("order_date", "sale_date"):
            self._wb__safe_date_col(df, c)
        for c in ("price", "wb_realization_price", "spp", "commision_percent",
                  "price_with_discount", "acquiring_amount", "acquiring_percent",
                  "seller_payout"):
            self._wb__safe_float_col(df, c)

        def _as_str(col):
            if col in df.columns:
                s = df[col].astype("object")
                s = s.where(pd.notna(s), None)
                df[col] = s.map(lambda x: str(x) if x is not None else None)

        for c in (
                "barcode", "srid", "supplier_article", "tech_size",
                "name", "brand_name", "subject", "warehouse_name", "country",
                "payment_reason", "acquiring_bank"
        ):
            _as_str(c)

        df = self._wb__with_meta(df, meta)

        target_cols = [
            "business_dttm", "company_id", "request_uuid", "response_dttm",
            "income_id", "subject", "nm_id", "brand_name", "supplier_article",
            "name", "tech_size", "barcode", "payment_reason",
            "order_date", "sale_date", "quantity", "price",
            "wb_realization_price", "spp", "commision_percent",
            "price_with_discount", "acquiring_amount", "acquiring_percent",
            "acquiring_bank", "warehouse_name", "country", "seller_payout", "srid",
        ]
        df = self._wb__ensure_cols(df, target_cols)
        return df[target_cols].to_dict(orient="records")

    # -------------------- LOGISTICS --------------------
    def norm__wb_logistics_1d(self, payload, meta=None):
        """
        → silver.wb_logistics_1d
        Фильтр по "Обоснование для оплаты" == "Логистика".
        """
        import pandas as pd
        meta = meta or {}
        df = self._wb__xlsx_from_bronze(payload)
        if df is None or df.empty:
            return []

        reason_col = "Обоснование для оплаты"
        if reason_col in df.columns:
            df = df[df[reason_col] == "Логистика"].copy()
        if df.empty:
            return []

        rename_map = {
            "Номер поставки": "income_id",
            "Код номенклатуры": "nm_id",
            "Бренд": "brand_name",
            "Артикул поставщика": "supplier_article",
            "Название": "name",
            "Размер": "tech_size",
            "Баркод": "barcode",
            "Обоснование для оплаты": "payment_reason",
            "Дата заказа покупателем": "order_date",
            "Дата продажи": "sale_date",
            "Кол-во": "delivery_quantity",
            # в схеме — logistic_cost (без s)
            "Услуги по доставке товара покупателю": "logistic_cost",
            # в схеме — logistics_type
            "Виды логистики, штрафов и корректировок ВВ": "logistics_type",
            "Склад": "warehouse_name",
            "Страна": "country",
            "Srid": "srid",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        for c in ("income_id", "nm_id", "delivery_quantity"):
            self._wb__safe_int_col(df, c)
        for c in ("order_date", "sale_date"):
            self._wb__safe_date_col(df, c)
        for c in ("logistic_cost",):
            self._wb__safe_float_col(df, c)

        def _as_str(col):
            if col in df.columns:
                s = df[col].astype("object")
                s = s.where(pd.notna(s), None)
                df[col] = s.map(lambda x: str(x) if x is not None else None)

        for c in (
                "barcode", "srid", "supplier_article", "tech_size",
                "name", "brand_name", "payment_reason", "warehouse_name", "country", "logistics_type"
        ):
            _as_str(c)

        df = self._wb__with_meta(df, meta)

        target_cols = [
            "business_dttm", "company_id", "request_uuid", "response_dttm",
            "income_id", "nm_id", "brand_name", "supplier_article", "name",
            "tech_size", "barcode", "payment_reason", "order_date", "sale_date",
            "delivery_quantity", "logistic_cost", "logistics_type",
            "warehouse_name", "country", "srid",
        ]
        df = self._wb__ensure_cols(df, target_cols)
        return df[target_cols].to_dict(orient="records")

    def norm__wb_supplier_orders_1d(self, payload, meta=None):
        """
        /supplier/orders → строки для silver.wb_supplier_orders_1d

        Используются общие хелперы: _to_int, _to_float, _to_str, _to_date, _ensure_list
        Фильтр по партиции: оставляем записи, у которых date попадает в день meta['business_dttm'].
        PK в silver: (business_dttm, srid)
        """
        import json
        from datetime import datetime

        meta = meta or {}
        req_uuid = meta.get("request_uuid")
        resp_dttm = meta.get("response_dttm")
        company_id = meta.get("company_id")
        business_dt = meta.get("business_dttm")
        biz_day = _to_date(business_dt)

        # --- локальные хелперы (добавляем только то, чего нет среди общих) ---
        def _to_ts(x):
            """Строки ISO/`YYYY-MM-DD HH:MM:SS` → datetime (naive); 'Z' → '+00:00'."""
            if x is None or x == "":
                return None
            if isinstance(x, datetime):
                return x
            s = str(x).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(s.replace("T", " "))
            except Exception:
                return None

        def _to_bool(x):
            if x is None or x == "":
                return None
            if isinstance(x, bool):
                return x
            s = str(x).strip().lower()
            if s in {"1", "true", "t", "yes", "y"}:
                return True
            if s in {"0", "false", "f", "no", "n"}:
                return False
            try:
                return bool(int(float(s)))
            except Exception:
                return None

        # --- достаём JSON ---
        raw = payload
        if not isinstance(raw, (str, bytes, bytearray, dict, list)):
            raw = getattr(raw, "response_body", raw)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        items = _ensure_list(data)

        out = []
        for r in items:
            if not isinstance(r, dict):
                continue

            dt = _to_ts(r.get("date"))
            last_dt = _to_ts(r.get("lastChangeDate"))
            cancel_dt = _to_ts(r.get("cancelDate"))

            # фильтр по партиции (если указана)
            if biz_day and dt and _to_date(dt) != biz_day:
                continue

            # WB иногда присылает isCancel вместо isCansel
            is_cancel_raw = r.get("isCansel") if "isCansel" in r else r.get("isCancel")

            # числа, которые иногда приходят как "3.0" (float-like строки) → аккуратный фолбэк
            nm_id = _to_int(r.get("nmId"))
            if nm_id is None:
                try:
                    nm_id = int(float(str(r.get("nmId"))))
                except Exception:
                    nm_id = None

            income_id = _to_int(r.get("incomeID"))
            if income_id is None:
                try:
                    income_id = int(float(str(r.get("incomeID"))))
                except Exception:
                    income_id = None

            spp = _to_int(r.get("spp"))
            if spp is None:
                try:
                    spp = int(float(str(r.get("spp"))))
                except Exception:
                    spp = None

            rec = {
                # meta
                "request_uuid": req_uuid,
                "response_dttm": resp_dttm,
                "company_id": company_id,
                "business_dttm": business_dt,

                # данные
                "g_number": _to_str(r.get("gNumber")),
                "srid": _to_str(r.get("srid")),
                "date": dt,
                "last_change_date": last_dt,
                "warehouse_name": _to_str(r.get("warehouseName")),
                "warehouse_type": _to_str(r.get("warehouseType")),
                "country_name": _to_str(r.get("countryName")),
                "oblast_okrug_name": _to_str(r.get("oblastOkrugName")),
                "region_name": _to_str(r.get("regionName")),
                "supplier_article": _to_str(r.get("supplierArticle")),
                "nm_id": nm_id,
                "barcode": _to_str(r.get("barcode")),
                "category_name": _to_str(r.get("category")),
                "subject_name": _to_str(r.get("subject")),
                "brand_name": _to_str(r.get("brand")),
                "tech_size": _to_str(r.get("techSize")),
                "income_id": income_id,
                "is_supply": _to_bool(r.get("isSupply")),
                "is_realization": _to_bool(r.get("isRealization")),
                "total_price": _to_float(r.get("totalPrice")),
                "discount_percent": _to_float(r.get("discountPercent")),  # int→real по silver
                "spp": spp,  # real→int по silver
                "finished_price": _to_float(r.get("finishedPrice")),
                "price_with_discount": _to_float(r.get("priceWithDisc")),
                "is_cansel": _to_bool(is_cancel_raw),
                "cansel_date": cancel_dt,
                "stiker": _to_str(r.get("sticker")),
            }

            # обязательная часть PK
            if not rec["srid"]:
                continue

            out.append(rec)

        return out

    @staticmethod
    def norm__wb_supplier_sales_1d(payload, meta: dict, partition_ctx=None) -> list[dict]:
        """
        /supplier/sales → silver.wb_supplier_sales_1d
        PK: (business_dttm, srid)
        """
        from datetime import datetime, timedelta, timezone as std_timezone

        def _to_msk(s):
            """ISO 'YYYY-MM-DDTHH:MM:SS[Z|+hh:mm]' → tz-aware MSK; пустые и '0001-01-01...' → None."""
            if not s:
                return None
            ss = str(s).strip()
            if ss.startswith("0001-01-01"):
                return None
            try:
                if ss.endswith("Z"):
                    ss = ss[:-1] + "+00:00"
                dt = datetime.fromisoformat(ss)
            except Exception:
                return None
            if MSK:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC or std_timezone.utc)
                return dt.astimezone(MSK)
            return dt  # без tz-инфры вернём как есть

        items = _ensure_list(payload)
        rows: list[dict] = []

        for it in items:
            row = {
                # meta
                "business_dttm": meta.get("business_dttm"),
                "company_id": meta.get("company_id"),
                "request_uuid": meta.get("request_uuid"),
                "response_dttm": meta.get("response_dttm"),
                "request_parameters": meta.get("request_parameters"),

                # PK / идентификаторы
                "srid": _to_str(it.get("srid")),
                "g_number": _to_str(it.get("gNumber")),
                "sale_id": _to_str(it.get("saleID")),

                # даты/время
                "date": _to_msk(it.get("date")),
                "last_change_date": _to_msk(it.get("lastChangeDate")),

                # локации
                "warehouse_name": _to_str(it.get("warehouseName")),
                "warehouse_type": _to_str(it.get("warehouseType")),
                "country_name": _to_str(it.get("countryName")),
                "oblast_okrug_name": _to_str(it.get("oblastOkrugName")),
                "region_name": _to_str(it.get("regionName")),

                # товарные атрибуты (имена = column_lineage)
                "supplier_article": _to_str(it.get("supplierArticle")),
                "nm_id": _to_int(it.get("nmId")),
                "barcode": _to_str(it.get("barcode")),
                "category": _to_str(it.get("category")),
                "subject": _to_str(it.get("subject")),
                "brand": _to_str(it.get("brand")),
                "tech_size": _to_str(it.get("techSize")),
                "income_id": _to_int(it.get("incomeID")),
                "sticker": _to_str(it.get("sticker")),

                # флаги
                "is_supply": bool(it.get("isSupply")) if it.get("isSupply") is not None else None,
                "is_realization": bool(it.get("isRealization")) if it.get("isRealization") is not None else None,

                # деньги/проценты
                "total_price": _to_float(it.get("totalPrice")),
                "discount_percent": _to_float(it.get("discountPercent")),
                "spp": _to_int(it.get("spp")),
                "payment_sale_amount": _to_float(it.get("paymentSaleAmount")),
                "for_pay": _to_float(it.get("forPay")),
                "finished_price": _to_float(it.get("finishedPrice")),
                "price_with_disc": _to_float(it.get("priceWithDisc")),
            }
            # контроль PK
            if row["srid"]:
                rows.append(row)

        return rows

    @staticmethod
    def norm__wb_supplier_stocks_1d(payload, meta: dict, partition_ctx=None) -> list[dict]:
        """
        /supplier/stocks → silver.wb_supplier_stocks_1d
        PK: (business_dttm, warehouse_name, barcode, nm_id)
        """
        from datetime import datetime, timedelta, timezone as std_timezone

        def _to_msk(s):
            """ISO 'YYYY-MM-DDTHH:MM:SS[Z|+hh:mm]' → tz-aware MSK; пустые и '0001-01-01...' → None."""
            if not s:
                return None
            ss = str(s).strip()
            if ss.startswith("0001-01-01"):
                return None
            try:
                if ss.endswith("Z"):
                    ss = ss[:-1] + "+00:00"
                dt = datetime.fromisoformat(ss)
            except Exception:
                return None
            if MSK:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC or std_timezone.utc)
                return dt.astimezone(MSK)
            return dt  # без tz-инфры вернём как есть

        items = _ensure_list(payload)
        rows: list[dict] = []

        for it in items:
            row = {
                # meta
                "business_dttm": meta.get("business_dttm"),
                "company_id": meta.get("company_id"),
                "request_uuid": meta.get("request_uuid"),
                "response_dttm": meta.get("response_dttm"),
                "request_parameters": meta.get("request_parameters"),

                # даты/время
                "last_change_date": _to_msk(it.get("lastChangeDate")),

                # PK + товарные атрибуты (имена = column_lineage)
                "warehouse_name": _to_str(it.get("warehouseName")),
                "barcode": _to_str(it.get("barcode")),
                "nm_id": _to_int(it.get("nmId")),
                "supplier_article": _to_str(it.get("supplierArticle")),
                "category": _to_str(it.get("category")),
                "subject": _to_str(it.get("subject")),
                "brand": _to_str(it.get("brand")),
                "tech_size": _to_str(it.get("techSize")),

                # остатки/движение
                "quantity": _to_int(it.get("quantity")),
                "in_way_to_client": _to_int(it.get("inWayToClient")),
                "in_way_from_client": _to_int(it.get("inWayFromClient")),
                "quantity_full": _to_int(it.get("quantityFull")),

                # цены/скидки
                "price": _to_float(it.get("Price")),
                "discount": _to_float(it.get("Discount")),

                # флаги
                "is_supply": bool(it.get("isSupply")) if it.get("isSupply") is not None else None,
                "is_realization": bool(it.get("isRealization")) if it.get("isRealization") is not None else None,

                "sc_code": _to_str(it.get("SCCode")),
            }
            # контроль PK
            if row["barcode"] and row["warehouse_name"] and row["nm_id"] is not None and row[
                "last_change_date"] is not None:
                rows.append(row)
        return rows

    @staticmethod
    def norm__wb_warehouse_remains_1d(payload, meta: dict, partition_ctx=None) -> list[dict]:
        """
        Download /warehouse_remains → silver.wb_warehouse_remains_1d
        PK: (business_dttm, warehouse_name, barcode)
        Без каких-либо фильтров по названию склада.
        """
        import json, base64

        def _s(x):
            v = x if isinstance(x, str) else (str(x) if x is not None else None)
            return v.strip() if isinstance(v, str) else v

        # --- привести payload к Python-объекту (поддержка JSON / base64(JSON) / уже-списка) ---
        if isinstance(payload, (bytes, bytearray)):
            try:
                payload = payload.decode("utf-8", errors="ignore")
            except Exception:
                payload = ""

        data = []
        if isinstance(payload, list):
            data = payload
        elif isinstance(payload, dict):
            data = payload.get("data") if isinstance(payload.get("data"), list) else []
        elif isinstance(payload, str):
            try:
                data = json.loads(payload)
            except Exception:
                try:
                    decoded = base64.b64decode(payload)
                    data = json.loads(decoded.decode("utf-8", errors="ignore"))
                except Exception:
                    data = []
        if not isinstance(data, list):
            data = []

        # --- нормализация и разворот warehouses[] → строки ---
        out: list[dict] = []
        for item in data:
            if not isinstance(item, dict):
                continue

            base = {
                "business_dttm": meta.get("business_dttm"),
                "company_id": meta.get("company_id"),
                "request_uuid": meta.get("request_uuid"),
                "response_dttm": meta.get("response_dttm"),
                "request_parameters": meta.get("request_parameters"),
                "brand_name": _s(item.get("brand")) or "",
                "subject_name": _s(item.get("subjectName")) or "",
                "supplier_article": _s(item.get("vendorCode")) or "",
                "nm_id": item.get("nmId"),
                "barcode": _s(item.get("barcode")),
                "tech_size": _s(item.get("techSize")) or "",
                "volume": _to_float(item.get("volume")),
            }

            whs = item.get("warehouses") or []
            if not isinstance(whs, list) or not whs:
                continue

            for wh in whs:
                if not isinstance(wh, dict):
                    continue
                row = dict(base)
                row["warehouse_name"] = _s(wh.get("warehouseName"))
                row["quantity"] = _to_int(wh.get("quantity"))

                # минимальные требования к PK
                if row["warehouse_name"] and row["barcode"]:
                    out.append(row)

        return out
