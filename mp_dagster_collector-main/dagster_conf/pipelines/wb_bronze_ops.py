import asyncio
import uuid
import json
from typing import Any, Dict, List, Union, Mapping, Optional, Tuple
from datetime import datetime, timezone
from dagster import op, In, Out, String, Int, get_dagster_logger, graph, DynamicOut, DynamicOutput, Dict as DagsterDict, Any as DagsterAny, List as DagsterList
from sqlalchemy import text
from src.connectors.wb.wb_api import WildberriesAsyncClient
from src.db.bronze.writer import save_json_response, save_binary_response
from src.db.bronze.models import (
    WBOrders,
    WBCommission,
    WBAdConfig,
    WBSalesFunnel,
    WBStocksReport,
    WBPaidStorage,
    WBPaidAcceptions,
    WBSuppliers,
    WBAdStats,
    WBAdInfo,
    WBClusters,
    WBKeywords,
    WBSkuApi,
    WbProductsSearchTexts,
    WbAdvFullstats1d
)
from dagster_conf.pipelines.utils import chunked
from src.db.bronze.model_protocol import TableModelProtocol
from email.utils import parsedate_to_datetime

# ─────────────────────────────────────────────────────────────────────────────
# 1) Генерация batch_id
# ─────────────────────────────────────────────────────────────────────────────

@op(
    out=Out(String),
    description="Генерирует единый batch_id (UUID4) для всех опов job-а"
)
def generate_batch_id() -> str:
    return str(uuid.uuid4())


# ─────────────────────────────────────────────────────────────────────────────
# 2) FETCH-опы: асинхронно собирают данные из Wildberries и возвращают их
# ─────────────────────────────────────────────────────────────────────────────

@op(
    ins={
        "batch_id": In(String),
        "date_from": In(String, description="ISO datetime, e.g. '2025-06-01T00:00:00Z'"),
        "date_to": In(String,   description="ISO datetime, e.g. '2025-06-01T23:59:59Z'")
    },
    out=Out(DagsterDict),
    required_resource_keys={"wildberries_client"},
    description="Асинхронно получает wb_orders (JSON) и возвращает его"
)
async def fetch_wb_orders(context, batch_id: str, date_from: str, date_to: str) -> Dict[str, Any]:
    logger = get_dagster_logger()
    async with context.resources.wildberries_client as client:
        try:
            data, headers = await client.fetch_orders(date_from=date_from, date_to=date_to)
            logger.info(f"[fetch_wb_orders] batch_id={batch_id}, headers={headers}")
            return {
                "orders": data,
                "_headers": headers
            }
        except Exception as e:
            logger.error(f"[fetch_wb_orders] ошибка: {e}")
            raise


@op(
    ins={"batch_id": In(String)},
    out=Out(DagsterDict),
    required_resource_keys={"wildberries_client"},
    description="Асинхронно получает wb_commission (JSON) и возвращает его"
)
async def fetch_wb_commission(context, batch_id: str) -> Dict[str, Any]:
    logger = get_dagster_logger()
    async with context.resources.wildberries_client as client:
        try:
            data, headers = await client.fetch_commission(locale="ru")
            logger.info(f"[fetch_wb_commission] batch_id={batch_id}, headers={headers}")
            return {
                "commission": data,
                "_headers": headers
            }
        except Exception as e:
            logger.error(f"[fetch_wb_commission] ошибка: {e}")
            raise


@op(
    ins={"batch_id": In(String)},
    out=Out(DagsterDict),
    required_resource_keys={"wildberries_client"},
    description="Асинхронно получает wb_ad_config (JSON) и возвращает его"
)
async def fetch_wb_ad_config(context, batch_id: str) -> Dict[str, Any]:
    logger = get_dagster_logger()
    async with context.resources.wildberries_client as client:
        try:
            data, headers = await client.fetch_ad_config()
            logger.info(f"[fetch_wb_ad_config] batch_id={batch_id}, headers={headers}")
            return {
                "ad_config": data,
                "_headers": headers
            }
        except Exception as e:
            logger.error(f"[fetch_wb_ad_config] ошибка: {e}")
            raise


@op(
    ins={
        "batch_id": In(String),
        "filters": In(DagsterDict, description="Фильтры для воронки продаж")
    },
    out=Out(DagsterDict),
    required_resource_keys={"wildberries_client"},
    description="Асинхронно получает wb_sales_funnel (JSON) и возвращает его"
)
async def fetch_wb_sales_funnel(context, batch_id: str, filters: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_dagster_logger()
    async with context.resources.wildberries_client as client:
        try:
            data, headers = await client.fetch_sales_funnel(filters)
            logger.info(f"[fetch_wb_sales_funnel] batch_id={batch_id}, headers={headers}")
            return {
                "sales_funnel": data,
                "_headers": headers
            }
        except Exception as e:
            logger.error(f"[fetch_wb_sales_funnel] ошибка: {e}")
            raise


@op(
    ins={
        "batch_id": In(str),
        "filters": In(dict, description="Параметры для запроса остатков"),
    },
    out=Out(DagsterAny),
    required_resource_keys={"wildberries_client"},
    description="Асинхронно получает wb_stocks_report и возвращает уже распарсенный JSON или бинарные данные"
)
async def fetch_wb_stocks_report(context, batch_id: str, filters: dict) -> Any:
    logger = get_dagster_logger()
    async with context.resources.wildberries_client as client:
        try:
            task_resp = await client.request_stocks_report(filters)
            task_id = task_resp.get("data", {}).get("taskId")
            if not task_id:
                raise RuntimeError(f"No taskId in response: {task_resp!r}")
            logger.info(f"[fetch_wb_stocks_report] batch_id={batch_id} taskId={task_id}")

            while True:
                status_resp = await client.get_stocks_report_status(task_id)
                status = status_resp.get("data", {}).get("status")
                logger.debug(f"[fetch_wb_stocks_report] status={status} for taskId={task_id}")
                if status == "done":
                    break
                await asyncio.sleep(5)

            raw_bytes, headers = await client.download_stocks_report(task_id)
            logger.info(f"[fetch_wb_stocks_report] downloaded {len(raw_bytes)} bytes batch_id={batch_id}")

            try:
                text = raw_bytes.decode("utf-8")
                data = json.loads(text)
                logger.info(f"[fetch_wb_stocks_report] parsed JSON list length={len(data)}")
                return {
                    "content": data,
                    "_headers": headers
                }
            except Exception as parse_err:
                logger.warning(f"[fetch_wb_stocks_report] could not parse JSON, returning raw bytes: {parse_err}")
                return {
                    "content": raw_bytes,
                    "_headers": headers
                }

        except Exception:
            logger.exception(f"[fetch_wb_stocks_report] batch_id={batch_id} encountered an error")
            raise


def _parse_or_bytes(raw: bytes, logger):
    """
    Пытаемся декодировать как UTF-8 JSON; если не получается — вернём сырые bytes.
    """
    try:
        text = raw.decode("utf-8")
        return json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        logger.warning(f"Could not parse JSON, returning raw bytes: {err}")
        return raw


@op(
    ins={
        "batch_id": In(str),
        "date_from": In(str),
        "date_to": In(str),
    },
    out=Out(Any),
    required_resource_keys={"wildberries_client"},
    description="Асинхронно получает wb_paid_storage и возвращает уже распарсенный JSON или бинарник"
)
async def fetch_wb_paid_storage(
    context, batch_id: str, date_from: str, date_to: str
) -> Any:
    logger = get_dagster_logger()
    async with context.resources.wildberries_client as client:
        resp = await client.fetch_paid_storage(date_from=date_from, date_to=date_to)

        tid = None
        if isinstance(resp, dict):
            tid = resp.get("data", {}).get("taskId") or resp.get("taskId")

        if tid:
            logger.info(f"[fetch_wb_paid_storage] batch_id={batch_id} taskId={tid}")
            while True:
                status = (await client.get_paid_storage_status(tid)).get("data", {}).get("status")
                logger.debug(f"[fetch_wb_paid_storage] status={status}")
                if status == "done":
                    break
                await asyncio.sleep(5)

            raw, headers = await client.download_paid_storage(tid)
            logger.info(f"[fetch_wb_paid_storage] downloaded {len(raw)} bytes")

            try:
                text = raw.decode("utf-8")
                data = json.loads(text)
                logger.info(f"[fetch_wb_paid_storage] parsed JSON list length={len(data)}")
                return {
                    "content": data,
                    "_headers": headers
                }
            except Exception as parse_err:
                logger.warning(f"[fetch_wb_paid_storage] could not parse JSON, returning raw bytes: {parse_err}")
                return {
                    "content": raw,  # возвращаем бинарные данные
                    "_headers": headers
                }

        logger.info(f"[fetch_wb_paid_storage] immediate JSON batch_id={batch_id}")
        return {
            "content": resp,
            "_headers": {}
        }


@op(
    ins={
        "batch_id": In(str),
        "date_from": In(str),
        "date_to": In(str),
    },
    out=Out(Any),
    required_resource_keys={"wildberries_client"},
    description="Асинхронно получает wb_paid_acceptions и возвращает уже распарсенный JSON или бинарник"
)
async def fetch_wb_paid_acceptions(
    context, batch_id: str, date_from: str, date_to: str
) -> Any:
    logger = get_dagster_logger()
    async with context.resources.wildberries_client as client:
        resp = await client.fetch_paid_acceptions(date_from=date_from, date_to=date_to)

        tid = None
        if isinstance(resp, dict):
            tid = resp.get("data", {}).get("taskId") or resp.get("taskId")

        if tid:
            logger.info(f"[fetch_wb_paid_acceptions] batch_id={batch_id} taskId={tid}")
            while True:
                status = (await client.get_paid_acceptions_status(tid)
                          ).get("data", {}).get("status")
                logger.debug(f"[fetch_wb_paid_acceptions] status={status}")
                if status == "done":
                    break
                await asyncio.sleep(5)

            raw, headers = await client.download_paid_acceptions(tid)
            logger.info(f"[fetch_wb_paid_acceptions] downloaded {len(raw)} bytes")

            try:
                text = raw.decode("utf-8")
                data = json.loads(text)
                logger.info(f"[fetch_wb_paid_acceptions] parsed JSON list length={len(data)}")
                return {
                    "paid_acceptions": data,
                    "_headers": headers
                }
            except Exception as parse_err:
                logger.warning(f"[fetch_wb_paid_acceptions] could not parse JSON, returning raw bytes: {parse_err}")
                return {
                    "paid_acceptions_raw": raw.decode("utf-8", errors="replace"),
                    "_headers": headers
                }

        logger.info(f"[fetch_wb_paid_acceptions] immediate JSON batch_id={batch_id}")
        return {
            "paid_acceptions": resp,
            "_headers": {}
        }


@op(
    ins={
        "batch_id": In(str),
        "date_from": In(str),
        "date_to": In(str)
    },
    out=Out(List[Dict[str, Any]]),
    required_resource_keys={"wildberries_client"},
    description="Асинхронно получает wb_suppliers (JSON list) и возвращает его"
)
async def fetch_wb_suppliers(context, batch_id: str, date_from: str, date_to: str) -> List[Dict[str, Any]]:
    logger = get_dagster_logger()
    async with context.resources.wildberries_client as client:
        try:
            raw, headers = await client.fetch_suppliers(date_from=date_from, date_to=date_to)
            logger.info(f"[fetch_wb_suppliers] batch_id={batch_id}, rows={len(raw)}")
            return [
                {
                    "content": row,
                    "_headers": headers
                }
                for row in raw
            ]
        except Exception as e:
            logger.error(f"[fetch_wb_suppliers] ошибка: {e}")
            raise


@op(
    ins={
        "batch_id": In(str),
        "date_from": In(str),
        "date_to": In(str)
    },
    out=Out(List[int]),
    required_resource_keys={"wildberries_client"},
    description="Асинхронно получает список ID рекламных кампаний WB за указанный период"
)
async def fetch_wb_advert_ids(context, batch_id: str, date_from: str, date_to: str) -> List[int]:
    logger = get_dagster_logger()
    async with context.resources.wildberries_client as client:
        try:
            ids = await client.get_advert_ids(date_from=date_from, date_to=date_to)
            logger.info(f"[fetch_wb_advert_ids] batch_id={batch_id}, найдено кампаний: {len(ids)}")
            return ids
        except Exception as e:
            logger.error(f"[fetch_wb_advert_ids] ошибка: {e}")
            raise


@op(
    ins={
        "batch_id": In(str),
        "filters": In(List[Dict[str, Any]], description="Список словарей: [{id, interval:{begin, end}}]")
    },
    out=Out(List[Dict[str, Any]]),
    required_resource_keys={"wildberries_client"},
    description="Асинхронно получает wb_ad_stats (JSON) и возвращает его"
)
async def fetch_wb_ad_stats(context, batch_id: str, filters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    logger = get_dagster_logger()
    async with context.resources.wildberries_client as client:
        try:
            result, headers = await client.fetch_ad_stats(filters)
            logger.info(f"[fetch_wb_ad_stats] batch_id={batch_id}, записей={len(result)}")
            return [{"_headers": headers, **row} for row in result]
        except Exception as e:
            logger.error(f"[fetch_wb_ad_stats] ошибка: {e}")
            raise


@op(
    ins={
        "batch_id": In(str),
        "params": In(List[int], description="Список ID рекламных кампаний")
    },
    out=Out(List[Dict[str, Any]]),
    required_resource_keys={"wildberries_client"},
    description="Асинхронно получает wb_ad_info (JSON) и возвращает его"
)
async def fetch_wb_ad_info(context, batch_id: str, params: List[int]) -> List[Dict[str, Any]]:
    logger = get_dagster_logger()
    logger.info(f"[fetch_wb_ad_info] IDs: {params}")
    async with context.resources.wildberries_client as client:
        try:
            result, headers = await client.fetch_ad_info(params)
            logger.info(f"[fetch_wb_ad_info] batch_id={batch_id}, записей={len(result)}")
            return [{"_headers": headers, **row} for row in result]
        except Exception as e:
            logger.error(f"[fetch_wb_ad_info] ошибка: {e}")
            raise


@op(
    ins={
        "batch_id": In(String),
        "advert_ids": In(List[int])
    },
    out=Out(List[Dict[str, Any]]),
    required_resource_keys={"wildberries_client"},
    description="Получает кластеры слов по списку рекламных кампаний WB"
)
async def fetch_wb_clusters_batch(
    context,
    batch_id: str,
    advert_ids: List[int]
) -> List[Dict[str, Any]]:
    logger = get_dagster_logger()
    async with context.resources.wildberries_client as client:
        try:
            logger.info(f"[fetch_wb_clusters_batch] batch_id={batch_id}, кампаний: {len(advert_ids)}")
            raw_results = await client.fetch_clusters_batch(advert_ids=advert_ids)

            formatted = [{"_headers": headers, **row} for row, headers in raw_results]
            logger.info(f"[fetch_wb_clusters_batch] получено кластеров: {len(formatted)}")
            return formatted
        except Exception as e:
            logger.error(f"[fetch_wb_clusters_batch] ошибка: {e}")
            raise


@op(
    ins={
        "batch_id": In(str),
        "advert_ids": In(List[int]),
        "date_from": In(str),
        "date_to": In(str),
    },
    out=Out(List[Dict[str, Any]]),
    required_resource_keys={"wildberries_client"},
    description="Асинхронно получает wb_keywords (JSON) по каждому advert_id"
)
async def fetch_wb_keywords_batch(
    context,
    batch_id: str,
    advert_ids: List[int],
    date_from: str,
    date_to: str
) -> List[Dict[str, Any]]:
    logger = get_dagster_logger()
    async with context.resources.wildberries_client as client:
        try:
            logger.info(f"[fetch_wb_keywords_batch] batch_id={batch_id}, adverts={len(advert_ids)}")
            raw_results = await client.fetch_keywords_batch(advert_ids, date_from, date_to)

            formatted = [{"_headers": headers, **row} for row, headers in raw_results]
            logger.info(f"[fetch_wb_keywords_batch] получено ключевых слов: {len(formatted)}")
            return formatted
        except Exception as e:
            logger.error(f"[fetch_wb_keywords_batch] ошибка: {e}")
            raise


@op(
    ins={"batch_id": In(str)},
    out=Out(List[Dict[str, Any]]),
    required_resource_keys={"wildberries_client"},
    description="Получает все карточки SKU через пагинацию WB API"
)
async def fetch_wb_sku_api(context, batch_id: str) -> List[Dict[str, Any]]:
    logger = get_dagster_logger()
    async with context.resources.wildberries_client as client:
        try:
            logger.info(f"[fetch_wb_sku_api] batch_id={batch_id}")
            cards, headers = await client.fetch_sku_cards_all()
            logger.info(f"[fetch_wb_sku_api] получено карточек: {len(cards)}")

            return [
                {**card, "_headers": headers}
                for card in cards
            ]
        except Exception as e:
            logger.error(f"[fetch_wb_sku_api] ошибка: {e}")
            raise

@op(
    required_resource_keys={"postgres", "wildberries_client"},
    out=Out(DagsterList[int]),
    description="Для текущего token_id вернуть список DISTINCT nm_id (mp_sku) из ads.ad_campaign_stat"
)
async def get_nm_ids_for_token(context) -> List[int]:
    session_factory = context.resources.postgres
    token_id = context.resources.wildberries_client.token_id

    async with session_factory() as session:
        # 1) company_id по токену
        row = await session.execute(
            text("SELECT company_id FROM core.tokens WHERE token_id = :token_id"),
            {"token_id": token_id}
        )
        company_row = row.first()
        if not company_row:
            raise ValueError(f"Не найден token_id={token_id} в core.tokens")
        company_id = company_row.company_id

        # 2) собираем все nm_id для этого company_id
        result = await session.execute(
            text("""
                SELECT DISTINCT mp_sku AS nm_id
                  FROM ads.ad_campaign_stat
                 WHERE legal_entity = :company_id
            """),
            {"company_id": company_id}
        )
        nm_ids = [r.nm_id for r in result]

    return nm_ids


@op(
    ins={
        "batch_id":    In(String),
        "nm_ids":      In(DagsterList[int], description="Список nmId (до 50)"),
        "date_filter": In(DagsterDict,   description="{'start':'YYYY-MM-DD','end':'YYYY-MM-DD'}"),
    },
    out=Out(DagsterList[DagsterDict]),
    required_resource_keys={"wildberries_client"},
    description=(
        "POST /search-report/product/search-texts: дробим nm_ids на чанки ≤50, "
        "добавляем currentPeriod и limit, возвращаем List[{search_texts, _headers}]"
    )
)
async def fetch_wb_products_search_texts(
    context,
    batch_id: str,
    nm_ids: List[int],
    date_filter: Dict[str, str],
) -> List[Dict[str, Any]]:
    logger = get_dagster_logger()
    token_id = context.resources.wildberries_client.token_id
    results: List[Dict[str, Any]] = []

    async with context.resources.wildberries_client as client:
        for chunk in chunked(nm_ids, 50):
            payload = {
                "currentPeriod": {
                    "start": date_filter["date_from"],
                    "end":   date_filter["date_to"],
                },
                "nmIds":      chunk,
                "topOrderBy": "openCard",
                "orderBy":    {"field": "openToCart", "mode": "desc"},
                "limit":      30,
            }

            data, headers = await client.fetch_product_search_texts(payload)
            logger.info(
                f"[fetch_wb_products_search_texts] "
                f"token={token_id} batch={batch_id} chunk_size={len(chunk)}"
            )

            results.append({
                "search_texts": data,
                "_headers":     headers,
            })

    return results


# ─────────────────────────────────────────────────────────────────────────────
# 3) WRITE-опы:
# ─────────────────────────────────────────────────────────────────────────────
def extract_metadata_from_headers(headers: Mapping[str, str]) -> Tuple[Optional[str], datetime]:
    """
    Возвращает (response_uuid, response_dttm), где response_dttm — tz-aware UTC.
    Ожидает стандартный HTTP-заголовок Date (IMF-fixdate), который по спецификации всегда в GMT.
    """
    # 1) нормализуем ключи
    normalized = {str(k).lower(): str(v) for k, v in headers.items()}

    # 2) request id (что найдётся)
    response_uuid = normalized.get("x-request-id") or normalized.get("x-response-id")

    # 3) парсим дату
    raw_date = normalized.get("date")
    if not raw_date:
        raise ValueError("Отсутствует заголовок 'Date' в headers")

    try:
        dt = parsedate_to_datetime(raw_date)  # обычно вернёт aware-UTC
        # На всякий случай гарантируем tz-aware:
        if dt.tzinfo is None or dt.utcoffset() is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except Exception:
        # Fallback на строгий формат "Wed, 13 Aug 2025 12:34:56 GMT"
        try:
            dt = datetime.strptime(raw_date, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=timezone.utc)
        except Exception as e:
            raise ValueError(f"Не удалось распарсить заголовок Date='{raw_date}'") from e

    return response_uuid, dt


def make_json_op(model_class, description, raw_is_list: bool, return_batch: bool = False):
    if raw_is_list:
        ins = {"batch_id": In(str), "raw_data": In(List[DagsterDict])}
        ann = List[Dict[str, Any]]
    else:
        ins = {"batch_id": In(str), "raw_data": In(DagsterDict)}
        ann = Dict[str, Any]

    @op(
        name=f"write_{model_class.__tablename__}",
        ins=ins,
        out=Out(str) if return_batch else Out(),
        required_resource_keys={"postgres", "wildberries_client"},
        description=description
    )
    async def _inner(context, raw_data: ann, batch_id: str) -> str | None:
        logger = get_dagster_logger()
        session_factory = context.resources.postgres
        token_id = context.resources.wildberries_client.token_id

        if isinstance(raw_data, list):
            if not raw_data:
                logger.warning(f"[{model_class.__tablename__}] Пустой список данных, batch_id={batch_id}")
                return batch_id if return_batch else None

            headers = raw_data[0]["_headers"]
        else:
            headers = raw_data.get("_headers")

        if not headers:
            raise ValueError("Не переданы заголовки ответа (_headers)")

        response_uuid, response_dttm = extract_metadata_from_headers(headers)

        if isinstance(raw_data, list):
            clean_data = [{k: v for k, v in d.items() if k != "_headers"} for d in raw_data]
        else:
            clean_data = {k: v for k, v in raw_data.items() if k != "_headers"}

        async with session_factory() as session:
            await save_json_response(session, model_class, clean_data, batch_id, token_id, response_uuid, response_dttm)

        logger.info(f"[{model_class.__tablename__}] JSON записан batch_id={batch_id}")
        return batch_id if return_batch else None

    return _inner


def make_dual_mode_op(model_class, description):
    op_name = f"write_{model_class.__tablename__}"

    @op(
        name=op_name,
        ins={"batch_id": In(str), "raw_data": In(DagsterAny)},
        required_resource_keys={"postgres", "wildberries_client"},
        description=description
    )
    async def _inner(
        context,
        raw_data: Union[Dict[str, Any], Dict[str, Union[bytes, dict]]],
        batch_id: str
    ) -> None:
        logger = get_dagster_logger()
        session_factory = context.resources.postgres
        token_id = context.resources.wildberries_client.token_id

        headers = raw_data.get("_headers")
        if not headers:
            raise ValueError("Не переданы заголовки ответа (_headers)")

        response_uuid, response_dttm = extract_metadata_from_headers(headers)

        async with session_factory() as session:
            content = raw_data.get("content")
            if isinstance(content, (bytes, bytearray)):
                await save_binary_response(
                    session, model_class, content, batch_id,
                    token_id, response_uuid, response_dttm
                )
                logger.info(f"[{model_class.__tablename__}] BINARY записан batch_id={batch_id}")
            else:
                await save_json_response(
                    session, model_class, content, batch_id,
                    token_id, response_uuid, response_dttm
                )
                logger.info(f"[{model_class.__tablename__}] JSON записан batch_id={batch_id}")

    return _inner


write_wb_orders        = make_json_op(
    WBOrders,
    "Записывает wb_orders (JSON) в bronze.wb_orders",
    raw_is_list=False)
write_wb_commission    = make_json_op(
    WBCommission,
  "Записывает wb_commission (JSON) в bronze.wb_commission",
  raw_is_list=False)
write_wb_ad_config     = make_json_op(
    WBAdConfig,
    "Записывает wb_ad_config (JSON) в bronze.wb_ad_config",
    raw_is_list=False)
write_wb_sales_funnel  = make_json_op(
    WBSalesFunnel,
    "Записывает wb_sales_funnel (JSON) в bronze.wb_sales_funnel",
    raw_is_list=False)
write_wb_suppliers     = make_json_op(
    WBSuppliers,
    "Записывает wb_suppliers (JSON) в bronze.wb_suppliers",
    raw_is_list=True)
write_wb_ad_stats      = make_json_op(
    WBAdStats,
    "Записывает wb_ad_stats (JSON) в bronze.wb_ad_stats",
    raw_is_list=True, return_batch=True)
write_wb_ad_info       = make_json_op(
    WBAdInfo,
    "Записывает wb_ad_info (JSON) в bronze.wb_ad_info",
    raw_is_list=True, return_batch=True)
write_wb_clusters      = make_json_op(
    WBClusters,
    "Записывает wb_clusters (JSON) в bronze.wb_clusters",
    raw_is_list=True, return_batch=True)
write_wb_keywords      = make_json_op(
    WBKeywords,
    "Записывает wb_keywords (JSON) в bronze.wb_keywords",
    raw_is_list=True, return_batch=True)
write_wb_sku_api       = make_json_op(
    WBSkuApi,
    "Записывает wb_sku_api (JSON) в bronze.wb_sku_api",
    raw_is_list=True)
write_wb_products_search_texts = make_json_op(
    WbProductsSearchTexts,
    "Записывает wb_products_search_texts (JSON) в bronze.wb_products_search_texts",
    raw_is_list=True,
    return_batch=True
)
write_wb_stocks_report = make_dual_mode_op(
    WBStocksReport,
    "Записывает wb_stocks_report (binary) в bronze.wb_stocks_report"
)
write_wb_paid_storage  = make_dual_mode_op(
    WBPaidStorage,
    "Записывает wb_paid_storage (JSON или binary) в bronze.wb_paid_storage"
)
write_wb_paid_acceptions = make_dual_mode_op(
    WBPaidAcceptions,
    "Записывает wb_paid_acceptions (JSON или binary) в bronze.wb_paid_acceptions"
)
