import json
from json import JSONDecodeError
from datetime import date
from typing import List, Tuple, Dict, Any
from uuid import UUID
from datetime import datetime


def transform_type6_ad_info(
    entry: dict, response_uuid: str, response_dttm: datetime
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Преобразует запись с type=6 (params[].nms[].nm) в:
      - ad_campaigns_1d (основная информация)
      - ad_campaigns_products_1d (одна строка на товар)
    """
    campaign = {
        "response_uuid": response_uuid,
        "response_dttm": response_dttm,
        "campaign_id": entry["advertId"],
        "name": entry.get("name"),
        "create_time": entry.get("createTime"),
        "change_time": entry.get("changeTime"),
        "start_time": entry.get("startTime"),
        "end_time": entry.get("endTime"),
        "status": entry.get("status"),
        "type": entry.get("type"),
        "payment_type": entry.get("paymentType"),
        "daily_budget": entry.get("dailyBudget"),
        "search_pluse_state": entry.get("searchPluseState"),
    }

    products = []
    for param in entry.get("params", []):
        subject_id = param.get("subjectId")
        subject_name = param.get("subjectName")
        subject_active = param.get("active")

        for nm_item in param.get("nms", []):
            products.append({
                "response_uuid": response_uuid,
                "response_dttm": response_dttm,
                "campaign_id": entry["advertId"],
                "product_id": nm_item.get("nm"),
                "product_is_active": nm_item.get("active"),
                "subject_cpm_current": param.get("price"),
                "subject_id": subject_id,
                "subject_name": subject_name,
                "subject_is_active": subject_active,
            })

    return campaign, products


def transform_type8_ad_info(
    entry: dict, response_uuid: str, response_dttm: datetime
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Преобразует запись с type=8 (autoParams.nms[], autoParams.nmCPM) в:
      - ad_campaigns_1d
      - ad_campaigns_auto_1d
    """
    campaign = {
        "response_uuid": response_uuid,
        "response_dttm": response_dttm,
        "campaign_id": entry["advertId"],
        "name": entry.get("name"),
        "create_time": entry.get("createTime"),
        "change_time": entry.get("changeTime"),
        "start_time": entry.get("startTime"),
        "end_time": entry.get("endTime"),
        "status": entry.get("status"),
        "type": entry.get("type"),
        "payment_type": entry.get("paymentType"),
        "daily_budget": entry.get("dailyBudget"),
        "search_pluse_state": entry.get("searchPluseState"),
    }

    products = []
    nms = entry.get("autoParams", {}).get("nms", [])
    nm_cpm_map = {
        item["nm"]: item.get("cpm")
        for item in entry.get("autoParams", {}).get("nmCPM", []) or []
        if isinstance(item, dict)
    }

    subject = entry.get("autoParams", {}).get("subject", {})
    active = entry.get("autoParams", {}).get("active", {})

    for nm in nms:
        products.append({
            "response_uuid": response_uuid,
            "response_dttm": response_dttm,
            "campaign_id": entry["advertId"],
            "product_id": nm,
            "subject_id": subject.get("id"),
            "subject_name": subject.get("name"),
            "cpm": nm_cpm_map.get(nm),
            "is_carousel": active.get("carousel"),
            "is_recom": active.get("recom"),
            "is_booster": active.get("booster"),
        })

    return campaign, products


def transform_type9_ad_info(
    entry: dict, response_uuid: str, response_dttm: datetime
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Преобразует запись с type=9 (unitedParams[].nms[], searchCPM, catalogCPM) в:
      - ad_campaigns_1d
      - ad_campaigns_united_1d
    """
    campaign = {
        "response_uuid": response_uuid,
        "response_dttm": response_dttm,
        "campaign_id": entry["advertId"],
        "name": entry.get("name"),
        "create_time": entry.get("createTime"),
        "change_time": entry.get("changeTime"),
        "start_time": entry.get("startTime"),
        "end_time": entry.get("endTime"),
        "status": entry.get("status"),
        "type": entry.get("type"),
        "payment_type": entry.get("paymentType"),
        "daily_budget": entry.get("dailyBudget"),
        "search_pluse_state": entry.get("searchPluseState"),
    }

    products = []
    for param in entry.get("unitedParams", []):
        subject = param.get("subject", {})
        for nm in param.get("nms", []):
            products.append({
                "response_uuid": response_uuid,
                "response_dttm": response_dttm,
                "campaign_id": entry["advertId"],
                "product_id": nm,
                "subject_id": subject.get("id"),
                "subject_name": subject.get("name"),
                "search_cpm": param.get("searchCPM"),
                "catalog_cpm": param.get("catalogCPM"),
            })

    return campaign, products


def transform_ad_stats(entry: Dict[str, Any]) -> Tuple[
    List[Dict[str, Any]],  # products stats
    List[Dict[str, Any]],  # positions
    List[Dict[str, Any]]   # distinct products
]:
    campaign_id = entry["advertId"]
    stats_rows = []
    pos_rows = []
    products = set()

    # 1) основная статистика по товарам
    for day in entry.get("days", []):
        date = datetime.fromisoformat(day["date"]).date()
        for app in day.get("apps", []):
            platform = app.get("appType")
            for nm in app.get("nm", []):
                pid = nm.get("nmId")
                products.add(pid)
                stats_rows.append({
                    "campaign_id": campaign_id,
                    "date": date,
                    "platform_id": platform,
                    "product_id": pid,
                    "views": nm.get("views"),
                    "clicks": nm.get("clicks"),
                    "ctr": nm.get("ctr"),
                    "cpc": nm.get("cpc"),
                    "cost": nm.get("sum"),
                    "carts": nm.get("atbs"),
                    "orders": nm.get("orders"),
                    "cr": nm.get("cr"),
                    "items": nm.get("shks"),
                    "revenue": nm.get("sum_price"),
                })

    # 2) позиции из boosterStats
    for bs in entry.get("boosterStats", []):
        pid = bs.get("nm")
        products.add(pid)
        pos_rows.append({
            "campaign_id": campaign_id,
            "product_id": pid,
            "date": datetime.fromisoformat(bs["date"].replace("Z", "+00:00")).date(),
            "avg_position": bs.get("avg_position"),
        })

    # 3) уникальные продукты
    prod_rows = [{"product_id": pid} for pid in products]

    return stats_rows, pos_rows, prod_rows


def transform_wb_product_search_texts(
    entry: Dict[str, Any],
    response_uuid: UUID,
    response_dttm: datetime
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Преобразует одну запись из bronze.wb_products_search_texts в два набора:
      - silver.wb_products
      - silver.wb_product_search_terms
    """
    # округляем dttm до часа
    dttm = response_dttm.replace(minute=0, second=0, microsecond=0)

    products: List[Dict[str, Any]] = []
    terms:    List[Dict[str, Any]] = []

    # из JSON-строки берём именно список items
    items = entry.get("search_texts", {}) \
                 .get("data", {}) \
                 .get("items", [])

    for item in items:
        product_id = item.get("nmId")

        # общий набор колонок для обеих таблиц
        common = {
            "api_token_id":  entry.get("api_token_id"),
            "response_uuid": str(response_uuid),
            "time_grain":    "hour",
            "dttm":          dttm,
            "product_id":    product_id,
        }

        # --- silver.wb_products ---
        products.append({
            **common,
            "subject_name":    item.get("subjectName"),
            "brand_name":      item.get("brandName"),
            "vendor_code":     item.get("vendorCode"),
            "name":            item.get("name"),
            "is_rated":        item.get("isCardRated"),
            "rating":          item.get("rating"),
            "feedback_rating": item.get("feedbackRating"),
        })

        # --- silver.wb_product_search_terms ---
        terms.append({
            **common,
            "search_term":     item.get("text"),
            "price_min":       item.get("price", {}).get("minPrice"),
            "price_max":       item.get("price", {}).get("maxPrice"),
            "week_frequency":  item.get("weekFrequency"),
            "frequency":       item.get("frequency", {}).get("current"),
            "median_position": item.get("medianPosition", {}).get("current"),
            "avg_position":    item.get("avgPosition", {}).get("current"),
            "open_card":       item.get("openCard", {}).get("current"),
            "add_to_cart":     item.get("addToCart", {}).get("current"),
            "open_to_cart":    item.get("openToCart", {}).get("current"),
            "orders":          item.get("orders", {}).get("current"),
            "cart_to_order":   item.get("cartToOrder", {}).get("current"),
            "visibility":      item.get("visibility", {}).get("current"),
        })

    return products, terms


def extract_clusters_dict(
    bronze_rows: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Преобразует bronze.wb_clusters → silver.ad_keyword_clusters
    """
    clusters = set()
    for row in bronze_rows:
        payload = json.loads(row["content"])
        for cluster in payload.get("clusters", []):
            clusters.add(cluster["cluster"])
    return [{"keyword_cluster": c} for c in clusters]


def extract_keywords_dict(
    bronze_rows: List[Dict[str, Any]]
) -> List[Dict[str, str]]:
    """
    Преобразует список записей bronze.wb_clusters в список уникальных
    слов (keyword) и их кластеров (keyword_cluster) для загрузки в silver.ad_keywords.
    Отбрасывает записи с некорректным JSON или без списка clusters.
    """
    out: List[Dict[str, str]] = []
    for row in bronze_rows:
        content = row.get("content")
        if not content:
            continue
        try:
            payload = json.loads(content)
        except JSONDecodeError:
            continue

        for cluster in payload.get("clusters", []):
            cluster_name = cluster.get("cluster")
            if not isinstance(cluster_name, str):
                continue
            for kw in cluster.get("keywords", []):
                if not isinstance(kw, str):
                    continue
                out.append({
                    "keyword": kw,
                    "keyword_cluster": cluster_name,
                })

    # дедупликация по (keyword, keyword_cluster)
    unique = {
        (r["keyword"], r["keyword_cluster"]): r
        for r in out
    }
    return list(unique.values())


def transform_stats_keywords_1d(
    bronze_rows: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Преобразует данные из bronze.wb_keywords → silver.wb_ad_stats_keywords_1d
    """
    out: List[Dict[str, Any]] = []
    for row in bronze_rows:
        payload = json.loads(row["content"])
        campaign_id = payload["advertId"]
        for kw_block in payload.get("keywords", []):
            dt = date.fromisoformat(kw_block["date"])
            for stat in kw_block.get("stats", []):
                out.append({
                    "last_response_uuid": UUID(row["response_uuid"]),
                    "last_response_dttm": row["response_dttm"],
                    "campaign_id": campaign_id,
                    "date": dt,
                    "keyword": stat["keyword"],
                    "views": stat["views"],
                    "clicks": stat["clicks"],
                    "ctr": stat["ctr"],
                    "cost": stat["sum"],
                })
    return out


def transform_keyword_clusters_1d(
    bronze_rows: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Преобразует данные из bronze.wb_auto_stat_words → silver.wb_ad_keyword_clusters_1d
    """
    out: List[Dict[str, Any]] = []
    for row in bronze_rows:
        payload = json.loads(row["content"])
        campaign_id = payload["advertId"]
        # дата берётся из response_dttm
        dt = row["response_dttm"].date()
        excluded_set = set(payload.get("excluded") or [])
        for cluster_block in payload.get("clusters", []):
            cluster_name = cluster_block["cluster"]
            views = cluster_block["count"]
            for kw in cluster_block.get("keywords", []):
                out.append({
                    "last_response_uuid": UUID(row["response_uuid"]),
                    "last_response_dttm": row["response_dttm"],
                    "campaign_id": campaign_id,
                    "date": dt,
                    "keyword_cluster": cluster_name,
                    "cluster_views": views,
                    "is_excluded": kw in excluded_set,
                })
    return out
