import pandas as pd
from datetime import date
from api_clients.wb import WildberriesAPI


def fetch_cluster_stats(
    wb_marketplace_keys: dict,
    advert_ids_by_legal_entity: dict[str, list[int]],
    report_date: date
) -> pd.DataFrame:
    """
    Собирает агрегированную статистику по кластерам и ключевым словам
    для рекламных кампаний Wildberries за одну дату (report_date).

    Args:
        wb_marketplace_keys: словарь {seller_legal: api_key}
        advert_ids_by_legal_entity: словарь {seller_legal: [ad_id, ...]}
        report_date: дата, за которую собираем статистику

    Returns:
        DataFrame с колонками: ad_id, date, cluster_name, views, clicks, ctr, sum
    """
    date_str = report_date.strftime("%Y-%m-%d")
    records = []

    for seller_legal, ad_ids in advert_ids_by_legal_entity.items():
        api_key = wb_marketplace_keys.get(seller_legal)
        if not api_key or not ad_ids:
            continue

        wb = WildberriesAPI(seller_legal)

        # Получаем кластеры по первому ad_id
        first_ad_id = ad_ids[0]
        cluster_resp = wb.get_clusters_by_ad_id(first_ad_id).get("data", {})
        clusters = cluster_resp.get("clusters", [])

        # Сопоставление ключевых слов с кластерами
        keyword_to_cluster: dict[str, str] = {}
        for c in clusters:
            cluster_name = c.get("cluster") or "UNKNOWN"
            for kw in c.get("keywords", []):
                keyword_to_cluster[kw] = cluster_name

        # Проходим по каждому ad_id и собираем статистику ключевых слов
        for ad_id in ad_ids:
            key_resp = wb.get_keyword_stats_by_date(ad_id, date_str, date_str)
            keywords_list = []
            if isinstance(key_resp, dict):
                keywords_list = key_resp.get("keywords", [])
            elif isinstance(key_resp, list):
                keywords_list = key_resp

            for keyword_chunk in keywords_list:
                if not isinstance(keyword_chunk, dict):
                    continue
                stats_list = keyword_chunk.get("stats", [])
                for stats in stats_list:
                    kw_text = stats.get("keyword", "")
                    views = stats.get("views", 0)
                    clicks = stats.get("clicks", 0)
                    cost = stats.get("sum", 0.0)

                    cluster_name = keyword_to_cluster.get(kw_text, "UNKNOWN")
                    ctr = clicks / views if views else 0.0

                    records.append({
                        "ad_id": ad_id,
                        "date": report_date,
                        "cluster_name": cluster_name,
                        "views": views,
                        "clicks": clicks,
                        "ctr": ctr,
                        "sum": cost,
                    })

    # Формируем итоговый DataFrame
    if records:
        df = pd.DataFrame(records)
        grouped = (
            df.groupby(["ad_id", "date", "cluster_name"], as_index=False)
              .agg({
                  "views": "sum",
                  "clicks": "sum",
                  "sum": "sum"
              })
        )
        # Пересчитываем ctr на уровне групп
        grouped["ctr"] = grouped.apply(
            lambda row: row["clicks"] / row["views"] if row["views"] else 0.0,
            axis=1
        )
        return grouped

    # Пустой DataFrame с нужными колонками
    columns = ["ad_id", "date", "cluster_name", "views", "clicks", "ctr", "sum"]
    return pd.DataFrame([], columns=columns)
