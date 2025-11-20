import pandas as pd
import logging
from datetime import date
from api_clients.wb import WildberriesAPI


def fetch_keywords_stats(config: dict, advert_ids_by_legal_entity: dict, date_from: date, date_to: date) -> pd.DataFrame:
    all_rows = []

    for legal_entity, advert_ids in advert_ids_by_legal_entity.items():
        client = WildberriesAPI(legal_entity)
        date_from = date_from.strftime("%Y-%m-%d")
        date_to = date_to.strftime("%Y-%m-%d")

        for advert_id in advert_ids:
            try:
                data = client.get_keyword_stats(
                    advert_id=advert_id,
                    date_from=date_from,
                    date_to=date_to
                ).get("keywords", [])
            except Exception as e:
                logging.warning(f"⚠️ Ошибка получения keyword stats для {advert_id} ({legal_entity}): {e}")
                continue

            for keyword_entry in data:
                stats_list = keyword_entry.get("stats") or []
                if not isinstance(stats_list, list):
                    stats_list = [stats_list]

                for stat_item in stats_list:
                    stat_hour = stat_item.get("hour", 0)
                    clicks = stat_item.get("clicks", 0)
                    views = stat_item.get("views", 0)
                    all_rows.append({
                        "date": keyword_entry.get("date"),
                        "hour": stat_hour,
                        "ad_campaign_id": advert_id,
                        "ad_cluster": None,
                        "ad_keyword": stat_item.get("keyword"),
                        "ad_keyword_clicks": clicks,
                        "ad_keyword_views": views,
                        "ad_keyword_ctr": round(clicks / views, 4) if views else 0,
                        "ad_keyword_cost": stat_item.get("sum", 0.0),
                        "ad_keyword_avg_position": stat_item.get("avg_position"),
                        "legal_entity": legal_entity
                    })

    return pd.DataFrame(all_rows)