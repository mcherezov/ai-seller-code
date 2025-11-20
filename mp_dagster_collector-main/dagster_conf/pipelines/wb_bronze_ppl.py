from dagster import job
from dagster_conf.pipelines.wb_bronze_ops import (
    generate_batch_id,
    get_nm_ids_for_token,
    fetch_wb_orders,
    fetch_wb_commission,
    fetch_wb_ad_config,
    fetch_wb_sales_funnel,
    fetch_wb_stocks_report,
    fetch_wb_paid_storage,
    fetch_wb_paid_acceptions,
    fetch_wb_suppliers,
    fetch_wb_advert_ids,
    fetch_wb_ad_stats,
    fetch_wb_ad_info,
    fetch_wb_clusters_batch,
    fetch_wb_keywords_batch,
    fetch_wb_sku_api,
    fetch_wb_products_search_texts,
    write_wb_orders,
    write_wb_commission,
    write_wb_ad_config,
    write_wb_sales_funnel,
    write_wb_stocks_report,
    write_wb_paid_storage,
    write_wb_paid_acceptions,
    write_wb_suppliers,
    write_wb_ad_stats,
    write_wb_ad_info,
    write_wb_clusters,
    write_wb_keywords,
    write_wb_sku_api,
    write_wb_products_search_texts,
)
from dagster_conf.pipelines.wb_silver_ops import (
    normalize_ad_info_bronze_to_silver,
    normalize_ad_stats_bronze_to_silver,
    normalize_wb_product_search_texts_bronze_to_silver,
    normalize_ad_keyword_clusters_dict,
    normalize_ad_keywords_dict,
    normalize_wb_clusters_bronze_to_silver,
    normalize_wb_keywords_bronze_to_silver,
)
from dagster_conf.pipelines.utils import (
    today_date,
    make_today_filter_dict,
    make_stocks_filters,
    make_ad_info_params,
    make_sales_funnel_filter_dict,
    make_ad_info_payload,
    make_ad_stats_payload,
)
from dagster_conf.resources.wb_client import wildberries_client
from dagster_conf.resources.pg_resource import postgres_resource


@job(
    resource_defs={
        "wildberries_client": wildberries_client,
        "postgres": postgres_resource,
    }
)
def wb_full_job():
    # 1) Генерируем batch_id
    batch = generate_batch_id()

    # 2) Даты/фильтры
    today          = today_date()
    date_filters   = make_today_filter_dict(today)
    stocks_filters = make_stocks_filters()
    funnel_filters = make_sales_funnel_filter_dict(today)
    ad_info_params = make_ad_info_params()

    # ─── FETCH ──────────────────────────────────────────────────────────────
    orders_raw         = fetch_wb_orders(batch, today, today)
    commission_raw     = fetch_wb_commission(batch)
    ad_config_raw      = fetch_wb_ad_config(batch)
    sales_funnel_raw   = fetch_wb_sales_funnel(batch, filters=funnel_filters)
    stocks_raw         = fetch_wb_stocks_report(batch, filters=stocks_filters)
    paid_storage_raw   = fetch_wb_paid_storage(batch, today, today)
    paid_acceptions_raw= fetch_wb_paid_acceptions(batch, today, today)
    suppliers_raw      = fetch_wb_suppliers(batch, today, today)
    advert_ids         = fetch_wb_advert_ids(batch, today, today)

    ad_stats_payload = make_ad_stats_payload(advert_ids, date_filters)
    ad_info_payload  = make_ad_info_payload(advert_ids)

    ad_stats_raw   = fetch_wb_ad_stats(batch, ad_stats_payload)
    ad_info_raw    = fetch_wb_ad_info(batch, ad_info_payload)
    clusters_raw   = fetch_wb_clusters_batch(batch, advert_ids)
    keywords_raw   = fetch_wb_keywords_batch(batch, advert_ids, today, today)
    sku_api_raw    = fetch_wb_sku_api(batch)

    nm_ids         = get_nm_ids_for_token()
    raw_search_list= fetch_wb_products_search_texts(
        batch_id    = batch,
        nm_ids      = nm_ids,
        date_filter = date_filters,
    )

    # ─── WRITE (bronze) ────────────────────────────────────────────────────
    write_wb_orders(orders_raw, batch)
    write_wb_commission(commission_raw, batch)
    write_wb_ad_config(ad_config_raw, batch)
    write_wb_sales_funnel(sales_funnel_raw, batch)
    write_wb_stocks_report(stocks_raw, batch)
    write_wb_paid_storage(paid_storage_raw, batch)
    write_wb_paid_acceptions(paid_acceptions_raw, batch)
    write_wb_suppliers(suppliers_raw, batch)

    ad_stats_batch    = write_wb_ad_stats(ad_stats_raw, batch)
    ad_info_batch     = write_wb_ad_info(ad_info_raw, batch)
    clusters_batch    = write_wb_clusters(clusters_raw, batch)
    keywords_batch    = write_wb_keywords(keywords_raw, batch)
    write_wb_sku_api(sku_api_raw, batch)
    prod_search_batch = write_wb_products_search_texts(raw_search_list, batch)

    # ─── TRANSFORM → INSERT (silver) ─────────────────────────────────────────
    clusters_dict_batch = normalize_ad_keyword_clusters_dict(clusters_batch)
    keywords_dict_batch = normalize_ad_keywords_dict(clusters_batch)
    # normalize_wb_clusters_bronze_to_silver(
    #     batch_id=clusters_batch,
    #     dict_batch=clusters_dict_batch,
    # )
    # normalize_wb_keywords_bronze_to_silver(
    #     batch_id=keywords_batch,
    #     dict_batch=keywords_dict_batch,
    # )
    # normalize_ad_stats_bronze_to_silver(ad_stats_batch)
    normalize_ad_info_bronze_to_silver(ad_info_batch)
    normalize_wb_product_search_texts_bronze_to_silver(prod_search_batch)
