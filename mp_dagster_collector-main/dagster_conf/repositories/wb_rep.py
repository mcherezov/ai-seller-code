from dagster import repository
from dagster_conf.pipelines.promotions_assets import bronze_wb_adv_promotions_1h, silver_adv_promotions_1h
from dagster_conf.pipelines.fullstats_assets import bronze_wb_adv_fullstats, silver_wb_adv_fullstats
from dagster_conf.pipelines.stats_keywords_assets import bronze_wb_adv_stats_keywords, silver_wb_adv_keyword_stats
from dagster_conf.pipelines.auto_stat_words_assets import bronze_wb_adv_auto_stat_words_1d, silver_wb_adv_keyword_clusters_1d
from dagster_conf.pipelines.promotion_adverts_assets import bronze_wb_adv_promotion_adverts, silver_wb_adv_campaigns, silver_wb_adv_product_rates
from dagster_conf.pipelines.supplier_orders_assets import bronze_wb_supplier_orders_1d, silver_order_items_1d
from dagster_conf.pipelines.tariffs_commission_assets import bronze_wb_commission_1d, silver_wb_commission_1d
from dagster_conf.pipelines.warehouse_remains_assets import bronze_wb_stocks_report_1d, silver_wb_stocks_1d
# from dagster_conf.pipelines.paid_storage_assets import bronze_wb_paid_storage_1d, silver_paid_storage_1d

# from dagster_conf.pipelines.paid_storage.paid_storage_partition_assets import bronze_wb_paid_storage_1d_new, silver_wb_paid_storage_1d_new
# from dagster_conf.pipelines.paid_storage.paid_storage_partition_job import (
#     paid_storage_1d_job_v2,
#     paid_storage_daily,
#     PIPE as PAID_PIPE,
# )
# from dagster_conf.pipelines.sync_company_partitions import (
#     sync_companies_job,
#     make_sync_companies_schedule,
# )

from dagster_conf.pipelines.paid_acceptance_assets import bronze_wb_paid_acceptance_1d, silver_wb_paid_acceptances_1d
from dagster_conf.pipelines.nm_report_detail_assets import bronze_wb_nm_report_detail_1d, silver_wb_buyouts_percent_1d, silver_wb_sales_funnels_1d
from dagster_conf.pipelines.supplier_incomes_assets import bronze_wb_supplier_incomes_1d, silver_wb_supplies_1d
from dagster_conf.pipelines.cards_list_assets import bronze_wb_cards_list_1d, silver_wb_mp_skus_1d
from dagster_conf.pipelines.www_text_search_assets import bronze_wb_www_text_search_1d, silver_wb_www_text_search_1d
from dagster_conf.pipelines.wb_search_results_6h_assets import bronze_wb_search_results_6h, silver_wb_search_results_6h
from dagster_conf.pipelines.wb_bronze_ppl import wb_full_job
from dagster_conf.pipelines.promotions_job import wb_adv_promotions_1h_job
from dagster_conf.pipelines.fullstats_jobs import wb_adv_fullstats_1d_job, wb_adv_fullstats_1h_job
from dagster_conf.pipelines.stats_keywords_jobs import wb_adv_stats_keywords_1d_job, wb_adv_stats_keywords_1h_job
from dagster_conf.pipelines.auto_stat_words_jobs import wb_adv_auto_stat_words_1d_job
from dagster_conf.pipelines.promotion_adverts_job import wb_adv_promotion_adverts_1d_job, wb_adv_promotion_adverts_1h_job
from dagster_conf.pipelines.supplier_orders_job import wb_supplier_orders_1d_job
from dagster_conf.pipelines.tariffs_commission_job import wb_tariffs_commission_1d_job
from dagster_conf.pipelines.wb_stocks_1d_job import wb_stocks_1d_job
# from dagster_conf.pipelines.paid_storage_job import wb_paid_storage_1d_job
from dagster_conf.pipelines.paid_acceptance_job import wb_acceptance_report_1d_job
from dagster_conf.pipelines.nm_report_detail_job import wb_nm_report_detail_1d_job
from dagster_conf.pipelines.supplier_incomes_job import wb_supplier_incomes_1d_job
from dagster_conf.pipelines.cards_list_job import wb_cards_list_1d_job
from dagster_conf.pipelines.ako_V2_job import ako_V2_wrapper_job
from dagster_conf.pipelines.ucb_bandit_job import ucb_bandit_wrapper_job
from dagster_conf.pipelines.llm_wrapper_job import llm_wrapper_job
from dagster_conf.pipelines.www_text_search_job import wb_text_search_1d_job
from dagster_conf.pipelines.wb_search_results_6h_job import wb_search_results_6h_job
from dagster_conf.pipelines.www_fin_reports_job import wb_www_fin_report_job
from dagster_conf.pipelines.www_cashback_reports_job import wb_www_cashback_job
from dagster_conf.sensor_schedulers.multi_key_sensor import wb_multi_key_sensor
from dagster_conf.sensor_schedulers.promotions_sensor import wb_adv_promotions_hourly_sensor
from dagster_conf.sensor_schedulers.fullstats_sensor import bronze_fullstats_daily_sensor, bronze_fullstats_hourly_sensor
from dagster_conf.sensor_schedulers.stats_keywords_sensor import bronze_stats_keywords_daily_sensor, bronze_stats_keywords_hourly_sensor
from dagster_conf.sensor_schedulers.auto_stat_words_sensor import bronze_auto_stat_words_daily_sensor
from dagster_conf.sensor_schedulers.promotion_adverts_sensor import bronze_wb_adv_promotion_adverts_daily_sensor, bronze_wb_adv_promotion_adverts_hourly_sensor
from dagster_conf.sensor_schedulers.supplier_orders_sensor import bronze_supplier_orders_daily_sensor
from dagster_conf.sensor_schedulers.tariffs_commission_schedule import daily_tariffs_commission_schedule
from dagster_conf.sensor_schedulers.wb_stocks_1d_sensor import bronze_wb_stocks_report_daily_sensor
# from dagster_conf.sensor_schedulers.paid_storage_sensor import bronze_wb_paid_storage_daily_sensor
from dagster_conf.sensor_schedulers.paid_acceptance_sensor import bronze_wb_paid_acceptance_daily_sensor
from dagster_conf.sensor_schedulers.nm_report_detail_sensor import bronze_wb_nm_report_detail_daily_sensor
from dagster_conf.sensor_schedulers.supplier_incomes_sensor import bronze_wb_supplier_incomes_daily_sensor
from dagster_conf.sensor_schedulers.cards_list_sensor import bronze_wb_cards_list_daily_sensor
from dagster_conf.sensor_schedulers.ako_V2_schedule import ako_V2_wrapper_schedule
from dagster_conf.sensor_schedulers.ucb_bandit_schedule import ucb_bandit_daily_schedule
from dagster_conf.sensor_schedulers.llm_schedule_sensor import llm_wrapper_schedule
from dagster_conf.sensor_schedulers.trigger_adv_jobs import trigger_wrapper_jobs_sensor
from dagster_conf.sensor_schedulers.trigger_hourly_adv_jobs import trigger_hourly_adv_jobs_after_promotions
from dagster_conf.sensor_schedulers.www_fin_reports_schedule import wb_www_fin_report_schedule
from dagster_conf.sensor_schedulers.www_cashback_reports_schedule import wb_www_cashback_schedule
from dagster_conf.sensor_schedulers.www_text_search_schedule import wb_text_search_1d_schedule
from dagster_conf.sensor_schedulers.wb_search_results_6h_schedule import wb_search_results_6h_schedule


@repository
def wb_repository():
    """
    Репозиторий Wildberries.
    Содержит:
      • Сами jobs
      • schedules
      • sensors
    """
    return [
        # Активы (assets)
        bronze_wb_adv_promotions_1h,
        silver_adv_promotions_1h,
        bronze_wb_adv_fullstats,
        silver_wb_adv_fullstats,
        bronze_wb_adv_stats_keywords,
        silver_wb_adv_keyword_stats,
        bronze_wb_adv_auto_stat_words_1d,
        silver_wb_adv_keyword_clusters_1d,
        bronze_wb_adv_promotion_adverts,
        silver_wb_adv_campaigns,
        silver_wb_adv_product_rates,
        bronze_wb_supplier_orders_1d,
        silver_order_items_1d,
        bronze_wb_commission_1d,
        silver_wb_commission_1d,
        bronze_wb_stocks_report_1d,
        silver_wb_stocks_1d,
        # bronze_wb_paid_storage_1d,
        # silver_paid_storage_1d,
        bronze_wb_paid_acceptance_1d,
        silver_wb_paid_acceptances_1d,
        bronze_wb_nm_report_detail_1d,
        silver_wb_buyouts_percent_1d,
        silver_wb_sales_funnels_1d,
        bronze_wb_supplier_incomes_1d,
        silver_wb_supplies_1d,
        bronze_wb_cards_list_1d,
        silver_wb_mp_skus_1d,
        bronze_wb_www_text_search_1d,
        silver_wb_www_text_search_1d,
        bronze_wb_search_results_6h,
        silver_wb_search_results_6h,



        # bronze_wb_paid_storage_1d_new,
        # silver_wb_paid_storage_1d_new,
        # sync_companies_job,
        # make_sync_companies_schedule(PAID_PIPE),


        # Job’ы
        wb_full_job,
        wb_adv_promotions_1h_job,
        wb_adv_fullstats_1d_job,
        wb_adv_fullstats_1h_job,
        wb_adv_stats_keywords_1d_job,
        wb_adv_stats_keywords_1h_job,
        wb_adv_auto_stat_words_1d_job,
        wb_adv_promotion_adverts_1d_job,
        wb_adv_promotion_adverts_1h_job,
        wb_supplier_orders_1d_job,
        wb_tariffs_commission_1d_job,
        wb_stocks_1d_job,
        # wb_paid_storage_1d_job,
        wb_acceptance_report_1d_job,
        wb_nm_report_detail_1d_job,
        wb_supplier_incomes_1d_job,
        wb_cards_list_1d_job,
        ako_V2_wrapper_job,
        ucb_bandit_wrapper_job,
        llm_wrapper_job,
        wb_text_search_1d_job,
        wb_www_fin_report_job,
        wb_www_cashback_job,
        wb_search_results_6h_job,

        # paid_storage_1d_job_v2,

        # Сенсоры
        wb_multi_key_sensor,
        wb_adv_promotions_hourly_sensor,
        bronze_fullstats_daily_sensor,
        bronze_fullstats_hourly_sensor,
        bronze_stats_keywords_daily_sensor,
        bronze_stats_keywords_hourly_sensor,
        bronze_auto_stat_words_daily_sensor,
        bronze_wb_adv_promotion_adverts_daily_sensor,
        bronze_wb_adv_promotion_adverts_hourly_sensor,
        bronze_supplier_orders_daily_sensor,
        daily_tariffs_commission_schedule,
        bronze_wb_stocks_report_daily_sensor,
        # bronze_wb_paid_storage_daily_sensor,
        bronze_wb_paid_acceptance_daily_sensor,
        bronze_wb_nm_report_detail_daily_sensor,
        bronze_wb_supplier_incomes_daily_sensor,
        bronze_wb_cards_list_daily_sensor,
        ako_V2_wrapper_schedule,
        ucb_bandit_daily_schedule,
        llm_wrapper_schedule,
        trigger_wrapper_jobs_sensor,
        trigger_hourly_adv_jobs_after_promotions,
        wb_www_fin_report_schedule,
        wb_www_cashback_schedule,
        wb_text_search_1d_schedule,
        wb_search_results_6h_schedule,

        # paid_storage_daily,
    ]
