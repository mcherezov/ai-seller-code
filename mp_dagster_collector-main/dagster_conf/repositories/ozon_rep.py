from dagster import repository
from dagster_conf.pipelines.ozon_pipelines.analytics_data.analytics_data_assets import bronze_ozon_analytics_data_1d
from dagster_conf.pipelines.ozon_pipelines.postings_create.postings_create_assets import bronze_ozon_report_postings_file_1d
from dagster_conf.pipelines.ozon_pipelines.statistics_expense.statistics_expense_assets import bronze_ozon_adv_expense_1d
from dagster_conf.pipelines.ozon_pipelines.product_info_prices.product_info_prices_assets import bronze_ozon_product_info_prices_1d
from dagster_conf.pipelines.ozon_pipelines.finance_transaction_list.finance_transaction_list_assets import bronze_ozon_finance_transaction_list_1d
from dagster_conf.pipelines.ozon_pipelines.analytics_data.analytics_data_job import ozon_analytics_data_1d_job
from dagster_conf.pipelines.ozon_pipelines.postings_create.postings_create_job import ozon_report_postings_file_1d_job
from dagster_conf.pipelines.ozon_pipelines.statistics_expense.statistic_expense_job import ozon_adv_expense_1d_job
from dagster_conf.pipelines.ozon_pipelines.product_info_prices.product_info_prices_job import ozon_product_info_prices_1d_job
from dagster_conf.pipelines.ozon_pipelines.finance_transaction_list.finance_transaction_list_job import ozon_finance_transaction_list_1d_job


@repository
def ozon_repository():
    """
    Репозиторий Ozon.
    Содержит:
      • Сами jobs
      • schedules
      • sensors
    """
    return [
        # Assets
        bronze_ozon_analytics_data_1d,
        bronze_ozon_report_postings_file_1d,
        bronze_ozon_adv_expense_1d,
        bronze_ozon_product_info_prices_1d,
        bronze_ozon_finance_transaction_list_1d,

        # Jobs
        ozon_analytics_data_1d_job,
        ozon_report_postings_file_1d_job,
        ozon_adv_expense_1d_job,
        ozon_product_info_prices_1d_job,
        ozon_finance_transaction_list_1d_job,
        ]


