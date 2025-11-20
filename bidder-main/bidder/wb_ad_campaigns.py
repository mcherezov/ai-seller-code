from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from statsmodels.stats.proportion import proportion_confint

from . import daily_orders
from . import utils


@dataclass
class ProcessedWbAdCampaigns:
    dt: datetime
    df: pd.DataFrame
    margin_wo_other_expenses: pd.DataFrame
    advert_hour_margin_wo_other_expenses: pd.DataFrame
    hour_margin_wo_other_expenses: pd.DataFrame
    overall_avg_margin_wo_other_expenses: pd.DataFrame
    advert_hour_cpm_to_stat: pd.DataFrame


def calc_margin_wo_other_expenses(processed_wb_ad_campaigns_df: pd.DataFrame,
                                  processed_daily_orders_df: pd.DataFrame) -> pd.DataFrame:
    cost_without_ads = processed_daily_orders_df[['date', 'legal_entity', 'product_id', 'cost_price_per_unit', 'cost_price_with_sales_fee_per_unit',
                                                  'avg_receipt']]
    df = pd.merge(processed_wb_ad_campaigns_df, cost_without_ads, on=['date', 'legal_entity', 'product_id'], how='left')

    df['margin_wo_other_expenses'] = (df['avg_receipt'] - df['cost_price_with_sales_fee_per_unit'] - df['hour_sum']) / df['avg_receipt']
    df.loc[df['hour_orders'] == 0, 'margin_wo_other_expenses'] = 0
    return df


def process_wb_ad_campaigns(dt: datetime,
                            wb_ad_campaign: pd.DataFrame,
                            sku: pd.DataFrame,
                            processed_daily_orders: daily_orders.ProcessedDailyOrders) -> ProcessedWbAdCampaigns:
    sku = sku[['legal_entity', 'product_id', 'mp_id', 'category']]
    sku = sku[sku['mp_id'] != -1].dropna()

    df = wb_ad_campaign[wb_ad_campaign['hour'] > -1].copy()

    # add product_id, category, datetime fields and sort by (date, hour)
    df['mp_id'] = df['nm_id'].astype('float64')
    df = pd.merge(df, sku, on=['mp_id'], suffixes=['_wb_ad_campaigns', ''], how='left')
    df.sort_values(['date', 'hour', 'nm_id', 'advert_id', 'app_type'], inplace=True, ignore_index=True)
    df['dt'] = utils.date_hour_to_dt(df)
    utils.fill_timefields(df)

    # from cumulative fields to stat in hour
    group = df.groupby(['date', 'nm_id', 'advert_id', 'app_type'])
    cum_cols = ['views', 'clicks', 'atbs', 'orders', 'sum']

    for col in cum_cols:
        df[f'hour_{col}'] = df[col] - group[col].shift(1)
        df[f'hour_{col}'] = df[f'hour_{col}'].where(~df[f'hour_{col}'].isna(), df[col])

    df.drop(columns=cum_cols, inplace=True)

    # aggregate stat on all platforms
    df = df.groupby(['dt', 'nm_id', 'advert_id', 'legal_entity', 'product_id'], as_index=False).agg(
        {
            'mp_id': 'first',
            'category': 'first',
            'week_day': 'first',
            'week_start': 'first',
            'date': 'first',
            'hour_of_week': 'first',
            'hour': 'first',

            'hour_views': 'sum',
            'hour_clicks': 'sum',
            'hour_orders': 'sum',
            'hour_atbs': 'sum',
            'hour_sum': 'sum'
        }
    )

    # add cpm, conversion fields
    df['cpm_1'] = utils.div_non_zero(df['hour_sum'], df['hour_views'])
    df['cpm_1_round'] = df['cpm_1'].round(2)
    df['cr_view'] = utils.div_non_zero(df['hour_orders'], df['hour_views'])
    df['ctr'] = utils.div_non_zero(df['hour_clicks'], df['hour_views'])
    df['cr_click'] = utils.div_non_zero(df['hour_orders'], df['hour_clicks'])

    # TODO: Add actual bid and actual price for hour

    # calculate margin df
    margin_wo_other_expenses = calc_margin_wo_other_expenses(df, processed_daily_orders.df)
    advert_hour_margin_wo_other_expenses = margin_wo_other_expenses.groupby(
        ['nm_id', 'advert_id', 'hour_of_week'], as_index=False)['margin_wo_other_expenses'].agg(['mean', 'max'])
    hour_margin_wo_other_expenses = margin_wo_other_expenses.groupby(['hour_of_week'], as_index=False)['margin_wo_other_expenses'].agg(['mean', 'max'])
    overall_avg_margin_wo_other_expenses = margin_wo_other_expenses['margin_wo_other_expenses'].mean()

    # history bids stat
    advert_hour_cpm_to_stat = margin_wo_other_expenses.groupby(['nm_id', 'advert_id', 'hour_of_week', 'cpm_1_round'], as_index=False).agg(
        {
            'hour_views': 'sum',
            'hour_clicks': 'sum',
            'hour_orders': 'sum',
            'hour_atbs': 'sum',
            'margin_wo_other_expenses': 'mean'
        }
    )

    advert_hour_cpm_to_stat = advert_hour_cpm_to_stat[advert_hour_cpm_to_stat['hour_views'] > 0]
    advert_hour_cpm_to_stat = advert_hour_cpm_to_stat[advert_hour_cpm_to_stat['hour_views'] >= advert_hour_cpm_to_stat['hour_orders']].copy()
    ci = proportion_confint(advert_hour_cpm_to_stat['hour_orders'], advert_hour_cpm_to_stat['hour_views'], method='wilson')
    advert_hour_cpm_to_stat['left_ci_95'] = ci[0]
    advert_hour_cpm_to_stat['right_ci_95'] = ci[1]

    return ProcessedWbAdCampaigns(
        dt=dt,
        df=df,
        margin_wo_other_expenses=margin_wo_other_expenses,
        advert_hour_margin_wo_other_expenses=advert_hour_margin_wo_other_expenses,
        hour_margin_wo_other_expenses=hour_margin_wo_other_expenses,
        overall_avg_margin_wo_other_expenses=overall_avg_margin_wo_other_expenses,
        advert_hour_cpm_to_stat=advert_hour_cpm_to_stat
    )
