import typing
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from bidder import db
from bidder.daily_orders import process_daily_orders, ProcessedDailyOrders
from bidder.wb_ad_campaigns import process_wb_ad_campaigns, ProcessedWbAdCampaigns
from bidder import utils


@dataclass
class Product:
    nm_id: str
    price: float
    cost_price_with_sales_fee: float


@dataclass
class Data:
    hour_margin_wo_other_expenses: pd.DataFrame
    hour_cpm_to_stat: pd.DataFrame
    hour_max_margin_wo_other_expenses: pd.DataFrame
    hour_all_campaigns_margin_wo_other_expenses: pd.DataFrame
    overall_avg_margin_wo_other_expenses: float


@dataclass
class AdsCampaign:
    nm_id: str
    advert_id: str
    product: Product
    data: Data
    cr_view_model: typing.Callable[[float], float] = None


def make_product(campaign: pd.DataFrame):
    return Product(
        nm_id=campaign['nm_id'],
        price=campaign['price_in_rub'],
        cost_price_with_sales_fee=campaign['cost_price_with_sales_fee']
    )


def select(df: pd.DataFrame, nm_id: str = None, advert_id: str = None, hour_of_week: int = None):
    if hour_of_week is not None:
        df = df[df['hour_of_week'] == hour_of_week]

    if advert_id is not None and nm_id is not None:
        df = df[(df['advert_id'] == advert_id) & (df['nm_id'] == nm_id)]

    if len(df) == 0:
        return None

    return df.reset_index(drop=True)


class DataFetcher:
    def __init__(self, dt: datetime, db_password: str, selected_nm_ids: set[str] = None):
        self.dt = dt
        with db.PostgressDB(db_password) as conn:
            self.daily_orders = conn.daily_orders(dt)
            # # unlimited history for bids
            self.wb_ad_campaigns = conn.wb_ad_campaigns(dt)
            self.hourly_price_margin = conn.hourly_price_margin(dt)
            self.sku = conn.sku()
            self.legal_entities = conn.legal_entities()
            self.cost = conn.cost(dt)

        self.legal_entities = self.legal_entities[['seller_id', 'code', 'display_name']].rename(
            columns={'display_name': 'legal_entity', 'code': 'legal_entity_code'}
        )
        self.cost_df = self.cost.merge(self.legal_entities, on=['seller_id'])
        self.cost_df.rename(columns={'sku': 'product_id'}, inplace=True)

        self.daily_orders['legal_entity'] = self.daily_orders['legal_entity'].str.upper()
        self.hourly_price_margin['legal_entity'] = self.hourly_price_margin['legal_entity'].str.upper()
        self.sku['legal_entity'] = self.sku['legal_entity'].str.upper()

        self.sku.rename(columns={'sku': 'product_id'}, inplace=True)

        self.wb_ad_campaigns.rename(columns={'legal_entity': 'legal_entity_code'}, inplace=True)
        self.wb_ad_campaigns = self.wb_ad_campaigns.merge(self.legal_entities, on=['legal_entity_code'], how='left')
        self.wb_ad_campaigns['legal_entity'] = self.wb_ad_campaigns['legal_entity'].combine_first(self.wb_ad_campaigns['legal_entity_code'])

        self.daily_orders = self.daily_orders.merge(self.sku[['legal_entity', 'product_id', 'mp_id']], on=['legal_entity', 'product_id'], how='left')

        # get campaigns info from wb
        campaigns_df = self.wb_ad_campaigns[self.wb_ad_campaigns['hour'] >= 0].copy()
        campaigns_df['dt'] = campaigns_df.apply(lambda x: datetime(x['date'].year, x['date'].month, x['date'].day, hour=int(x['hour'])), axis=1)
        campaigns_df = campaigns_df[campaigns_df['dt'] == campaigns_df['dt'].max()]
        self.campaigns_df = campaigns_df[['nm_id', 'advert_id']].drop_duplicates(ignore_index=True)

        # Use pricer here
        self.prices_df = self.hourly_price_margin[self.hourly_price_margin['date'] == self.hourly_price_margin['date'].max()]
        self.prices_df = self.prices_df[self.prices_df['hour'] == self.prices_df['hour'].max()].reset_index(drop=True)
        self.prices_df['price_in_rub'] = self.prices_df['current_price']
        self.prices_df = self.prices_df[['mp_id', 'price_in_rub']].copy()

        # May be more relible source of data?
        self.cost_df = self.cost_df.merge(self.sku, on=['product_id', 'legal_entity'])
        self.cost_df['cost_in_rub'] = self.cost_df['cost_in_kopecks'] / 100.0
        self.cost_df = self.cost_df[['legal_entity', 'product_id', 'mp_id', 'cost_in_rub']].copy()

        if selected_nm_ids is not None:
            self.campaigns_df = self.campaigns_df[self.campaigns_df['nm_id'].isin(selected_nm_ids)].drop_duplicates(ignore_index=True)


@dataclass
class ProcessedData:
    campaigns_df: pd.DataFrame
    sku: pd.DataFrame
    processed_daily_orders: ProcessedDailyOrders
    processed_wb_ad_campaigns: ProcessedWbAdCampaigns


def process(data_fetcher: DataFetcher) -> ProcessedData:
    dt = data_fetcher.dt
    daily_orders = data_fetcher.daily_orders
    sku = data_fetcher.sku
    wb_ad_campaigns_history = data_fetcher.wb_ad_campaigns

    campaigns_df = data_fetcher.campaigns_df

    sku.dropna(inplace=True)
    sku['nm_id'] = sku['mp_id'].apply(lambda x: str(int(x)))
    cost_df = data_fetcher.cost_df.copy()
    cost_df.dropna(inplace=True, ignore_index=True)
    cost_df['nm_id'] = cost_df['mp_id'].apply(lambda x: str(int(x)))

    prices_df = data_fetcher.prices_df.copy()
    prices_df['nm_id'] = prices_df['mp_id'].apply(lambda x: str(int(x)))

    processed_daily_orders = process_daily_orders(dt, daily_orders)
    processed_wb_ad_campaigns = process_wb_ad_campaigns(dt, wb_ad_campaigns_history, sku, processed_daily_orders)

    campaigns_df = campaigns_df.merge(cost_df[['nm_id', 'cost_in_rub']], on='nm_id', how='left')
    campaigns_df = campaigns_df.merge(sku, on='nm_id', how='left')
    campaigns_df = campaigns_df.merge(
        processed_daily_orders.products_stat[['legal_entity', 'product_id', 'cost_price_with_sales_fee_to_cost_price']],
        on=['legal_entity', 'product_id'], how='left')

    campaigns_df.fillna({'cost_price_with_sales_fee_to_cost_price': processed_daily_orders.avg_price_with_sales_fee_to_cost_price}, inplace=True)
    campaigns_df['cost_price_with_sales_fee'] = campaigns_df['cost_in_rub'] * campaigns_df['cost_price_with_sales_fee_to_cost_price']
    campaigns_df = campaigns_df.merge(prices_df[['nm_id', 'price_in_rub']], on='nm_id', how='left')

    return ProcessedData(
        campaigns_df=campaigns_df,
        sku=sku,
        processed_daily_orders=processed_daily_orders,
        processed_wb_ad_campaigns=processed_wb_ad_campaigns
    )


def get_campaigns(dt: datetime, processed_data: ProcessedData) -> list[AdsCampaign]:
    # get hour_of_week, advert_id history
    ads_campaigns = []
    products = {}
    hour_of_week = utils.hour_of_week(dt)

    for _, row in processed_data.campaigns_df.iterrows():
        nm_id = row['nm_id']

        if nm_id not in products:
            products[nm_id] = make_product(row)

        product = products[nm_id]
        advert_id = row['advert_id']

        data = Data(
            hour_margin_wo_other_expenses=select(
                processed_data.processed_wb_ad_campaigns.advert_hour_margin_wo_other_expenses,
                nm_id,
                advert_id,
                hour_of_week
            ),
            hour_cpm_to_stat=select(
                processed_data.processed_wb_ad_campaigns.advert_hour_cpm_to_stat,
                nm_id,
                advert_id,
                hour_of_week),
            hour_max_margin_wo_other_expenses=select(
                processed_data.processed_wb_ad_campaigns.advert_hour_margin_wo_other_expenses,
                nm_id,
                advert_id,
                hour_of_week),
            hour_all_campaigns_margin_wo_other_expenses=select(
                processed_data.processed_wb_ad_campaigns.hour_margin_wo_other_expenses,
                hour_of_week=hour_of_week).iloc[0],
            overall_avg_margin_wo_other_expenses=processed_data.processed_wb_ad_campaigns.overall_avg_margin_wo_other_expenses
        )

        ads_campaigns.append(AdsCampaign(
            nm_id=nm_id,
            advert_id=advert_id,
            product=product,
            data=data
        ))

    return ads_campaigns
