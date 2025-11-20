from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd


@dataclass
class ProcessedDailyOrders:
    dt: datetime
    df: pd.DataFrame
    products_stat: pd.DataFrame
    avg_price_with_sales_fee_to_cost_price: float


sales_fee_total_cols = [
    'commission',
    'storage_fee',
    'logistics_fee',
    'acquiring_fee',
    'cross_docking',
    'fulfillment'
]


def process_daily_orders(dt: datetime, daily_orders: pd.DataFrame) -> ProcessedDailyOrders:
    df = daily_orders[daily_orders['orders_count'] > 0].copy()

    df['sales_fee_per_unit'] = df[sales_fee_total_cols].sum(axis=1) / df['orders_count']

    df.loc[df['cost_price_per_unit'] <= 0.0, 'cost_price_per_unit'] = np.nan
    df.fillna({'cost_price_per_unit': df['cost_price_per_unit'].mean()}, inplace=True)

    df['cost_price_with_sales_fee_per_unit'] = df['cost_price_per_unit'] + df['sales_fee_per_unit']
    df['cost_price_with_sales_fee_to_cost_price'] = (df['cost_price_with_sales_fee_per_unit']) / df['cost_price_per_unit']

    products_stat = df.groupby(['legal_entity', 'product_id'], as_index=False)['cost_price_with_sales_fee_to_cost_price'].mean()
    return ProcessedDailyOrders(
        dt=dt,
        df=df,
        products_stat=products_stat,
        avg_price_with_sales_fee_to_cost_price=df['cost_price_with_sales_fee_to_cost_price'].mean()
    )
