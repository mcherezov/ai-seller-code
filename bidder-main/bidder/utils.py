from datetime import datetime, timedelta

import pandas as pd
import numpy as np


def bid_for_margin(price: float, cost_price_with_sales_fee_per_unit: float, cr_view: float, margin: float) -> float:
    r = (1 - margin) * price - cost_price_with_sales_fee_per_unit
    r *= cr_view
    return r


def margin_for_bid(price: float, cost_price_with_sales_fee_per_unit: float, cr_view: float, bid: float) -> float:
    r = (1.0 - cost_price_with_sales_fee_per_unit / price)
    r -= bid / (cr_view * price)
    return r


def fill_timefields(df: pd.DataFrame):
    df['date_dt'] = df.apply(lambda x: datetime(x['dt'].year, x['dt'].month, x['dt'].day), axis=1)
    df['week_day'] = df['dt'].dt.day_of_week
    df['week_start'] = df.apply(lambda x: x['date_dt'] - timedelta(days=x['week_day']), axis=1)
    df['hour_of_week'] = df['week_day'] * 24 + df['dt'].dt.hour


def hour_of_week(dt: datetime):
    return dt.weekday() * 24 + dt.hour


def div_non_zero(a, b):
    r = (a / b).where(b != 0, np.nan)
    assert np.isnan(r[~np.isfinite(r)]).all()
    return r


def date_hour_to_dt(df):
    return df.apply(lambda x: datetime(x['date'].year, x['date'].month, x['date'].day, hour=int(x['hour'])), axis=1)
