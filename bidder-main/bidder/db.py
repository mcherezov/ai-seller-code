import json
from datetime import datetime, timedelta
import pandas as pd

import psycopg2
import pandas.io.sql as sqlio

from . import utils


def datetime_to_date_pg(dt):
    return dt.strftime('%Y-%m-%d')


def datetime_to_timestamp_pg(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')


class PostgressDB:
    def __init__(self, password: str):
        self.conn = psycopg2.connect(
            host='158.160.57.59',
            port=57212,
            user='user',
            database='app_db',
            password=password
        )

    def wb_ad_campaigns(self, dt: datetime, days: int = 368, from_date: datetime = datetime(2025, 4, 21)) -> pd.DataFrame:
        from_date = max(from_date, dt - timedelta(days=days))
        to_dt = dt - timedelta(hours=1)
        return sqlio.read_sql_query(
            f'''
            SELECT
                *
            FROM
                wb_ad_campaign
            WHERE date >= '{datetime_to_date_pg(from_date)}' AND date <= '{datetime_to_date_pg(to_dt)}'
            ''', self.conn)


    def cost(self, dt: datetime) -> pd.DataFrame:
        return sqlio.read_sql_query(
            f'''
            SELECT DISTINCT ON (sku, seller_id)
                *
            FROM
                cost
            WHERE date <= '{datetime_to_date_pg(dt)}'
            ORDER BY sku, seller_id, date DESC
            ''', self.conn)

    def daily_orders(self, dt: datetime, days: int = 368, from_date: datetime = datetime(2025, 4, 21), marketplace: str = 'Wildberries') -> pd.DataFrame:
        from_date = max(from_date, dt - timedelta(days=days))
        return sqlio.read_sql_query(
            f'''
            SELECT
                *
            FROM
                daily_orders
            WHERE
                date >= '{datetime_to_date_pg(from_date)}' AND date <= '{datetime_to_date_pg(dt)}'
                AND marketplace='{marketplace}'
            ''', self.conn)

    def hourly_price_margin(self, dt: datetime, days: int = 368, from_date: datetime = datetime(2025, 4, 21), marketplace: str = 'WILDBERRIES') -> pd.DataFrame:
        from_date = max(from_date, dt - timedelta(days=days))
        return sqlio.read_sql_query(
            f'''
            SELECT
                *
            FROM
                hourly_price_margin
            WHERE
                date >= '{datetime_to_date_pg(from_date)}' AND date <= '{datetime_to_date_pg(dt)}'
                AND marketplace='{marketplace}'
            ''', self.conn)

    def sku(self, marketplace: str = 'Wildberries') -> pd.DataFrame:
        return sqlio.read_sql_query(
            f'''
            SELECT
                *
            FROM
                sku
            WHERE
                marketplace_name='{marketplace}'
            ''', self.conn)

    def legal_entities(self) -> pd.DataFrame:
        return sqlio.read_sql_query(
            '''
            SELECT
                *
            FROM
                legal_entities
            ''', self.conn)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.conn.close()


class LogDB:
    def __init__(self, password: str):
        self.conn = psycopg2.connect(
            host='rc1a-6ic32g9da782j8kh.mdb.yandexcloud.net',
            port=6432,
            user='vkamanov',
            database='appvkamanov',
            password=password
        )
        self.cur = self.conn.cursor()

    def insert(self,
               dt: datetime,
               calculation_dt: datetime,
               nm_id: str,
               advert_id: str,
               duration_sec: float,
               cpm_1000: float,
               log: dict):
        query = f'''
            INSERT INTO wb_bidder_log_v1 (dt, calculation_dt, nm_id, advert_id, duration_sec, cpm_1000, log)
            VALUES
                (
                    '{datetime_to_timestamp_pg(dt)}',
                    '{datetime_to_timestamp_pg(calculation_dt)}',
                    {nm_id},
                    {advert_id},
                    {duration_sec},
                    {cpm_1000},
                    '{json.dumps(log)}'
                );
        '''
        self.cur.execute(query)

    def select_all(self):
        return sqlio.read_sql_query(
            '''
            SELECT
                *
            FROM
                wb_bidder_log_v1
            ''', self.conn)

    def commit(self):
        self.conn.commit()
