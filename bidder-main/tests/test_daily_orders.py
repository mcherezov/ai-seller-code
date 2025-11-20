import unittest

import datetime

import pandas as pd
from bidder import daily_orders


data = pd.DataFrame([{
        'id': 447031,
        'marketplace': 'Wildberries',
        'legal_entity': 'АТ',
        'product_id': 'FP1-1',
        'category': 'Сковороды',
        'orders_count': 12,
        'orders_total': 29856.0,
        'avg_receipt': 2488.0,
        'cost_price_per_unit': 1922.0,
        'cost_price_total': 23064.0,
        'commission': 5821.92,
        'storage_fee': 629.423424,
        'logistics_fee': 2147.383667,
        'acquiring_fee': 390.6857143,
        'cross_docking': 0.0,
        'advertising': 2355.89,
        'other_expenses': 24.0,
        'fulfillment': 1200.0,
        'purchase_rate': 100.0,
        'stock': 909,
        'avg_weekly_sales': 8.43,
        'turnover': 107.82918149466192,
        'commercial_margin': -5777.3028053,
        'commercial_margin_percent': -19.3505586994239,
        'spp_percent': 27.6,
        'spp_avg_receipt': 1801.312,
        'total_clicks': 409,
        'ad_views': 7889,
        'ad_clicks': 150,
        'ad_cart_additions': 19,
        'funnel_cart_additions': 49,
        'date': datetime.date(2025, 4, 9),
        'is_valid': True,
        'is_historical': True,
        'planned_orders': 15172.55733,
        'planned_orders_per_day': 4.446,
        'commercial_margin_per_order': -481.44190044166663,
        'advertising_per_order': 196.32416666666666,
        'mp': 851.117733775,
        'sales_fee_without_other_expenses_and_ads_per_order': 849.117733775,
        'cost_without_ads_per_order': 23913.117733775,
        'cost_without_ads_per_order_to_cost': 12.441788623191988,
        'margin_without_other_expenses_percent': -0.19270172847333875,
        'commercial_margin_without_other_expenses_percent': -19.270172847333875,
        'sales_fee': 849.117733775,
        'cost_price_with_sales_fee_to_cost_price': 1.4417886231919876,
        'cost_price_with_sales_fee': 2771.117733775
    }, {
        'id': 447032,
        'marketplace': 'Wildberries',
        'legal_entity': 'АТ',
        'product_id': 'FP1-2',
        'category': 'Сковороды',
        'orders_count': 14,
        'orders_total': 48076.0,
        'avg_receipt': 3434.0,
        'cost_price_per_unit': 2286.0,
        'cost_price_total': 32004.0,
        'commission': 9374.82,
        'storage_fee': 320.6410056,
        'logistics_fee': 2854.25,
        'acquiring_fee': 602.2767442,
        'cross_docking': 0.0,
        'advertising': 3234.69,
        'other_expenses': 28.0,
        'fulfillment': 1400.0,
        'purchase_rate': 100.0,
        'stock': 312,
        'avg_weekly_sales': 10.86,
        'turnover': 28.729281767955804,
        'commercial_margin': -1742.6777498000001,
        'commercial_margin_percent': -3.624839316498877,
        'spp_percent': 27.23,
        'spp_avg_receipt': 2498.9218,
        'total_clicks': 706,
        'ad_views': 10408,
        'ad_clicks': 348,
        'ad_cart_additions': 43,
        'funnel_cart_additions': 76,
        'date': datetime.date(2025, 4, 9),
        'is_valid': True,
        'is_historical': True,
        'planned_orders': 22899.245,
        'planned_orders_per_day': 4.914,
        'commercial_margin_per_order': -124.47698212857144,
        'advertising_per_order': 231.04928571428573,
        'mp': 1041.4276964142855,
        'sales_fee_without_other_expenses_and_ads_per_order': 1039.4276964142857,
        'cost_without_ads_per_order': 33043.427696414285,
        'cost_without_ads_per_order_to_cost': 14.454692780583677,
        'margin_without_other_expenses_percent': -0.03566598198269409,
        'commercial_margin_without_other_expenses_percent': -3.5665981982694093,
        'sales_fee': 1039.4276964142857,
        'cost_price_with_sales_fee_to_cost_price': 1.454692780583677,
        'cost_price_with_sales_fee': 3325.4276964142855
    }]
)


class TestProcessDailyOrders(unittest.TestCase):

    def test_basic(self):
        processed_daily_orders = daily_orders.process_daily_orders(datetime.datetime(2025, 5, 4), data)
        self.assertEqual((5821.92 + 629.423424 + 2147.383667 + 390.6857143 + 0.0 + 1200.0) / 12, processed_daily_orders.df.iloc[0]['sales_fee_per_unit'])
        self.assertEqual(1922.0 + (5821.92 + 629.423424 + 2147.383667 + 390.6857143 + 0.0 + 1200.0) / 12,
                         processed_daily_orders.df.iloc[0]['cost_price_with_sales_fee_per_unit'])
        self.assertAlmostEqual(1.441788623, processed_daily_orders.df.iloc[0]['cost_price_with_sales_fee_to_cost_price'])

        self.assertAlmostEqual(1.441789, processed_daily_orders.products_stat.iloc[0]['cost_price_with_sales_fee_to_cost_price'], places=6)
        self.assertAlmostEqual(1.454693, processed_daily_orders.products_stat.iloc[1]['cost_price_with_sales_fee_to_cost_price'], places=6)

        self.assertAlmostEqual((1.441789 + 1.454693) / 2.0, processed_daily_orders.avg_price_with_sales_fee_to_cost_price, places=6)
