import unittest

from bidder import wb_bidder
from bidder import wb_data


class TestMarginToCpm1Grid(unittest.TestCase):

    def test_basic(self):
        cpm1_to_ctr = {
            1: 0.1,
            2: 0.2,
            3: 0.1,
            4: 0.8
        }

        # margin -> bid: (0.2 -> 3, 0.4 -> 1, 0.4 -> 2, 0.45 -> 4)
        campaign = wb_data.AdsCampaign('', wb_data.Product('', 100, 50), None, lambda x: cpm1_to_ctr[x])
        margin_to_cpm1 = wb_bidder.MarginToCpm1Grid(campaign, [1, 2, 3, 4])
        # leftmost, rightmost margin
        self.assertEqual(margin_to_cpm1.find_closest_cpm1(0.1), 3)
        self.assertEqual(margin_to_cpm1.find_closest_cpm1(0.5), 4)

        self.assertEqual(margin_to_cpm1.find_closest_cpm1(0.25), 3)
        self.assertEqual(margin_to_cpm1.find_closest_cpm1(0.35), 1)
        self.assertEqual(margin_to_cpm1.find_closest_cpm1(0.3), 1)
        self.assertEqual(margin_to_cpm1.find_closest_cpm1(0.4), 1)

