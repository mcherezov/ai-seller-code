import unittest

from bidder import utils


class TestBidder(unittest.TestCase):

    def test_margin_for_bid(self):
        self.assertAlmostEqual(0.5, utils.margin_for_bid(1000, 500, 1.0, 0))
        self.assertAlmostEqual(0.25, utils.margin_for_bid(1000, 500, 1.0, 250))
        self.assertAlmostEqual(0.25, utils.margin_for_bid(1000, 500, 0.5, 125))

    def test_bid_for_margin(self):
        self.assertAlmostEqual(0, utils.bid_for_margin(1000, 500, 1.0, 0.5))
        self.assertAlmostEqual(250, utils.bid_for_margin(1000, 500, 1.0, 0.25))
        self.assertAlmostEqual(125, utils.bid_for_margin(1000, 500, 0.5, 0.25))
