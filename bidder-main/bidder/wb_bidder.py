import logging
import dataclasses
from dataclasses import dataclass
import math
import bisect

from scipy.stats import beta

from bidder import wb_data, utils, config

logger = logging.getLogger(__name__)


@dataclass
class ArmPullResult:
    name: str
    cpm_1: float
    cr_view: float
    arm_no: int = None


class BanditArm:
    def __init__(self, cpm_1: float, name: str, historical: bool, info: dict[str, str] = {}):
        self.name = name
        self.cpm_1 = cpm_1
        self.info = info
        self.historical = historical

    def pull(self) -> ArmPullResult:
        raise NotImplementedError()


class BetaBanditArm(BanditArm):
    def __init__(self, cr_view: float, views: int, cpm_1: float, name: str, historical: bool, info: dict[str, str] = {}):
        super().__init__(cpm_1, name, historical, info)
        assert cr_view > 0
        assert views > 0

        self.cr_view = cr_view
        self.views = views

    def pull(self) -> ArmPullResult:
        return ArmPullResult(
            self.name,
            self.cpm_1,
            beta(self.cr_view * self.views, (1 - self.cr_view) * self.views).rvs()
        )

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'info': self.info,
            'cpm_1': self.cpm_1,
            'cr_view': self.cr_view,
            'views': self.views,
            'historical': self.historical
        }


class BanditArmsHolder:
    def __init__(self):
        self.cpm_1_to_arm = {}

    def select_best_bid(self) -> tuple[ArmPullResult, list[ArmPullResult]]:
        arms_pull_results = []

        for arms in self.cpm_1_to_arm.values():
            if len(arms) == 0:
                continue

            arm_no = self.select_historical_arm_no(arms)
            if arm_no is None:
                arm_no = 0

            arm = arms[arm_no]
            arm_pull_result = arm.pull()
            arm_pull_result.arm_no = arm_no

            arms_pull_results.append(arm_pull_result)

        if len(arms_pull_results) > 0:
            best_arm = min(arms_pull_results, key=lambda apr: apr.cpm_1 / apr.cr_view)
            return best_arm, arms_pull_results
        else:
            return None

    def select_historical_arm_no(self, arms: list[BanditArm]) -> int:
        for arm_no, arm in enumerate(arms):
            if arm.historical:
                return arm_no
        return None

    def append(self, arm: BanditArm) -> None:
        if arm.cpm_1 not in self.cpm_1_to_arm:
            self.cpm_1_to_arm[arm.cpm_1] = []

        self.cpm_1_to_arm[arm.cpm_1].append(arm)

    def to_dict(self) -> dict:
        return {k: [arm.to_dict() for arm in v] for k, v in self.cpm_1_to_arm.items()}


class MarginToCpm1Grid:
    def __init__(self, campaign: wb_data.AdsCampaign, cpm_1_grid: list[float] = None):
        if cpm_1_grid is None:
            cpm_1000_step = config.CPM_1000_GRID_STEP
            cpm_1_grid = [cpm_1000 / 1000.0 for cpm_1000 in range(config.MIN_CPM_1000, config.MAX_CPM_1000 + cpm_1000_step, cpm_1000_step)]
        self.product = campaign.product

        self.margin_to_bid_not_filtered = sorted(zip(
            [utils.margin_for_bid(self.product.price, self.product.cost_price_with_sales_fee, campaign.cr_view_model(cpm_1), cpm_1) for cpm_1 in cpm_1_grid],
            cpm_1_grid,
            [campaign.cr_view_model(cpm_1) for cpm_1 in cpm_1_grid],
        ))

        self.margin_to_bid = [(margin, cpm_1) for margin, cpm_1, _ in self.margin_to_bid_not_filtered if margin >= 0.0]

    def find_closest_cpm1(self, margin: float) -> float | None:
        i = bisect.bisect_left(self.margin_to_bid, margin, key=lambda x: x[0])
        if i == 0:
            if len(self.margin_to_bid) > 0:
                return self.margin_to_bid[0][1]
            else:
                return None
        elif i == len(self.margin_to_bid):
            return self.margin_to_bid[i - 1][1]
        else:
            left_margin, left_bid = self.margin_to_bid[i - 1]
            right_margin, right_bid = self.margin_to_bid[i]

            left_dist = abs(left_margin - margin)
            right_dist = abs(right_margin - margin)

            if math.isclose(left_dist, right_dist):
                return min(left_bid, right_bid)
            elif left_dist < right_dist:
                return left_bid
            else:
                return right_bid

    def to_dict(self) -> dict:
        return {
            'price': self.product.price,
            'cost_price_with_sales_fee': self.product.cost_price_with_sales_fee,
            'grid_origin': [
                {
                    'margin': margin,
                    'cpm_1': cpm_1,
                    'cr_view': cr_view
                } for margin, cpm_1, cr_view in self.margin_to_bid_not_filtered
            ]
        }


class BetaBanditArmFactory:
    def __init__(self, campaign):
        self.campaign = campaign
        self.margin_to_cpm1 = MarginToCpm1Grid(campaign)

        if self.campaign.data.hour_cpm_to_stat is not None:
            self.cpm_1_stat_dict = {row['cpm_1_round']: row for _, row in self.campaign.data.hour_cpm_to_stat.iterrows()}
        else:
            self.cpm_1_stat_dict = {}

    def has_positive_margin_cpms(self) -> bool:
        return len(self.margin_to_cpm1.margin_to_bid) > 0

    def create_using_cr_view_model(self, margin: float, name: str, result_arms: list[BanditArm]) -> bool:
        cpm_1 = self.margin_to_cpm1.find_closest_cpm1(margin)

        if cpm_1 is not None:
            if cpm_1 in self.cpm_1_stat_dict:
                views = self.cpm_1_stat_dict[cpm_1]['hour_views']
            else:
                views = 1000.0

            result_arms.append(BetaBanditArm(
                self.campaign.cr_view_model(cpm_1),
                views,
                cpm_1,
                name,
                False,
                {
                    'expected_margin': margin
                }
            ))
            return True
        else:
            return False


class Bidder:
    def compute_bids(self, campaigns: list[wb_data.AdsCampaign]) -> list[tuple[float, dict]]:
        result_cpm_1_info = []
        for campaign in campaigns:
            arms, margin_to_cpm1 = self.create_bid_arms(campaign)
            result = arms.select_best_bid()
            if result is not None:
                best_arm_result, arms_pull_results = result

                result_cpm_1_info.append(
                    (
                        best_arm_result.cpm_1,
                        {
                            'nm_id': campaign.nm_id,
                            'advert_id': campaign.advert_id,
                            'arms': arms.to_dict(),
                            'best_arm': dataclasses.asdict(best_arm_result),
                            'arms_results': [dataclasses.asdict(apr) for apr in arms_pull_results],
                            'margin_to_cpm1': margin_to_cpm1
                        }
                    )
                )
            else:
                result_cpm_1_info.append((None, {'nm_id': campaign.nm_id, 'advert_id': campaign.advert_id, 'arms': arms.to_dict(), 'margin_to_cpm1': margin_to_cpm1}))

        return result_cpm_1_info

    def create_bid_arms(self, campaign: wb_data.AdsCampaign) -> tuple[BanditArmsHolder, dict]:
        arms = BanditArmsHolder()
        arm_factory = BetaBanditArmFactory(campaign)
        data = campaign.data

        if not arm_factory.has_positive_margin_cpms():
            logger.info(f'nm_id = "{campaign.nm_id}" advert_id = "{campaign.advert_id}", has not positive margin cpms')

        # historical bids
        if data.hour_cpm_to_stat is not None and len(data.hour_cpm_to_stat) > 0:
            max_index = data.hour_cpm_to_stat['margin_wo_other_expenses'].idxmax()
            cpm_stat = data.hour_cpm_to_stat.loc[max_index]

            if cpm_stat['hour_orders'] > 0 and cpm_stat['hour_views'] >= 100:
                arms.append(BetaBanditArm(
                    cpm_stat['hour_orders'] / cpm_stat['hour_views'],
                    cpm_stat['hour_views'],
                    cpm_stat['cpm_1_round'],
                    'max_margin_campaign_hour_history',
                    True
                ))
                arm_factory.create_using_cr_view_model(cpm_stat['margin_wo_other_expenses'] + 0.02, 'campaign_hour_max_margin_history_p02', arms)

        # campaign hour bid with margin 0
        arm_factory.create_using_cr_view_model(0, 'campaign_hour_margin_0', arms)
        arm_factory.create_using_cr_view_model(0.02, 'campaign_hour_margin_0.02', arms)

        # bids with average margin by all campaigns
        if data.hour_margin_wo_other_expenses is not None and len(data.hour_margin_wo_other_expenses) > 0:
            arm_factory.create_using_cr_view_model(
                data.hour_all_campaigns_margin_wo_other_expenses['mean'],
                'hour_overall_avg_margin',
                arms)
            arm_factory.create_using_cr_view_model(
                data.hour_all_campaigns_margin_wo_other_expenses['mean'] + 0.02,
                'hour_overall_avg_margin_hour_p02',
                arms)
        else:
            arm_factory.create_using_cr_view_model(data.overall_avg_margin_wo_other_expenses, 'overall_avg_margin', arms)
            arm_factory.create_using_cr_view_model(data.overall_avg_margin_wo_other_expenses + 0.02, 'overall_avg_margin_p02', arms)

        # campaign hour bid max margin
        if data.hour_margin_wo_other_expenses is not None and len(data.hour_margin_wo_other_expenses) > 0:
            mean_margin = data.hour_margin_wo_other_expenses['mean'].iloc[0]
            max_margin = data.hour_margin_wo_other_expenses['max'].iloc[0]

            if mean_margin > 0:
                arm_factory.create_using_cr_view_model(mean_margin, 'campaign_hour_avg_margin', arms)
                arm_factory.create_using_cr_view_model(mean_margin + 0.02, 'campaign_hour_avg_margin_p02', arms)

            if max_margin > 0:
                arm_factory.create_using_cr_view_model(max_margin, 'campaign_hour_max_margin', arms)
                arm_factory.create_using_cr_view_model(max_margin + 0.02, 'campaign_hour_max_margin_p02', arms)

        return arms, arm_factory.margin_to_cpm1.to_dict()
