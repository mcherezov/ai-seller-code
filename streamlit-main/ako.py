import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import logging
from collections import defaultdict
from scipy import stats as scipy_stats

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('keyword_optimizer.log')
    ]
)
logger = logging.getLogger(__name__)


class AdvancedKeywordOptimizer:
    def __init__(
            self,
            min_views: int = 5,
            min_ctr: float = 0.01,
            max_cpc: float = 1.0,
            stat_significance_level: float = 0.1,
            warmup_period_days: int = 7,
            cluster_mapping: Optional[Dict[str, str]] = None,
            weights: Optional[Dict[str, float]] = None
    ):
        if min_views < 0:
            raise ValueError("min_views must be non-negative")
        if min_ctr < 0 or min_ctr > 1:
            raise ValueError("min_ctr must be between 0 and 1")
        if max_cpc <= 0:
            raise ValueError("max_cpc must be positive")

        self.min_views = min_views
        self.min_ctr = min_ctr
        self.max_cpc = max_cpc
        self.max_cpm = None
        self.stat_significance_level = stat_significance_level
        self.warmup_period_days = warmup_period_days
        self.cluster_mapping = cluster_mapping or {}
        self.cluster_history = defaultdict(list)
        self.median_metrics = {'ctr': 0.0, 'cpc': 0.0, 'cpm': 0.0}
        self.dynamic_thresholds = {'max_cpc': max_cpc, 'max_cpm': None}
        self.weights = weights or {'ctr': 0.3, 'cpc': 0.4, 'shows': 0.3}
        if not (0.99 <= sum(self.weights.values()) <= 1.01):
            raise ValueError("Weights must sum to approximately 1.0")

        logger.info(f"Initialized AdvancedKeywordOptimizer: min_views={min_views}, min_ctr={min_ctr}, "
                    f"max_cpc={max_cpc}, warmup_period_days={warmup_period_days}, "
                    f"weights={self.weights}")

    def _map_keyword_to_cluster(self, keyword: str) -> str:
        if pd.isna(keyword):
            logger.warning("NaN detected in keyword, returning 'unknown'")
            return "unknown"
        if not self.cluster_mapping:
            return keyword
        cluster = self.cluster_mapping.get(keyword, keyword)
        return cluster

    def update_dynamic_thresholds(self, df: pd.DataFrame):
        agg_df = df.groupby(['advertId']).agg({
            'shows': 'sum',
            'clicks': 'sum',
            'spend': 'sum'
        }).reset_index()
        agg_df['cpc'] = agg_df['spend'] / agg_df['clicks'].replace(0, 1)
        agg_df['cpm'] = (agg_df['spend'] / agg_df['shows'].replace(0, 1)) * 1000

        self.dynamic_thresholds['max_cpc'] = agg_df['cpc'].quantile(0.9) if not agg_df['cpc'].empty else self.max_cpc
        self.dynamic_thresholds['max_cpm'] = agg_df['cpm'].quantile(0.95) if not agg_df['cpm'].empty else self.max_cpm
        self.min_views = max(agg_df['shows'].median() * 0.5, 3)
        logger.info(f"Dynamically updated thresholds: max_cpc={self.dynamic_thresholds['max_cpc']:.2f}, "
                    f"min_views={self.min_views}")

    def update_history(self, df: pd.DataFrame) -> None:
        if df.empty:
            logger.warning("Received empty DataFrame, skipping update")
            return
        try:
            required_columns = ['date', 'keyword', 'advertId', 'shows', 'clicks', 'spend']
            if not all(col in df.columns for col in required_columns):
                missing = [col for col in required_columns if col not in df.columns]
                logger.error(f"Missing columns: {missing}")
                return

            df = df.copy()
            df['date'] = pd.to_datetime(df['date'])
            df['ctr'] = (df['clicks'] / df['shows']).clip(upper=1.0)
            invalid_rows = df[df['clicks'] > df['shows']]
            if not invalid_rows.empty:
                logger.warning(f"Found rows with clicks > shows: {len(invalid_rows)}")
                df.loc[df['clicks'] > df['shows'], 'ctr'] = 1.0

            df['advertId'] = df['keyword'].apply(self._map_keyword_to_cluster)
            df = df.dropna(subset=['advertId'])

            self.update_dynamic_thresholds(df)
            agg_df = df.groupby(['advertId', 'date']).agg({
                'shows': 'sum',
                'clicks': 'sum',
                'spend': 'sum',
                'ctr': 'mean'
            }).reset_index()

            if agg_df['shows'].sum() == 0:
                logger.warning("Aggregated data contains zero shows, check input data")

            logger.debug(f"Aggregated spend values: {agg_df[['advertId', 'spend']].to_dict()}")

            self.median_metrics = {
                'ctr': agg_df['ctr'].median() if 'ctr' in agg_df.columns else 0.0,
                'cpc': (agg_df['spend'] / agg_df['clicks']).median() if agg_df['clicks'].sum() > 0 else 0.0,
                'cpm': ((agg_df['spend'] / agg_df['shows']) * 1000).median() if agg_df['shows'].sum() > 0 else 0.0
            }
            logger.info(f"Updated median metrics: {self.median_metrics}")

            for _, row in agg_df.iterrows():
                advert_id = row['advertId']
                stat_entry = {
                    'date': row['date'],
                    'shows': row['shows'],
                    'clicks': row['clicks'],
                    'spend': row['spend'],
                    'ctr': row['ctr']
                }
                if not any(s['date'] == row['date'] for s in self.cluster_history[advert_id]):
                    self.cluster_history[advert_id].append(stat_entry)

            max_history_days = 30
            for advert_id in self.cluster_history:
                cutoff_date = datetime.now() - timedelta(days=max_history_days)
                self.cluster_history[advert_id] = [s for s in self.cluster_history[advert_id] if
                                                   s.get('date') >= cutoff_date]
            logger.info(f"Updated history for {len(self.cluster_history)} clusters, limited to {max_history_days} days")

        except Exception as e:
            logger.error(f"Error updating history: {str(e)}")

    def compute_efficiency_score(self, stats: List[Dict]) -> float:
        total_shows = sum(s.get('shows', 0) for s in stats)
        total_clicks = sum(s.get('clicks', 0) for s in stats)
        total_spend = sum(s.get('spend', 0) for s in stats)

        ctr = total_clicks / total_shows if total_shows > 0 else 0.0
        cpc = total_spend / total_clicks if total_clicks > 0 else float('inf')

        base_ctr = max(self.median_metrics.get('ctr', 0.0), self.min_ctr)
        base_cpc = max(self.median_metrics.get('cpc', 0.0), self.max_cpc / 2)

        ctr_score = min(ctr / base_ctr, 2.0)
        cpc_score = max(self.max_cpc / cpc, 0.1) if cpc < float('inf') else 0.1
        shows_score = min(total_shows / self.min_views, 2.0)

        stability_bonus = 0.0
        if len(stats) >= 14:
            ctr_values = [s.get('clicks', 0) / s.get('shows', 1) for s in stats]
            cpc_values = [s.get('spend', 0) / s.get('clicks', 1) if s.get('clicks', 0) > 0 else float('inf') for s in
                          stats]
            ctr_std = np.std(ctr_values) / (base_ctr + 1e-6)
            cpc_std = np.std([c for c in cpc_values if c < float('inf')]) / (base_cpc + 1e-6) if any(
                c < float('inf') for c in cpc_values) else 1.0
            stability_bonus = max(0.2 * (1 - min(ctr_std, 1.0)) + 0.2 * (1 - min(cpc_std, 1.0)), 0.0)

        score = (
                self.weights['ctr'] * ctr_score +
                self.weights['cpc'] * cpc_score +
                self.weights['shows'] * shows_score +
                stability_bonus
        )

        return min(max(score, 0.0), 3.0)

    def compute_click_trend(self, stats: List[Dict]) -> float:
        if len(stats) < 2:
            return 0.0
        clicks = [s.get('clicks', 0) for s in stats]
        alpha = 0.3
        smoothed = [clicks[0]]
        for i in range(1, len(clicks)):
            smoothed.append(alpha * clicks[i] + (1 - alpha) * smoothed[-1])
        return smoothed[-1] - smoothed[0]

    def compute_metric_trend(self, stats: List[Dict], metric: str) -> float:
        if len(stats) < 2:
            return 0.0
        values = []
        for s in stats:
            if metric == 'ctr':
                shows = s.get('shows', 0)
                clicks = s.get('clicks', 0)
                value = (clicks / shows) if shows > 0 else 0.0
            elif metric == 'cpm':
                shows = s.get('shows', 0)
                spend = s.get('spend', 0)
                value = (spend / shows * 1000) if shows > 0 else 0.0
            elif metric == 'cpc':
                clicks = s.get('clicks', 0)
                spend = s.get('spend', 0)
                value = (spend / clicks) if clicks > 0 else float('inf')
            else:
                value = s.get(metric, 0)
            values.append(value)

        alpha = 0.3
        smoothed = [values[0]]
        for i in range(1, len(values)):
            smoothed.append(alpha * values[i] + (1 - alpha) * smoothed[-1])
        return smoothed[-1] - smoothed[0]

    def _check_statistical_significance(self, stats: List[Dict], metric: str = 'ctr') -> bool:
        if len(stats) < 3 or sum(s.get('clicks', 0) for s in stats) < 2:
            return False
        try:
            days = np.arange(len(stats))
            if metric == 'ctr':
                values = [(s.get('clicks', 0) / s.get('shows', 1)) for s in stats]
            elif metric == 'cpm':
                values = [(s.get('spend', 0) / s.get('shows', 1) * 1000) for s in stats]
            elif metric == 'cpc':
                values = [(s.get('spend', 0) / s.get('clicks', 1)) if s.get('clicks', 0) > 0 else float('inf') for s in
                          stats]
                values = [v for v in values if v < float('inf')]
            else:
                values = [s.get(metric, 0) for s in stats]
            if len(values) < 3:
                return False
            slope, _, p_value, _, _ = scipy_stats.linregress(days[-len(values):], values)
            return slope < 0 and p_value < self.stat_significance_level
        except Exception as e:
            logger.error(f"Error in statistical analysis: {str(e)}")
            return False

    def analyze_cluster(self, advert_id: str) -> dict:
        default_result = {
            'decision': 'keep',
            'reason': 'default',
            'bid_modifier': 1.0,
            'ctr': 0.0,
            'cpc': 0.0,
            'cpm': 0.0,
            'total_shows': 0,
            'total_clicks': 0,
            'total_spend': 0,
            'efficiency_score': 0.0
        }

        if advert_id not in self.cluster_history:
            logger.warning(f"Cluster {advert_id} not found in history")
            return default_result

        try:
            stats = self.cluster_history[advert_id]
            if not stats:
                logger.warning(f"Empty stats for cluster {advert_id}")
                return default_result

            total_stats = {
                'shows': sum(s.get('shows', 0) for s in stats),
                'clicks': sum(s.get('clicks', 0) for s in stats),
                'spend': sum(s.get('spend', 0) for s in stats)
            }

            if total_stats['shows'] == 0:
                logger.warning(f"Cluster {advert_id} has zero shows, check data")
                return default_result

            ctr = total_stats['clicks'] / total_stats['shows'] if total_stats['shows'] > 0 else 0.0
            cpc = total_stats['spend'] / total_stats['clicks'] if total_stats['clicks'] > 0 else float('inf')
            cpm = (total_stats['spend'] / total_stats['shows'] * 1000) if total_stats['shows'] > 0 else 0.0
            days_in_history = (datetime.now() - min(s.get('date') for s in stats if s.get('date'))).days + 1
            efficiency_score = self.compute_efficiency_score(stats)
            click_trend = self.compute_click_trend(stats)
            ctr_trend = self.compute_metric_trend(stats, 'ctr')
            cpm_trend = self.compute_metric_trend(stats, 'cpm')
            cpc_trend = self.compute_metric_trend(stats, 'cpc')

            result = {
                **default_result,
                'ctr': ctr,
                'cpc': cpc if cpc < float('inf') else 0.0,
                'cpm': cpm,
                'total_shows': total_stats['shows'],
                'total_clicks': total_stats['clicks'],
                'total_spend': total_stats['spend'],
                'efficiency_score': efficiency_score
            }

            if total_stats['shows'] < 30:
                result.update({
                    'decision': 'insufficient_data',
                    'reason': 'insufficient_data',
                    'bid_modifier': 1.0
                })
                logger.info(
                    f"Cluster {advert_id}: insufficient data ({total_stats['shows']}<30), decision=insufficient_data")
                return result

            if days_in_history <= self.warmup_period_days:
                result.update({
                    'decision': 'keep',
                    'reason': 'warmup_period',
                    'bid_modifier': 1.0
                })
                logger.info(f"Cluster {advert_id}: in warmup period ({days_in_history} days), decision=keep")
                return result

            if total_stats['shows'] < self.min_views:
                if click_trend > 0 or total_stats['shows'] / self.min_views >= 0.5:
                    result.update({'reason': 'low_views_keep', 'decision': 'keep'})
                else:
                    result.update({'reason': 'low_views', 'decision': 'reduce_bid', 'bid_modifier': 0.7})
            else:
                if efficiency_score < 0.8:
                    result.update({'reason': 'low_efficiency', 'decision': 'stop'})
                elif efficiency_score < 1.3:
                    result.update({'reason': 'moderate_efficiency', 'decision': 'reduce_bid', 'bid_modifier': 0.8})
                elif efficiency_score <= 2.1:
                    result.update({'decision': 'keep', 'reason': 'good_efficiency', 'bid_modifier': 1.0})
                else:
                    result.update({'decision': 'increase_bid', 'reason': 'high_efficiency', 'bid_modifier': 1.2})

                if click_trend > 0 or ctr_trend > 0:
                    result['bid_modifier'] = min(result['bid_modifier'] * 1.1, 1.2)
                    result['reason'] = f"{result['reason']} (positive_trend)"
                elif ctr_trend < -0.01 or cpm_trend > 50 or cpc_trend > self.median_metrics.get('cpc', 0.0):
                    result['bid_modifier'] = max(result['bid_modifier'] * 0.9, 0.7)
                    result['reason'] = f"{result['reason']} (negative_trend)"

                if cpc < float('inf') and cpc > self.median_metrics.get('cpc', 0.0) * 10:
                    result.update({'decision': 'stop', 'reason': 'extreme_high_cpc'})
                elif self.max_cpm is not None and cpm > self.max_cpm:
                    result.update({'decision': 'stop', 'reason': 'extreme_high_cpm'})

            logger.info(f"Cluster {advert_id}: shows={total_stats['shows']}, clicks={total_stats['clicks']}, "
                        f"ctr={ctr:.2%}, cpc={cpc:.2f}, cpm={cpm:.2f}, "
                        f"efficiency_score={efficiency_score:.2f}, decision={result['decision']}, "
                        f"reason={result['reason']}, bid_modifier={result['bid_modifier']:.2f}")
            return result

        except Exception as e:
            logger.error(f"Error analyzing cluster {advert_id}: {str(e)}")
            return default_result

    def optimize(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            if df.empty:
                logger.warning("Empty input DataFrame, returning empty optimization result")
                return pd.DataFrame(columns=['advertId', 'decision', 'reason'])

            self.update_history(df)
            results = []

            for advert_id in self.cluster_history:
                analysis = self.analyze_cluster(advert_id)
                results.append({
                    'advertId': advert_id,
                    'days_in_history': (datetime.now() - min(
                        s.get('date') for s in self.cluster_history[advert_id] if s.get('date'))).days + 1,
                    **analysis
                })

            columns = [
                'advertId', 'days_in_history', 'decision', 'reason',
                'ctr', 'cpc', 'cpm', 'total_shows', 'total_clicks', 'total_spend',
                'bid_modifier', 'efficiency_score'
            ]
            result_df = pd.DataFrame(results, columns=columns).sort_values('efficiency_score', ascending=False)
            logger.info(f"Optimization completed, returned {len(result_df)} recommendations")
            return result_df

        except Exception as e:
            logger.error(f"Error during optimization: {str(e)}")
            return pd.DataFrame(columns=['advertId', 'decision', 'reason'])

    def filter_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            optimization_df = self.optimize(df)
            if optimization_df.empty:
                logger.warning("Empty optimization result, returning original DataFrame")
                return df.copy()

            filtered_df = df.copy()
            filtered_df['advertId'] = filtered_df['keyword'].apply(self._map_keyword_to_cluster)
            filtered_df = filtered_df.dropna(subset=['advertId'])

            for _, row in optimization_df.iterrows():
                mask = filtered_df['advertId'] == row['advertId']
                if row['decision'] == 'stop':
                    filtered_df = filtered_df[~mask]
                else:
                    filtered_df.loc[mask, 'spend'] *= row.get('bid_modifier', 1.0)

            logger.info(f"Filtering completed, returned {len(filtered_df)} records")
            return filtered_df

        except Exception as e:
            logger.error(f"Error during filtering: {str(e)}")
            return df.copy()

        except Exception as e:
            logger.error(f"Error during filtering: {str(e)}")
            return df.copy()