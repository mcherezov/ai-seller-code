import pickle
import pandas as pd

from sklearn.metrics import root_mean_squared_error

from bidder import utils


class AnalyticsBaselineV1:
    def fit(self, calculations_df):
        self.mean_cr_view = calculations_df['cr_view'].mean()

        self.hourly_cr_view = calculations_df.groupby(['hour_of_week'], as_index=False)['cr_view'].mean()
        self.hourly_cr_view.rename(columns={'cr_view': 'hourly_cr_view'}, inplace=True)

        self.nm_id_hourly_cr_view = calculations_df.groupby(['hour_of_week', 'nm_id'], as_index=False)['cr_view'].mean()
        self.nm_id_hourly_cr_view.rename(columns={'cr_view': 'nm_id_hourly_cr_view'}, inplace=True)

        self.hour_nm_advert_cr_view = calculations_df.reset_index(drop=True)
        self.hour_nm_advert_cr_view['nm_advert_id'] = self.hour_nm_advert_cr_view['nm_id'] + '_' + self.hour_nm_advert_cr_view['advert_id']
        self.hour_nm_advert_cr_view = self.hour_nm_advert_cr_view.groupby(['hour_of_week', 'nm_id', 'nm_advert_id'], as_index=False)['cr_view'].mean()
        return self.score(calculations_df)

    def predict(self, df):
        df = df[['nm_id', 'advert_id', 'hour_of_week']].copy()
        df['nm_advert_id'] = df['nm_id'] + '_' + df['advert_id']

        df = df.merge(self.hour_nm_advert_cr_view, on=['hour_of_week', 'nm_id', 'nm_advert_id'], how='left')

        df = df.merge(self.nm_id_hourly_cr_view, on=['hour_of_week', 'nm_id'], how='left')
        df['cr_view'] = df['cr_view'].combine_first(df['nm_id_hourly_cr_view'])
        df.drop(columns=['nm_id_hourly_cr_view'], inplace=True)

        df = df.merge(self.hourly_cr_view, on=['hour_of_week'], how='left')
        df['cr_view'] = df['cr_view'].combine_first(df['hourly_cr_view'])
        df.drop(columns=['hourly_cr_view'], inplace=True)

        df.fillna({'cr_view': self.mean_cr_view}, inplace=True)
        return df['cr_view'].values

    def score(self, calculation_df):
        prediction = self.predict(calculation_df)
        return {
            'rmse_cr_view': root_mean_squared_error(calculation_df['cr_view'], prediction)
        }

    def predict_one(self, nm_id, advert_id, calculation_dt, cpm_1):
        df = pd.DataFrame([{
            'calculation_dt': calculation_dt,
            'nm_id': nm_id,
            'advert_id': advert_id,
            'hour_of_week': utils.hour_of_week(calculation_dt),
            'cpm_1': cpm_1
        }])
        pred = self.predict(df)[0]
        return pred

    def load(self, path):
        with open(path, 'rb') as fi:
            self.mean_cr_view, self.hourly_cr_view, self.nm_id_hourly_cr_view, self.hour_nm_advert_cr_view = pickle.load(fi)

    def save(self, path):
        with open(path, 'wb') as fo:
            pickle.dump((self.mean_cr_view, self.hourly_cr_view, self.nm_id_hourly_cr_view, self.hour_nm_advert_cr_view), fo)
