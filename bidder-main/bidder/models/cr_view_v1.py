import pickle
import numpy as np
import pandas as pd

from sklearn.preprocessing import OneHotEncoder, MinMaxScaler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import root_mean_squared_error

from bidder import utils


class FeaturesExtractor:
    def __init__(self, scale_cpm=True):
        self.keys = ['calculation_dt', 'nm_id', 'advert_id', 'hour_of_week']
        self.scale_cpm = scale_cpm

    def fit_transform(self, df):
        result_df = df[self.keys].reset_index(drop=True)
        result_df = result_df.merge(df[self.keys + ['cpm_1']], on=self.keys, how='left')
        result_df['nm_advert_id'] = result_df['nm_id'] + '_' + result_df['advert_id']

        self.nm_id_ohe = self.fit_ohe(result_df['nm_id'])
        self.hour_of_week_ohe = self.fit_ohe(result_df['hour_of_week'])
        self.advert_id_ohe = self.fit_ohe(result_df['nm_advert_id'])

        if self.scale_cpm:
            self.cpm_1_scaller = self.fit_minmax_scaler(result_df['cpm_1'])
            self.cpm_1_15_scaller = self.fit_minmax_scaler(result_df['cpm_1'] ** 1.5)
            self.cpm_1_2_scaller = self.fit_minmax_scaler(result_df['cpm_1'] ** 2)

        return self.transform(df)

    def transform(self, df):
        result_df = df[self.keys].reset_index(drop=True)
        size = len(result_df)
        result_df = result_df.merge(df[self.keys + ['cpm_1']], on=self.keys, how='left')
        result_df['nm_advert_id'] = result_df['nm_id'] + '_' + result_df['advert_id']

        if self.scale_cpm:
            result_df['F_cpm_15'] = self.cpm_1_15_scaller.transform((result_df['cpm_1']**1.5).values[:, np.newaxis])
        else:
            result_df['F_cpm_15'] = result_df['cpm_1']**1.5

        result_df = result_df.join(self.transform_ohe(self.hour_of_week_ohe, result_df['hour_of_week']), how='left')
        result_df = result_df.join(self.transform_ohe(self.nm_id_ohe, result_df['nm_id']), how='left')
        result_df = result_df.join(self.transform_ohe(self.advert_id_ohe, result_df['nm_advert_id']), how='left')
        result_df.drop(columns=['nm_advert_id'], inplace=True)

        result_df.drop(columns=['cpm_1'], inplace=True)
        unwanted_cols = [c for c in result_df.columns if not ((c in self.keys) or c.startswith('F_'))]
        assert len(unwanted_cols) == 0, unwanted_cols
        assert len(result_df) == size
        self.features = [c for c in result_df.columns if c not in self.keys]
        return result_df

    def fit_ohe(self, srs, drop=None):
        encoder = OneHotEncoder(sparse_output=False, drop=drop)
        encoder.fit(srs.values[:, np.newaxis])
        return encoder

    def transform_ohe(self, ohe_encoder, srs):
        values = ohe_encoder.transform(srs.values[:, np.newaxis])
        return pd.DataFrame(values, columns=ohe_encoder.get_feature_names_out([f'F_{srs.name}']), index=srs.index)

    def fit_minmax_scaler(self, srs):
        scaler = MinMaxScaler()
        scaler.fit(srs.values[:, np.newaxis])
        return scaler


class CrViewModelV1:
    def __init__(self, scale_cpm):
        self.feature_extractor = FeaturesExtractor(scale_cpm=scale_cpm)

    def fit(self, calculations_df):
        keys = ['calculation_dt', 'nm_id', 'advert_id', 'hour_of_week']
        train_df_targets = calculations_df[keys + ['cr_view']]
        train_df_transformed = self.feature_extractor.fit_transform(calculations_df)
        train_df_transformed = train_df_transformed.merge(train_df_targets, on=keys, how='left')

        # TODO hyperopt
        self.model = LinearRegression()
        self.model.fit(train_df_transformed[self.feature_extractor.features], train_df_targets['cr_view'])

        return self.score(calculations_df)

    def predict(self, df):
        df = self.feature_extractor.transform(df)
        pred = self.model.predict(df[self.feature_extractor.features])
        pred = np.clip(pred, 0.001, 1)
        return pred

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
            self.feature_extractor, self.model = pickle.load(fi)

    def save(self, path):
        with open(path, 'wb') as fo:
            pickle.dump((self.feature_extractor, self.model), fo)
