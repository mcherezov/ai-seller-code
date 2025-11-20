import os
import time
import requests
import logging
import logging.handlers
import lzma
import pickle

import schedule

from datetime import datetime, timedelta
from bidder import wb_data, wb_bidder, config, db

from bidder.models import cr_view_v1, analytics_baseline

logger = logging.getLogger(__name__)


def send_telegram_message(message):
    token = os.environ['TELEGRAM_TOKEN']
    chat_id = '-4722971752'
    send_text = f'''https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&parse_mode=Markdown&text={message}'''
    requests.get(send_text)


class ModelFactory:
    def __init__(self):
        self.model = cr_view_v1.CrViewModelV1(True)
        self.model.load('./models/cr_view_model_v1_250615T070000.pckl')

        self.fallback = analytics_baseline.AnalyticsBaselineV1()
        self.fallback.load('./models/analytics_baseline_250615T070000.pckl')

    def __call__(self, calculation_dt, nm_id, advert_id):
        def predict(cpm_1):
            try:
                return self.model.predict_one(nm_id, advert_id, calculation_dt, cpm_1)
            except ValueError:
                return self.fallback.predict_one(nm_id, advert_id, calculation_dt, cpm_1)
        return predict


def set_bids(log_in_db=True, data_fetcher=None, save_fetched=True):
    db_password = os.environ['DB_PASSWORD']
    log_db_password = os.environ['LOG_DB_PASSWORD']
    dt = datetime.now()

    if data_fetcher is None:
        calculation_dt = dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        calculation_dt = data_fetcher.dt

    logger.info(f'calc_dt={calculation_dt} Start bid calculation')

    start_time = time.time()
    log_db = db.LogDB(log_db_password)

    # TODO Hard code for selected campaign
    if data_fetcher is None:
        data_fetcher = wb_data.DataFetcher(calculation_dt, db_password)
        logger.info(f'calc_dt={calculation_dt} data is fetched')
        if save_fetched:
            with lzma.open(f'fetch_{dt}_{calculation_dt}.pckl.lzma', 'wb') as fo:
                pickle.dump(data_fetcher, fo)

    processed_data = wb_data.process(data_fetcher)
    campaigns = wb_data.get_campaigns(calculation_dt, processed_data)

    models_factory = ModelFactory()
    for campaign in campaigns:
        campaign.cr_view_model = models_factory(calculation_dt, campaign.nm_id, campaign.advert_id)

    bidder = wb_bidder.Bidder()
    cpm_1_info = bidder.compute_bids(campaigns)
    duration_sec = time.time() - start_time

    logger.info(f'calc_dt={calculation_dt} cpm_1_info count: {len(cpm_1_info)}')
    for cpm_1, info in cpm_1_info:
        nm_id = info['nm_id']
        advert_id = info['advert_id']

        if cpm_1 is None:
            cpm_1000 = config.MIN_CPM_1000
            logger.warning(f'calc_dt={calculation_dt} bid for nm_id={nm_id}, advert_id={advert_id} not calculated. Use: {cpm_1000}')
        else:
            cpm_1000 = cpm_1 * 1000

        if log_in_db:
            log_db.insert(dt, calculation_dt, nm_id, advert_id, duration_sec, cpm_1000, info)

    if log_in_db:
        log_db.commit()
    logger.info(f'Calculation for {calculation_dt} done')


def set_bids_no_except(save_fetched=True):
    try:
        set_bids(save_fetched)
    except Exception:
        # Send to telegram
        logger.exception('Set bids exception', exc_info=True)


def main():
    rootLogger = logging.getLogger()
    logFormatter = logging.Formatter('%(asctime)s\t%(filename)s:%(lineno)d\t%(levelname)s\t%(message)s')
    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(logFormatter)

    file_handler = logging.handlers.TimedRotatingFileHandler('bidder.log', when='D')
    file_handler.setFormatter(logFormatter)

    rootLogger.addHandler(file_handler)
    rootLogger.addHandler(stderr_handler)
    rootLogger.setLevel(logging.DEBUG)

    schedule.every().hour.at(":55").do(set_bids_no_except)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
