import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

log_dir = Path('../logs')
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_dir / 'export_to_excel.log')]
)
logger = logging.getLogger(__name__)

def load_db_config():
    env_path = Path('../config') / '.env'
    if not env_path.exists():
        logger.error(f"File .env not found at {env_path}")
        return {}
    load_dotenv(dotenv_path=env_path)
    cert_path = Path('../') / 'CA.pem'
    if not cert_path.exists():
        logger.error(f"SSL root certificate file not found at {cert_path}")
    db_config = {
        'host': os.getenv('DEST_DB_HOST'),
        'port': os.getenv('DEST_DB_PORT', '5432'),
        'name': os.getenv('DEST_DB_NAME'),
        'user': os.getenv('DEST_DB_USER'),
        'password': os.getenv('DEST_DB_PASSWORD'),
        'sslmode': os.getenv('DEST_DB_SSLMODE'),
        'sslrootcert': str(cert_path) if cert_path.exists() else None
    }
    logger.debug(f"Loaded config: {db_config}")
    missing_params = [key for key, value in db_config.items() if not value and key != 'sslrootcert']
    if missing_params:
        logger.error(f"Missing parameters: {', '.join(missing_params)}")
        return {}
    return db_config

# def get_product_costs_dataframe(start_date: str = None, end_date: str = None, seller_id: int = None, sku: str = None):
#     """Retrieve placeholder data (sebest cost is hardcoded in optimizer)."""
#     try:
#         logger.info("Starting retrieval of product_costs table")
#         db_config = load_db_config()
#         if not db_config:
#             logger.error("Invalid database configuration")
#             return None
#
#         conn_string = (
#             f"postgresql://{db_config['user']}:{db_config['password']}@"
#             f"{db_config['host']}:{db_config['port']}/{db_config['name']}?sslmode={db_config['sslmode']}"
#             f"{'&sslrootcert=' + db_config['sslrootcert'] if db_config.get('sslrootcert') else ''}"
#         )
#         engine = create_engine(conn_string)
#
#         query = "SELECT sku, date, seller_id FROM core.product_costs"
#         params = {}
#         conditions = []
#         if start_date:
#             conditions.append("date >= :start_date")
#             params['start_date'] = start_date
#         if end_date:
#             conditions.append("date <= :end_date")
#             params['end_date'] = end_date
#         if seller_id is not None:
#             conditions.append("seller_id = :seller_id")
#             params['seller_id'] = seller_id
#         if sku:
#             conditions.append("sku = :sku")
#             params['sku'] = sku
#         if conditions:
#             query += " WHERE " + " AND ".join(conditions)
#         query += " ORDER BY date, sku"
#
#         with engine.connect() as conn:
#             df = pd.read_sql(text(query), conn, params=params, index_col='date')
#             df = df[['sku', 'seller_id']]
#
#         logger.info("Data retrieved successfully")
#         return df
#
#     except Exception as e:
#         logger.error(f"Error: {str(e)}")
#         return None

def get_ad_stats_dataframe(start_date: str = None, end_date: str = None, campaign_id: str = None, product_id: str = None):
    """Retrieve ad stats data from silver.wb_ad_stats_products_1d with cost."""
    try:
        logger.info("Starting retrieval of wb_ad_stats_products_1d table")
        db_config = load_db_config()
        if not db_config:
            logger.error("Cannot proceed due to invalid database configuration")
            return None

        conn_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['name']}?sslmode={db_config['sslmode']}"
            f"{'&sslrootcert=' + db_config['sslrootcert'] if db_config.get('sslrootcert') else ''}"
        )
        engine = create_engine(conn_string)

        query = "SELECT campaign_id, product_id, date, revenue, items, cost FROM silver.wb_ad_stats_products_1d"
        params = {}
        conditions = []
        if start_date:
            conditions.append("date >= :start_date")
            params['start_date'] = start_date
        if end_date:
            conditions.append("date <= :end_date")
            params['end_date'] = end_date
        if campaign_id:
            conditions.append("campaign_id = :campaign_id")
            params['campaign_id'] = campaign_id
        if product_id:
            conditions.append("product_id = :product_id")
            params['product_id'] = product_id
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY date, campaign_id, product_id"

        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params, index_col='date')
            total_revenue = df['revenue'].sum()
            total_items = df['items'].sum()
            avg_price = total_revenue / total_items if total_items > 0 else 0
            df['revenue_per_item'] = df['revenue'].div(df['items'].replace(0, float('nan')))
            df = df[['campaign_id', 'product_id', 'revenue', 'items', 'cost', 'revenue_per_item']]
            df['avg_price'] = avg_price

        logger.info("Data retrieved successfully")
        return df

    except Exception as e:
        logger.error(f"Error during retrieval: {str(e)}")
        return None

# def get_ad_campaign_stats_dataframe(start_date: str = None, end_date: str = None, ad_id: int = None, mp_sku: int = None):
#     """Retrieve minimal data from ads.ad_campaign_stat (only ad_id and mp_sku)."""
#     try:
#         logger.info("Starting retrieval of ad_campaign_stat table")
#         db_config = load_db_config()
#         if not db_config:
#             logger.error("Invalid database configuration")
#             return None
#
#         conn_string = (
#             f"postgresql://{db_config['user']}:{db_config['password']}@"
#             f"{db_config['host']}:{db_config['port']}/{db_config['name']}?sslmode={db_config['sslmode']}"
#             f"{'&sslrootcert=' + db_config['sslrootcert'] if db_config.get('sslrootcert') else ''}"
#         )
#         engine = create_engine(conn_string)
#
#         query = "SELECT ad_id, mp_sku FROM ads.ad_campaign_stat"
#         params = {}
#         conditions = []
#         if start_date:
#             conditions.append("DATE(updated_at) >= :start_date")
#             params['start_date'] = start_date
#         if end_date:
#             conditions.append("DATE(updated_at) <= :end_date")
#             params['end_date'] = end_date
#         if ad_id is not None:
#             conditions.append("ad_id = :ad_id")
#             params['ad_id'] = ad_id
#         if mp_sku is not None:
#             conditions.append("mp_sku = :mp_sku")
#             params['mp_sku'] = mp_sku
#         if conditions:
#             query += " WHERE " + " AND ".join(conditions)
#         query += " ORDER BY ad_id"
#
#         with engine.connect() as conn:
#             df = pd.read_sql(text(query), conn, params=params, index_col='ad_id')
#             df = df[['ad_id', 'mp_sku']]
#
#         logger.info("Data retrieved successfully")
#         return df
#
#     except Exception as e:
#         logger.error(f"Error: {str(e)}")
#         return None

def get_cluster_stats_dataframe(start_date: str = None, end_date: str = None, ad_id: int = None, cluster_name: str = None):
    """Retrieve cluster stats data from ads.cluster_stats with aggregated CPC."""
    try:
        logger.info("Starting retrieval of cluster_stats table")
        db_config = load_db_config()
        if not db_config:
            logger.error("Invalid database configuration")
            return None

        conn_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['name']}?sslmode={db_config['sslmode']}"
            f"{'&sslrootcert=' + db_config['sslrootcert'] if db_config.get('sslrootcert') else ''}"
        )
        engine = create_engine(conn_string)

        query = "SELECT ad_id, date, cluster_name, sum, clicks FROM ads.cluster_stats"
        params = {}
        conditions = []
        if start_date:
            conditions.append("DATE(date) >= :start_date")
            params['start_date'] = start_date
        if end_date:
            conditions.append("DATE(date) <= :end_date")
            params['end_date'] = end_date
        if ad_id is not None:
            conditions.append("ad_id = :ad_id")
            params['ad_id'] = ad_id
        if cluster_name:
            conditions.append("cluster_name = :cluster_name")
            params['cluster_name'] = cluster_name
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY date, ad_id"

        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params, index_col='date')
            total_sum = df['sum'].sum()
            total_clicks = df['clicks'].sum()
            avg_cluster_cpc = total_sum / total_clicks if total_clicks > 0 else 0
            df['cpc'] = df['sum'].div(df['clicks'].replace(0, float('nan')))
            df = df[['ad_id', 'cluster_name', 'cpc', 'clicks']]
            df['avg_cluster_cpc'] = avg_cluster_cpc

        logger.info("Data retrieved successfully")
        return df

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return None