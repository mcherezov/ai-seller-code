import os
import psycopg2
from psycopg2 import sql
import psycopg2
import pandas as pd
import logging

db_params = {
    "user": os.getenv("DEST_DB_USER", "aiadmin"),
    "password": os.getenv("DEST_DB_PASSWORD", "b1g8fqrgbp56ppg4uucc8jfi4"),
    "host": os.getenv("DEST_DB_HOST", "rc1a-6ic32g9da782j8kh.mdb.yandexcloud.net"),
    "port": os.getenv("DEST_DB_PORT", "6432"),
    "dbname": os.getenv("DEST_DB_NAME", "app"),
    "sslmode": os.getenv("DEST_DB_SSLMODE", "verify-full"),
    "sslrootcert": "CA.pem"
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('../keyword_optimizer.log')
    ]
)
logger = logging.getLogger(__name__)


def get_cluster_stats_dataframe(db_params: dict) -> pd.DataFrame:
    """Получает данные из таблицы ads.cluster_stats и возвращает DataFrame для AdvancedKeywordOptimizer"""
    schema = 'ads'
    table = 'cluster_stats'
    conn = None

    try:
        logger.info("Подключаемся к PostgreSQL...")
        conn = psycopg2.connect(**db_params)
        logger.info("Успешное подключение!")

        query = f"""
            SELECT 
                ad_id,
                date,
                cluster_name AS keyword,
                cluster_name AS advertId,
                views AS shows,
                clicks,
                sum AS spend
            FROM {schema}.{table}
        """
        logger.info(f"Получаем данные из {schema}.{table}...")

        df = pd.read_sql_query(query, conn)

        if not df.empty:
            logger.info(f"Получено {len(df)} строк из {schema}.{table}")
            required_columns = ['date', 'keyword', 'advertId', 'shows', 'clicks', 'spend']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Отсутствуют необходимые колонки: {missing_columns}")
                return pd.DataFrame()

            df['date'] = pd.to_datetime(df['date'])
            df['shows'] = df['shows'].astype(int)
            df['clicks'] = df['clicks'].astype(int)
            df['spend'] = df['spend'].astype(float)

            df['ctr'] = (df['clicks'] / df['shows'].replace(0, 1)).clip(upper=1.0)

            return df[required_columns + ['ctr']]
        else:
            logger.warning(f"Данные из {schema}.{table} не получены. Возможные причины:")
            logger.warning("- Таблица не существует")
            logger.warning("- У пользователя нет прав на чтение таблицы")
            logger.warning("- Таблица пуста")

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_schema, table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s
                    AND table_name = %s;
                """, (schema, table))
                found_tables = cur.fetchall()
                if found_tables:
                    logger.info(f"Таблица {schema}.{table} найдена")
                else:
                    logger.error(f"Таблица {schema}.{table} не найдена")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Ошибка: {str(e)}")
        return pd.DataFrame()

    finally:
        if conn is not None:
            conn.close()
            logger.info("Соединение закрыто.")


if __name__ == "__main__":
    df = get_cluster_stats_dataframe(db_params)
    if not df.empty:
        logger.info("\nDataFrame создан успешно. Первые 5 строк:")
        logger.info(df.head())