import psycopg2
import pandas as pd
import os

db_params = {
    "user": os.getenv("DEST_DB_USER", "aiadmin"),
    "password": os.getenv("DEST_DB_PASSWORD", "b1g8fqrgbp56ppg4uucc8jfi4"),
    "host": os.getenv("DEST_DB_HOST", "rc1a-6ic32g9da782j8kh.mdb.yandexcloud.net"),
    "port": os.getenv("DEST_DB_PORT", "6432"),
    "dbname": os.getenv("DEST_DB_NAME", "app"),
    "sslmode": os.getenv("DEST_DB_SSLMODE", "verify-full"),
    "sslrootcert": "CA.pem"
}

def get_table_data(conn, schema, table):
    """Получает данные из указанной таблицы и возвращает DataFrame"""
    query = f"SELECT * FROM {schema}.{table}"
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            df = pd.DataFrame(data, columns=columns)
            return df
    except Exception as e:
        print(f"Ошибка при получении данных из {schema}.{table}: {e}")
        return None


def main():
    schema = 'ads'
    table = 'cluster_stats'

    print("Подключаемся к PostgreSQL...")
    try:
        conn = psycopg2.connect(**db_params)
        print("Успешное подключение!")

        print(f"\nПолучаем данные из {schema}.{table}...")
        df = get_table_data(conn, schema, table)

        if df is not None and not df.empty:
            print(f"Получено {len(df)} строк из {schema}.{table}")
            return df
        else:
            print(f"Не удалось получить данные из {schema}.{table}. Возможные причины:")
            print("- Таблица не существует")
            print("- У пользователя нет прав на чтение таблицы")
            print("- Таблица пуста")

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_schema, table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s
                    AND table_name = %s;
                """, (schema, table))
                found_tables = cur.fetchall()
                if found_tables:
                    print(f"\nТаблица {schema}.{table} найдена")
                else:
                    print(f"\nТаблица {schema}.{table} не найдена")
            return None

    except Exception as e:
        print(f"Ошибка подключения: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()
            print("\nСоединение закрыто.")


if __name__ == "__main__":
    df = main()
    if df is not None:
        print("\nDataFrame создан успешно. Первые 5 строк:")
        print(df.head())