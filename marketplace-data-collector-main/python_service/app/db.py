import os
import uuid
from psycopg2 import connect, sql
from functools import wraps
from collections import defaultdict
from typing import Callable, Dict, List, Tuple
import pandas as pd
from datetime import datetime, timedelta
import pytz
from pandas.api.types import is_datetime64_any_dtype
from config_loader import load_db_configs


_db_configs = load_db_configs()
_SOURCE_DB = _db_configs['source']
_DEST_DB = _db_configs['dest']


def _get_source_connection():
    return connect(
        dbname=_SOURCE_DB['dbname'],
        user=_SOURCE_DB['user'],
        password=_SOURCE_DB['password'],
        host=_SOURCE_DB['host'],
        port=_SOURCE_DB['port']
    )


def _get_dest_connection():
    return connect(
        dbname=_DEST_DB['dbname'],
        user=_DEST_DB['user'],
        password=_DEST_DB['password'],
        host=_DEST_DB['host'],
        port=_DEST_DB['port'],
        sslmode=_DEST_DB.get('sslmode', 'disable')
    )

def inject_source_cursor(func: Callable):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'cursor' in kwargs:
            return func(*args, **kwargs)
        conn = _get_source_connection()
        try:
            with conn.cursor() as cursor:
                result = func(*args, cursor=cursor, **kwargs)
                conn.commit()
                return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    return wrapper


def inject_dest_cursor(func: Callable):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'cursor' in kwargs:
            return func(*args, **kwargs)
        conn = _get_dest_connection()
        try:
            with conn.cursor() as cursor:
                result = func(*args, cursor=cursor, **kwargs)
                conn.commit()
                return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    return wrapper


@inject_source_cursor
def records_presented_at(table_name, date, date_column="date", **kwargs):
    cursor = kwargs.get('cursor')
    cursor.execute(
        sql.SQL('''
            SELECT COUNT(*) FROM {table}
            WHERE {date_column} = %s
        ''').format(table=sql.Identifier(table_name), date_column=sql.Identifier(date_column)),
        (date.strftime('%Y-%m-%d'),)
    )
    count = cursor.fetchone()[0]
    return count > 0


def _truncate_utf8(s, max_bytes=63):
    encoded = s.encode('utf-8')
    if len(encoded) <= max_bytes:
        return s

    truncated = encoded[:max_bytes]
    while True:
        try:
            return truncated.decode('utf-8')
        except UnicodeDecodeError:
            truncated = truncated[:-1]


@inject_source_cursor
def delete_previous_and_insert_new_postgres_table(df, table_name, date_column="date", hour_column=None, **kwargs):
    cursor = kwargs.get('cursor')

    # Получаем столбцы из БД
    cursor.execute(f"SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = %s;", (table_name,))
    db_columns_info = cursor.fetchall()
    db_columns = {col[0]: col[1] for col in db_columns_info}  # col[0] - имя столбца, col[1] - nullable

    # Преобразуем названия столбцов df
    columns_df = [col for col in df.columns]
    columns_df_percent_replaced = [_truncate_utf8(col.replace("%", "percent")) for col in columns_df]
    columns_escaped = [f'"{col}"' for col in columns_df_percent_replaced]

    # Проверяем наличие NOT NULL столбцов из БД в df
    missing_columns = [col for col, nullable in db_columns.items() if nullable == 'NO' and col not in columns_df_percent_replaced]
    all_db_columns = set([col for col, _ in db_columns.items()])
    new_columns = [col for col in columns_df_percent_replaced if col not in all_db_columns]
    if missing_columns or new_columns:
        raise ValueError(f"Следующие NOT NULL столбцы отсутствуют в DataFrame: {missing_columns}\nПрисутствуют следующие: {new_columns}")

    # Преобразование даты, если необходимо
    if is_datetime64_any_dtype(df[date_column]):
        df[date_column] = df[date_column].dt.strftime('%Y-%m-%d')

    current_date = df[date_column].iloc[0]
    delete_query = f"""
        DELETE FROM {table_name}
        WHERE "{date_column}"::date = %s
    """ + (f' and "{hour_column}"::int = %s' if hour_column else ';')
    cursor.execute(delete_query, (current_date, int(df[hour_column].iloc[0])) if hour_column else (current_date,))

    # Вставка новых записей
    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns_escaped)})
        VALUES ({', '.join(['%s' for _ in columns_df])});
    """

    for _, row in df.iterrows():
        values = [row[col] for col in columns_df]
        cursor.execute(insert_query, values)


@inject_dest_cursor
def delete_previous_and_insert_new_dest_table(df, table_name, date_column="date", hour_column=None, **kwargs):
    cursor = kwargs.get('cursor')

    # Получаем информацию о колонках таблицы
    cursor.execute(
        "SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = %s;",
        (table_name,)
    )
    db_columns_info = cursor.fetchall()
    db_columns = {col: nullable for col, nullable in db_columns_info}

    # Подготовка колонок из DataFrame
    df_columns = list(df.columns)
    df_columns_safe = [_truncate_utf8(col.replace("%", "percent")) for col in df_columns]
    columns_escaped = [f'"{col}"' for col in df_columns_safe]

    # Валидация NOT NULL колонок
    missing = [col for col, nullable in db_columns.items() if nullable == 'NO' and col not in df_columns_safe]
    extra = [col for col in df_columns_safe if col not in db_columns]
    if missing or extra:
        raise ValueError(f"NOT NULL columns missing: {missing}, extra in DF: {extra}")

    # Преобразуем дату
    if is_datetime64_any_dtype(df[date_column]):
        df[date_column] = df[date_column].dt.strftime('%Y-%m-%d')

    current_date = df[date_column].iloc[0]
    params = [current_date]
    delete_cond = f'"{date_column}"::date = %s'
    if hour_column:
        delete_cond += f' AND "{hour_column}"::int = %s'
        params.append(int(df[hour_column].iloc[0]))

    # Удаляем старые записи
    cursor.execute(
        f"DELETE FROM {table_name} WHERE {delete_cond};",
        tuple(params)
    )

    # Вставляем новые
    placeholders = ', '.join(['%s'] * len(df_columns_safe))
    insert_sql = (
        f"INSERT INTO {table_name} ({', '.join(columns_escaped)}) "
        f"VALUES ({placeholders});"
    )
    for _, row in df.iterrows():
        values = [row[col] for col in df_columns]
        cursor.execute(insert_sql, values)


@inject_source_cursor
def load_current_day_or_latest_cost_sku(**kwargs):
    cursor = kwargs.get('cursor')
    moscow_tz = pytz.timezone('Europe/Moscow')
    current_date = datetime.now(moscow_tz).date()

    cursor.execute(
        '''
        SELECT sku, date
        FROM cost
        WHERE date = (
            SELECT MAX(date)
            FROM cost
            WHERE date <= %s
        )
        ''',
        (current_date,)
    )
    rows = cursor.fetchall()

    # Если данные все еще отсутствуют, ищем последнюю доступную дату (включая будущие)
    if not rows:
        cursor.execute(
            '''
            SELECT sku, date
            FROM cost
            WHERE date = (
                SELECT MIN(date)
                FROM cost
                WHERE date > %s
            )
            ''',
            (current_date,)
        )
        rows = cursor.fetchall()

    if rows:
        sku_list = [row[0] for row in rows]
        return sku_list, rows[0][1]

    return [], None


@inject_source_cursor
def load_current_day_or_latest_cost_mp_ids(**kwargs) -> Tuple[Dict[str, List[str]], datetime]:
    cursor = kwargs.get('cursor')
    moscow_tz = pytz.timezone('Europe/Moscow')
    current_date = datetime.now(moscow_tz).date()

    def fetch_grouped_skus(target_date_query: str) -> Tuple[Dict[str, List[str]], datetime]:
        cursor.execute(f"""
            SELECT s.mp_id, s.legal_entity, c.date
            FROM cost c
            JOIN sku s ON s.seller_id = c.sku
            WHERE s.marketplace_name = 'OZON'
              AND s.mp_id IS NOT NULL
              AND c.date = ({target_date_query})
        """, (current_date,))
        rows = cursor.fetchall()

        grouped = {}
        for mp_id, legal_entity, d in rows:
            if mp_id:
                grouped.setdefault(legal_entity, []).append(str(mp_id))
        return grouped, rows[0][2] if rows else None

    # Сначала — последняя прошедшая дата
    sku_dict, result_date = fetch_grouped_skus("SELECT MAX(date) FROM cost WHERE date <= %s")

    # Если ничего не нашли — ближайшая будущая дата
    if not sku_dict:
        sku_dict, result_date = fetch_grouped_skus("SELECT MIN(date) FROM cost WHERE date > %s")

    return sku_dict, result_date


@inject_source_cursor
def load_data(table_name, **kwargs):
    cursor = kwargs.get('cursor')
    cursor.execute(
        f'''
        SELECT *
        FROM {table_name}
        '''
    )
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=column_names)
    return df



@inject_source_cursor
def load_latest_advert_ids(table_name: str, seller_code: str, cursor=None) -> list[int]:
    """
    Возвращает список advert_id (INTEGER) из таблицы table_name
    для заданного seller_code (legal_entity) за максимальный ts.
    """
    query = sql.SQL(
        "SELECT advert_id::INTEGER "
        "FROM {table} "
        "WHERE legal_entity = %s "
        "  AND ts = (SELECT MAX(ts) FROM {table} WHERE legal_entity = %s)"
    ).format(table=sql.Identifier(table_name))

    cursor.execute(query, (seller_code, seller_code))
    return [row[0] for row in cursor.fetchall()]


@inject_source_cursor
def fill_missing_dates_by_previous_day_data(table_name, date_column="date", **kwargs):
    cursor = kwargs.get('cursor')
    log_prefix = f"Table: {table_name}"

    cursor.execute(f"SELECT DISTINCT {date_column} FROM {table_name};")
    result = cursor.fetchall()

    if not result:
        return

    available_dates = sorted([row[0] for row in result])
    min_date, max_date = available_dates[0], available_dates[-1]

    all_dates = set(min_date + timedelta(days=i) for i in range((max_date - min_date).days + 1))
    missing_dates = sorted(all_dates - set(available_dates))

    if not missing_dates:
        return

    for missing_date in missing_dates:
        last_available_date = max(d for d in available_dates if d < missing_date)
        cursor.execute(f"SELECT * FROM {table_name} WHERE {date_column} = %s;", (last_available_date.strftime('%Y-%m-%d'),))
        rows = cursor.fetchall()

        if not rows:
            continue
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)

        df[date_column] = missing_date.strftime('%Y-%m-%d')

        columns_escaped = [f'"{col}"' for col in df.columns]
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns_escaped)})
            VALUES ({', '.join(['%s' for _ in columns_escaped])});
        """
        for _, row in df.iterrows():
            values = [row[col] for col in df.columns]
            cursor.execute(insert_query, values)

    missing_dates_str = ', '.join([date.strftime('%Y-%m-%d') for date in missing_dates])
    print(f"\033[91mWARNING: {log_prefix}.\033[0m Восстановлены пропущенные дни: {missing_dates_str}")


@inject_source_cursor
def fill_missing_dates_by_none(table_name, none_fields, date_column="date", empty_list_fields=None, **kwargs):
    empty_list_fields = empty_list_fields or []
    cursor = kwargs.get('cursor')
    log_prefix = f"Table: {table_name}"

    cursor.execute(f"SELECT DISTINCT {date_column} FROM {table_name};")
    result = cursor.fetchall()

    if not result:
        return

    available_dates = sorted([row[0] for row in result])
    min_date, max_date = available_dates[0], available_dates[-1]

    all_dates = set(min_date + timedelta(days=i) for i in range((max_date - min_date).days + 1))
    missing_dates = sorted(all_dates - set(available_dates))

    if not missing_dates:
        return

    for missing_date in missing_dates:
        last_available_date = max(d for d in available_dates if d < missing_date)
        cursor.execute(f"SELECT * FROM {table_name} WHERE {date_column} = %s;", (last_available_date.strftime('%Y-%m-%d'),))
        rows = cursor.fetchall()

        if not rows:
            continue
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)

        df[date_column] = missing_date.strftime('%Y-%m-%d')

        if 'uuid' in df.columns:
            df['uuid'] = [str(uuid.uuid4()) for _ in range(len(df))]

        for field in none_fields:
            if field in df.columns:
                df[field] = None

        for field in empty_list_fields:
            if field in df.columns:
                df[field] = '[]'

        columns_escaped = [f'"{col}"' for col in df.columns]
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns_escaped)})
            VALUES ({', '.join(['%s' for _ in columns_escaped])});
        """

        for _, row in df.iterrows():
            values = [row[col] for col in df.columns]
            cursor.execute(insert_query, values)

    missing_dates_str = ', '.join([date.strftime('%Y-%m-%d') for date in missing_dates])
    print(f"\033[91mWARNING: {log_prefix}.\033[0m Восстановлены пропущенные дни: {missing_dates_str}")


@inject_source_cursor
def load_newest_date(table_name, **kwargs):
    cursor = kwargs.get('cursor')
    query = sql.SQL('''
        SELECT MAX(date)::date
        FROM {table}
    ''').format(
        table=sql.Identifier(table_name)
    )

    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result and result[0] else None


@inject_source_cursor
def load_newest_date_and_hour(table_name, **kwargs):
    cursor = kwargs.get('cursor')
    query = sql.SQL('''
        SELECT date::date, MAX(hour)
        FROM {table}
        WHERE date = (SELECT MAX(date) FROM {table})
        GROUP BY date
    ''').format(
        table=sql.Identifier(table_name)
    )

    cursor.execute(query)
    result = cursor.fetchone()
    return (result[0], int(result[1])) if result and result[0] else (None, None)


@inject_source_cursor
def load_newest_date_with_all_values(table_name, column_with_all_values, **kwargs):
    cursor = kwargs.get('cursor')
    query = sql.SQL('''
        WITH unique_values AS (
            SELECT DISTINCT {column} FROM {table}
        ),
        date_counts AS (
            SELECT date::date AS record_date, COUNT(DISTINCT {column}) AS unique_count
            FROM {table}
            GROUP BY record_date
        )
        SELECT MAX(record_date)
        FROM date_counts
        WHERE unique_count = (SELECT COUNT(*) FROM unique_values)
    ''').format(
        table=sql.Identifier(table_name),
        column=sql.Identifier(column_with_all_values)
    )

    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result and result[0] else None


@inject_source_cursor
def load_oldest_date(table_name, **kwargs):
    cursor = kwargs.get('cursor')
    query = sql.SQL('''
        SELECT MIN(date)::date
        FROM {table}
    ''').format(
        table=sql.Identifier(table_name)
    )

    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result and result[0] else None


@inject_source_cursor
def load_oldest_date_and_hour(table_name, **kwargs):
    cursor = kwargs.get('cursor')
    query = sql.SQL('''
        SELECT date, MIN(hour)
        FROM {table}
        WHERE date = (SELECT MIN(date) FROM {table})
        GROUP BY date
    ''').format(
        table=sql.Identifier(table_name)
    )

    cursor.execute(query)
    result = cursor.fetchone()
    return result if result else (None, None)


@inject_source_cursor
def find_skipped_dates_grouped_by(table_name, grouped_by_column, **kwargs):
    cursor = kwargs.get('cursor')
    query = sql.SQL('''
WITH date_range AS (
    SELECT
        MIN(date) AS min_date,
        MAX(date) AS max_date
    FROM {table}
),
all_dates AS (
    SELECT
        generate_series(
            (SELECT min_date FROM date_range),
            (SELECT max_date FROM date_range),
            '1 day'::interval
        )::date AS generated_date
),
unique_groups AS (
    SELECT DISTINCT {grouped_by_column} FROM {table}
),
missing_dates AS (
    SELECT
        ad.generated_date
    FROM all_dates ad
    CROSS JOIN unique_groups ug
    LEFT JOIN {table} t
        ON ad.generated_date = t.date
        AND ug.{grouped_by_column} = t.{grouped_by_column}
    WHERE t.date IS NULL
)
SELECT DISTINCT generated_date
FROM missing_dates
ORDER BY generated_date;
    ''').format(
        table=sql.Identifier(table_name),
        grouped_by_column=sql.Identifier(grouped_by_column)
    )

    cursor.execute(query)
    result = cursor.fetchall()
    return [row[0] for row in result] if result else []


@inject_source_cursor
def find_skipped_dates_and_hours_grouped_by(table_name, grouped_by_column, **kwargs):
    cursor = kwargs.get('cursor')
    query = sql.SQL('''
WITH RECURSIVE all_dates AS (
    SELECT (SELECT MIN(date) FROM {table})::DATE AS date
    UNION ALL
    SELECT (date + INTERVAL '1 day')::DATE
    FROM all_dates
    WHERE date < (SELECT MAX(date) FROM {table})
),
hour_ranges AS (
    SELECT
        (SELECT MIN(hour) FROM {table} WHERE date = (SELECT MIN(date) FROM {table})) AS min_hour_first_date,
        (SELECT MAX(hour) FROM {table} WHERE date = (SELECT MAX(date) FROM {table})) AS max_hour_last_date
),
all_hours AS (
    SELECT date, generate_series(
        CASE
            WHEN date = (SELECT MIN(date) FROM all_dates)
            THEN (SELECT min_hour_first_date FROM hour_ranges)
            ELSE 0
        END,
        CASE
            WHEN date = (SELECT MAX(date) FROM all_dates)
            THEN (SELECT max_hour_last_date FROM hour_ranges)
            ELSE 23
        END
    ) AS hour
    FROM all_dates
),
all_legal_entities AS (
    SELECT DISTINCT {grouped_by_column} FROM {table}
),
all_date_hours AS (
    SELECT d.date, h.hour
    FROM all_dates d
    JOIN all_hours h ON d.date = h.date
),
expected_combinations AS (
    SELECT adh.date, adh.hour, ale.{grouped_by_column}
    FROM all_date_hours adh
    CROSS JOIN all_legal_entities ale
),
existing_combinations AS (
    SELECT date, hour, {grouped_by_column}
    FROM {table}
),
missing_combinations AS (
    SELECT ec.date, ec.hour
    FROM expected_combinations ec
    LEFT JOIN existing_combinations ex
    ON ec.date = ex.date AND ec.hour = ex.hour AND ec.{grouped_by_column} = ex.{grouped_by_column}
    WHERE ex.{grouped_by_column} IS NULL
    GROUP BY ec.date, ec.hour
)
SELECT date, hour FROM missing_combinations
ORDER BY date, hour;
    ''').format(
        table=sql.Identifier(table_name),
        grouped_by_column=sql.Identifier(grouped_by_column)
    )

    cursor.execute(query)
    result = cursor.fetchall()
    return [(row[0], row[1]) for row in result] if result else []


@inject_source_cursor
def get_top_keywords_by_campaign(limit: int = 50, **kwargs) -> Dict[int, List[str]]:
    """
    Возвращает словарь: ключ — ad_campaign_id, значение — список топ-N ключевых фраз
    по количеству кликов за прошлый час (МСК).

    Args:
        limit (int): Максимальное количество ключей на одну кампанию.

    Returns:
        Dict[int, List[str]]: {ad_campaign_id: [ad_keyword1, ad_keyword2, ...]}
    """
    cursor = kwargs.get("cursor")
    now = datetime.now(pytz.timezone("Europe/Moscow"))

    if now.hour == 0:
        target_date = (now - timedelta(days=1)).date()
        target_hour = 23
    else:
        target_date = now.date()
        target_hour = now.hour - 1
    query = """
        SELECT ad_campaign_id, ad_keyword, ad_keyword_clicks
        FROM wb_keyword_stats
        WHERE date = %s AND hour = %s
        ORDER BY ad_campaign_id, ad_keyword_clicks DESC
    """
    cursor.execute(query, (target_date.strftime("%Y-%m-%d"), target_hour))
    rows = cursor.fetchall()

    grouped = defaultdict(list)
    for campaign_id, keyword, clicks in rows:
        if len(grouped[campaign_id]) < limit:
            grouped[campaign_id].append(keyword)

    return dict(grouped)


@inject_source_cursor
def get_existing_legal_entities(
    table_name: str,
    date_column: str,
    group_column: str,
    date_value,
    **kwargs
) -> list:
    """
    Возвращает список уникальных значений group_column из table_name
    для заданной даты date_value.
    """
    cursor = kwargs['cursor']
    query = f'''
        SELECT DISTINCT "{group_column}"
        FROM {table_name}
        WHERE "{date_column}"::date = %s;
    '''
    cursor.execute(query, (date_value,))
    return [row[0] for row in cursor.fetchall()]


@inject_source_cursor
def delete_existing_and_insert_sales_report(
    df,
    table_name,
    date_column: str = "date",
    group_column: str = "legal_entity",
    **kwargs
):
    """
    Удаляет из таблицы только те записи, у которых:
      - колонка date_column совпадает с датой из df,
      - колонка group_column совпадает со значением из df;
    затем вставляет все строки из df.
    """
    cursor = kwargs.get('cursor')

    # 1) Получаем метаданные таблицы
    cursor.execute(
        "SELECT column_name, is_nullable "
        "FROM information_schema.columns "
        "WHERE table_name = %s;",
        (table_name,)
    )
    db_columns_info = cursor.fetchall()
    db_columns = {col: nullable for col, nullable in db_columns_info}

    # 2) Подготовка списка колонок из DataFrame
    columns_df = list(df.columns)
    columns_clean = [_truncate_utf8(col.replace("%", "percent")) for col in columns_df]
    columns_escaped = [f'"{col}"' for col in columns_clean]

    # 3) Валидация NOT NULL колонок
    missing = [
        col for col, nullable in db_columns.items()
        if nullable == 'NO' and col not in columns_clean
    ]
    extra = [col for col in columns_clean if col not in db_columns]
    if missing or extra:
        raise ValueError(
            f"NOT NULL столбцы, отсутствующие в DataFrame: {missing}; "
            f"лишние в DataFrame: {extra}"
        )

    # 4) Приведение даты к строке
    if is_datetime64_any_dtype(df[date_column]):
        df[date_column] = df[date_column].dt.strftime('%Y-%m-%d')

    current_date = df[date_column].iloc[0]
    group_value = df[group_column].iloc[0]

    # 5) DELETE с фильтрами по дате и группе
    delete_query = (
        f"DELETE FROM {table_name} "
        f"WHERE \"{date_column}\"::date = %s "
        f"AND \"{group_column}\" = %s;"
    )
    cursor.execute(delete_query, (current_date, group_value))

    # 6) Вставка новых строк
    insert_query = (
        f"INSERT INTO {table_name} ({', '.join(columns_escaped)}) "
        f"VALUES ({', '.join(['%s'] * len(columns_df))});"
    )
    for _, row in df.iterrows():
        values = [row[col] for col in columns_df]
        cursor.execute(insert_query, values)
