from dotenv import load_dotenv

load_dotenv()
import pandas as pd
import json
from openai import OpenAI
import os
from sqlalchemy import create_engine, text
import psycopg2
import numpy as np
import requests
import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
chat_id = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram_message(message: str, bot_token: str, chat_id: str):
    """Отправка сообщения в Telegram."""
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'Markdown'
        }
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            logger.info("Уведомление успешно отправлено в Telegram")
        else:
            logger.error(f"Ошибка отправки в Telegram: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления в Telegram: {str(e)}")


def create_db_engine():
    connection_params = {
        'host': 'rc1a-6ic32g9da782j8kh.mdb.yandexcloud.net',
        'port': '6432',
        'database': 'app',
        'user': 'aiadmin',
        'password': 'b1g8fqrgbp56ppg4uucc8jfi4'
    }

    connection_string = "postgresql://{user}:{password}@{host}:{port}/{database}".format(**connection_params)
    print(
        f"Подключение: postgresql://{connection_params['user']}:***@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}")

    try:
        engine = create_engine(
            connection_string,
            connect_args={"sslmode": "require"},
            pool_size=5,
            max_overflow=10,
            pool_timeout=30
        )
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("Подключение к базе данных успешно!")
        return engine
    except Exception as e:
        print(f"Ошибка создания подключения: {e}")
        return None


def clean_markdown_from_json(content):
    if content.startswith("```json"):
        content = content[7:]
    elif content.startswith("```"):
        content = content[3:]

    if content.endswith("```"):
        content = content[:-3]

    if "```" in content:
        lines = content.split("\n")
        cleaned_lines = []
        in_code_block = False

        for line in lines:
            if line.startswith("```"):
                in_code_block = not in_code_block
                continue
            if not in_code_block or (in_code_block and not line.startswith("```")):
                cleaned_lines.append(line)

        content = "\n".join(cleaned_lines)

    return content.strip()


def convert_numpy_types(obj):
    if isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    else:
        return obj


def analyze_campaign_clusters(campaign_data):
    client = OpenAI(base_url="https://openrouter.ai/api/v1",
                    api_key=os.getenv("api_key"))
    campaign_data = convert_numpy_types(campaign_data)

    first_entry = campaign_data[0]
    ad_id = first_entry['ads_id']
    mp_sku = first_entry['mp_sku']
    category = first_entry['category']
    subject_name = first_entry['subject_name']
    brand_name = first_entry['brand_name']
    current_cpm = first_entry['cpm']
    end_prices = first_entry['total_statistics']['Price_full']
    net_price = first_entry['total_statistics']['price_self']
    seller_commission_rate = first_entry['total_statistics']['seller_commission_rate']
    commercial_margins = first_entry['total_statistics']['commercial_margins']
    commercial_margins_percent = first_entry['total_statistics']['commercial_margins_percent']
    roas = first_entry['total_statistics']['roas']
    total_revenue = first_entry['total_statistics']['total_revenue']
    daily_sales_rate = first_entry['total_statistics']['daily_sales_rate']
    sales_velocity = first_entry['total_statistics']['sales_velocity']
    total_cost_per_item = first_entry['total_statistics']['total_cost_per_item']
    current_stock = first_entry['total_statistics']['current_stock']
    total_stock_coverage_days = first_entry['total_statistics']['total_stock_coverage_days']

    campaign_json = json.dumps(campaign_data, ensure_ascii=False, indent=2)

    prompt = f"""
        Ты эксперт по рекламе и продажам на маркетплейсе Wildberries. Твоя главная задача состоит в том, чтобы повышать абсолютную маржинальность товара. Тебе будет дан обширный набор данных о товаре и соответствующей ему рекламной кампании. Эти данные отражают тенденции за последние 30 дней. 
        Далее ниже будет описана структура данных и приведены инструкции, на которые ты должен будешь опираться в своем анализе.  Проанализируй все, что будет тебе приведено и дай конкретные рекомендации.

        ДАННЫЕ КАМПАНИИ:
        {campaign_json}

        СТРУКТУРА ДАННЫХ:
        - dt_val: дата статистики
        - ads_id: ID рекламной кампании
        - mp_sku: ID товара в кампании
        - category: категория товара
        - subject_name: название товара
        - brand_name: бренд товара
        - current_cpm: текущая ставка 
        - total_statistics: общая статистика рекламной кампании (просмотры, клики, заказы, CTR, расходы), а также цена товара, себестоимость товара, коммерческая маржа в рублях, оборачиваемость товара, остатки на складах и другие метрики
        - current_price: текущая цена товара
        - current_net_price: текущая себестоимость товара
        - seller_commission_rate: ставка комиссии маркетплейса
        - commercial_margins: абсолютная коммерческая маржа (в рублях) - один из ключевых показателей, который ты должен использовать для анализа
        - commercial_margins_percent: коммерческая маржа (в процентах %) 
        - roas: ROAS (возврат на рекламные расходы)  
        - total_revenue: общая выручка от продаж товара
        - daily_sales_rate: среднее количество продаж товара в день
        - sales_velocity:  Скорость, с которой товар продаётся (то есть, сколько раз оборачивается запас за период)
        - total_cost_per_item: полная стоимость расходов на товар с учетом себестоимости, рекламных расходов и других затрат
        - current_stock: текущие остатки товара на складе
        - total_stock_coverage_days: количество дней, на которое хватит текущих остатков
        - keyword_clusters: кластеры ключевых слов с их статистикой (просмотры, клики, CTR, расходы) 

        КРИТЕРИИ ЭФФЕКТИВНОСТИ ПО КАТЕГОРИЯМ:

        **Массовые товары (одежда, аксессуары, товары для дома до 3000 руб):**
        - CTR > 1.2% = хорошо, 0.8-1.2% = приемлемо
        - CPC < 30 руб = отлично, 30-50 руб = приемлемо
        - Конверсия > 6% = отлично, 3-6% = приемлемо

        Средний сегмент (3000-10000 руб):
        - CTR > 0.8% = хорошо, 0.5-0.8% = приемлемо
        - CPC < 50 руб = отлично, 50-80 руб = приемлемо
        - Конверсия > 4% = отлично, 2-4% = приемлемо

        Премиум товары (>10000 руб):
        - CTR > 0.5% = хорошо, 0.3-0.5% = приемлемо
        - CPC < 100 руб = отлично, 100-150 руб = приемлемо
        - Конверсия > 2% = отлично, 1-2% = приемлемо


    ОБРАБОТКА НЕПОЛНЫХ ДАННЫХ:
    - Если отсутствуют данные за 30 дней (менее 14 дней статистики): пометить как "insufficient_data" и дать осторожные рекомендации
    - Если отсутствует ROAS или конверсия: использовать CTR и CPC как основные метрики
    - Если нет данных по коммерческой марже: использовать себестоимость и цену товара для оценки маржинальности
    - Для новых кампаний (менее 7 дней статистики): рекомендовать "тестовый режим" с невысокими ставками



        ЗАДАЧИ:

        1. ОПТИМИЗАЦИЯ CPM СТАВОК: Определить оптимальную ставку CPM на основе данных
            Для решения этой задачи необходимо обратить особое внимание на текущую ставку, roas и коммерческую маржу. Если маржа отрицательная, то необходима оптимизация ставки CPM, чтобы снизить расходы на рекламу.
            Учитывай при этом, что снижение ставки CPM может привести к снижению видимости товара и, как следствие, уменьшению количества продаж и увеличению убытков. Важно найти баланс между ставкой и маржой. 
            При положительной марже текущую ставку CPM можно оставить без изменений или даже увеличить, если товар продается хорошо. 


        2. КЛАСТЕРЫ КЛЮЧЕВЫХ СЛОВ: Какие кластеры эффективны, а какие следует исключить 
            Здесь необходимо проанализировать CTR по каждому кластеру ключевых слов. Если CTR по кластеру ниже 1%, а CPC выше 50 рублей, то это плохой кластер, который нужно отключить. 
            Если CTR выше 1%, а конверсия ниже 5%, то это приемлемый кластер, но его нужно оптимизировать. 
            Если CTR выше 1% и конверсия выше 5%, то это хороший кластер, который можно оставить без изменений или даже увеличить ставки.


        4. ЦЕНООБРАЗОВАНИЕ НА ТОВАР: Установить оптимальную цену в соответствии с данными о текущей цене товара: 
            его себестоимости, его маржинальности, полной стоимости товара с учетом себестоимости, рекламных расходов и других затрат (total_cost_per_item).
            В регуляции ценообразования задача состоит, как и в задаче по оптимизации CPM ставок, же в том, чтобы повысить маржинальность товара. 
            Если товар убыточен, нужно поднимать цену, чтобы была положительная маржа. Даже если мы рискуем просесть в продажах. 

        5. РЕКОМЕНДАЦИИ ПО УПРАВЛЕНИЮ ОСТАТКАМИ

        Здесь тебе необходимо проанализировать данные по остаткам товара на складе, количестве дней, на которые хватит текущих остатков, скорости продажи товара (оборачиваемость) в день. Обязательно учитывай также и маржинальность товара: нет смысла закупать убыточный товар, особенно в больших количествах. 
        Возможно, если товара совсем мало (1-3 штуки), то нужно закупить небольшой объем, но в целом при убыточности товара нужно скорее отказаться от закупок и заниматься оптимизации cpm и оптимизацией ценообразования. 
        Допустим, если товара остается менее, чем на 5 дней, то нужно повысить цену, чтобы снизить количество заказов.  


        ФОРМАТ ОТВЕТА (JSON):
        {{
        "campaigns": [
            {{
            "ad_id": "{ad_id}",
            "mp_sku": "{mp_sku}",
            "category": "{category}",
            "subject_name": "{subject_name}",
            "brand_name": "{brand_name}",
            "current_cpm": {current_cpm},
            "current_price": {end_prices},
            "current_net_price": {net_price}, 
            "seller_commission_rate": {seller_commission_rate},
            "commercial_margins": {commercial_margins},
            "commercial_margins_percent": {commercial_margins_percent},
            "roas": {roas},
            "total_revenue": {total_revenue},
            "daily_sales_rate": {daily_sales_rate},
            "sales_velocity": {sales_velocity},
            "total_cost_per_item": {total_cost_per_item},
            "current_stock": {current_stock},
            "total_stock_coverage_days": {total_stock_coverage_days},
            "recommendation": {{
            "campaign_action": "increase|decrease|maintain|stop",
            "new_cpm_rate": число_или_null,
            "price_action": "increase|decrease|maintain|stop",
            "new_price": число (float),
            "stock_advice": "закупать|не закупать; кол-во (n штук)",
            "reasoning": "обоснование на основе анализа данных"
          }},
          "cluster_advice": {{
            "effective_clusters": ["название_кластера1", "название_кластера2"],
            "ineffective_clusters": ["название_кластера3", "название_кластера4"],
            "reasoning": "объяснение рекомендаций по кластерам"
          }}
        }}
      ],
      "summary": {{
        "total_campaigns_analyzed": 1,
        "campaigns_to_optimize": 0 или 1,
        "campaigns_to_stop": 0 или 1
      }}
    }}

    ВАЖНО: 
    1. Давай конкретные цифры ставок, а не общие советы
    2. СТРОГО соблюдай формат JSON. Не используй никаких пояснений вне JSON структуры
    3. Учитывай специфику категории товара при анализе данных
    4. Учитывай тренды за весь период (30 дней)
    5. Анализируй эффективность каждого кластера ключевых слов отдельно
    6. При недостатке данных четко указывай на это и давай осторожные рекомендации
    7. Всегда указывай возможные риски изменений CPM и цен

    """
    try:

        response = client.chat.completions.create(
            model="deepseek/deepseek-chat-v3-0324:free",
            messages=[
                {"role": "system",
                 "content": "Ты эксперт по рекламе и аналитике данных. Отвечай ТОЛЬКО в формате JSON без комментариев."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=4000
        )

        raw_content = response.choices[0].message.content

        cleaned_content = raw_content
        if cleaned_content.startswith("```json"):
            cleaned_content = cleaned_content[7:]
        if cleaned_content.endswith("```"):
            cleaned_content = cleaned_content[:-3]
        cleaned_content = cleaned_content.strip()

        try:
            result = json.loads(cleaned_content)
            print("Анализ рекламной кампании успешно завершен")
            return result

        except json.JSONDecodeError as json_err:
            print(f"Ошибка при разборе JSON: {json_err}")
            print(f"Первые 200 символов ответа: {raw_content[:200]}...")
            print(f"Первые 200 символов очищенного ответа: {cleaned_content[:200]}...")
            return None

    except Exception as e:
        print(f"Ошибка при обращении к LLM по API: {str(e)}")
        return None


def save_analysis_to_db_extended(analysis_result):
    if not analysis_result:
        print("Нет данных для сохранения в БД")
        return False

    host = 'rc1a-6ic32g9da782j8kh.mdb.yandexcloud.net'
    port = 6432
    database = 'app'
    user = 'aiadmin'
    password = 'b1g8fqrgbp56ppg4uucc8jfi4'
    table_name = 'algo.llm_recommendations'

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            sslmode='require'
        )

        cursor = conn.cursor()
        cursor.execute("CREATE SCHEMA IF NOT EXISTS algo;")

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            data JSONB, 

            ad_id BIGINT,
            mp_sku BIGINT,
            category VARCHAR(255),
            subject_name VARCHAR(255),
            brand_name VARCHAR(255),
            current_cpm DECIMAL(10,2),
            current_price DECIMAL(10,2),
            seller_commission_rate DECIMAL(10,2),
            current_net_price DECIMAL(10,2),
            commercial_margins DECIMAL(10,2),
            commercial_margins_percent DECIMAL (10,2), 
            roas DECIMAL(10,2),
            total_revenue DECIMAL(10,2),
            daily_sales_rate DECIMAL(10,2),
            sales_velocity DECIMAL(10,2),
            total_cost_per_item DECIMAL(10,2),
            current_stock INTEGER,
            total_stock_coverage_days DECIMAL(10,2),


            campaign_action VARCHAR(20),  -- increase|decrease|maintain|stop
            new_cpm_rate DECIMAL(10,2),
            price_action VARCHAR(20),
            new_price DECIMAL(10,2),
            reasoning TEXT,

            total_campaigns_analyzed INTEGER,
            campaigns_to_optimize INTEGER,
            campaigns_to_stop INTEGER,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)

        campaigns = analysis_result.get('campaigns', [])
        summary = analysis_result.get('summary', {})

        campaign = campaigns[0] if campaigns else {}
        recommendation = campaign.get('recommendation', {})

        cursor.execute(f"""
            INSERT INTO {table_name} 
            (data, ad_id, mp_sku, category, subject_name, brand_name, current_cpm, current_price, current_net_price, seller_commission_rate, 
             commercial_margins, commercial_margins_percent, roas, total_revenue, daily_sales_rate, sales_velocity,
             total_cost_per_item, current_stock, total_stock_coverage_days,
             campaign_action, new_cpm_rate, price_action, new_price, stock_advice,
             reasoning, total_campaigns_analyzed, campaigns_to_optimize, 
             campaigns_to_stop)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            json.dumps(analysis_result),
            campaign.get('ad_id'),
            campaign.get('mp_sku'),
            campaign.get('category'),
            campaign.get('subject_name'),
            campaign.get('brand_name'),
            campaign.get('current_cpm'),
            campaign.get('current_price'),
            campaign.get('current_net_price'),
            campaign.get('seller_commission_rate'),
            campaign.get('commercial_margins'),
            campaign.get('commercial_margins_percent'),
            campaign.get('roas'),
            campaign.get('total_revenue'),
            campaign.get('daily_sales_rate'),
            campaign.get('sales_velocity'),
            campaign.get('total_cost_per_item'),
            campaign.get('current_stock'),
            campaign.get('total_stock_coverage_days'),
            recommendation.get('campaign_action'),
            recommendation.get('new_cpm_rate'),
            recommendation.get('price_action'),
            recommendation.get('new_price'),
            recommendation.get('stock_advice'),
            recommendation.get('reasoning'),
            summary.get('total_campaigns_analyzed'),
            summary.get('campaigns_to_optimize'),
            summary.get('campaigns_to_stop')
        ))

        conn.commit()
        print(f"✅ Данные успешно сохранены в таблицу {table_name}")
        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Ошибка при сохранении в БД: {e}")
        return False


def add_missing_columns_if_needed():
    host = 'rc1a-6ic32g9da782j8kh.mdb.yandexcloud.net'
    port = 6432
    database = 'app'
    user = 'aiadmin'
    password = 'b1g8fqrgbp56ppg4uucc8jfi4'
    table_name = 'algo.llm_recommendations'

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            sslmode='require'
        )

        cursor = conn.cursor()

        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'algo' 
            AND table_name = 'llm_recommendations'
        """)

        existing_columns = [row[0] for row in cursor.fetchall()]

        if not existing_columns:
            print("⚠️ Таблица не существует, будет создана автоматически")
            cursor.close()
            conn.close()
            return True

        print(f"📋 Найдено {len(existing_columns)} существующих колонок")

        required_columns = [
            ("ad_id", "BIGINT"),
            ("mp_sku", "BIGINT"),
            ("category", "VARCHAR(255)"),
            ("subject_name", "VARCHAR(255)"),
            ("brand_name", "VARCHAR(255)"),
            ("current_cpm", "DECIMAL(10,2)"),
            ("current_price", "DECIMAL(10,2)"),
            ("current_net_price", "DECIMAL(10,2)"),
            ("seller_commission_rate", "DECIMAL(10,2)"),
            ("commercial_margins", "DECIMAL(10,2)"),
            ("commercial_margins_percent", "DECIMAL(10,2)"),
            ("roas", "DECIMAL(10,2)"),
            ("total_revenue", "DECIMAL(10,2)"),
            ("daily_sales_rate", "DECIMAL(10,2)"),
            ("sales_velocity", "DECIMAL(10,2)"),
            ("total_cost_per_item", "DECIMAL(10,2)"),
            ("current_stock", "INTEGER"),
            ("total_stock_coverage_days", "DECIMAL(10,2)"),
            ("campaign_action", "VARCHAR(20)"),
            ("new_cpm_rate", "DECIMAL(10,2)"),
            ("price_action", "VARCHAR(20)"),
            ("new_price", "DECIMAL(10,2)"),
            ("stock_advice", "TEXT"),
            ("reasoning", "TEXT"),
            ("total_campaigns_analyzed", "INTEGER"),
            ("campaigns_to_optimize", "INTEGER"),
            ("campaigns_to_stop", "INTEGER")
        ]

        for column_name, column_type in required_columns:
            if column_name not in existing_columns:
                try:
                    cursor.execute(f"""
                        ALTER TABLE {table_name} 
                        ADD COLUMN {column_name} {column_type}
                    """)
                    print(f"✅ Добавлена колонка: {column_name}")
                except Exception as e:
                    print(f"❌ Ошибка при добавлении {column_name}: {e}")
            else:
                print(f"⚪ Колонка {column_name} уже существует")

        conn.commit()
        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Ошибка при проверке/добавлении колонок: {e}")
        return False


def generate_human_readable_message(analysis_result: Dict[str, Any]) -> str:
    if not analysis_result:
        return "❌ Не удалось получить рекомендации по кампании"

    message_parts = []

    message_parts.append("📊 **АНАЛИЗ РЕКЛАМНОЙ КАМПАНИИ**")
    message_parts.append("═" * 30)

    campaigns = analysis_result.get('campaigns', [])
    if campaigns:
        campaign = campaigns[0]

        ad_id = campaign.get('ad_id', 'N/A')
        mp_sku = campaign.get('mp_sku', 'N/A')

        ad_url = f"https://cmp.wildberries.ru/campaigns/edit/{ad_id}" if ad_id != 'N/A' else None
        product_url = f"https://www.wildberries.ru/catalog/{mp_sku}/detail.aspx?targetUrl=GP" if mp_sku != 'N/A' else None

        message_parts.append("📊 **ТЕКУЩИЕ ПОКАЗАТЕЛИ:**")
        message_parts.append(f"Рекламная кампания: **[Открыть]({ad_url})**")
        message_parts.append(f"Товар: **[Посмотреть на WB]({product_url})**")
        message_parts.append(f"Артикул рекламной кампании: {ad_id}")
        message_parts.append(f"Артикул товара: {mp_sku}")
        message_parts.append(f"Категория товара: **{campaign.get('category', 'N/A')}**")
        message_parts.append(f"Название товара: **{campaign.get('subject_name', 'N/A')}**")
        message_parts.append(f"Бренд: **{campaign.get('brand_name', 'N/A')}**")
        message_parts.append(f"💰 CPM: {campaign.get('current_cpm', 0)} руб.")
        message_parts.append(f"🏷️ Цена: {campaign.get('current_price', 0)} руб.")
        message_parts.append(f"Коммерческая маржа (в рублях): {campaign.get('commercial_margins', 0)} руб.")
        message_parts.append(f"Коммерческая маржа (в процентах): {campaign.get('commercial_margins_percent', 0)} %")
        message_parts.append(f"📦 Остаток на складах: {campaign.get('current_stock', 0)} шт.")
        message_parts.append("")

        recommendation = campaign.get('recommendation', {})
        message_parts.append("🎯 **РЕКОМЕНДАЦИИ:**")

        campaign_action_emoji = {
            'increase': '📈 УВЕЛИЧИТЬ СТАВКУ',
            'decrease': '📉 СНИЗИТЬ СТАВКУ',
            'maintain': '➡️ СОХРАНИТЬ',
            'stop': '🛑 ОСТАНОВИТЬ'
        }

        price_action_emoji = {
            'increase': '📈 Повысить цену',
            'decrease': '📉 Снизить цену',
            'maintain': '➡️ СОХРАНИТЬ ЦЕНУ'
        }

        campaign_action = recommendation.get('campaign_action', 'maintain')
        message_parts.append(f"Кампания: **{campaign_action_emoji.get(campaign_action, campaign_action)}**")

        if recommendation.get('new_cpm_rate'):
            message_parts.append(f"💰 Новая ставка: **{recommendation.get('new_cpm_rate')} руб.**")

        price_action = recommendation.get('price_action', 'maintain')
        message_parts.append(f"Цена: **{price_action_emoji.get(price_action, price_action)}**")

        if recommendation.get('new_price'):
            message_parts.append(f"🏷️ Новая цена: **{recommendation.get('new_price')} руб.**")

        if recommendation.get('stock_advice'):
            message_parts.append(f"📦 Закупки: **{recommendation.get('stock_advice')}**")

        if recommendation.get('reasoning'):
            message_parts.append("")
            message_parts.append("💡 **ОБОСНОВАНИЕ:**")
            message_parts.append(recommendation.get('reasoning'))

        cluster_advice = campaign.get('cluster_advice', {})
        if cluster_advice.get('effective_clusters'):
            message_parts.append("")
            message_parts.append("✅ **Эффективные кластеры:**")
            message_parts.append(", ".join(cluster_advice.get('effective_clusters', [])))

        if cluster_advice.get('ineffective_clusters'):
            message_parts.append("")
            message_parts.append("❌ **Неэффективные кластеры:**")
            message_parts.append(", ".join(cluster_advice.get('ineffective_clusters', [])))

    message_parts.append("")
    message_parts.append(f"🕐 {datetime.now().strftime('%d.%m.%Y в %H:%M')}")

    return "\n".join(message_parts)


def process_analysis(campaign_data):
    print("🔧 Проверяем структуру базы данных...")
    add_missing_columns_if_needed()

    analysis_result = analyze_campaign_clusters(campaign_data)

    if not analysis_result:
        return None, "Не удалось получить анализ"

    db_success = save_analysis_to_db_extended(analysis_result)

    telegram_message = generate_human_readable_message(analysis_result)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f'campaign_analysis_{timestamp}.json', 'w', encoding='utf-8') as f:
        json.dump(analysis_result, f, ensure_ascii=False, indent=2)

    return analysis_result, telegram_message


def get_campaign_data(engine, advert_id):
    query = f'''
WITH campaign_metrics AS (
    SELECT
        CAST(date AS DATE) AS dt_val,
        advert_id,
        nm_id,
        SUM(views) AS campaign_views,
        SUM(clicks) AS campaign_clicks,
        SUM(ctr) AS campaign_ctr,
        SUM(cpc) AS campaign_cpc,
        SUM(CAST(cost AS FLOAT)) AS campaign_cost,
        SUM(carts) AS campaign_carts,
        SUM(orders) AS campaign_orders,
        SUM(items) AS total_items_sold,
        CASE 
            WHEN SUM(clicks) > 0 
            THEN SUM(orders)::float / SUM(clicks)::float 
            ELSE 0 
        END AS conversion_rate
    FROM silver.wb_adv_product_stats_1d
    WHERE date >= NOW() - INTERVAL '30 days'
        AND advert_id = {advert_id}
    GROUP BY
        CAST(date AS DATE),
        advert_id,
        nm_id
),
campaign_cpm AS (
    SELECT
        CAST(run_dttm AS DATE) AS dt_val,
        advert_id,
        subject_id,
        AVG(cpm_current) AS campaign_cpm_rate
    FROM silver.wb_adv_product_rates_1d
    WHERE run_dttm >= NOW() - INTERVAL '30 days'
        AND advert_id = {advert_id}
    GROUP BY
        CAST(run_dttm AS DATE),
        advert_id,
        subject_id
),
commission_data AS (
    SELECT DISTINCT
        subject_id,
        seller_commission
    FROM core.individual_commissions
),
sales_metrics AS (
    SELECT
        nm_id,
        SUM(price_with_discount) AS total_revenue
    FROM silver.wb_order_items_1d
    WHERE date >= NOW() - INTERVAL '7 days'
    GROUP BY nm_id
),
stock_data AS (
    SELECT
        nm_id,
        MAX(quantity) AS current_stock,
        AVG(quantity) AS average_stock_7d
    FROM silver.wb_stocks_1d
    WHERE date >= NOW() - INTERVAL '7 days'
    GROUP BY nm_id
),
sales_metrics_7d AS (
    SELECT
        nm_id,
        SUM(orders) / 7.0 AS avg_daily_orders_7d
    FROM silver.wb_adv_product_stats_1d
    WHERE date >= NOW() - INTERVAL '7 days'
        AND advert_id = {advert_id}
    GROUP BY nm_id
),
product_data AS (
    SELECT DISTINCT 
        oi.nm_id,
        FIRST_VALUE(oi.category) OVER (PARTITION BY oi.nm_id ORDER BY oi.date DESC) AS category,
        FIRST_VALUE(oi.subject) OVER (PARTITION BY oi.nm_id ORDER BY oi.date DESC) AS subject_name,
        FIRST_VALUE(oi.brand) OVER (PARTITION BY oi.nm_id ORDER BY oi.date DESC) AS brand_name,
        FIRST_VALUE(oi.barcode) OVER (PARTITION BY oi.nm_id ORDER BY oi.date DESC) AS barcode,
        FIRST_VALUE(oi.price_with_discount) OVER (PARTITION BY oi.nm_id ORDER BY oi.date DESC) AS current_price,
        FIRST_VALUE(pc.self_cost) OVER (PARTITION BY oi.nm_id ORDER BY oi.date DESC) AS self_cost
    FROM silver.wb_order_items_1d oi
    LEFT JOIN core.product_costs pc ON oi.barcode = pc.barcode
    WHERE oi.date >= NOW() - INTERVAL '30 days'
),
keyword_metrics AS (
    SELECT
        CAST(date AS DATE) AS dt_val,
        advert_id,
        keyword,
        SUM(views) AS keyword_views,
        SUM(clicks) AS keyword_clicks,
        SUM(CAST(cost AS FLOAT)) AS keyword_ad_cost
    FROM silver.wb_adv_keyword_stats_1d
    WHERE date >= NOW() - INTERVAL '30 days'
        AND advert_id = {advert_id}
    GROUP BY
        CAST(date AS DATE),
        advert_id,
        keyword
)
SELECT
    cm.dt_val,
    cm.advert_id,
    cm.nm_id,
    pd.barcode,
    pd.category,
    pd.subject_name,
    pd.brand_name,
    cm.campaign_views,
    cm.campaign_clicks,
    cm.campaign_ctr,
    cm.campaign_cpc,
    cm.campaign_cost,
    cm.campaign_carts,
    cm.campaign_orders,
    cm.total_items_sold,
    cm.conversion_rate,
    cpm.campaign_cpm_rate,
    pd.current_price,
    pd.self_cost,
    cd.seller_commission AS seller_commission_rate,
    CASE
        WHEN pd.current_price > 0 AND cm.campaign_orders > 0
        THEN (
            pd.current_price::NUMERIC 
            - pd.self_cost::NUMERIC 
            - pd.current_price::NUMERIC * cd.seller_commission
            - (cm.campaign_cost::NUMERIC / cm.campaign_orders::NUMERIC) 
            - (pd.current_price::NUMERIC * 0.0135) 
            - 100
        )
        ELSE 0
    END AS commercial_margins_absolute,
    CASE
        WHEN pd.current_price > 0 AND cm.campaign_orders > 0
        THEN (
            (pd.current_price::NUMERIC
             - pd.self_cost::NUMERIC
             - pd.current_price::NUMERIC * cd.seller_commission
             - (cm.campaign_cost::NUMERIC / cm.campaign_orders::NUMERIC)
             - (pd.current_price::NUMERIC * 0.0135)
             - 100
            ) / pd.current_price::NUMERIC * 100
        )
        ELSE 0
    END AS commercial_margins_percent,
    CASE
        WHEN sm7.avg_daily_orders_7d > 0 AND sd.average_stock_7d > 0
        THEN sd.average_stock_7d / sm7.avg_daily_orders_7d
        ELSE NULL
    END AS sales_velocity,
    ROUND(
        CASE
            WHEN cm.total_items_sold > 0 AND cm.campaign_cost > 0
            THEN (
                pd.self_cost::NUMERIC 
                + pd.current_price::NUMERIC * cd.seller_commission
                + 100 
                + pd.current_price::NUMERIC * 0.0135 
                + (cm.campaign_cost::NUMERIC / cm.total_items_sold::NUMERIC)
            )
            ELSE pd.self_cost::NUMERIC
        END, 2
    ) AS total_cost_per_item,
    CASE 
        WHEN cm.campaign_cost > 0 
        THEN (cm.campaign_orders * pd.current_price) / cm.campaign_cost 
        ELSE 0 
    END AS roas,
    sm.total_revenue,
    sd.current_stock,
    CASE 
        WHEN cm.campaign_orders > 0 
        THEN cm.campaign_orders::float / 30.0
        ELSE 0 
    END AS daily_sales_rate,
    CASE
        WHEN sm7.avg_daily_orders_7d > 0 AND sd.current_stock > 0
        THEN sd.current_stock::float / sm7.avg_daily_orders_7d
        ELSE NULL
    END AS total_stock_coverage_days,
    km.keyword AS keyword_name,
    km.keyword_views,
    km.keyword_clicks,
    CASE 
        WHEN km.keyword_views > 0 
        THEN km.keyword_clicks::float / km.keyword_views::float 
        ELSE 0 
    END AS keyword_ctr,
    km.keyword_ad_cost
FROM campaign_metrics cm
LEFT JOIN campaign_cpm cpm ON cm.dt_val = cpm.dt_val AND cm.advert_id = cpm.advert_id
LEFT JOIN commission_data cd ON cpm.subject_id = cd.subject_id
LEFT JOIN sales_metrics sm ON cm.nm_id = sm.nm_id
LEFT JOIN stock_data sd ON cm.nm_id = sd.nm_id
LEFT JOIN sales_metrics_7d sm7 ON cm.nm_id = sm7.nm_id
LEFT JOIN product_data pd ON cm.nm_id = pd.nm_id
LEFT JOIN keyword_metrics km ON cm.advert_id = km.advert_id AND cm.dt_val = km.dt_val
ORDER BY cm.dt_val DESC, km.keyword;
    '''

    try:
        data = pd.read_sql(query, engine)
        return data
    except Exception as e:
        print(f"Ошибка при выполнении SQL запроса для кампании {advert_id}: {e}")
        return None


def main():
    engine = create_db_engine()
    if not engine:
        print("Ошибка: Не удалось создать подключение к БД")
        return

    pd.set_option('display.max_columns', None)

    campaign_ids = [26970394, 26661406]

    for advert_id in campaign_ids:
        print(f"\n=== Обработка кампании {advert_id} ===")

        try:
            data = get_campaign_data(engine, advert_id)

            if data is None or data.empty:
                print(f"Нет данных для кампании {advert_id}")
                continue

            print(f"Получено {len(data)} записей для кампании {advert_id}")
            print("Колонки:", data.columns.tolist())
            print("Первые несколько записей:")
            print(data.head())

            group_cols = [
                'dt_val', 'advert_id', 'nm_id', 'category',
                'subject_name', 'brand_name',
                'campaign_cpm_rate', 'campaign_views', 'campaign_clicks',
                'campaign_orders', 'campaign_ctr', 'campaign_cost',
                'current_price', 'self_cost', 'seller_commission_rate', 'commercial_margins_absolute',
                'commercial_margins_percent',
                'roas', 'total_revenue', 'daily_sales_rate',
                'sales_velocity', 'total_cost_per_item',
                'current_stock', 'total_stock_coverage_days',
            ]

            output = []
            for key_values, grp in data.groupby(group_cols):
                (
                    dt_val, ad_id, mp_sku,
                    category, subject_name, brand_name,
                    cpm, views, clicks,
                    orders, ctr, cost,
                    current_price, self_cost, seller_commission_rate,
                    commercial_margins, commercial_margins_percent, roas,
                    total_revenue, daily_sales_rate,
                    sales_velocity, total_cost_per_item,
                    current_stock, total_stock_coverage_days

                ) = key_values

                keyword_clusters = {
                    row['keyword_name']: {
                        'views': int(row['keyword_views']) if pd.notna(row['keyword_views']) else 0,
                        'clicks': int(row['keyword_clicks']) if pd.notna(row['keyword_clicks']) else 0,
                        'ctr': round(float(row['keyword_ctr']), 4) if pd.notna(row['keyword_ctr']) else 0.0,
                        'cost': round(float(row['keyword_ad_cost']), 4) if pd.notna(row['keyword_ad_cost']) else 0.0
                    }
                    for _, row in grp.iterrows()
                    if row['keyword_name'] is not None and pd.notna(row['keyword_name'])
                }

                output.append({
                    'dt_val': dt_val.isoformat(),
                    'ads_id': int(ad_id),
                    'mp_sku': int(mp_sku),
                    'category': category,
                    'subject_name': subject_name,
                    'brand_name': brand_name,
                    'cpm': float(cpm),
                    'total_statistics': {
                        'views': int(views),
                        'clicks': int(clicks),
                        'orders': int(orders),
                        'ctr': round(float(ctr), 4),
                        'cost': round(float(cost), 4),
                        'Price_full': round(float(current_price), 1),
                        'price_self': round(float(self_cost), 1),
                        'seller_commission_rate': round(float(seller_commission_rate), 4),
                        'commercial_margins': round(float(commercial_margins), 4),
                        'commercial_margins_percent': round(float(commercial_margins_percent), 4),
                        'roas': round(float(roas), 4),
                        'total_revenue': round(float(total_revenue), 4),
                        'daily_sales_rate': round(float(daily_sales_rate), 4),
                        'sales_velocity': round(float(sales_velocity), 4),
                        'total_cost_per_item': round(float(total_cost_per_item), 4),
                        'current_stock': int(current_stock),
                        'total_stock_coverage_days': round(float(total_stock_coverage_days), 4)
                    },
                    'keyword_clusters': keyword_clusters
                })

            if not output:
                print(f"Нет данных для обработки для кампании {advert_id}")
                continue

            analysis_result, telegram_message = process_analysis(output)

            if analysis_result and telegram_message:
                print(f"\n=== Отправка результатов в Telegram ===")
                bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
                chat_id = '-1002895549641'
                if bot_token and chat_id:
                    send_telegram_message(telegram_message, bot_token, chat_id)
                else:
                    print("Telegram bot token или chat_id не найдены")

        except Exception as e:
            print(f"Ошибка при обработке кампании {advert_id}: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()