import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging
from typing import List, Optional, Dict
from combo import WildberriesOptimizer
from bandit import UCBBandit
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('wb_optimizer.log')
    ]
)
logger = logging.getLogger(__name__)

DEFAULT_DAYS = 7
DEFAULT_MIN_VIEWS = 5
DEFAULT_MIN_CTR = 0.005
DEFAULT_MAX_CPC = 25.0
DEFAULT_DAILY_BUDGET = 1000.0

class WildberriesOptimizerApp:
    def __init__(self):
        self.initialize_session_state()
        # Настраиваем кастомный логгер для перехвата сообщений
        self.setup_logger()

    def initialize_session_state(self):
        defaults = {
            'api_key': '',
            'campaign_ids': [],
            'selected_campaign_id': '',
            'start_date': (datetime.now().date() - timedelta(days=DEFAULT_DAYS)).isoformat(),
            'end_date': (datetime.now().date() - timedelta(days=1)).isoformat(),
            'min_views': DEFAULT_MIN_VIEWS,
            'min_ctr': DEFAULT_MIN_CTR,
            'max_cpc': DEFAULT_MAX_CPC,
            'optimization_results': {},
            'filtered_dfs': {},
            'api_initialized': False,
            'campaign_list': [],
            'optimizer': None,
            'bandit_results': {},
            'bandit_stats': {},
            'log_messages': []  # Для хранения сообщений логов
        }
        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value

    def setup_logger(self):
        # Создаём кастомный Handler для Streamlit
        class StreamlitHandler(logging.Handler):
            def emit(self, record):
                msg = self.format(record)
                if "Found rows with clicks > shows" in msg:
                    st.session_state.log_messages.append(msg)

        # Добавляем кастомный Handler к логгеру
        handler = StreamlitHandler()
        handler.setLevel(logging.WARNING)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger = logging.getLogger(__name__)
        logger.addHandler(handler)

    def setup_sidebar(self):
        with st.sidebar:
            st.header("⚙️ Настройки")

            with st.expander("API Wildberries", expanded=True):
                api_key = st.text_input(
                    "API ключ",
                    value=st.session_state.api_key,
                    type="password"
                )
                if st.button("Проверить API ключ", key="check_api"):
                    self.check_api_key(api_key)

            with st.expander("Кампании"):
                if st.session_state.api_initialized:
                    campaign_list = st.session_state.campaign_list
                    if not campaign_list:
                        st.warning("Кампании не найдены. Проверьте API ключ.")
                    else:
                        campaign_options = [(str(cid), f"Кампания {cid}") for cid in campaign_list]
                        selected_campaign = st.selectbox(
                            "Выберите кампанию",
                            options=[f"{name} (ID: {cid})" for cid, name in campaign_options],
                            index=0,
                            key="campaign_select"
                        )
                        selected_campaign_id = selected_campaign.split("ID: ")[-1].strip(")")
                        st.session_state.selected_campaign_id = selected_campaign_id
                else:
                    st.warning("Введите и проверьте API ключ для загрузки кампаний")

            with st.expander("Параметры оптимизации"):
                start_date = st.date_input(
                    "Начальная дата",
                    value=datetime.strptime(st.session_state.start_date, '%Y-%m-%d').date(),
                    min_value=datetime.now().date() - timedelta(days=365),
                    max_value=datetime.now().date() - timedelta(days=1)
                )
                end_date = st.date_input(
                    "Конечная дата",
                    value=datetime.strptime(st.session_state.end_date, '%Y-%m-%d').date(),
                    min_value=datetime.now().date() - timedelta(days=365),
                    max_value=datetime.now().date() - timedelta(days=1)
                )
                if start_date > end_date:
                    st.error("Начальная дата не может быть позже конечной!")
                    return

                min_views = st.number_input(
                    "Минимальное количество показов",
                    min_value=0, value=st.session_state.min_views
                )
                min_ctr = st.number_input(
                    "Минимальный CTR (%)",
                    min_value=0.0, max_value=100.0,
                    value=st.session_state.min_ctr * 100, step=0.1
                ) / 100
                max_cpc = st.number_input(
                    "Максимальный CPC (₽)",
                    min_value=0.0, value=st.session_state.max_cpc, step=0.1
                )

            if st.button("Применить настройки", key="apply_settings"):
                self.apply_settings(api_key, start_date.isoformat(), end_date.isoformat(), min_views, min_ctr, max_cpc)

            if st.button("Сбросить кэш", key="clear_cache"):
                st.cache_data.clear()
                st.success("Кэш очищен!")

            st.markdown("---")
            st.header("📥 Экспорт")
            self.download_results()

    def check_api_key(self, api_key: str):
        try:
            optimizer = WildberriesOptimizer(
                token=api_key,
                min_views=st.session_state.min_views,
                min_ctr=st.session_state.min_ctr,
                max_cpc=st.session_state.max_cpc
            )
            _, campaign_list = optimizer.api.get_campaigns_count()
            logger.info(f"Campaign list received: {campaign_list}")
            if not campaign_list:
                st.warning("Кампании не найдены. Проверьте API ключ или доступ.")
                logger.warning("Campaign list is empty")
            else:
                st.session_state.campaign_list = campaign_list
                st.session_state.api_initialized = True
                st.session_state.api_key = api_key
                st.session_state.optimizer = optimizer
                st.success(f"API ключ валиден! Загружено {len(campaign_list)} кампаний.")
                logger.info(f"Loaded {len(campaign_list)} campaigns")
        except Exception as e:
            st.error(f"Ошибка проверки API ключа: {str(e)}")
            logger.error(f"API key validation error: {str(e)}")
            st.session_state.api_initialized = False
            st.session_state.optimizer = None

    def apply_settings(self, api_key: str, start_date: str, end_date: str, min_views: int, min_ctr: float, max_cpc: float):
        if not api_key:
            st.error("Введите API ключ")
            logger.error("API key is empty")
            return

        try:
            reinitialize = (
                    st.session_state.optimizer is None or
                    st.session_state.api_key != api_key or
                    st.session_state.min_views != min_views or
                    st.session_state.min_ctr != min_ctr or
                    st.session_state.max_cpc != max_cpc
            )
            if reinitialize:
                logger.info("Reinitializing optimizer due to changed settings")
                st.session_state.optimizer = WildberriesOptimizer(
                    token=api_key,
                    min_views=min_views,
                    min_ctr=min_ctr,
                    max_cpc=max_cpc
                )
            st.session_state.api_key = api_key
            st.session_state.start_date = start_date
            st.session_state.end_date = end_date
            st.session_state.min_views = min_views
            st.session_state.min_ctr = min_ctr
            st.session_state.max_cpc = max_cpc
            st.session_state.api_initialized = True
            _, campaign_list = st.session_state.optimizer.api.get_campaigns_count()
            logger.info(f"Campaign list after settings: {campaign_list}")
            st.session_state.campaign_list = campaign_list
            st.success(f"Настройки применены! Загружено {len(campaign_list)} кампаний.")
            logger.info(f"Settings applied, loaded {len(campaign_list)} campaigns")
        except Exception as e:
            st.error(f"Ошибка инициализации: {str(e)}")
            logger.error(f"Initialization error: {str(e)}")
            st.session_state.api_initialized = False
            st.session_state.optimizer = None

    @st.cache_data
    def run_optimization(_self, campaign_id: str, start_date: str, end_date: str) -> tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        if st.session_state.optimizer is None:
            logger.error("Optimizer is not initialized")
            st.error("Оптимизатор не инициализирован. Проверьте API ключ и настройки.")
            return None, None

        try:
            campaign_id = int(campaign_id)
            logger.info(f"Running optimization for campaign {campaign_id} from {start_date} to {end_date}")
            optimization_result = st.session_state.optimizer.optimize_campaign(campaign_id, start_date=start_date,
                                                                               end_date=end_date)
            filtered_df = st.session_state.optimizer.filter_and_update_campaign(campaign_id, start_date=start_date,
                                                                                end_date=end_date)
            if optimization_result is None or optimization_result.empty:
                logger.warning(f"No optimization results for campaign {campaign_id}")
                st.warning(
                    f"Нет данных для оптимизации кампании {campaign_id}. Проверьте данные API или настройки (min_views={st.session_state.min_views}).")
            else:
                logger.info(f"Raw optimization result for {campaign_id}: {optimization_result.head().to_dict()}")
            if filtered_df is None or filtered_df.empty:
                logger.warning(f"No filtered data for campaign {campaign_id}")
            return optimization_result, filtered_df
        except Exception as e:
            logger.error(f"Optimization error for campaign {campaign_id}: {str(e)}")
            st.error(f"Ошибка оптимизации кампании {campaign_id}: {str(e)}")
            return None, None

    @st.cache_data
    def run_bandit(_self, campaign_ids: List[str], start_date: str, end_date: str, iterations: int, reward_metric: str,
                   daily_budget: float, max_cpm_change: float = 0.3) -> tuple[List[Dict], Dict]:
        if st.session_state.optimizer is None:
            logger.error("Optimizer is not initialized")
            st.error("Оптимизатор не инициализирован. Проверьте API ключ и настройки.")
            return [], {}

        try:
            campaign_info = st.session_state.optimizer.api.get_campaigns_info([int(cid) for cid in campaign_ids])
            if campaign_info is None or not campaign_info:
                logger.error(f"get_campaigns_info вернул {campaign_info} для {len(campaign_ids)} кампаний")
                st.error("Не удалось получить информацию о кампаниях. Проверьте API или список кампаний.")
                return [], {}

            campaign_info_dict = {str(campaign.get("advertId")): campaign for campaign in campaign_info if
                                  campaign.get("advertId")}

            active_campaigns = []
            current_time = datetime.now().isoformat()
            wb_api = st.session_state.optimizer.api

            for cid in campaign_ids:
                campaign = campaign_info_dict.get(str(cid))
                if campaign:
                    campaign_status = campaign.get("status")
                    end_time = campaign.get("endTime")
                    if campaign_status not in [7, 11] and (not end_time or end_time > current_time):
                        active_campaigns.append(str(cid))
                        logger.info(
                            f"Кампания {cid} добавлена для бандита: status={campaign_status}, endTime={end_time}")
                    else:
                        logger.warning(f"Кампания {cid} исключена: status={campaign_status}, endTime={end_time}")
                else:
                    logger.warning(f"Не удалось получить информацию о кампании {cid}")

            if not active_campaigns:
                logger.error("Нет активных кампаний для бандита")
                st.error("Нет активных кампаний для анализа бандитом.")
                return [], {}

            bandit = UCBBandit(arms=active_campaigns, reward_metric=reward_metric, daily_budget=daily_budget,
                               max_cpm_change=max_cpm_change)
            recommendations = []

            for i in range(iterations):
                recommendation = bandit.recommend_action(
                    wb_api=wb_api,
                    start_date=start_date,
                    end_date=end_date,
                    max_days=30,
                    campaign_info_dict=campaign_info_dict
                )
                if recommendation:
                    arm = recommendation['arm']
                    current_cpm = recommendation['current_cpm']
                    recommended_cpm = recommendation['recommended_cpm']

                    if current_cpm and recommended_cpm:
                        cpm_diff = recommended_cpm - current_cpm
                        if abs(cpm_diff) < 1.0:
                            cpm_adjustment = "Оставить без изменений"
                        elif cpm_diff > 0:
                            cpm_adjustment = f"Увеличить на {cpm_diff:.2f} руб."
                        else:
                            cpm_adjustment = f"Уменьшить на {-cpm_diff:.2f} руб."
                        initial_cpm = bandit.initial_cpms.get(arm, current_cpm)
                        max_change = initial_cpm * max_cpm_change
                        if abs(recommended_cpm - initial_cpm) > max_change:
                            logger.warning(
                                f"Рекомендуемый CPM для {arm} ({recommended_cpm:.2f}) превышает предел {max_cpm_change * 100}% от исходного ({initial_cpm:.2f})")
                    else:
                        cpm_adjustment = "Недостаточно данных"

                    recommendation['cpm_adjustment'] = cpm_adjustment
                    logger.info(
                        f"Итерация {i + 1}: {recommendation['recommendation']}, Текущий CPM={current_cpm:.2f}, Рекомендуемый CPM={recommended_cpm:.2f}, {cpm_adjustment}")
                    recommendations.append(recommendation)

            stats = bandit.get_arm_stats()
            return recommendations, stats
        except Exception as e:
            logger.error(f"Bandit error: {str(e)}")
            st.error(f"Ошибка выполнения бандита: {str(e)}")
            return [], {}

    def download_results(self):
        if st.session_state.optimization_results:
            for campaign_id, result in st.session_state.optimization_results.items():
                if result is not None and not result.empty:
                    csv = result.to_csv(index=False)
                    st.download_button(
                        label=f"Скачать результаты оптимизации (ID: {campaign_id})",
                        data=csv,
                        file_name=f"wb_optimization_{campaign_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key=f"download_optimization_{campaign_id}"
                    )

        if st.session_state.bandit_results:
            recommendations_df = pd.DataFrame(st.session_state.bandit_results)
            if not recommendations_df.empty:
                csv = recommendations_df.to_csv(index=False)
                st.download_button(
                    label="Скачать рекомендации бандита",
                    data=csv,
                    file_name=f"bandit_recommendations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key="download_bandit_recommendations"
                )

        if st.session_state.bandit_stats:
            stats_df = pd.DataFrame.from_dict(st.session_state.bandit_stats, orient='index')
            stats_df.index.name = 'campaign_id'
            csv = stats_df.to_csv()
            st.download_button(
                label="Скачать статистику бандита",
                data=csv,
                file_name=f"bandit_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key="download_bandit_stats"
            )

    def display_optimization_tab(self):
        if st.button("🚀 Запустить оптимизацию", key="run_optimization"):
            if not self.check_ready_for_optimization():
                return
            with st.spinner("Оптимизация кампании..."):
                # Очищаем предыдущие сообщения перед запуском
                st.session_state.log_messages = []
                campaign_id = st.session_state.selected_campaign_id
                optimization_result, filtered_df = self.run_optimization(
                    campaign_id,
                    st.session_state.start_date,
                    st.session_state.end_date
                )
                st.session_state.optimization_results = {campaign_id: optimization_result}
                st.session_state.filtered_dfs = {campaign_id: filtered_df}
                if optimization_result is not None and not optimization_result.empty:
                    st.success(f"Оптимизация кампании {campaign_id} завершена!")
                else:
                    st.error(
                        f"Не удалось выполнить оптимизацию для кампании {campaign_id}. Проверьте данные кампании или настройки (min_views={st.session_state.min_views}).")

        if not st.session_state.optimization_results:
            st.info("Запустите оптимизацию для отображения результатов")
            return

        st.header("📊 Анализ кампании")

        campaign_id = st.session_state.selected_campaign_id
        optimization_result = st.session_state.optimization_results.get(campaign_id)
        if optimization_result is None or optimization_result.empty:
            st.warning(
                f"Нет данных для кампании {campaign_id}. Проверьте API или настройки кампании (min_views={st.session_state.min_views}).")
        else:
            filtered_df = st.session_state.filtered_dfs.get(campaign_id)
            if filtered_df is None or filtered_df.empty:
                st.warning(f"Нет отфильтрованных данных для кампании {campaign_id}. Запустите оптимизацию заново.")
            else:
                st.subheader(f"Кампания ID: {campaign_id}")
                with st.container():
                    st.markdown(
                        """
                        <style>
                        .card {
                            background-color: #f0f2f6; 
                            padding: 15px; 
                            border-radius: 8px; 
                            margin: 8px 0; 
                            text-align: center; 
                            width: 100%; 
                            box-sizing: border-box; 
                            font-size: 16px; 
                            min-height: 60px; 
                            display: flex; 
                            align-items: center; 
                            justify-content: center; 
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                        }
                        .card i {
                            margin-right: 8px;
                        }
                        .savings-card {
                            background-color: #d4edda; 
                            padding: 20px; 
                            border-radius: 8px; 
                            margin: 8px 0; 
                            text-align: center; 
                            width: 100%; 
                            box-sizing: border-box; 
                            font-size: 18px; 
                            min-height: 80px; 
                            display: flex; 
                            align-items: center; 
                            justify-content: center; 
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                        }
                        </style>
                        """,
                        unsafe_allow_html=True)
                    cols = st.columns(3)
                    total_stats = {
                        'shows': optimization_result['total_shows'].sum(),
                        'clicks': optimization_result['total_clicks'].sum(),
                        'spend': optimization_result['total_spend'].sum(),
                        'ctr': (optimization_result['total_clicks'].sum() / optimization_result[
                            'total_shows'].sum() * 100) if optimization_result['total_shows'].sum() > 0 else 0.0,
                        'cpm': (optimization_result['total_spend'].sum() / optimization_result[
                            'total_shows'].sum() * 1000) if optimization_result['total_shows'].sum() > 0 else 0.0,
                        'cpc': (optimization_result['total_spend'].sum() / optimization_result['total_clicks'].sum()) if
                        optimization_result['total_clicks'].sum() > 0 else 0.0
                    }
                    with cols[0]:
                        st.markdown('<div class="card"><i class="fas fa-eye"></i> Показы: {:,}</div>'.format(
                            int(total_stats['shows'])), unsafe_allow_html=True)
                        st.markdown('<div class="card"><i class="fas fa-mouse-pointer"></i> Клики: {:,}</div>'.format(
                            int(total_stats['clicks'])), unsafe_allow_html=True)
                    with cols[1]:
                        st.markdown('<div class="card"><i class="fas fa-chart-line"></i> CTR: {:.2f}%</div>'.format(
                            total_stats['ctr']), unsafe_allow_html=True)
                        st.markdown('<div class="card"><i class="fas fa-dollar-sign"></i> СРМ: {:.2f} ₽</div>'.format(
                            total_stats['cpm']), unsafe_allow_html=True)
                    with cols[2]:
                        st.markdown('<div class="card"><i class="fas fa-dollar-sign"></i> СРС: {:.2f} ₽</div>'.format(
                            total_stats['cpc']), unsafe_allow_html=True)
                        st.markdown(
                            '<div class="card"><i class="fas fa-money-bill-wave"></i> Затраты: {:.2f} ₽</div>'.format(
                                total_stats['spend']), unsafe_allow_html=True)

                    savings = optimization_result[optimization_result['decision'].isin(['stop', 'reduce_bid'])]['total_spend'].sum()
                    savings_text = f"Потенциальная экономия от удаления ненужных кластеров за неделю: {savings:.2f} ₽" if savings > 0 else "Нет экономии от удаления"
                    st.markdown(
                        f'<div class="savings-card"><i class="fas fa-sack-dollar"></i> {savings_text}</div>',
                        unsafe_allow_html=True
                    )

                st.subheader("Данные по кластерам")
                with st.expander("🔍 Фильтры", expanded=True):
                    decision_filter = st.multiselect(
                        "Фильтр по решению",
                        options=optimization_result['decision'].unique(),
                        default=optimization_result['decision'].unique(),
                        key=f"decision_filter_{campaign_id}"
                    )
                    keyword_search = st.text_input(
                        "Поиск по кластеру",
                        key=f"keyword_search_{campaign_id}"
                    )

                    filtered_result = optimization_result
                    if decision_filter:
                        filtered_result = filtered_result[filtered_result['decision'].isin(decision_filter)]
                    if keyword_search:
                        filtered_result = filtered_result[
                            filtered_result['advertId'].str.contains(keyword_search, case=False, na=False)]

                table_data = filtered_result.copy()
                table_data = table_data.rename(columns={
                    'advertId': 'Кластер',
                    'total_shows': 'Показы',
                    'total_clicks': 'Клики',
                    'ctr': 'CTR',
                    'total_spend': 'Затраты',
                    'cpm': 'СРМ',
                    'efficiency_score': 'Оценка эффективности',
                    'decision': 'Решение'
                })
                table_data['СРС'] = (table_data['Затраты'] / table_data['Клики']).replace([float('inf'), -float('inf')], 0)
                st.dataframe(
                    table_data[
                        ['Кластер', 'Показы', 'Клики', 'CTR', 'СРМ', 'СРС', 'Затраты', 'Оценка эффективности', 'Решение']],
                    use_container_width=True,
                    height=400,
                    column_config={
                        "CTR": st.column_config.NumberColumn(format="%.2f%%"),
                        "СРМ": st.column_config.NumberColumn(format="%.2f ₽"),
                        "СРС": st.column_config.NumberColumn(format="%.2f ₽"),
                        "Затраты": st.column_config.NumberColumn(format="%.2f ₽"),
                        "Оценка эффективности": st.column_config.NumberColumn(format="%.2f")
                    }
                )

                insufficient_data_clusters = filtered_result[filtered_result['decision'] == 'insufficient_data'].copy()
                if not insufficient_data_clusters.empty:
                    st.subheader("Кластеры с недостаточными данными")
                    insufficient_table = insufficient_data_clusters[['advertId', 'total_shows', 'total_clicks', 'total_spend']].rename(
                        columns={
                            'advertId': 'Кластер',
                            'total_shows': 'Показы',
                            'total_clicks': 'Клики',
                            'total_spend': 'Затраты'
                        })
                    insufficient_table['СРС'] = (insufficient_table['Затраты'] / insufficient_table['Клики']).replace(
                        [float('inf'), -float('inf')], 0)
                    st.dataframe(
                        insufficient_table[['Кластер', 'Показы', 'Клики', 'Затраты', 'СРС']],
                        use_container_width=True,
                        height=200,
                        column_config={
                            "Затраты": st.column_config.NumberColumn(format="%.2f ₽"),
                            "СРС": st.column_config.NumberColumn(format="%.2f ₽")
                        }
                    )
                else:
                    st.info("Нет кластеров с недостаточными данными.")

                # Отображаем проблемы с данными (кликов больше, чем показов)
                st.subheader("Проблемы с данными")
                if st.session_state.log_messages:
                    for msg in st.session_state.log_messages:
                        st.warning(msg)
                else:
                    st.info("Проблем с данными (кликов больше, чем показов) не обнаружено.")

                st.subheader("Тренды по дням")
                if filtered_df is not None and not filtered_df.empty:
                    filtered_df['date'] = pd.to_datetime(filtered_df['date'])
                    trend_df = filtered_df.groupby('date').agg({
                        'shows': 'sum',
                        'clicks': 'sum',
                        'spend': 'sum',
                        'ctr': 'mean'
                    }).reset_index()

                    with st.expander("📅 Фильтрация по дате"):
                        min_date = trend_df['date'].min().to_pydatetime().date()
                        max_date = trend_df['date'].max().to_pydatetime().date()
                        trend_start_date = st.date_input(
                            "Начальная дата тренда",
                            value=min_date,
                            min_value=min_date,
                            max_value=max_date,
                            key=f"trend_start_date_{campaign_id}"
                        )
                        trend_end_date = st.date_input(
                            "Конечная дата тренда",
                            value=max_date,
                            min_value=min_date,
                            max_value=max_date,
                            key=f"trend_end_date_{campaign_id}"
                        )
                        if trend_start_date > trend_end_date:
                            st.error("Начальная дата не может быть позже конечной!")
                            return
                        trend_df = trend_df[
                            (trend_df['date'].dt.date >= trend_start_date) &
                            (trend_df['date'].dt.date <= trend_end_date)
                            ]

                    metric_options = ['Показы', 'Клики', 'CTR', 'Затраты']
                    selected_metrics = st.multiselect(
                        "Выберите метрики для отображения",
                        options=metric_options,
                        default=['Показы', 'Клики'],
                        key=f"metrics_select_{campaign_id}"
                    )

                    use_log_scale = st.checkbox(
                        "Использовать логарифмический масштаб",
                        value=False,
                        key=f"log_scale_{campaign_id}"
                    )

                    fig = go.Figure()
                    colors = ['#00CC96', '#EF553B', '#636EFA', '#FF6692']

                    if 'Показы' in selected_metrics:
                        fig.add_trace(
                            go.Scatter(x=trend_df['date'], y=trend_df['shows'], mode='lines+markers', name='Показы',
                                       line=dict(color=colors[0])))
                    if 'Клики' in selected_metrics:
                        fig.add_trace(
                            go.Scatter(x=trend_df['date'], y=trend_df['clicks'], mode='lines+markers', name='Клики',
                                       line=dict(color=colors[1])))
                    if 'CTR' in selected_metrics:
                        fig.add_trace(
                            go.Scatter(x=trend_df['date'], y=trend_df['ctr'] * 100, mode='lines+markers', name='CTR',
                                       line=dict(color=colors[2]), yaxis='y2'))
                    if 'Затраты' in selected_metrics:
                        fig.add_trace(
                            go.Scatter(x=trend_df['date'], y=trend_df['spend'], mode='lines+markers', name='Затраты',
                                       line=dict(color=colors[3])))

                    fig.update_layout(
                        xaxis_title="Дата",
                        yaxis_title="Количество (Показы, Клики, Затраты, ₽)",
                        yaxis2=dict(
                            title="CTR (%)",
                            overlaying='y',
                            side='right',
                            range=[0, trend_df['ctr'].max() * 200 if not trend_df[
                                'ctr'].empty else 100] if not use_log_scale else None
                        ),
                        yaxis=dict(
                            type='log' if use_log_scale else 'linear',
                            range=[0, None] if not use_log_scale else None
                        ),
                        height=500,
                        template="plotly_dark",
                        showlegend=True,
                        xaxis=dict(rangeslider=dict(visible=True), type="date")
                    )

                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Нет данных для построения трендов.")

                st.subheader("Рекомендации по управлению кластерами")
                recommendation_data = filtered_result.copy()
                recommendation_data['Рекомендация'] = recommendation_data['decision'].map({
                    'keep': 'Оставить',
                    'stop': 'Убрать',
                    'reduce_bid': 'Проверять',
                    'increase_bid': 'Оставить',
                    'insufficient_data': 'Недостаточно данных'
                })

                st.subheader("Сомнительные кластеры")
                minus_clusters = recommendation_data[
                    recommendation_data['Рекомендация'].isin(['Убрать', 'Проверять'])].copy()
                if not minus_clusters.empty:
                    minus_clusters = minus_clusters[['advertId', 'Рекомендация']].rename(columns={'advertId': 'Кластер'})
                    st.dataframe(
                        minus_clusters,
                        use_container_width=True,
                        height=200
                    )
                else:
                    st.info("Нет кластеров для минуса.")

                st.subheader("Хорошие кластеры")
                good_clusters = recommendation_data[recommendation_data['Рекомендация'] == 'Оставить'].copy()
                if not good_clusters.empty:
                    good_clusters = good_clusters[['advertId', 'Рекомендация']].rename(columns={'advertId': 'Кластер'})
                    st.dataframe(
                        good_clusters,
                        use_container_width=True,
                        height=200
                    )
                else:
                    st.info("Нет хороших кластеров.")

    def display_bandit_tab(self):
        st.header("UCB1 Бандит")

        with st.expander("⚙️ Настройки бандита", expanded=True):
            iterations = st.number_input(
                "Количество итераций",
                min_value=1,
                max_value=500,
                value=len(st.session_state.campaign_list) if st.session_state.campaign_list else 1
            )
            reward_metric = st.selectbox(
                "Метрика награды",
                options=["ctr", "clicks", "sum", "cpm", "ctr/cpm"],
                index=0
            )
            daily_budget = st.number_input(
                "Дневной бюджет (руб.)",
                min_value=0.0,
                value=DEFAULT_DAILY_BUDGET,
                step=100.0
            )
            daily_budget = None if daily_budget == 0 else daily_budget
            max_cpm_change_percent = st.slider(
                "Максимальное изменение CPM (%)",
                min_value=10.0,
                max_value=100.0,
                value=30.0,
                step=5.0,
                help="Максимальное изменение CPM относительно исходного значения за весь цикл оптимизации (в процентах)."
            )
            max_cpm_change = max_cpm_change_percent / 100.0
            st.write(f"Текущее ограничение изменения CPM: ±{max_cpm_change_percent}%")

            if st.button("Сбросить исходные CPM", key="reset_initial_cpm"):
                if hasattr(st.session_state, 'optimizer') and st.session_state.optimizer:
                    bandit = UCBBandit(arms=[str(cid) for cid in st.session_state.campaign_list],
                                       reward_metric=reward_metric, daily_budget=daily_budget,
                                       max_cpm_change=max_cpm_change)
                    bandit.reset_initial_cpms()
                    st.success("Исходные CPM сброшены")
                    logger.info("Исходные CPM сброшены пользователем")
                else:
                    st.error("Оптимизатор не инициализирован. Проверьте API ключ.")
                    logger.error("Попытка сбросить CPM без инициализированного оптимизатора")

        if st.button("🚀 Запустить бандита", key="run_bandit"):
            if not self.check_ready_for_bandit():
                return
            with st.spinner("Запуск бандита..."):
                campaign_ids = [str(cid) for cid in st.session_state.campaign_list]
                recommendations, stats = self.run_bandit(
                    campaign_ids,
                    st.session_state.start_date,
                    st.session_state.end_date,
                    iterations,
                    reward_metric,
                    daily_budget,
                    max_cpm_change
                )
                st.session_state.bandit_results = recommendations
                st.session_state.bandit_stats = stats
                if recommendations:
                    st.success(f"Бандит завершил работу! Выполнено {len(recommendations)} итераций.")
                else:
                    st.error("Не удалось выполнить бандита. Проверьте данные кампаний.")

        if not st.session_state.bandit_results:
            st.info("Запустите бандита для отображения результатов")
            return

        st.subheader("Рекомендации бандита")
        recommendations_df = pd.DataFrame(st.session_state.bandit_results)
        if not recommendations_df.empty:
            st.dataframe(
                recommendations_df[
                    ['arm', 'reward', 'metric', 'current_cpm', 'recommended_cpm', 'cpm_adjustment', 'recommendation']],
                use_container_width=True,
                column_config={
                    "arm": st.column_config.TextColumn("Кампания"),
                    "reward": st.column_config.NumberColumn("Награда", format="%.4f"),
                    "metric": st.column_config.TextColumn("Метрика"),
                    "current_cpm": st.column_config.NumberColumn("Средний CPM", format="%.2f руб."),
                    "recommended_cpm": st.column_config.NumberColumn("Рекомендуемый CPM", format="%.2f руб."),
                    "cpm_adjustment": st.column_config.TextColumn("Изменение CPM"),
                    "recommendation": st.column_config.TextColumn("Рекомендация с текущим CPM")
                }
            )

        st.subheader("Статистика кампаний")
        stats_df = pd.DataFrame.from_dict(st.session_state.bandit_stats, orient='index')
        stats_df.index.name = 'campaign_id'
        st.dataframe(
            stats_df,
            use_container_width=True,
            column_config={
                "pulls": st.column_config.NumberColumn("Выборы"),
                "total_reward": st.column_config.NumberColumn("Общая награда", format="%.4f"),
                "avg_reward": st.column_config.NumberColumn("Средняя награда", format="%.4f"),
                "avg_cpm": st.column_config.NumberColumn("Средний CPM", format="%.2f")
            }
        )

        st.subheader("CTR vs CPM")
        if not recommendations_df.empty:
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=recommendations_df['current_cpm'],
                    y=recommendations_df['reward'],
                    mode='markers+text',
                    name='Текущий CPM',
                    text=recommendations_df['arm'],
                    textposition='top center',
                    marker=dict(size=10, color='blue')
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=recommendations_df['recommended_cpm'],
                    y=recommendations_df['reward'],
                    mode='markers+text',
                    name='Рекомендуемый CPM',
                    text=recommendations_df['arm'],
                    textposition='bottom center',
                    marker=dict(size=10, color='red')
                )
            )
            for _, row in recommendations_df.iterrows():
                fig.add_trace(
                    go.Scatter(
                        x=[row['current_cpm'], row['recommended_cpm']],
                        y=[row['reward'], row['reward']],
                        mode='lines',
                        showlegend=False,
                        line=dict(color='gray', dash='dash')
                    )
                )
            fig.update_layout(
                xaxis_title="CPM (руб.)",
                yaxis_title="CTR (%)",
                height=500,
                showlegend=True
            )
            st.plotly_chart(fig, use_container_width=True)

    def check_ready_for_optimization(self) -> bool:
        if not st.session_state.get('api_initialized', False):
            st.warning("Введите и проверьте API ключ в боковой панели")
            logger.warning("API not initialized")
            return False
        if not st.session_state.selected_campaign_id:
            st.warning("Выберите ID кампании")
            logger.warning("No campaign ID selected")
            return False
        if st.session_state.optimizer is None:
            st.error("Оптимизатор не инициализирован. Проверьте API ключ и настройки.")
            logger.error("Optimizer is None during check_ready_for_optimization")
            return False
        return True

    def check_ready_for_bandit(self) -> bool:
        if not st.session_state.get('api_initialized', False):
            st.warning("Введите и проверьте API ключ в боковой панели")
            logger.warning("API not initialized")
            return False
        if not st.session_state.campaign_list:
            st.warning("Нет доступных кампаний для анализа")
            logger.warning("No campaigns available")
            return False
        if st.session_state.optimizer is None:
            st.error("Оптимизатор не инициализирован. Проверьте API ключ и настройки.")
            logger.error("Optimizer is None during check_ready_for_bandit")
            return False
        return True

    def run(self):
        st.set_page_config(
            page_title="Wildberries Keyword Optimizer",
            page_icon="📈",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        st.markdown(
            '<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">',
            unsafe_allow_html=True)

        st.title("Wildberries Оптимизатор ключевых слов и кампаний")
        st.markdown("Оптимизируйте рекламные кампании Wildberries с помощью анализа ключевых слов или UCB1 бандита.")

        self.setup_sidebar()

        optimization_tab, bandit_tab = st.tabs(["Оптимизация ключевых слов", "UCB1 Бандит"])

        with optimization_tab:
            self.display_optimization_tab()

        with bandit_tab:
            self.display_bandit_tab()

if __name__ == "__main__":
    app = WildberriesOptimizerApp()
    app.run()