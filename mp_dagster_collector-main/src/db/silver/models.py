import uuid
from sqlalchemy import (
    Column,
    Text,
    TIMESTAMP,
    Integer,
    SmallInteger,
    Boolean,
    String,
    Date,
    BigInteger,
    func,
    Float,
    DateTime,
    ForeignKey,
    PrimaryKeyConstraint,
    Numeric,
    Index
)
from sqlalchemy.dialects.postgresql import UUID, REAL, ENUM
from sqlalchemy.schema import Sequence
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text
from enum import Enum as PyEnum
from src.db.base import Base


class SilverAdCampaign1D(Base):
    __tablename__ = "wb_ad_campaigns_1d"
    __table_args__ = {"schema": "silver"}

    id = Column(
        BigInteger,
        Sequence("wb_ad_campaigns_1d_id_seq", schema="silver"),
        primary_key=True,
    )
    response_uuid = Column(UUID(as_uuid=True), nullable=False)
    response_dttm = Column(TIMESTAMP(timezone=True), nullable=False)

    campaign_id = Column(Integer, nullable=False)
    name = Column(Text)

    create_time = Column(Text)
    change_time = Column(Text)
    start_time = Column(Text)
    end_time = Column(Text)

    status = Column(Integer)
    type = Column(Integer)
    payment_type = Column(Text)
    daily_budget = Column(Integer)
    search_pluse_state = Column(Boolean)


class SilverAdCampaignsProducts1D(Base):
    __tablename__ = "wb_ad_campaigns_products_1d"
    __table_args__ = {"schema": "silver"}

    id = Column(
        BigInteger,
        Sequence("wb_ad_campaigns_products_1d_id_seq", schema="silver"),
        primary_key=True,
    )

    response_uuid = Column(UUID(as_uuid=True), nullable=False)
    response_dttm = Column(TIMESTAMP(timezone=True), nullable=False)

    campaign_id = Column(Integer, nullable=False)
    product_id = Column(Integer, nullable=False)
    product_is_active = Column(Boolean)

    subject_cpm_current = Column(Integer)
    subject_id = Column(Integer)
    subject_name = Column(Text)
    subject_is_active = Column(Boolean)


class SilverAdCampaignsAuto1D(Base):
    __tablename__ = "wb_ad_campaigns_auto_1d"
    __table_args__ = {"schema": "silver"}

    id = Column(
        BigInteger,
        Sequence("wb_ad_campaigns_auto_1d_id_seq", schema="silver"),
        primary_key=True,
    )

    response_uuid = Column(UUID(as_uuid=True), nullable=False)
    response_dttm = Column(TIMESTAMP(timezone=True), nullable=False)

    campaign_id = Column(Integer, nullable=False)
    product_id = Column(Integer, nullable=False)

    subject_id = Column(Integer)
    subject_name = Column(Text)

    cpm = Column(Integer)  # из autoParams.nmCPM[].cpm

    is_carousel = Column(Boolean)
    is_recom = Column(Boolean)
    is_booster = Column(Boolean)


class SilverAdCampaignsUnited1D(Base):
    __tablename__ = "wb_ad_campaigns_united_1d"
    __table_args__ = {"schema": "silver"}

    id = Column(
        BigInteger,
        Sequence("wb_ad_campaigns_united_1d_id_seq", schema="silver"),
        primary_key=True,
    )

    response_uuid = Column(UUID(as_uuid=True), nullable=False)
    response_dttm = Column(TIMESTAMP(timezone=True), nullable=False)

    campaign_id = Column(Integer, nullable=False)
    product_id = Column(Integer, nullable=False)

    subject_id = Column(Integer)
    subject_name = Column(Text)

    search_cpm = Column(Integer)
    catalog_cpm = Column(Integer)


class SilverAdStatsKeywords1D(Base):
    __tablename__ = "wb_ad_stats_keywords_1d"
    __table_args__ = {"schema": "silver"}

    last_response_uuid = Column(UUID(as_uuid=True))
    last_response_dttm = Column(TIMESTAMP(timezone=True))
    campaign_id       = Column(Integer, primary_key=True)
    date              = Column(Date,    primary_key=True)
    keyword           = Column(Text,    ForeignKey("silver.ad_keywords.keyword"), primary_key=True)
    views             = Column(Integer)
    clicks            = Column(Integer)
    ctr               = Column(REAL)
    cost              = Column(REAL)
    keyword_dim = relationship("SilverAdKeyword", back_populates="stats")


class SilverAdKeywordClusters1D(Base):
    __tablename__ = "wb_ad_keyword_clusters_1d"
    __table_args__ = {"schema": "silver"}
    last_response_uuid  = Column(UUID(as_uuid=True), primary_key=True)
    last_response_dttm  = Column(TIMESTAMP(timezone=True), nullable=False)
    campaign_id         = Column(Integer, primary_key=True)
    date                = Column(Date,    primary_key=True)
    keyword_cluster     = Column(
        Text,
        ForeignKey("silver.ad_keyword_clusters.keyword_cluster"),
        primary_key=True
    )
    cluster_views       = Column(Integer)
    is_excluded         = Column(Boolean, default=False)
    cluster_dim         = relationship("SilverAdKeywordCluster", back_populates="daily")


class SilverAdKeyword(Base):
    __tablename__ = "ad_keywords"
    __table_args__ = {"schema": "silver"}

    keyword         = Column(Text, primary_key=True)
    keyword_cluster = Column(
        Text,
        ForeignKey("silver.ad_keyword_clusters.keyword_cluster"),
        nullable=False
    )
    stats           = relationship("SilverAdStatsKeywords1D", back_populates="keyword_dim")
    cluster         = relationship("SilverAdKeywordCluster", back_populates="keywords")


class SilverAdKeywordCluster(Base):
    __tablename__ = "ad_keyword_clusters"
    __table_args__ = {"schema": "silver"}

    keyword_cluster = Column(Text, primary_key=True)
    keywords         = relationship("SilverAdKeyword",      back_populates="cluster")
    daily            = relationship("SilverAdKeywordClusters1D", back_populates="cluster_dim")


class SilverAdStatsProducts1D(Base):
    __tablename__ = "wb_ad_stats_products_1d"
    __table_args__ = {"schema": "silver"}

    campaign_id = Column(Integer, primary_key=True)
    date = Column(Date, primary_key=True)
    platform_id = Column(Integer, primary_key=True)
    product_id = Column(Integer, primary_key=True)

    views = Column(Integer)
    clicks = Column(Integer)
    ctr = Column(REAL)
    cpc = Column(REAL)
    cost = Column(REAL)
    carts = Column(Integer)
    orders = Column(Integer)
    cr = Column(REAL)
    items = Column(Integer)
    revenue = Column(REAL)
    updated_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

class SilverAdStatsProductPosition1D(Base):
    __tablename__ = "wb_ad_stats_product_position_1d"
    __table_args__ = {"schema": "silver"}

    campaign_id = Column(Integer, primary_key=True)
    product_id = Column(Integer, primary_key=True)
    date = Column(Date, primary_key=True)

    avg_position = Column(REAL)


class SilverProducts(Base):
    __tablename__ = "products"
    __table_args__ = {"schema": "silver"}

    product_id = Column(Integer, primary_key=True)


class TimeGrain(PyEnum):
    hour = "hour"
    day = "day"
    week = "week"

# И создаём тип для SQLAlchemy
time_grain_type = ENUM(
    TimeGrain,
    name="time_grain_enum",
    create_type=True,
)


class SilverWbProducts(Base):
    __tablename__ = 'wb_products'
    __table_args__ = {'schema': 'silver'}

    api_token_id = Column(Integer, nullable=False)
    response_uuid = Column(UUID(as_uuid=True), nullable=False)
    time_grain     = Column(time_grain_type, nullable=False, server_default=TimeGrain.hour.value)
    dttm = Column(
        DateTime(timezone=True),
        primary_key=True,
    )
    product_id = Column(BigInteger, primary_key=True)
    subject_name = Column(Text)
    brand_name = Column(Text)
    vendor_code = Column(Text)
    name = Column(Text)
    is_rated = Column(Boolean)
    rating = Column(Float)
    feedback_rating = Column(Float)


class SilverWbProductSearchTerms(Base):
    __tablename__ = 'wb_product_search_terms'
    __table_args__ = {'schema': 'silver'}

    api_token_id = Column(Integer, nullable=False)
    response_uuid = Column(UUID(as_uuid=True), nullable=False)
    time_grain     = Column(time_grain_type, nullable=False, server_default=TimeGrain.hour.value)
    dttm = Column(
        DateTime(timezone=True),
        primary_key=True,
    )
    product_id = Column(BigInteger, primary_key=True)
    search_term = Column(Text, primary_key=True)
    price_min = Column(BigInteger)
    price_max = Column(BigInteger)
    week_frequency = Column(Integer)
    frequency = Column(Integer)
    median_position = Column(Integer)
    avg_position = Column(Integer)
    open_card = Column(Integer)
    add_to_cart = Column(Integer)
    open_to_cart = Column(Integer)
    orders = Column(Integer)
    cart_to_order = Column(Integer)
    visibility = Column(Integer)


class SilverWbAdvProductStats1d(Base):
    __tablename__  = "wb_adv_product_stats_1d"
    __table_args__ = {"schema": "silver"}

    # ── служебные поля ODS ─────────────────────────────────────────────────────
    company_id    = Column("company_id", Integer, nullable=False, doc="FK → core.tokens.token_id")
    request_uuid  = Column("request_uuid", UUID(as_uuid=True), nullable=False, doc="Ссылка на запись в bronze")
    inserted_at   = Column("inserted_at", DateTime(timezone=True), server_default=func.now(), nullable=False, doc="Время вставки записи (MSK)")
    response_dttm = Column("response_dttm", DateTime(timezone=True), nullable=False, doc="Из bronze по request_uuid")
    business_dttm = Column("business_dttm", DateTime(timezone=True), primary_key=True, nullable=False, doc="Бизнес-партиция (сутки, MSK)")

    # ── бизнес-ключ ────────────────────────────────────────────────────────────
    advert_id = Column("advert_id", Integer, primary_key=True, doc="ID рекламной кампании")
    app_type  = Column("app_type",  Integer, primary_key=True, doc="Тип площадки")
    nm_id     = Column("nm_id",     Integer, primary_key=True, doc="NM ID (артикул)")

    # (сохраняем поле из старой схемы, но оно больше не в PK)
    request_dttm = Column("request_dttm", DateTime(timezone=True), nullable=False, doc="Дата (день) статистики")

    # ── метрики ────────────────────────────────────────────────────────────────
    views   = Column("views", Integer, nullable=False, doc="Показы")
    clicks  = Column("clicks", Integer, nullable=False, doc="Клики")
    cost    = Column("cost",   Float,   nullable=False, doc="Стоимость кликов")
    carts   = Column("carts",  Integer, nullable=False, doc="Добавления в корзину")
    orders  = Column("orders", Integer, nullable=False, doc="Заказы")
    items   = Column("items",  Integer, nullable=False, doc="Проданные единицы")
    revenue = Column("revenue", Float,  nullable=False, doc="Выручка")


class SilverWbAdvProductPositions1d(Base):
    __tablename__  = "wb_adv_product_positions_1d"
    __table_args__ = {"schema": "silver"}

    # ── служебные поля ODS ─────────────────────────────────────────────────────
    company_id    = Column("company_id", Integer, nullable=False, doc="FK → core.tokens.token_id")
    request_uuid  = Column("request_uuid", UUID(as_uuid=True), nullable=False, doc="Ссылка на запись в bronze")
    inserted_at   = Column("inserted_at", DateTime(timezone=True), server_default=func.now(), nullable=False, doc="Время вставки записи (MSK)")
    response_dttm = Column("response_dttm", DateTime(timezone=True), nullable=False, doc="Из bronze по request_uuid")
    business_dttm = Column("business_dttm", DateTime(timezone=True), primary_key=True, nullable=False, doc="Бизнес-партиция (сутки, MSK)")

    # ── бизнес-ключ ────────────────────────────────────────────────────────────
    advert_id = Column("advert_id", Integer, primary_key=True, doc="ID рекламной кампании")
    nm_id     = Column("nm_id",     Integer, primary_key=True, doc="NM ID (артикул)")

    # (оставляем исходное поле даты, но выводим его из PK)
    date = Column("date", DateTime(timezone=True), nullable=False, doc="Дата (день) позиций")

    # ── метрика ────────────────────────────────────────────────────────────────
    avg_position = Column("avg_position", Integer, nullable=False, doc="Средняя позиция")


class SilverWbAdvProductStats1h(Base):
    __tablename__  = "wb_adv_product_stats_1h"
    __table_args__ = {"schema": "silver"}

    # ── метаданные запроса ──────────────────────────────────────────────────────
    company_id    = Column("company_id", Integer, nullable=False)
    request_uuid  = Column("request_uuid", UUID(as_uuid=True), nullable=False)
    inserted_at   = Column("inserted_at", DateTime(timezone=True), server_default=func.now(), nullable=False)
    response_dttm = Column("response_dttm", DateTime(timezone=True), nullable=False)

    # ── ключи ───────────────────────────────────────────────────────────────────
    business_dttm = Column("business_dttm", DateTime(timezone=True), nullable=False, primary_key=True)
    advert_id     = Column("advert_id", Integer, primary_key=True)
    app_type      = Column("app_type",  Integer, primary_key=True)
    nm_id         = Column("nm_id",     Integer, primary_key=True)

    # аудит/трейсинг
    request_dttm  = Column("request_dttm", DateTime(timezone=True), nullable=False)

    # ── метрики ────────────────────────────────────────────────────────────────
    views   = Column("views", Integer, nullable=False, doc="Показы")
    clicks  = Column("clicks", Integer, nullable=False, doc="Клики")
    # ctr     = Column("ctr", Float,   nullable=False, doc="CTR (%)")
    # cpc     = Column("cpc", Float,   nullable=False, doc="Цена за клик")
    cost    = Column("cost", Float,  nullable=False, doc="Общая стоимость кликов")
    carts   = Column("carts", Integer, nullable=False, doc="Добавления в корзину")
    orders  = Column("orders", Integer, nullable=False, doc="Заказы")
    # cr      = Column("cr", Float,    nullable=False, doc="CR (%)")
    items   = Column("items", Integer, nullable=False, doc="Кол-во проданных единиц")
    revenue = Column("revenue", Float, nullable=False, doc="Выручка")


class SilverWbAdvProductPositions1h(Base):
    __tablename__ = "wb_adv_product_positions_1h"
    __table_args__ = {"schema": "silver"}

    # ── метаданные запроса ──────────────────────────────────────────────────────
    company_id   = Column(
        "company_id",
        Integer,
        nullable=False,
        doc="FK → core.tokens.token_id",
    )
    request_uuid = Column(
        "request_uuid",
        UUID(as_uuid=True),
        primary_key=True,
        doc="Уникальный UUID запроса",
    )
    inserted_at  = Column(
        "inserted_at",
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        doc="Время вставки записи",
    )

    # ── ключи ───────────────────────────────────────────────────────────────────
    advert_id    = Column(
        "advert_id",
        Integer,
        primary_key=True,
        doc="ID рекламной кампании",
    )
    nm_id        = Column(
        "nm_id",
        Integer,
        primary_key=True,
        doc="NM ID (артикул)",
    )
    date         = Column(
        "date",
        DateTime(timezone=True),
        primary_key=True,
        doc="Дата (день) позиций",
    )

    # ── метрика ─────────────────────────────────────────────────────────────────
    avg_position = Column(
        "avg_position",
        Integer,
        nullable=False,
        doc="Средняя позиция",
    )


class SilverWbAdvKeywordStats1d(Base):
    __tablename__ = "wb_adv_keyword_stats_1d"
    __table_args__ = {"schema": "silver"}

    # ── метаданные запроса ──────────────────────────────────────────────────────
    company_id   = Column(
        "company_id",
        Integer,
        nullable=False,
        doc="FK → core.tokens.token_id",
    )
    request_uuid = Column(
        "request_uuid",
        UUID(as_uuid=True),
        primary_key=True,
        doc="Уникальный UUID запроса",
    )
    inserted_at  = Column(
        "inserted_at",
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        doc="Время вставки записи",
    )

    # ── ключи ───────────────────────────────────────────────────────────────────
    advert_id    = Column(
        "advert_id",
        Integer,
        primary_key=True,
        doc="ID рекламной кампании",
    )
    date         = Column(
        "date",
        DateTime(timezone=True),
        primary_key=True,
        doc="Дата статистики (день)",
    )
    keyword      = Column(
        "keyword",
        Text,
        primary_key=True,
        doc="Поисковый запрос (keyword)",
    )

    # ── метрики ─────────────────────────────────────────────────────────────────
    views        = Column(
        "views",
        Integer,
        nullable=False,
        doc="Число показов по этому ключевому слову",
    )
    clicks       = Column(
        "clicks",
        Integer,
        nullable=False,
        doc="Число кликов по этому ключевому слову",
    )
    ctr          = Column(
        "ctr",
        Float,
        nullable=False,
        doc="Click-Through Rate (%)",
    )
    cost         = Column(
        "cost",
        Float,
        nullable=False,
        doc="Суммарная стоимость кликов (руб.)",
    )


class SilverWbAdvKeywordStats1h(Base):
    __tablename__ = "wb_adv_keyword_stats_1h"
    __table_args__ = {"schema": "silver"}

    # ── метаданные запроса ──────────────────────────────────────────────────────
    company_id   = Column(
        "company_id",
        Integer,
        nullable=False,
        doc="FK → core.tokens.token_id",
    )
    request_uuid = Column(
        "request_uuid",
        UUID(as_uuid=True),
        primary_key=True,
        doc="Уникальный UUID запроса",
    )
    inserted_at  = Column(
        "inserted_at",
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        doc="Время вставки записи",
    )

    # ── ключи ───────────────────────────────────────────────────────────────────
    advert_id    = Column(
        "advert_id",
        Integer,
        primary_key=True,
        doc="ID рекламной кампании",
    )
    date         = Column(
        "date",
        DateTime(timezone=True),
        primary_key=True,
        doc="Дата статистики (день)",
    )
    keyword      = Column(
        "keyword",
        Text,
        primary_key=True,
        doc="Поисковый запрос (keyword)",
    )

    # ── метрики ─────────────────────────────────────────────────────────────────
    views        = Column(
        "views",
        Integer,
        nullable=False,
        doc="Число показов по этому ключевому слову",
    )
    clicks       = Column(
        "clicks",
        Integer,
        nullable=False,
        doc="Число кликов по этому ключевому слову",
    )
    ctr          = Column(
        "ctr",
        Float,
        nullable=False,
        doc="Click-Through Rate (%)",
    )
    cost         = Column(
        "cost",
        Float,
        nullable=False,
        doc="Суммарная стоимость кликов (руб.)",
    )


class SilverWbAdvKeywordClusters1d(Base):
    __tablename__ = "wb_adv_keyword_clusters_1d"
    __table_args__ = {"schema": "silver"}

    # ── метаданные запроса ──────────────────────────────────────────────────────
    company_id   = Column(
        "company_id",
        Integer,
        nullable=False,
        doc="FK → core.tokens.token_id",
    )
    request_uuid = Column(
        "request_uuid",
        UUID(as_uuid=True),
        nullable=False,
        doc="UUID запроса из bronze",
    )
    inserted_at  = Column(
        "inserted_at",
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        doc="Время вставки записи",
    )
    run_dttm     = Column(
        "run_dttm",
        DateTime(timezone=True),
        primary_key=True,
        nullable=False,
        doc="Время выполнения bronze-ассета (день выборки)",
    )

    # ── ключи ───────────────────────────────────────────────────────────────────
    advert_id        = Column(
        "advert_id",
        Integer,
        primary_key=True,
        nullable=False,
        doc="ID рекламной кампании",
    )
    keyword_cluster  = Column(
        "keyword_cluster",
        Text,
        primary_key=True,
        nullable=False,
        doc="Название группы похожих ключевых слов",
    )
    keyword          = Column(
        "keyword",
        Text,
        primary_key=True,
        nullable=False,
        doc="Конкретное ключевое слово из кластера",
    )

    # ── метрики ────────────────────────────────────────────────────────────────
    keyword_cluster_views = Column(
        "keyword_cluster_views",
        Integer,
        nullable=False,
        doc="Суммарное число показов по всему кластеру",
    )
    is_excluded           = Column(
        "is_excluded",
        Boolean,
        nullable=False,
        server_default="false",
        doc="Флаг, что кластер исключён из анализа",
    )


class SilverOrderItems1d(Base):
    __tablename__ = "wb_order_items_1d"
    __table_args__ = (
        PrimaryKeyConstraint("business_dttm", "sr_id", name="wb_order_items_1d_pkey"),
        {"schema": "silver"},
    )
    __mapper_args__ = {"always_refresh": True}

    request_uuid = Column("request_uuid", UUID(as_uuid=True), nullable=False, default=uuid.uuid4,
                          doc="Уникальный идентификатор запроса")
    response_dttm = Column("response_dttm", TIMESTAMP(timezone=True), nullable=False, doc="timestamp ответа API (MSK)")
    business_dttm = Column("business_dttm", TIMESTAMP(timezone=True), nullable=False, doc="бизнес-время (MSK)")
    inserted_at = Column("inserted_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False, doc="Время вставки записи")

    g_number = Column("g_number", String, nullable=False, doc="Номер заказа (g_number)")
    sr_id = Column("sr_id", String, nullable=False, doc="Внутренний ID возврата/статуса (natural key)")
    date = Column("date", TIMESTAMP(timezone=True), nullable=False, doc="Дата заказа")
    last_change_date = Column("last_change_date", TIMESTAMP(timezone=True), nullable=False, doc="Дата последнего изменения")
    warehouse_name = Column("warehouse_name", String, nullable=False, doc="ID склада")
    warehouse_type = Column("warehouse_type", String, nullable=False, doc="Тип склада")
    country_name = Column("country_name", String, nullable=False, doc="Страна")
    oblast_okrug_name = Column("oblast_okrug_name", String, nullable=False, doc="Область / округ")
    region_name = Column("region_name", String, nullable=False, doc="Регион")
    supplier_article = Column("supplier_article", String, nullable=False, doc="Артикул поставщика")
    nm_id = Column("nm_id", Integer, nullable=False, doc="ID товара")
    barcode = Column("barcode", String, nullable=False, doc="Штрихкод")
    category = Column("category", String, nullable=False, doc="Категория")
    subject = Column("subject", String, nullable=False, doc="Тема / подраздел")
    brand = Column("brand", String, nullable=False, doc="Бренд")
    tech_size = Column("tech_size", String, nullable=False, doc="Технический размер")
    income_id = Column("income_id", Integer, nullable=False, doc="ID прихода")
    total_price = Column("total_price", Float, nullable=False, doc="Сумма заказа")
    discount_percent = Column("discount_percent", Float, nullable=False, doc="Процент скидки")
    spp = Column("spp", Integer, nullable=False, doc="СПП")
    finished_price = Column("finished_price", Float, nullable=False, doc="Цена после всех скидок")
    price_with_discount = Column("price_with_discount", Float, nullable=False, doc="Цена со скидкой")
    is_cancel = Column("is_cancel", Boolean, nullable=False, server_default="false", doc="Флаг отмены заказа")
    cancel_date = Column("cancel_date", TIMESTAMP(timezone=True), nullable=True, doc="Дата отмены заказа")
    sticker = Column("sticker", String, nullable=False, doc="Стикер заказа")
    company_id = Column("company_id", Integer, nullable=False, doc="FK → core.tokens.token_id")



class SilverWbCommission1d(Base):
    __tablename__ = "wb_commission_1d"
    __table_args__ = (
        PrimaryKeyConstraint("request_uuid", "date", "subject_id", name="pk_wb_commission_1d"),
        {"schema": "silver"},
    )
    __mapper_args__ = {"always_refresh": True}

    request_uuid = Column(
        UUID(as_uuid=True),
        nullable=False,
        default=uuid.uuid4,
        doc="Уникальный идентификатор запроса",
    )
    date = Column(
        DateTime(timezone=True),
        nullable=False,
        doc="Дата расчёта комиссий",
    )
    subject_id = Column(
        Integer,
        nullable=False,
        doc="ID темы/подраздела (натуральный ключ)",
    )

    response_dttm = Column(
        DateTime(timezone=True),
        nullable=False,
        doc="Время получения ответа от API",
    )

    kgvp_booking = Column(Float, nullable=True, doc="Сумма kgvp booking")
    kgvp_marketplace = Column(Float, nullable=True, doc="Сумма kgvp marketplace")
    kgvp_pick_up = Column(Float, nullable=True, doc="Сумма kgvp pick up")
    kgvp_supplier = Column(Float, nullable=True, doc="Сумма kgvp supplier")
    kgvp_supplier_express = Column(Float, nullable=True, doc="Сумма kgvp supplier express")
    paid_storage_kgvp = Column(Float, nullable=True, doc="Сумма paid storage kgvp")

    category_id = Column(Integer, nullable=True, doc="ID категории")
    category_name = Column(String, nullable=True, doc="Название категории")
    subject_name = Column(String, nullable=True, doc="Название темы/подраздела")


class SilverWbMpSkus1d(Base):
    __tablename__ = "wb_mp_skus_1d"
    __table_args__ = (
        PrimaryKeyConstraint(
            "request_uuid", "response_dttm", "barcode", "company_id", name="pk_wb_mp_skus_1d"
        ),
        {"schema": "silver"},
    )
    __mapper_args__ = {"always_refresh": True}

    request_uuid = Column(
        "request_uuid",
        UUID(as_uuid=True),
        nullable=False,
        default=uuid.uuid4,
        doc="Уникальный идентификатор запроса",
    )
    response_dttm = Column(
        "response_dttm",
        TIMESTAMP(timezone=True),
        nullable=False,
        doc="Время получения ответа от API",
    )

    company_id = Column(
        "company_id",
        Integer,
        nullable=False,
        index=True,
        doc="ID компании (FK → core.tokens.company_id)",
    )
    nm_id = Column(
        "nm_id",
        Integer,
        nullable=False,
        doc="ID товара (nm_id)",
    )
    imt_id = Column(
        "imt_id",
        Integer,
        nullable=False,
        doc="ID IMT",
    )
    subject_id = Column(
        "subject_id",
        Integer,
        nullable=False,
        doc="ID темы/подраздела",
    )
    subject_name = Column(
        "subject_name",
        String,
        nullable=True,
        doc="Название темы/подраздела",
    )
    supplier_article = Column(
        "supplier_article",
        String,
        nullable=True,
        doc="Артикул поставщика",
    )
    brand = Column(
        "brand",
        String,
        nullable=True,
        doc="Бренд",
    )
    title = Column(
        "title",
        String,
        nullable=True,
        doc="Название SKU",
    )
    description = Column(
        "description",
        Text,
        nullable=True,
        doc="Описание",
    )
    length = Column(
        "length",
        Integer,
        nullable=True,
        doc="Длина",
    )
    width = Column(
        "width",
        Integer,
        nullable=True,
        doc="Ширина",
    )
    height = Column(
        "height",
        Integer,
        nullable=True,
        doc="Высота",
    )
    weight = Column(
        "weight",
        Float,
        nullable=True,
        doc="Вес",
    )
    chrt_id = Column(
        "chrt_id",
        Integer,
        nullable=True,
        doc="ID чартриджа",
    )
    tech_size = Column(
        "tech_size",
        String,
        nullable=True,
        doc="Технический размер",
    )
    barcode = Column(
        "barcode",
        String,
        nullable=False,
        doc="Штрихкод (natural key)",
    )


class SilverWbStocks1d(Base):
    __tablename__ = "wb_stocks_1d"
    __table_args__ = (
        PrimaryKeyConstraint("request_uuid", "response_dttm", "company_id", "date", "barcode", "warehouse_name"),
        {"schema": "silver"},
    )

    request_uuid = Column(
        UUID(as_uuid=True),
        nullable=False,
        default=uuid.uuid4,
        doc="ID запроса"
    )
    response_dttm = Column(
        DateTime(timezone=True),
        nullable=False,
        doc="Время получения ответа"
    )
    company_id = Column(
        Integer,
        nullable=False,
        doc="ID компании"
    )

    date = Column(
        Date,
        nullable=False,
        doc="Дата складских остатков"
    )
    supplier_article = Column(
        String,
        nullable=True,
        doc="Артикул поставщика"
    )
    nm_id = Column(
        Integer,
        nullable=True,
        doc="NM ID товара"
    )
    barcode = Column(
        String,
        nullable=False,
        doc="Штрихкод"
    )
    tech_size = Column(
        String,
        nullable=True,
        doc="Технический размер"
    )
    warehouse_name = Column(
        String,
        nullable=False,
        doc="Наименование склада"
    )
    volume = Column(
        Float,
        nullable=True,
        doc="Объём"
    )
    in_way_to_client = Column(
        Integer,
        nullable=True,
        doc="В пути до клиента"
    )
    in_way_from_client = Column(
        Integer,
        nullable=True,
        doc="В пути от клиента"
    )
    quantity = Column(
        Integer,
        nullable=True,
        doc="Количество на складе"
    )
    total = Column(
        Integer,
        nullable=True,
        doc="Общее количество (с учётом путей)"
    )


class SilverWBPaidStorage1d(Base):
    __tablename__ = "wb_paid_storage_1d"
    __table_args__ = (
        PrimaryKeyConstraint(
            "response_dttm",
            "date",
            "warehouse_name",
            "income_id",
            "barcode",
            "calc_type",
            name="pk_paid_storage_1d"
        ),
        {"schema": "silver"},
    )

    request_uuid = Column(UUID(as_uuid=True), nullable=True)
    response_dttm = Column(DateTime(timezone=True), nullable=False)
    company_id = Column(Integer, nullable=True)
    date = Column(Date, nullable=False)

    log_warehouse_coef = Column(Float, nullable=True)
    warehouse_id = Column(Integer, nullable=True)
    warehouse_name = Column(String, nullable=False)
    warehouse_coef = Column(Float, nullable=True)

    income_id = Column(Integer, nullable=False)
    chrt_id = Column(Integer, nullable=True)
    tech_size = Column(String, nullable=True)
    barcode = Column(String, nullable=False)
    supplier_article = Column(String, nullable=True)
    nm_id = Column(Integer, nullable=True)

    volume = Column(Float, nullable=True)
    calc_type = Column(String, nullable=True)
    warehouse_price = Column(Float, nullable=True)
    barcodes_count = Column(Integer, nullable=True)
    pallet_place_code = Column(Integer, nullable=True)
    pallet_count = Column(Float, nullable=True)
    loyalty_discount = Column(Float, nullable=True)


class SilverWBPaidAcceptances1d(Base):
    __tablename__ = "wb_paid_acceptances_1d"
    __table_args__ = (
        PrimaryKeyConstraint(
            "response_dttm",
            "income_id",
            "nm_id",
            name="pk_paid_acceptances_1d"
        ),
        {"schema": "silver"},
    )

    request_uuid = Column(UUID(as_uuid=True), nullable=True)
    response_dttm = Column(DateTime(timezone=True), nullable=False)
    company_id = Column(Integer, nullable=True)
    quantity = Column(Integer, nullable=True)
    gi_create_date = Column(Date, nullable=True)
    date = Column(Date, nullable=True)
    income_id = Column(Integer, nullable=False)
    nm_id = Column(Integer, nullable=False)
    total_price = Column(Float, nullable=True)


class SilverWbAdvCampaigns1d(Base):
    __tablename__ = "wb_adv_campaigns_1d"
    __table_args__ = (
        PrimaryKeyConstraint("advert_id", "request_uuid", "run_dttm"),
        {"schema": "silver"},
    )

    advert_id = Column(
        Integer,
        nullable=False,
        doc="ID рекламной кампании"
    )
    request_uuid = Column(
        UUID(as_uuid=True),
        nullable=False,
        default=uuid.uuid4,
        doc="UUID запроса"
    )
    run_dttm = Column(
        DateTime(timezone=True),
        nullable=False,
        doc="Время запуска/получения данных"
    )

    name = Column(
        Text,
        nullable=True,
        doc="Название кампании"
    )
    create_time = Column(
        Text,
        nullable=True,
        doc="Время создания кампании"
    )
    change_time = Column(
        Text,
        nullable=True,
        doc="Время последнего изменения кампании"
    )
    start_time = Column(
        Text,
        nullable=True,
        doc="Время начала кампании"
    )
    end_time = Column(
        Text,
        nullable=True,
        doc="Время окончания кампании"
    )

    status = Column(
        Integer,
        nullable=True,
        doc="Статус кампании"
    )
    type_ = Column(
        "type",
        Integer,
        nullable=True,
        doc="Тип кампании"
    )
    payment_type = Column(
        Text,
        nullable=True,
        doc="Тип оплаты"
    )
    daily_budget = Column(
        Integer,
        nullable=True,
        doc="Дневной бюджет"
    )
    search_pluse_state = Column(
        Boolean,
        nullable=True,
        doc="Флаг Search Plus"
    )


class SilverWbAdvProductRates1d(Base):
    __tablename__ = "wb_adv_product_rates_1d"
    __table_args__ = (
        PrimaryKeyConstraint("run_dttm", "advert_id", "nm_id"),
        {"schema": "silver"},
    )

    company_id = Column(
        Integer,
        nullable=False,
        doc="ID компании"
    )
    request_uuid = Column(
        UUID(as_uuid=True),
        nullable=False,
        default=uuid.uuid4,
        doc="UUID запроса"
    )
    inserted_at = Column(
        DateTime(timezone=True),
        nullable=False,
        doc="Время вставки записи"
    )
    run_dttm = Column(
        DateTime(timezone=True),
        nullable=False,
        doc="Время запуска/получения данных"
    )
    advert_id = Column(
        Integer,
        nullable=False,
        doc="ID рекламной кампании"
    )
    nm_id = Column(
        Integer,
        nullable=False,
        doc="NM ID товара"
    )

    advert_status = Column(
        Integer,
        nullable=True,
        doc="Статус объявления"
    )
    advert_type = Column(
        Integer,
        nullable=True,
        doc="Тип объявления"
    )
    cpm_current = Column(
        Integer,
        nullable=True,
        doc="Текущий CPM"
    )
    cpm_catalog = Column(
        Integer,
        nullable=True,
        doc="CPM в каталоге"
    )
    cpm_search = Column(
        Integer,
        nullable=True,
        doc="CPM в поиске"
    )
    cpm_first = Column(
        Integer,
        nullable=True,
        doc="CPM в первом показе"
    )

    active_carousel = Column(
        Boolean,
        nullable=True,
        doc="Активность карусели"
    )
    active_recom = Column(
        Boolean,
        nullable=True,
        doc="Активность рекомендаций"
    )
    active_booster = Column(
        Boolean,
        nullable=True,
        doc="Активность бустера"
    )

    subject_id = Column(
        Integer,
        nullable=True,
        doc="ID темы (subject_id)"
    )


class SilverWbAdvCampaigns1h(Base):
    __tablename__ = "wb_adv_campaigns_1h"
    __table_args__ = (
        PrimaryKeyConstraint("advert_id", "request_uuid", "run_dttm"),
        {"schema": "silver"},
    )

    advert_id = Column(
        Integer,
        nullable=False,
        doc="ID рекламной кампании"
    )
    request_uuid = Column(
        UUID(as_uuid=True),
        nullable=False,
        default=uuid.uuid4,
        doc="UUID запроса"
    )
    run_dttm = Column(
        DateTime(timezone=True),
        nullable=False,
        doc="Время запуска/получения данных"
    )

    name = Column(
        Text,
        nullable=True,
        doc="Название кампании"
    )
    create_time = Column(
        Text,
        nullable=True,
        doc="Время создания кампании"
    )
    change_time = Column(
        Text,
        nullable=True,
        doc="Время последнего изменения кампании"
    )
    start_time = Column(
        Text,
        nullable=True,
        doc="Время начала кампании"
    )
    end_time = Column(
        Text,
        nullable=True,
        doc="Время окончания кампании"
    )

    status = Column(
        Integer,
        nullable=True,
        doc="Статус кампании"
    )
    type_ = Column(
        "type",
        Integer,
        nullable=True,
        doc="Тип кампании"
    )
    payment_type = Column(
        Text,
        nullable=True,
        doc="Тип оплаты"
    )
    daily_budget = Column(
        Integer,
        nullable=True,
        doc="Дневной бюджет"
    )
    search_pluse_state = Column(
        Boolean,
        nullable=True,
        doc="Флаг Search Plus"
    )


class SilverWbAdvProductRates1h(Base):
    __tablename__ = "wb_adv_product_rates_1h"
    __table_args__ = (
        PrimaryKeyConstraint("run_dttm", "advert_id", "nm_id"),
        {"schema": "silver"},
    )

    company_id = Column(
        Integer,
        nullable=False,
        doc="ID компании"
    )
    request_uuid = Column(
        UUID(as_uuid=True),
        nullable=False,
        default=uuid.uuid4,
        doc="UUID запроса"
    )
    inserted_at = Column(
        DateTime(timezone=True),
        nullable=False,
        doc="Время вставки записи"
    )
    run_dttm = Column(
        DateTime(timezone=True),
        nullable=False,
        doc="Время запуска/получения данных"
    )
    advert_id = Column(
        Integer,
        nullable=False,
        doc="ID рекламной кампании"
    )
    nm_id = Column(
        Integer,
        nullable=False,
        doc="NM ID товара"
    )

    advert_status = Column(
        Integer,
        nullable=True,
        doc="Статус объявления"
    )
    advert_type = Column(
        Integer,
        nullable=True,
        doc="Тип объявления"
    )
    cpm_current = Column(
        Integer,
        nullable=True,
        doc="Текущий CPM"
    )
    cpm_catalog = Column(
        Integer,
        nullable=True,
        doc="CPM в каталоге"
    )
    cpm_search = Column(
        Integer,
        nullable=True,
        doc="CPM в поиске"
    )
    cpm_first = Column(
        Integer,
        nullable=True,
        doc="CPM в первом показе"
    )

    active_carousel = Column(
        Boolean,
        nullable=True,
        doc="Активность карусели"
    )
    active_recom = Column(
        Boolean,
        nullable=True,
        doc="Активность рекомендаций"
    )
    active_booster = Column(
        Boolean,
        nullable=True,
        doc="Активность бустера"
    )

    subject_id = Column(
        Integer,
        nullable=True,
        doc="ID темы (subject_id)"
    )


class SilverWbBuyoutsPercent1d(Base):
    __tablename__ = "wb_buyouts_percent_1d"
    __table_args__ = (
        PrimaryKeyConstraint(
            "business_dttm",
            "nm_id",
            name="pk_wb_buyouts_percent_1d",
        ),
        {"schema": "silver"},
    )

    company_id     = Column(Integer, nullable=False, doc="ID компании")
    request_uuid   = Column(UUID(as_uuid=True), nullable=False, default=uuid.uuid4, doc="UUID запроса")
    inserted_at    = Column(DateTime(timezone=True),
                            nullable=False,
                            server_default=text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"))
    response_dttm  = Column(DateTime(timezone=True), nullable=False, doc="Время ответа от API")
    business_dttm  = Column(DateTime(timezone=True), nullable=False, doc="Бизнес-таймстэмп (из bronze по request_uuid)")

    date            = Column(DateTime(timezone=True), nullable=False, doc="Дата (как в источнике)")
    nm_id           = Column(Integer, nullable=False)
    buyouts_percent = Column(REAL,    nullable=True)


class SilverWbSalesFunnels1d(Base):
    __tablename__ = "wb_sales_funnels_1d"
    __table_args__ = (
        PrimaryKeyConstraint(
            "business_dttm",
            "nm_id",
            name="pk_wb_sales_funnels_1d",
        ),
        {"schema": "silver"},
    )

    company_id     = Column(Integer, nullable=False, doc="ID компании")
    request_uuid   = Column(UUID(as_uuid=True), nullable=False, default=uuid.uuid4, doc="UUID запроса")
    inserted_at    = Column(DateTime(timezone=True),
                            nullable=False,
                            server_default=text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"))
    response_dttm  = Column(DateTime(timezone=True), nullable=False, doc="Время ответа от API")
    business_dttm  = Column(DateTime(timezone=True), nullable=False, doc="Бизнес-таймстэмп (из bronze по request_uuid)")

    date           = Column(Date, nullable=False, doc="Дата метрики (день)")  # остаётся, но не в PK
    nm_id          = Column(Integer, nullable=False, doc="NM ID товара")

    supplier_article = Column(String, nullable=False, doc="Артикул поставщика")

    brand             = Column(String,  nullable=False)
    subject_id        = Column(Integer, nullable=False)
    subject_name      = Column(String,  nullable=False)
    open_card_count   = Column(Integer, nullable=False)
    add_to_cart_count = Column(Integer, nullable=False)
    orders_count      = Column(Integer, nullable=False)
    orders_sum        = Column(REAL,    nullable=False)
    buyouts_count     = Column(Integer, nullable=False)
    buyouts_sum       = Column(Integer, nullable=False)
    cancel_count      = Column(Integer, nullable=False)
    cancel_sum        = Column(REAL,    nullable=False)
    avg_price         = Column(REAL,    nullable=False)
    stocks_mp         = Column(REAL,    nullable=False)
    stocks_wb         = Column(REAL,    nullable=False)


class SilverWbSupplies1d(Base):
    __tablename__ = "wb_supplies_1d"
    __table_args__ = (
        PrimaryKeyConstraint(
            "request_uuid", "company_id", "income_id", "barcode", name="pk_wb_supplies_1d"
        ),
        {"schema": "silver"},
    )

    request_uuid = Column(
        "request_uuid",
        UUID(as_uuid=True),
        nullable=False,
        default=uuid.uuid4,
        doc="UUID запроса",
    )
    response_dttm = Column(
        "response_dttm",
        DateTime(timezone=True),
        nullable=False,
        doc="Время получения ответа от API",
    )
    company_id = Column(
        "company_id",
        Integer,
        nullable=False,
        doc="ID компании",
    )
    income_id = Column(
        "income_id",
        Integer,
        nullable=False,
        doc="ID прихода",
    )
    date = Column(
        "date",
        Date,
        nullable=False,
        doc="Дата прихода (день)",
    )
    last_change_date = Column(
        "last_change_date",
        DateTime(timezone=True),
        nullable=False,
        doc="Дата последнего изменения",
    )
    supplier_article = Column(
        "supplier_article",
        String,
        nullable=True,
        doc="Артикул поставщика",
    )
    tech_size = Column(
        "tech_size",
        String,
        nullable=True,
        doc="Технический размер",
    )
    barcode = Column(
        "barcode",
        String,
        nullable=False,
        doc="Штрихкод",
    )
    quantity = Column(
        "quantity",
        Integer,
        nullable=True,
        doc="Количество",
    )
    total_price = Column(
        "total_price",
        REAL,
        nullable=True,
        doc="Общая стоимость",
    )
    date_close = Column(
        "date_close",
        DateTime(timezone=True),
        nullable=True,
        doc="Дата закрытия прихода",
    )
    warehouse_name = Column(
        "warehouse_name",
        String,
        nullable=True,
        doc="Наименование склада",
    )
    status = Column(
        "status",
        String,
        nullable=True,
        doc="Статус прихода",
    )
    nm_id = Column(
        "nm_id",
        BigInteger,
        nullable=True,
        doc="NM ID товара",
    )


class SilverWwwTextSearch1d(Base):
    __tablename__ = "wb_www_text_search_1d"
    __table_args__ = (
        PrimaryKeyConstraint(
            "business_dttm",
            "keyword",
            "nm_id",
            name="wb_www_text_search_1d_pkey",
        ),
        {"schema": "silver"},
    )

    # стандартные поля silver
    company_id    = Column(Integer, nullable=True,  doc="ID компании (из bronze.api_token_id)")
    request_uuid  = Column(UUID(as_uuid=True), nullable=True, default=uuid.uuid4, doc="UUID запроса из bronze")
    inserted_at   = Column(DateTime(timezone=True),
                           nullable=False,
                           server_default=text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"))
    response_dttm = Column(DateTime(timezone=True), nullable=False, doc="Время ответа API (из bronze)")
    business_dttm = Column(DateTime(timezone=True), nullable=False, doc="D+(-1) 00:00:00 MSK")

    # параметры поиска
    keyword     = Column(String,  nullable=False)
    date        = Column(DateTime(timezone=True), nullable=False)
    nm_position = Column(Integer, nullable=False)

    app_type   = Column(Integer, nullable=False)
    lang       = Column(String,  nullable=False)
    cur        = Column(String,  nullable=False)
    resultset  = Column(String,  nullable=False)
    page       = Column(Integer, nullable=False, server_default=text("1"))

    # metadata.search_result
    catalog_type       = Column(String,  nullable=False)
    catalog_value      = Column(String,  nullable=False)
    normquery          = Column(String)                # (не просили менять)
    search_result_name = Column(String,  nullable=False)
    search_result_rmi  = Column(String,  nullable=False)
    search_result_title= Column(String,  nullable=False)
    search_result_rs   = Column(Integer, nullable=False)
    search_result_qv   = Column(String,  nullable=False)

    # товар
    nm_id            = Column(BigInteger, nullable=False)  # бывший product_id
    product_time_1   = Column(Integer, nullable=False)
    product_time_2   = Column(Integer, nullable=False)
    product_wh       = Column(Integer, nullable=False)
    product_d_type   = Column(BigInteger, nullable=False)
    product_dist     = Column(Integer, nullable=False)
    product_root     = Column(Integer, nullable=False)
    product_kind_id  = Column(Integer, nullable=False)

    product_brand        = Column(String,  nullable=False)
    product_brand_id     = Column(Integer, nullable=False)
    product_site_brand_id= Column(Integer, nullable=False)

    product_colors_id   = Column(Integer)  # не просили менять
    product_colors_name = Column(String)

    product_subject_id        = Column(Integer, nullable=False)
    product_subject_parent_id = Column(Integer, nullable=False)

    product_name    = Column(String,  nullable=False)
    product_entity  = Column(String,  nullable=False)
    product_match_id= Column(Integer, nullable=False)

    product_supplier        = Column(String,  nullable=False)
    product_supplier_id     = Column(Integer, nullable=False)
    product_supplier_rating = Column(Float,   nullable=False)
    product_supplier_flags  = Column(Integer, nullable=False)
    product_pics            = Column(Integer, nullable=False)

    product_rating          = Column(Integer, nullable=False)
    product_review_rating   = Column(Float,   nullable=False)
    product_nm_review_rating= Column(Float,   nullable=False)
    product_feedbacks       = Column(Integer, nullable=False)
    product_nm_feedbacks    = Column(Integer, nullable=False)
    product_panel_promo_id  = Column(Integer)  # не просили менять
    product_volume          = Column(Integer, nullable=False)
    product_view_flags      = Column(Integer, nullable=False)

    # размеры
    product_sizes_name            = Column(String,  nullable=False)
    product_sizes_orig_name       = Column(String,  nullable=False)
    product_sizes_rank            = Column(Integer, nullable=False)
    product_sizes_option_id       = Column(Integer, nullable=False)
    product_sizes_wh              = Column(Integer, nullable=False)
    product_sizes_time_1          = Column(Integer, nullable=False)
    product_sizes_time_2          = Column(Integer, nullable=False)
    product_sizes_d_type          = Column(BigInteger, nullable=False)
    product_sizes_price_basic     = Column(Integer, nullable=False)
    product_sizes_price_product   = Column(Integer, nullable=False)
    product_sizes_price_logistics = Column(Integer, nullable=False)
    product_sizes_price_return    = Column(Integer, nullable=False)
    product_sizes_sale_conditions = Column(Integer, nullable=False)
    product_sizes_payload         = Column(Text,    nullable=False)

    product_total_quantity = Column(Integer)  # не просили менять

    # лог
    product_log_cpm          = Column(Integer)  # не просили менять
    product_log_promotion    = Column(Integer)
    product_log_promo_position = Column(Integer)
    product_log_position     = Column(Integer)
    product_log_advert_id    = Column(Integer)
    product_log_tp           = Column(String)
    product_logs             = Column(Text)

    # meta
    product_meta_tokens   = Column(String,  nullable=False)
    product_meta_preset_id= Column(Integer, nullable=False)

class WbSearchResults6h(Base):
    __tablename__ = "wb_search_results_6h"
    __table_args__ = (
        PrimaryKeyConstraint(
            "business_dttm",
            "keyword",
            "page",
            "nm_position",
            name="wb_search_results_6h_pkey",
        ),
        {"schema": "silver"},
    )

    # стандартные поля silver
    company_id    = Column(Integer, nullable=True,  doc="ID компании (из bronze.api_token_id)")
    request_uuid  = Column(UUID(as_uuid=True), nullable=True, default=uuid.uuid4, doc="UUID запроса из bronze")
    inserted_at   = Column(DateTime(timezone=True),
                           nullable=False,
                           server_default=text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"))
    response_dttm = Column(DateTime(timezone=True), nullable=False, doc="Время ответа API (из bronze)")
    business_dttm = Column(DateTime(timezone=True), nullable=False, doc="D+(-1) 00:00:00 MSK")

    # параметры поиска
    keyword     = Column(String,  nullable=False)
    date        = Column(DateTime(timezone=True), nullable=False)
    nm_position = Column(Integer, nullable=False)

    app_type   = Column(Integer, nullable=False)
    lang       = Column(String,  nullable=False)
    cur        = Column(String,  nullable=False)
    resultset  = Column(String,  nullable=False)
    page       = Column(Integer, nullable=False, server_default=text("1"))

    # metadata.search_result
    catalog_type       = Column(String,  nullable=False)
    catalog_value      = Column(String,  nullable=False)
    normquery          = Column(String)                # (не просили менять)
    search_result_name = Column(String,  nullable=False)
    search_result_rmi  = Column(String,  nullable=False)
    search_result_title= Column(String,  nullable=False)
    search_result_rs   = Column(Integer, nullable=False)
    search_result_qv   = Column(String,  nullable=False)

    # товар
    nm_id            = Column(BigInteger, nullable=False)  # бывший product_id
    product_time_1   = Column(Integer, nullable=False)
    product_time_2   = Column(Integer, nullable=False)
    product_wh       = Column(Integer, nullable=False)
    product_d_type   = Column(BigInteger, nullable=False)
    product_dist     = Column(Integer, nullable=False)
    product_root     = Column(Integer, nullable=False)
    product_kind_id  = Column(Integer, nullable=False)

    product_brand        = Column(String,  nullable=False)
    product_brand_id     = Column(Integer, nullable=False)
    product_site_brand_id= Column(Integer, nullable=False)

    product_colors_id   = Column(Integer)  # не просили менять
    product_colors_name = Column(String)

    product_subject_id        = Column(Integer, nullable=False)
    product_subject_parent_id = Column(Integer, nullable=False)

    product_name    = Column(String,  nullable=False)
    product_entity  = Column(String,  nullable=False)
    product_match_id= Column(Integer, nullable=False)

    product_supplier        = Column(String,  nullable=False)
    product_supplier_id     = Column(Integer, nullable=False)
    product_supplier_rating = Column(Float,   nullable=False)
    product_supplier_flags  = Column(Integer, nullable=False)
    product_pics            = Column(Integer, nullable=False)

    product_rating          = Column(Integer, nullable=False)
    product_review_rating   = Column(Float,   nullable=False)
    product_nm_review_rating= Column(Float,   nullable=False)
    product_feedbacks       = Column(Integer, nullable=False)
    product_nm_feedbacks    = Column(Integer, nullable=False)
    product_panel_promo_id  = Column(Integer)  # не просили менять
    product_volume          = Column(Integer, nullable=False)
    product_view_flags      = Column(Integer, nullable=False)

    # размеры
    product_sizes_name            = Column(String,  nullable=False)
    product_sizes_orig_name       = Column(String,  nullable=False)
    product_sizes_rank            = Column(Integer, nullable=False)
    product_sizes_option_id       = Column(Integer, nullable=False)
    product_sizes_wh              = Column(Integer, nullable=False)
    product_sizes_time_1          = Column(Integer, nullable=False)
    product_sizes_time_2          = Column(Integer, nullable=False)
    product_sizes_d_type          = Column(BigInteger, nullable=False)
    product_sizes_price_basic     = Column(Integer, nullable=False)
    product_sizes_price_product   = Column(Integer, nullable=False)
    product_sizes_price_logistics = Column(Integer, nullable=False)
    product_sizes_price_return    = Column(Integer, nullable=False)
    product_sizes_sale_conditions = Column(Integer, nullable=False)
    product_sizes_payload         = Column(Text,    nullable=False)

    product_total_quantity = Column(Integer)  # не просили менять

    # лог
    product_log_cpm          = Column(Integer)  # не просили менять
    product_log_promotion    = Column(Integer)
    product_log_promo_position = Column(Integer)
    product_log_position     = Column(Integer)
    product_log_advert_id    = Column(Integer)
    product_log_tp           = Column(String)
    product_logs             = Column(Text)

    # meta
    product_meta_tokens   = Column(String,  nullable=False)
    product_meta_preset_id= Column(Integer, nullable=False)


class WbRetentions1d(Base):
    __tablename__ = "wb_retentions_1d"
    __table_args__ = (
        PrimaryKeyConstraint("company_id", "request_uuid", "retention_id", "retentions_description"),
        {"schema": "silver"},
    )
    company_id = Column(
        Integer,
        nullable=False,
        server_default=text("99"),
        doc="ID компании (временный дефолт 99 для бэкфилла)",
    )
    inserted_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"),
        doc="Время вставки записи (МСК)",
    )
    request_uuid = Column(UUID(as_uuid=True), nullable=False)
    response_dttm = Column(TIMESTAMP(timezone=True), nullable=True)
    date = Column(Date, nullable=False)
    retention_id = Column(String, nullable=False)
    retention_amount = Column(Float, nullable=False)
    retentions_description = Column(
        String,
        nullable=False,
        server_default=text("''"),
    )

class WbCompensations1d(Base):
    __tablename__ = "wb_compensations_1d"
    __table_args__ = (
        PrimaryKeyConstraint(
            "company_id", "request_uuid", "compensation_id", "nm_id", "supplier_article"
        ),
        {"schema": "silver"},
    )

    company_id = Column(
        Integer,
        nullable=False,
        server_default=text("99"),
        doc="ID компании (временный дефолт 99 для бэкфилла)",
    )
    inserted_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"),
        doc="Время вставки записи (МСК)",
    )

    request_uuid = Column(UUID(as_uuid=True), nullable=False)
    response_dttm = Column(TIMESTAMP(timezone=True), nullable=True)
    date = Column(Date, nullable=False)
    compensation_id = Column(String, nullable=False)
    brand = Column(String, nullable=True)
    name = Column(String, nullable=True)
    nm_id = Column(Integer, nullable=False, server_default=text("0"))
    supplier_article = Column(String, nullable=False, server_default=text("''"))
    barcode = Column(String, nullable=True)
    subject = Column(String, nullable=True)
    compensation_amount = Column(Float, nullable=False)
    compensation_type = Column(String, nullable=True)

class WbLogistics1d(Base):
    __tablename__ = "wb_logistics_1d"
    __table_args__ = (
        PrimaryKeyConstraint(
            "business_dttm", "sr_id", "logistic_type"
        ),
        {"schema": "silver"},
    )

    company_id   = Column(Integer, nullable=False, server_default=text("99"))
    request_uuid = Column(UUID(as_uuid=True), nullable=False)
    inserted_at  = Column(DateTime(timezone=True),
                          nullable=False,
                          server_default=text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"))
    business_dttm = Column(TIMESTAMP(timezone=True), nullable=False)
    response_dttm = Column(TIMESTAMP(timezone=True), nullable=False)  # было nullable=True

    order_date = Column(Date, nullable=True)
    sale_date = Column(Date, nullable=True)
    payment_reason = Column(String, nullable=True)
    sr_id = Column(String, nullable=False)
    income_id = Column(Integer, nullable=True)
    brand = Column(String, nullable=True)
    name = Column(String, nullable=True)
    nm_id = Column(Integer, nullable=False)
    supplier_article = Column(String, nullable=False)
    barcode = Column(String, nullable=True)
    tech_size = Column(String, nullable=True)
    warehouse_name = Column(String, nullable=True)
    country = Column(String, nullable=True)
    logistics_cost = Column(Float, nullable=False)
    logistic_type = Column(String, nullable=False)
    delivery_quantity = Column(Integer, nullable=True)


class WbFines1d(Base):
    __tablename__ = "wb_fines_1d"
    __table_args__ = (
        PrimaryKeyConstraint(
            'company_id', 'request_uuid', 'fine_id', 'nm_id', 'supplier_article', 'fine_description'
        ),
        {'schema': 'silver'}
    )
    company_id = Column(
        Integer,
        nullable=False,
        server_default=text("99"),
        doc="ID компании (временный дефолт 99 для бэкфилла)",
    )
    inserted_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"),
        doc="Время вставки записи (МСК)",
    )
    request_uuid = Column(UUID(as_uuid=True), nullable=False)
    response_dttm = Column(TIMESTAMP(timezone=True), nullable=True)
    date = Column(Date, nullable=False)
    fine_id = Column(String, nullable=False)
    brand = Column(String, nullable=True)
    name = Column(String, nullable=True)
    nm_id = Column(Integer, nullable=False)
    supplier_article = Column(String, nullable=False)
    barcode = Column(String, nullable=True)
    subject = Column(String, nullable=True)
    fine_description = Column(String, nullable=True)
    fine_amount = Column(Float, nullable=False)


class WbSales1d(Base):
    __tablename__ = "wb_sales_1d"
    __table_args__ = (
        PrimaryKeyConstraint(
            "business_dttm", "sr_id", "payment_reason",
        ),
        {"schema": "silver"},
    )

    company_id   = Column(Integer, nullable=False, server_default=text("99"))
    request_uuid = Column(UUID(as_uuid=True), nullable=False)
    inserted_at  = Column(DateTime(timezone=True),
                          nullable=False,
                          server_default=text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"))
    business_dttm = Column(TIMESTAMP(timezone=True), nullable=False)
    response_dttm = Column(TIMESTAMP(timezone=True), nullable=False)
    order_date = Column(Date, nullable=False)
    sale_date = Column(Date, nullable=False)
    payment_reason = Column(String, nullable=False)
    sr_id = Column(String, nullable=False)
    income_id = Column(Integer, nullable=True)
    brand = Column(String, nullable=False)
    name = Column(String, nullable=False)
    nm_id = Column(Integer, nullable=False)
    supplier_article = Column(String, nullable=False)
    barcode = Column(String, nullable=False)
    tech_size = Column(String, nullable=False)
    subject = Column(String, nullable=False)
    quantity = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    price_with_discount = Column(Float, nullable=False)
    wb_realization_price = Column(Float, nullable=False)
    spp = Column(Float, nullable=False)
    seller_payout = Column(Float, nullable=False)
    commision_percent = Column(Float, nullable=False)
    acquiring_amount = Column(Float, nullable=False)
    acquiring_percent = Column(Float, nullable=False)
    acquiring_bank = Column(String, nullable=False)
    warehouse_name = Column(String, nullable=False)
    country = Column(String, nullable=False)
    loyalty_points_withheld_amount = Column(Numeric(14, 2), nullable=True)


class SilverAdjustmentsByProduct(Base):
    """
    Удержания/компенсации, которые можно атрибутировать к конкретным товарам.
    Источник: bronze.wb_www_fin_report_1d

    Фильтр по типам «Обоснование для оплаты» (supplier_oper_name):
      - Штраф
      - Удержание
      - Добровольная компенсация при возврате
      - Компенсация ущерба
      - Компенсация скидки по программе лояльности
      - Сумма удержанная за начисленные баллы программы лояльности
      - Стоимость участия в программе лояльности
    """
    __tablename__  = "adjustments_by_product"
    __table_args__ = (
        # составной PK — на уровне загрузки один и тот же sr_id может встречаться в разных типах операций
        PrimaryKeyConstraint("business_dttm", "sr_id", "supplier_oper_name", name="pk_adjustments_by_product"),
        Index("ix_adjustments_by_product_business_dttm", "business_dttm"),
        Index("ix_adjustments_by_product_company_id", "company_id"),
        {"schema": "silver"},
    )
    __mapper_args__ = {"always_refresh": True}

    # обязательные поля silver
    request_uuid  = Column(UUID(as_uuid=True), nullable=False, default=uuid.uuid4, doc="UUID из bronze")
    company_id    = Column(Integer,           nullable=False,                     doc="FK → core.tokens.token_id")
    inserted_at   = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, doc="Вставка (MSK)")
    response_dttm = Column(DateTime(timezone=True), nullable=False, doc="Из bronze по request_uuid")
    business_dttm = Column(DateTime(timezone=True), nullable=False, doc="Из bronze по request_uuid")

    # данные из отчёта
    income_id           = Column(BigInteger, nullable=True,  doc="Номер поставки")
    subject             = Column(String,    nullable=True,  doc="Предмет")
    nomenclature_code   = Column(BigInteger, nullable=True,  doc="Код номенклатуры (nm_id)")
    brand               = Column(String,    nullable=True,  doc="Бренд")
    supplier_article    = Column(String,    nullable=True,  doc="Артикул поставщика")
    name                = Column(String,    nullable=True,  doc="Название")
    tech_size           = Column(String,    nullable=True,  doc="Размер")
    barcode             = Column(String,    nullable=True,  doc="Баркод")
    doc_type_name       = Column(String,    nullable=True,  doc="Тип документа")
    supplier_oper_name  = Column(String,    nullable=False, doc="Обоснование для оплаты (тип операции)")
    order_date          = Column(Date,      nullable=True,  doc="Дата заказа покупателем")
    sale_date           = Column(Date,      nullable=True,  doc="Дата продажи")
    penalty             = Column(Numeric(10, 2), nullable=True, doc="Общая сумма штрафов")
    bonus_type_name     = Column(String,    nullable=True,  doc="Виды логистики, штрафов и доплат")
    office_number       = Column(String,    nullable=True,  doc="Номер офиса")
    warehouse_name      = Column(String,    nullable=True,  doc="Склад")
    country             = Column(String,    nullable=True,  doc="Страна")
    box_type            = Column(String,    nullable=True,  doc="Тип коробов")
    shk_id              = Column(String,    nullable=True,  doc="ШК")
    sr_id               = Column(String,    nullable=False, doc="Srid (идентификатор операции/заказа)")
    seller_payout       = Column(Numeric(10, 2), nullable=True, doc="К перечислению Продавцу за товар")
    additional_payment  = Column(Numeric(10, 2), nullable=True, doc="Корректировка вознаграждения WB")
    cashback_amount     = Column(Numeric(10, 2), nullable=True, doc="Удержано за баллы лояльности")
    cashback_discount   = Column(Numeric(10, 2), nullable=True, doc="Компенсация скидки по лояльности")

    # допустимые причины (для трансформаций)
    ALLOWED_REASONS = (
        "Штраф",
        "Удержание",
        "Добровольная компенсация при возврате",
        "Компенсация ущерба",
        "Компенсация скидки по программе лояльности",
        "Сумма удержанная за начисленные баллы программы лояльности",
        "Стоимость участия в программе лояльности",
    )


class SilverAdjustmentsGeneral(Base):
    """
    Удержания/компенсации, которые НЕЛЬЗЯ атрибутировать к конкретным товарам.
    Источник: bronze.wb_www_fin_report_1d

    Фильтр по типам «Обоснование для оплаты» (supplier_oper_name):
      - Штраф
      - Удержание
      - Добровольная компенсация при возврате
      - Компенсация ущерба
      - Компенсация скидки по программе лояльности
      - Сумма удержанная за начисленные баллы программы лояльности
      - Стоимость участия в программе лояльности
    """
    __tablename__  = "adjustments_general"
    __table_args__ = (
        PrimaryKeyConstraint("business_dttm", "sr_id", "supplier_oper_name", name="pk_adjustments_general"),
        Index("ix_adjustments_general_business_dttm", "business_dttm"),
        Index("ix_adjustments_general_company_id", "company_id"),
        {"schema": "silver"},
    )
    __mapper_args__ = {"always_refresh": True}

    # обязательные поля silver
    request_uuid  = Column(UUID(as_uuid=True), nullable=False, default=uuid.uuid4, doc="UUID из bronze")
    company_id    = Column(Integer,           nullable=False,                     doc="FK → core.tokens.token_id")
    inserted_at   = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, doc="Вставка (MSK)")
    response_dttm = Column(DateTime(timezone=True), nullable=False, doc="Из bronze по request_uuid")
    business_dttm = Column(DateTime(timezone=True), nullable=False, doc="Из bronze по request_uuid")

    # данные из отчёта
    income_id          = Column(BigInteger, nullable=True,  doc="Номер поставки")
    subject            = Column(String,    nullable=True,  doc="Предмет")
    nm_id              = Column(BigInteger, nullable=True,  doc="Код номенклатуры (nm_id)")
    brand              = Column(String,    nullable=True,  doc="Бренд")
    supplier_article   = Column(String,    nullable=True,  doc="Артикул поставщика")
    name               = Column(String,    nullable=True,  doc="Название")
    tech_size          = Column(String,    nullable=True,  doc="Размер")
    barcode            = Column(String,    nullable=True,  doc="Баркод")
    doc_type_name      = Column(String,    nullable=True,  doc="Тип документа")
    supplier_oper_name = Column(String,    nullable=False, doc="Обоснование для оплаты (тип операции)")
    order_date         = Column(Date,      nullable=True,  doc="Дата заказа покупателем")
    sale_date          = Column(Date,      nullable=True,  doc="Дата продажи")
    penalty            = Column(Numeric(10, 2), nullable=True, doc="Общая сумма штрафов")
    bonus_type_name    = Column(String,    nullable=True,  doc="Виды логистики, штрафов и доплат")
    office_number      = Column(String,    nullable=True,  doc="Номер офиса")
    warehouse_name     = Column(String,    nullable=True,  doc="Склад")
    country            = Column(String,    nullable=True,  doc="Страна")
    box_type           = Column(String,    nullable=True,  doc="Тип коробов")
    shk_id             = Column(String,    nullable=True,  doc="ШК")
    sr_id              = Column(String,    nullable=False, doc="Srid (идентификатор операции/заказа)")
    deduction          = Column(Numeric(10, 2), nullable=True, doc="Удержания (общая сумма)")

    ALLOWED_REASONS = SilverAdjustmentsByProduct.ALLOWED_REASONS


class SilverWbWwwCashbackReports1w(Base):
    __tablename__ = "wb_www_cashback_reports_1w"
    __table_args__ = (
        PrimaryKeyConstraint("company_id", "business_dttm"),
        {"schema": "silver"},
    )

    # ключи и техполя
    company_id    = Column(Integer, nullable=False, doc="ID компании")
    request_uuid  = Column(UUID(as_uuid=True), nullable=False, doc="UUID запроса из бронзы")
    business_dttm = Column(TIMESTAMP(timezone=True), nullable=False, doc="Начало бизнес-периода (MSK)")
    inserted_at  = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    response_dttm = Column(TIMESTAMP(timezone=True), nullable=True)

    # период отчёта
    period_start_date = Column(Date, nullable=False)
    period_end_date   = Column(Date, nullable=False)

    # показатели кэшбэка
    cashback_provided      = Column(Numeric(10, 2), nullable=True)  # начислено
    cashback_used          = Column(Numeric(10, 2), nullable=True)  # списано/использовано
    cashback_expired       = Column(Numeric(10, 2), nullable=True)  # сгорело
    cashback_returned      = Column(Numeric(10, 2), nullable=True)  # возвращено
    cashback_final_balance = Column(Numeric(10, 2), nullable=True)  # конечный остаток


class SilverWbAdvPromotions1h(Base):
    __tablename__ = "wb_adv_promotions_1h"
    __table_args__ = (
        PrimaryKeyConstraint("advert_id", "business_dttm", name="pk_adv_promotions_1h"),
        {"schema": "silver"},
    )
    request_uuid  = Column(UUID(as_uuid=True), nullable=False, default=uuid.uuid4)
    company_id    = Column(Integer, nullable=False)
    inserted_at   = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    business_dttm = Column(DateTime(timezone=True), nullable=False)
    response_dttm = Column(DateTime(timezone=True), nullable=False)
    advert_id     = Column(BigInteger, nullable=False)
    advert_status = Column(SmallInteger, nullable=False)
    advert_type   = Column(SmallInteger, nullable=True)
    change_time   = Column(DateTime(timezone=True), nullable=True)


class SilverWbPaidStorage1DNew(Base):
    """
    Сильвер-таблица WB Paid Storage (1d), нормализованные записи из bronze.
    Требования:
      - PK: (business_dttm, warehouse_name, income_id, barcode, calc_type)
      - request_uuid → ссылка на bronze
      - company_id → из bronze (маппинг по токену)
      - response_dttm / business_dttm / inserted_at — TIMESTAMPTZ NOT NULL
      - бизнес-поля — NOT NULL
    """
    __tablename__  = "wb_paid_storage_1d_new"
    __table_args__ = (
        PrimaryKeyConstraint(
            "business_dttm", "warehouse_name", "income_id", "barcode", "calc_type",
            name="pk_paid_storage_1d_new"
        ),
        Index("ix_swps1d_new_company_biz", "company_id", "business_dttm"),
        Index("ix_swps1d_new_request_uuid", "request_uuid"),
        {"schema": "silver"},
    )
    request_uuid      = Column(UUID(as_uuid=True),
                               ForeignKey("bronze.wb_paid_storage_1d_new.request_uuid"),
                               nullable=False,
                               doc="UUID запроса из bronze")
    company_id        = Column(Integer,
                               ForeignKey("core.companies.company_id"),
                               nullable=False,
                               doc="ID компании (из bronze по api_token_id)")
    response_dttm     = Column(TIMESTAMP(timezone=True),
                               nullable=False,
                               doc="TIMESTAMPTZ из bronze по request_uuid")
    business_dttm     = Column(TIMESTAMP(timezone=True),
                               nullable=False,
                               doc="TIMESTAMPTZ из bronze по request_uuid")
    inserted_at       = Column(TIMESTAMP(timezone=True),
                               nullable=False,
                               server_default=text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"),
                               doc="TIMESTAMPTZ вставки строки (MSK, сервер БД)")
    date              = Column(Date, nullable=False, doc="Дата метрики (день)")
    warehouse_id      = Column(Integer, nullable=False, doc="ID склада")
    warehouse_name    = Column(String,  nullable=False, doc="Название склада")
    warehouse_coef    = Column(Float,   nullable=False, doc="Коэффициент склада")
    income_id         = Column(Integer, nullable=False, doc="Номер поставки / incomeId")
    chrt_id           = Column(Integer, nullable=False, doc="chrtId")
    tech_size         = Column(String,  nullable=False, doc="Размер/techSize")
    barcode           = Column(String,  nullable=False, doc="Штрихкод")
    supplier_article  = Column(String,  nullable=False, doc="Артикул поставщика")
    nm_id             = Column(Integer, nullable=False, doc="NM ID")
    volume            = Column(Float,   nullable=False, doc="Объём")
    calc_type         = Column(String,  nullable=False, doc="Тип расчёта")
    warehouse_price   = Column(Float,   nullable=False, doc="Стоимость хранения")
    barcodes_count    = Column(Integer, nullable=False, doc="Кол-во штрихкодов")
    pallet_place_code = Column(Integer, nullable=False, doc="Код паллетоместа")
    pallet_count      = Column(Float,   nullable=False, doc="Кол-во паллет")
    loyalty_discount  = Column(Float,   nullable=False, doc="Скидка лояльности")
