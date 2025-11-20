from sqlalchemy import Column, Text, TIMESTAMP, func, BigInteger, Index, desc, Integer, ForeignKey, text
from sqlalchemy.dialects.postgresql import UUID, JSONB, BYTEA
from sqlalchemy.orm import declared_attr
from sqlalchemy.schema import Sequence
from src.db.base import Base
import uuid


class BronzeBaseMixin:
    """
    Абстрактный миксин для всех «бронзовых» таблиц.
    Не будет создавать своей собственной таблицы, т.к. миксин не
    объявлен как __tablename__.
    Содержит базовые поля:
      - id              BIGINT PRIMARY KEY (через Sequence)
      - content         TEXT NOT NULL
      - batch_id        UUID NOT NULL
      - inserted_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
      - token_id        INTEGER (FK → core.tokens)
      - response_uuid   TEXT (x-request-id от API)
      - response_dttm   TIMESTAMP WITH TIME ZONE (время ответа API в МСК)
    """

    @declared_attr
    def __tablename__(self):
        return None

    @declared_attr
    def id(cls):
        seq_name = f"{cls.__tablename__}_id_seq"
        return Column(
            "id",
            BigInteger,
            Sequence(seq_name, schema="bronze"),
            primary_key=True,
            index=True,
        )

    content = Column("content", Text, nullable=False)
    batch_id = Column("batch_id", UUID(as_uuid=True), nullable=False, index=True)
    inserted_at = Column("inserted_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)

    token_id = Column("token_id", Integer, ForeignKey("core.tokens.token_id"), nullable=True)
    response_uuid = Column("response_uuid", Text, nullable=True)
    response_dttm = Column("response_dttm", TIMESTAMP(timezone=True), nullable=True)


class BronzeBazeExtendedMixin:
    """
    Абстрактный миксин для расширенных «бронзовых» таблиц.
    Не будет создавать своей собственной таблицы, т.к. миксин не
    объявлен как __tablename__.
    Содержит поля:
      - api_token_id       INTEGER (FK → core.tokens.token_id)
      - run_uuid           UUID NOT NULL (Идентификатор DAG Run)
      - run_dttm           TIMESTAMPTZ NOT NULL (Плановое время запуска DAG)
      - request_uuid       UUID PRIMARY KEY (Уникальный идентификатор запроса)
      - request_dttm       TIMESTAMPTZ NOT NULL DEFAULT NOW() (Время отправки запроса)
      - request_parameters JSONB NULL (Параметры GET-запроса)
      - request_body       JSONB NULL (Тело запроса)
      - response_dttm      TIMESTAMPTZ NULL (Время ответа от API)
      - response_code      INT NOT NULL (HTTP-код ответа)
      - response_body      TEXT NULL (Тело ответа API)
      - inserted_at        TIMESTAMPTZ NOT NULL DEFAULT NOW() (Время вставки записи)
    """

    @declared_attr
    def api_token_id(cls):
        return Column(
            "api_token_id",
            Integer,
            ForeignKey("core.tokens.token_id"),
            nullable=False,
            index=True,
            doc="Ссылка на токен из core.tokens"
        )

    @declared_attr
    def run_uuid(cls):
        return Column(
            "run_uuid",
            UUID(as_uuid=True),
            nullable=False,
            index=True,
            doc="Идентификатор DAG Run"
        )

    @declared_attr
    def run_dttm(cls):
        return Column(
            "run_dttm",
            TIMESTAMP(timezone=True),
            nullable=False,
            doc="Плановое время запуска DAG"
        )

    @declared_attr
    def request_uuid(cls):
        return Column(
            "request_uuid",
            UUID(as_uuid=True),
            primary_key=True,
            default=uuid.uuid4,
            doc="Уникальный идентификатор запроса"
        )

    @declared_attr
    def request_dttm(cls):
        return Column(
            "request_dttm",
            TIMESTAMP(timezone=True),
            server_default=func.now(),
            nullable=False,
            doc="Время отправки запроса"
        )

    @declared_attr
    def request_parameters(cls):
        return Column(
            "request_parameters",
            JSONB,
            nullable=True,
            doc="Параметры GET-запроса"
        )

    @declared_attr
    def request_body(cls):
        return Column(
            "request_body",
            JSONB,
            nullable=True,
            doc="Тело запроса"
        )

    @declared_attr
    def response_dttm(cls):
        return Column(
            "response_dttm",
            TIMESTAMP(timezone=True),
            nullable=True,
            doc="Время ответа API"
        )

    @declared_attr
    def response_code(cls):
        return Column(
            "response_code",
            Integer,
            nullable=False,
            doc="HTTP-код ответа"
        )

    @declared_attr
    def response_body(cls):
        return Column(
            "response_body",
            Text,
            nullable=True,
            doc="Тело ответа API"
        )

    @declared_attr
    def inserted_at(cls):
        return Column(
            "inserted_at",
            TIMESTAMP(timezone=True),
            server_default=func.now(),
            nullable=False,
            doc="Время вставки записи"
        )


class BronzeBazeV2Mixin:
    """
    Абстрактный миксин для расширенных «бронзовых» таблиц.
    Не будет создавать своей собственной таблицы, т.к. миксин не
    объявлен как __tablename__.
    Содержит поля:
      - api_token_id       INTEGER (FK → core.tokens.token_id)
      - run_uuid           UUID NOT NULL (Идентификатор DAG Run)
      - run_dttm           TIMESTAMPTZ NOT NULL (Плановое время запуска DAG)
      - request_uuid       UUID PRIMARY KEY (Уникальный идентификатор запроса)
      - request_dttm       TIMESTAMPTZ NOT NULL DEFAULT NOW() (Время отправки запроса)
      - request_parameters JSONB NULL (Параметры GET-запроса)
      - request_body       JSONB NULL (Тело запроса)
      - response_dttm      TIMESTAMPTZ NULL (Время ответа от API)
      - response_code      INT NOT NULL (HTTP-код ответа)
      - response_body      TEXT NULL (Тело ответа API)
      - inserted_at        TIMESTAMPTZ NOT NULL DEFAULT NOW() (Время вставки записи)
    """

    @declared_attr
    def api_token_id(cls):
        return Column(
            "api_token_id",
            Integer,
            ForeignKey("core.tokens.token_id"),
            nullable=False,
            index=True,
            doc="Ссылка на токен из core.tokens"
        )

    @declared_attr
    def run_uuid(cls):
        return Column(
            "run_uuid",
            UUID(as_uuid=True),
            nullable=False,
            index=True,
            doc="Идентификатор DAG Run"
        )

    @declared_attr
    def run_dttm(cls):
        return Column(
            "run_dttm",
            TIMESTAMP(timezone=True),
            nullable=False,
            doc="Реальное время запуска/перезапуска DAG Run (МСК). Одинаковое для всех записей с одним run_uuid."
        )

    @declared_attr
    def run_schedule_dttm(cls):
        return Column(
            "run_schedule_dttm",
            TIMESTAMP(timezone=True),
            nullable=False,
            doc="Плановое время запуска DAG Run (МСК). Одинаковое для записей с одним run_uuid."
        )

    @declared_attr
    def request_uuid(cls):
        return Column(
            "request_uuid",
            UUID(as_uuid=True),
            primary_key=True,
            default=uuid.uuid4,
            doc="Уникальный идентификатор запроса"
        )

    @declared_attr
    def request_dttm(cls):
        return Column(
            "request_dttm",
            TIMESTAMP(timezone=True),
            server_default=func.now(),
            nullable=False,
            doc="timestamp запроса, с таймзоной, время нашего сервера (в MSK)"
        )

    @declared_attr
    def request_parameters(cls):
        return Column(
            "request_parameters",
            JSONB,
            nullable=True,
            doc="Параметры GET-запроса"
        )

    @declared_attr
    def request_body(cls):
        return Column(
            "request_body",
            JSONB,
            nullable=True,
            doc="Тело запроса"
        )

    @declared_attr
    def response_code(cls):
        return Column(
            "response_code",
            Integer,
            nullable=False,
            server_default=text('0'),
            doc="HTTP-код ответа (0 — если ответа не было/ошибка транспорта)"
        )

    @declared_attr
    def response_dttm(cls):
        return Column(
            "response_dttm",
            TIMESTAMP(timezone=True),
            nullable=False,  # было True
            doc="timestamp в ответе API (TZ внешнего сервера). При ошибке — равен request_dttm."
        )

    @declared_attr
    def receive_dttm(cls):
        return Column(
            "receive_dttm",
            TIMESTAMP(timezone=True),
            nullable=False,  # было True
            doc="timestamp получения ответа (MSK). При ошибке — равен request_dttm."
        )

    @declared_attr
    def response_body(cls):
        return Column(
            "response_body",
            Text,
            nullable=True,
            doc="Тело ответа API"
        )

    @declared_attr
    def inserted_at(cls):
        return Column(
            "inserted_at",
            TIMESTAMP(timezone=True),
            server_default=func.now(),
            nullable=False,
            doc="timestamp вставки строки в таблицу, время СУБД (MSK)."
        )

    @declared_attr
    def business_dttm(cls):
        return Column(
            "business_dttm",
            TIMESTAMP(timezone=True),
            nullable=False,
            doc="Дата, указывающая на период лога/снэпшота."
        )


class WBOrders(BronzeBaseMixin, Base):
    __tablename__ = "wb_orders"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WBCommission(BronzeBaseMixin, Base):
    __tablename__ = "wb_commission"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WBAdConfig(BronzeBaseMixin, Base):
    __tablename__ = "wb_ad_config"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WBSalesFunnel(BronzeBaseMixin, Base):
    __tablename__ = "wb_sales_funnel"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WBStocksReport(BronzeBaseMixin, Base):
    __tablename__ = "wb_stocks_report"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WBPaidStorage(BronzeBaseMixin, Base):
    __tablename__ = "wb_paid_storage"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WBPaidAcceptions(BronzeBaseMixin, Base):
    __tablename__ = "wb_acceptions"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WBSuppliers(BronzeBaseMixin, Base):
    __tablename__ = "wb_suppliers"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WBAdStats(BronzeBaseMixin, Base):
    __tablename__ = "wb_ad_stats"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WBAdInfo(BronzeBaseMixin, Base):
    __tablename__ = "wb_ad_info"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WBClusters(BronzeBaseMixin, Base):
    __tablename__ = "wb_clusters"
    __table_args__ = (
        Index("wb_clusters_insat_desc_idx", desc("inserted_at")),
        {"schema": "bronze"},
    )
    __mapper_args__ = {"always_refresh": True}


class WBKeywords(BronzeBaseMixin, Base):
    __tablename__ = "wb_keywords"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WBSales(BronzeBaseMixin, Base):
    __tablename__ = "wb_sales"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WBSkuApi(BronzeBaseMixin, Base):
    __tablename__ = "wb_sku_api"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WbProductsSearchTexts(BronzeBaseMixin, Base):
    __tablename__ = "wb_products_search_texts"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WbAdvFullstats1d(BronzeBazeV2Mixin, Base):
    __tablename__  = "wb_adv_fullstats_1d"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

class WbAdvFullstats1h(BronzeBazeExtendedMixin, Base):
    __tablename__ = "wb_adv_fullstats_1h"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

    request_dttm = Column(
        "request_dttm",
        TIMESTAMP(timezone=True),
        nullable=False,
        doc="Время отправки запроса (MSK, задаётся приложением)"
    )

    run_schedule_dttm = Column(
        "run_schedule_dttm",
        TIMESTAMP(timezone=True),
        nullable=True,  # история заполняется NULL
        doc="Плановое время запуска DAG Run (MSK), одинаково в рамках run_uuid"
    )

    business_dttm = Column(
        "business_dttm",
        TIMESTAMP(timezone=True),
        nullable=True,  # история заполняется NULL
        doc="Начало бизнес-периода (MSK); для current-state: date_trunc('hour', run_dttm) - 1 hour"
    )

    receive_dttm = Column(
        "receive_dttm",
        TIMESTAMP(timezone=True),
        nullable=True,  # история заполняется NULL
        doc="Момент получения ответа на нашем сервере (MSK)"
    )

    response_dttm = Column(
            "response_dttm",
            TIMESTAMP(timezone=True),
            nullable=False,
            doc="Время ответа API"
        )

class WbAdvStatsKeywords1d(BronzeBazeExtendedMixin, Base):
    __tablename__ = "wb_adv_stats_keywords_1d"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

class WbAdvStatsKeywords1h(BronzeBazeExtendedMixin, Base):
    __tablename__ = "wb_adv_stats_keywords_1h"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

class WbAdvAutoStatWords1d(BronzeBazeExtendedMixin, Base):
    __tablename__ = "wb_adv_auto_stat_words_1d"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

class WbSupplierOrders1d(BronzeBazeV2Mixin, Base):
    __tablename__ = "wb_supplier_orders_1d"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

class WbTariffsCommission1d(BronzeBazeExtendedMixin, Base):
    __tablename__ = "wb_tariffs_commission_1d"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

class WbCardsList1d(BronzeBazeExtendedMixin, Base):
    __tablename__ = "wb_cards_list_1d"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

class WbWarehouseRemains1d(BronzeBazeExtendedMixin, Base):
    __tablename__ = "wb_warehouse_remains_1d"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

class WbPaidStorage1d(BronzeBazeExtendedMixin, Base):
    __tablename__ = "wb_paid_storage_1d"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

class WbAcceptanceReport1d(BronzeBazeExtendedMixin, Base):
    __tablename__ = "wb_acceptance_report_1d"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

class WbAdvPromotionAdverts1d(BronzeBazeExtendedMixin, Base):
    __tablename__ = "wb_adv_promotion_adverts_1d"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

class WbAdvPromotionAdverts1h(BronzeBazeExtendedMixin, Base):
    __tablename__ = "wb_adv_promotion_adverts_1h"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

class WbNmReportDetail1d(BronzeBazeV2Mixin, Base):
    __tablename__ = "wb_nm_report_detail_1d"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

class WbSupplierIncomes1d(BronzeBazeExtendedMixin, Base):
    __tablename__ = "wb_supplier_incomes_1d"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WbWwwFinReports1d(BronzeBazeV2Mixin, Base):
    __tablename__ = "wb_www_fin_reports_1d"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

    request_body = Column(
        "request_body",
        Text,
        nullable=True,
        doc="Download URL (TEXT)"
    )

    response_body = Column(
        "response_body",
        BYTEA,
        nullable=True,
        doc="Raw ZIP bytes (BYTEA)"
    )

class WbWwwTextSearch1d(BronzeBazeV2Mixin, Base):
    __tablename__ = "wb_www_text_search_1d"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class WbWwwCashbackReports1w(BronzeBazeExtendedMixin, Base):
    __tablename__ = "wb_www_cashback_reports_1w"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}

    response_body = Column(
        "response_body",
        BYTEA,
        nullable=True,
        doc="Raw XLSX binary",
    )

    run_schedule_dttm = Column(
        "run_schedule_dttm",
        TIMESTAMP(timezone=True),
        nullable=True,  # история заполняется NULL
        doc="Плановое время запуска DAG Run (MSK), одинаково в рамках run_uuid"
    )

    business_dttm = Column(
        "business_dttm",
        TIMESTAMP(timezone=True),
        nullable=True,  # история заполняется NULL
        doc="Начало бизнес-периода (MSK); для current-state: date_trunc('hour', run_dttm) - 1 hour"
    )

    receive_dttm = Column(
        "receive_dttm",
        TIMESTAMP(timezone=True),
        nullable=True,  # история заполняется NULL
        doc="Момент получения ответа на нашем сервере (MSK)"
    )


class WbAdvPromotions1h(BronzeBazeV2Mixin, Base):
    __tablename__ = "wb_adv_promotions_1h"
    __table_args__ = {"schema": "bronze"}
    __mapper_args__ = {"always_refresh": True}


class BronzeWbPaidStorage1DNew(BronzeBazeV2Mixin, Base):
    """
    Бронзовая таблица для метода WB /paid_storage (1d).
    Требования:
      - company_id используется НАРЯДУ с api_token_id
      - PK request_uuid (наследуется из миксина)
      - партиционирование/бэкфиллы по (business_dttm, company_id)
    """
    __tablename__  = "wb_paid_storage_1d_new"
    __table_args__ = (
        Index("ix_bz_wb_paid_storage_1d_new_company_biz", "company_id", "business_dttm"),
        Index("ix_bz_wb_paid_storage_1d_new_token_biz",  "api_token_id", "business_dttm"),
        {"schema": "bronze"},
    )

    company_id = Column(
        "company_id",
        Integer,
        ForeignKey("core.companies.company_id"),
        nullable=False,
        index=True,
        doc="ID компании (используется вместе с api_token_id; участвует в партиционировании Dagster)"
    )
