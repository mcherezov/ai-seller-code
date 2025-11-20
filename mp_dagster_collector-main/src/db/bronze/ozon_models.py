from sqlalchemy import Column, Text, TIMESTAMP, func, BigInteger, Index, desc, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB, BYTEA
from src.db.base import Base
from src.db.bronze.models import BronzeBazeV2Mixin


class OzonAdvExpense1d(Base, BronzeBazeV2Mixin):
    __tablename__  = "ozon_adv_expense_1d"
    __table_args__ = {"schema": "bronze"}

    response_body = Column(
        "response_body",
        BYTEA,
        nullable=False,
        doc="Raw CSV bytes (as returned by API)"
    )


class OzonProductInfoPrices1d(Base, BronzeBazeV2Mixin):
    __tablename__  = "ozon_product_info_prices_1d"
    __table_args__ = {"schema": "bronze"}


class OzonFinanceTransactionList1d(Base, BronzeBazeV2Mixin):
    __tablename__  = "ozon_finance_transaction_list_1d"
    __table_args__ = {"schema": "bronze"}


class OzonAnalyticsData1d(Base, BronzeBazeV2Mixin):
    __tablename__  = "ozon_analytics_data_1d"
    __table_args__ = {"schema": "bronze"}


class OzonPostingsCreate1d(Base, BronzeBazeV2Mixin):
    __tablename__ = "ozon_postings_create_1d"
    __table_args__ = {"schema": "bronze"}

    response_body = Column(
        "response_body",
        BYTEA,
        nullable=False,
        doc="Raw XLSX bytes (as returned by API)"
    )

