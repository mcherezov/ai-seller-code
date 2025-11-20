from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Text
from sqlalchemy.orm import relationship
from src.db.base import Base


class Seller(Base):
    __tablename__ = "sellers"
    __table_args__ = {"schema": "core"}

    id = Column("seller_id", Integer, primary_key=True)
    name = Column(Text, nullable=False)
    is_active = Column(Boolean, default=True)

    companies = relationship("Company", back_populates="seller")


class Company(Base):
    __tablename__ = "companies"
    __table_args__ = {"schema": "core"}

    id = Column("company_id", Integer, primary_key=True)
    seller_id = Column(Integer, ForeignKey("core.sellers.seller_id"), nullable=False)
    name = Column(Text, nullable=False)
    is_active = Column(Boolean, default=True)

    seller = relationship("Seller", back_populates="companies")
    tokens = relationship("Token", back_populates="company")


class Marketplace(Base):
    __tablename__ = "marketplaces"
    __table_args__ = {"schema": "core"}

    id = Column("mp_id", Integer, primary_key=True)
    name = Column(Text, nullable=False)
    is_active = Column(Boolean, default=True)

    tokens = relationship("Token", back_populates="marketplace")


class Token(Base):
    __tablename__ = "tokens"
    __table_args__ = {"schema": "core"}

    id = Column("token_id", Integer, primary_key=True)
    mp_id = Column(Integer, ForeignKey("core.marketplaces.mp_id"), nullable=False)
    company_id = Column(Integer, ForeignKey("core.companies.company_id"), nullable=False)
    token = Column(Text, nullable=False)
    is_active = Column(Boolean, default=True)

    marketplace = relationship("Marketplace", back_populates="tokens")
    company = relationship("Company", back_populates="tokens")
