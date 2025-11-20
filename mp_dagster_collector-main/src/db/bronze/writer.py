import json
import base64
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Any, Union, List
from datetime import datetime

from src.db.bronze.models import (
    WBOrders,
    WBCommission,
    WBClusters,
    WBKeywords,
    WBSuppliers,
    WBAdInfo,
    WBSkuApi,
    WBAdStats,
    WBAdConfig,
    WBPaidStorage,
    WBSalesFunnel,
    WBStocksReport,
    WBPaidAcceptions,
)

from .model_protocol import TableModelProtocol


async def save_json_response(
    session: AsyncSession,
    model: TableModelProtocol,
    raw_data: Union[dict, List[dict]],
    batch_id: str,
    token_id: int,
    response_uuid: str,
    response_dttm: datetime,
) -> None:
    """
    Сохраняет JSON-ответ по схеме: 1 запрос = 1 строка
    (или по одному insert'у на каждый элемент списка)
    """
    table_name = model.__tablename__
    table_args = model.__table_args__

    if isinstance(table_args, dict):
        schema = table_args.get("schema")
    elif isinstance(table_args, tuple):
        schema = next((arg.get("schema") for arg in table_args if isinstance(arg, dict)), None)
    else:
        schema = None

    if not schema:
        raise ValueError(f"Не удалось определить схему для модели {model.__name__}")

    full_table = f"{schema}.{table_name}"

    if isinstance(raw_data, list):
        for entry in raw_data:
            content_str = json.dumps(entry, ensure_ascii=False)
            stmt = text(f"""
                INSERT INTO {full_table} (content, batch_id, token_id, response_uuid, response_dttm)
                VALUES (:content, :batch_id, :token_id, :response_uuid, :response_dttm)
            """)
            await session.execute(stmt, {
                "content": content_str,
                "batch_id": batch_id,
                "token_id": token_id,
                "response_uuid": response_uuid,
                "response_dttm": response_dttm,
            })
    else:
        content_str = json.dumps(raw_data, ensure_ascii=False)
        stmt = text(f"""
            INSERT INTO {full_table} (content, batch_id, token_id, response_uuid, response_dttm)
            VALUES (:content, :batch_id, :token_id, :response_uuid, :response_dttm)
        """)
        await session.execute(stmt, {
            "content": content_str,
            "batch_id": batch_id,
            "token_id": token_id,
            "response_uuid": response_uuid,
            "response_dttm": response_dttm,
        })

    await session.commit()


async def save_binary_response(
    session: AsyncSession,
    model: TableModelProtocol,
    raw_data: bytes,
    batch_id: str,
    token_id: int,
    response_uuid: str,
    response_dttm: datetime,
) -> None:
    """
    Сохраняет бинарные ответы (например, CSV/XLSX) как base64-строку
    """
    b64 = base64.b64encode(raw_data).decode("ascii")
    table_args = model.__table_args__
    schema = table_args.get("schema") if isinstance(table_args, dict) else None
    if not schema:
        raise ValueError(f"Не удалось определить схему для модели {model.__name__}")
    full_table = f"{schema}.{model.__tablename__}"

    stmt = text(f"""
        INSERT INTO {full_table} (content, batch_id, token_id, response_uuid, response_dttm)
        VALUES (:content, :batch_id, :token_id, :response_uuid, :response_dttm)
    """)
    await session.execute(stmt, {
        "content": b64,
        "batch_id": batch_id,
        "token_id": token_id,
        "response_uuid": response_uuid,
        "response_dttm": response_dttm,
    })
    await session.commit()


MODEL_MAP = {
    "orders": WBOrders,
    "commission": WBCommission,
    "ad_config": WBAdConfig,
    "sales_funnel": WBSalesFunnel,
    "stocks_report": WBStocksReport,
    "paid_storage": WBPaidStorage,
    "paid_acceptions": WBPaidAcceptions,
    "suppliers": WBSuppliers,
    "ad_stats": WBAdStats,
    "ad_info": WBAdInfo,
    "clusters": WBClusters,
    "keywords": WBKeywords,
    "sku_api": WBSkuApi,
}
