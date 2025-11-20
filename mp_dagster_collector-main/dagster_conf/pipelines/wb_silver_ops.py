import json
from dagster import op, In, Out, get_dagster_logger
from typing import List, Dict, Any
from itertools import islice
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text, TEXT, func
from datetime import datetime
from uuid import UUID
from src.db.silver.models import (
    SilverAdCampaign1D,
    SilverAdCampaignsProducts1D,
    SilverAdCampaignsAuto1D,
    SilverAdCampaignsUnited1D,
    SilverAdStatsProducts1D,
    SilverAdStatsProductPosition1D,
    SilverProducts,
    SilverWbProducts,
    SilverWbProductSearchTerms,
    SilverAdStatsKeywords1D,
    SilverAdKeywordClusters1D,
    SilverAdKeyword,
    SilverAdKeywordCluster,
)
from src.db.silver.transformers import (
    transform_type6_ad_info,
    transform_type8_ad_info,
    transform_type9_ad_info,
    transform_ad_stats,
    transform_wb_product_search_texts,
    transform_stats_keywords_1d,
    transform_keyword_clusters_1d,
    extract_keywords_dict,
    extract_clusters_dict,
)


@op(
    ins={"batch_id": In(str)},
    out=Out(),
    required_resource_keys={"postgres"},
    description="Читает bronze.wb_ad_info, трансформирует и сохраняет в silver-слой"
)
async def normalize_ad_info_bronze_to_silver(context, batch_id: str):
    logger = get_dagster_logger()
    session_factory = context.resources.postgres

    async with session_factory() as session:  # type: AsyncSession
        result = await session.execute(text("""
            SELECT content, response_uuid, response_dttm
            FROM bronze.wb_ad_info
            WHERE batch_id = :batch_id
        """), {"batch_id": batch_id})

        rows = result.fetchall()
        logger.info(f"[normalize_ad_info] найдено записей: {len(rows)}")

        campaigns: List[Dict[str, Any]] = []
        products_6: List[Dict[str, Any]] = []
        products_8: List[Dict[str, Any]] = []
        products_9: List[Dict[str, Any]] = []

        for row in rows:
            content = row[0]
            response_uuid: UUID = UUID(row[1])
            response_dttm: datetime = row[2]
            try:
                entry = json.loads(content)
                typ = entry.get("type")

                if typ == 6:
                    campaign, prod = transform_type6_ad_info(entry, response_uuid, response_dttm)
                    campaigns.append(campaign)
                    products_6.extend(prod)
                elif typ == 8:
                    campaign, prod = transform_type8_ad_info(entry, response_uuid, response_dttm)
                    campaigns.append(campaign)
                    products_8.extend(prod)
                elif typ == 9:
                    campaign, prod = transform_type9_ad_info(entry, response_uuid, response_dttm)
                    campaigns.append(campaign)
                    products_9.extend(prod)
                else:
                    logger.warning(f"Неизвестный type={typ} в записи: advertId={entry.get('advertId')}")

            except Exception as e:
                logger.error(f"Ошибка при трансформации записи: {e}")
                continue

        logger.info(f"[normalize_ad_info] всего кампаний: {len(campaigns)}")
        logger.info(f"[normalize_ad_info] товаров type6: {len(products_6)}, type8: {len(products_8)}, type9: {len(products_9)}")

        await _bulk_insert(session, SilverAdCampaign1D, campaigns)
        await _bulk_insert(session, SilverAdCampaignsProducts1D, products_6)
        await _bulk_insert(session, SilverAdCampaignsAuto1D, products_8)
        await _bulk_insert(session, SilverAdCampaignsUnited1D, products_9)

        logger.info("[normalize_ad_info] вставка в silver завершена")


async def _bulk_insert(session: AsyncSession, model, data: List[Dict[str, Any]]):
    if not data:
        return
    await session.execute(model.__table__.insert().values(data))
    await session.commit()


@op(
    ins={"batch_id": In(str)},
    out=Out(),
    required_resource_keys={"postgres"},
    description="Читает bronze.wb_ad_stats, трансформирует и сохраняет в silver-слой"
)
async def normalize_ad_stats_bronze_to_silver(context, batch_id: str):
    logger = get_dagster_logger()
    session_factory = context.resources.postgres

    async with session_factory() as session:  # type: AsyncSession
        # 1) забираем все JSON…
        result = await session.execute(
            text("SELECT content FROM bronze.wb_ad_stats WHERE batch_id = :batch_id"),
            {"batch_id": batch_id}
        )
        rows = result.fetchall()

        all_stats = []
        all_positions = []
        all_products = []
        seen_products = set()

        for (content,) in rows:
            entry = json.loads(content)
            stats, pos, prods = transform_ad_stats(entry)
            all_stats.extend(stats)
            all_positions.extend(pos)
            for p in prods:
                pid = p["product_id"]
                if pid not in seen_products:
                    seen_products.add(pid)
                    all_products.append(p)

        logger.info(f"[normalize_ad_stats] stats: {len(all_stats)}, positions: {len(all_positions)}, products: {len(all_products)}")

        # 2) Upsert для основной таблицы
        if all_stats:
            stmt = insert(SilverAdStatsProducts1D.__table__).values(all_stats)
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    "campaign_id", "date", "platform_id", "product_id"
                ],
                set_={
                    "views":    stmt.excluded.views,
                    "clicks":   stmt.excluded.clicks,
                    "ctr":      stmt.excluded.ctr,
                    "cpc":      stmt.excluded.cpc,
                    "cost":     stmt.excluded.cost,
                    "carts":    stmt.excluded.carts,
                    "orders":   stmt.excluded.orders,
                    "cr":       stmt.excluded.cr,
                    "items":    stmt.excluded["items"],
                    "revenue":  stmt.excluded.revenue,
                    # обновляем timestamp
                    "updated_at": func.now(),
                }
            )
            await session.execute(stmt)
            await session.commit()

        # 3) Обычная вставка позиций и продуктов (они не конфликтуют по этому ключу)
        if all_positions:
            await session.execute(
                SilverAdStatsProductPosition1D.__table__.insert().values(all_positions)
            )
            await session.commit()

        if all_products:
            await session.execute(
                insert(SilverProducts.__table__).values(all_products)
                .on_conflict_do_nothing(index_elements=["product_id"])
            )
            await session.commit()

        logger.info("[normalize_ad_stats] вставка в silver завершена")


@op(
    ins={"batch_id": In(str)},
    out=Out(),
    required_resource_keys={"postgres"},
    description="Трансформирует bronze.wb_products_search_texts → silver.wb_products + silver.wb_product_search_terms"
)
async def normalize_wb_product_search_texts_bronze_to_silver(context, batch_id: str):
    logger = get_dagster_logger()
    session_factory = context.resources.postgres

    async with session_factory() as session:
        result = await session.execute(
            text("""
                SELECT
                  content,
                  token_id,
                  response_uuid::text,
                  response_dttm
                FROM bronze.wb_products_search_texts
                WHERE batch_id = :batch_id
            """),
            {"batch_id": batch_id}
        )
        rows = result.fetchall()
        logger.info(f"[normalize_wb_search_texts] rows: {len(rows)}")

        all_products: List[Dict] = []
        all_terms:    List[Dict] = []

        for content, token_id, resp_uuid_str, resp_dttm in rows:
            entry = json.loads(content)
            entry["api_token_id"] = token_id
            resp_uuid = UUID(resp_uuid_str)
            pr, tr = transform_wb_product_search_texts(entry, resp_uuid, resp_dttm)
            all_products.extend(pr)
            all_terms.extend(tr)

        # 1) убираем дубликаты в all_products по (dttm, product_id)
        all_products = list({
                                (r["dttm"], r["product_id"]): r
                                for r in all_products
                            }.values())

        # 2) убираем дубликаты в all_terms по (dttm, product_id, search_term)
        all_terms = list({
                             (r["dttm"], r["product_id"], r["search_term"]): r
                             for r in all_terms
                         }.values())

        if all_products:
            stmt_p = insert(SilverWbProducts.__table__).values(all_products)
            stmt_p = stmt_p.on_conflict_do_update(
                index_elements=["dttm", "product_id"],
                set_={
                    c.name: getattr(stmt_p.excluded, c.name)
                    for c in SilverWbProducts.__table__.columns
                    if c.name not in ("dttm", "product_id")
                }
            )
            await session.execute(stmt_p)
            await session.commit()

        if all_terms:
            stmt_t = insert(SilverWbProductSearchTerms.__table__).values(all_terms)
            stmt_t = stmt_t.on_conflict_do_nothing(
                index_elements=["dttm", "product_id", "search_term"]
            )
            await session.execute(stmt_t)
            await session.commit()

        logger.info("[normalize_wb_search_texts] done")


DICT_CHUNK_SIZE = 10_000


@op(
    ins={"batch_id": In(str)},
    out=Out(str),
    required_resource_keys={"postgres"},
    description="Преобразует bronze.wb_clusters → silver.ad_keyword_clusters (с чанками)"
)
async def normalize_ad_keyword_clusters_dict(
    context, batch_id: str
) -> str:
    logger = get_dagster_logger()
    session_factory = context.resources.postgres

    async with session_factory() as session:  # type: AsyncSession
        # 1) забрать все JSON из бронзы
        result = await session.execute(
            text("SELECT content FROM bronze.wb_clusters WHERE batch_id = :batch_id"),
            {"batch_id": batch_id},
        )
        bronze_rows = [{"content": c} for (c,) in result.fetchall()]

        # 2) вытащить уникальные кластерные имена
        data = extract_clusters_dict(bronze_rows)
        if not data:
            logger.info("[normalize_ad_keyword_clusters_dict] нет кластеров")
            return batch_id

        logger.info(f"[normalize_ad_keyword_clusters_dict] total clusters: {len(data)}")

        # 3) по чанкам upsert-им в silver.ad_keyword_clusters
        it = iter(data)
        chunk_idx = 0
        while True:
            chunk = list(islice(it, DICT_CHUNK_SIZE))
            if not chunk:
                break
            chunk_idx += 1
            logger.info(
                f"[normalize_ad_keyword_clusters_dict] inserting chunk {chunk_idx} ({len(chunk)} rows)"
            )
            stmt = (
                insert(SilverAdKeywordCluster.__table__)
                .values(chunk)
                .on_conflict_do_nothing(index_elements=["keyword_cluster"])
            )
            await session.execute(stmt)
            await session.commit()

        logger.info("[normalize_ad_keyword_clusters_dict] done")
        return batch_id


@op(
    ins={"batch_id": In(str)},
    out=Out(str),
    required_resource_keys={"postgres"},
    description="Читает bronze.wb_clusters → silver.ad_keywords (чанками upsert)"
)
async def normalize_ad_keywords_dict(
    context,
    batch_id: str
) -> str:
    logger = get_dagster_logger()
    session_factory = context.resources.postgres

    # 1) достаем все JSON из bronze.wb_clusters
    async with session_factory() as session:  # type: AsyncSession
        result = await session.execute(
            text("SELECT content FROM bronze.wb_clusters WHERE batch_id = :batch_id"),
            {"batch_id": batch_id},
        )
        bronze_rows = [{"content": row[0]} for row in result.fetchall()]

    # 2) извлекаем пары (keyword, keyword_cluster)
    data: List[Dict[str, str]] = extract_keywords_dict(bronze_rows)
    if not data:
        logger.info(f"[normalize_ad_keywords_dict] нет данных для batch_id={batch_id}")
        return batch_id

    logger.info(f"[normalize_ad_keywords_dict] всего уникальных keyword↔cluster: {len(data)}")

    # 3) чанками upsert’им в silver.ad_keywords
    async with session_factory() as session:
        it = iter(data)
        chunk_idx = 0
        while True:
            chunk = list(islice(it, DICT_CHUNK_SIZE))
            if not chunk:
                break
            chunk_idx += 1
            logger.info(f"[normalize_ad_keywords_dict] вставка чанка {chunk_idx}: {len(chunk)} строк")
            stmt = (
                insert(SilverAdKeyword.__table__)
                .values(chunk)
                .on_conflict_do_nothing(index_elements=["keyword"])
            )
            await session.execute(stmt)
            await session.commit()

    logger.info(f"[normalize_ad_keywords_dict] справочник ad_keywords обновлён, batch_id={batch_id}")
    return batch_id


KEYWORD_CHUNK_SIZE = 3000


@op(
    ins={
        "batch_id": In(str),
        "dict_batch": In(str),
    },
    out=Out(),
    required_resource_keys={"postgres"},
    description="Читает bronze.wb_keywords и сохраняет в silver.wb_ad_stats_keywords_1d (ждёт справочник ad_keywords)"
)
async def normalize_wb_keywords_bronze_to_silver(
    context, batch_id: str, dict_batch: str
):
    logger = get_dagster_logger()
    async with context.resources.postgres() as session:  # type: AsyncSession
        result = await session.execute(
            text("""
                SELECT content, response_uuid::text, response_dttm
                  FROM bronze.wb_keywords
                 WHERE batch_id = :batch_id
            """),
            {"batch_id": batch_id},
        )
        rows = result.fetchall()
        logger.info(f"[normalize_wb_keywords] rows fetched: {len(rows)}")

        bronze = [
            {"content": c, "response_uuid": uuid, "response_dttm": dttm}
            for c, uuid, dttm in rows
        ]
        stats: List[Dict[str, Any]] = transform_stats_keywords_1d(bronze)
        total = len(stats)
        logger.info(f"[normalize_wb_keywords] total records: {total}")

        if total:
            it = iter(stats)
            chunk_idx = 0
            while True:
                chunk = list(islice(it, KEYWORD_CHUNK_SIZE))
                if not chunk:
                    break
                chunk_idx += 1
                logger.info(f"[normalize_wb_keywords] inserting chunk {chunk_idx} ({len(chunk)})")
                stmt = insert(SilverAdStatsKeywords1D.__table__).values(chunk)
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=["campaign_id", "date", "keyword"]
                )
                await session.execute(stmt)
                await session.commit()

        logger.info("[normalize_wb_keywords] done")


CLUSTER_CHUNK_SIZE = 4000


@op(
    ins={
        "batch_id": In(str),
        "dict_batch": In(str),
    },
    out=Out(),
    required_resource_keys={"postgres"},
    description="Читает bronze.wb_clusters и сохраняет в silver.wb_ad_keyword_clusters_1d (ждёт справочник ad_keyword_clusters)"
)
async def normalize_wb_clusters_bronze_to_silver(
    context, batch_id: str, dict_batch: str
):
    logger = get_dagster_logger()
    async with context.resources.postgres() as session:  # type: AsyncSession
        result = await session.execute(
            text("""
                SELECT content, response_uuid::text, response_dttm
                  FROM bronze.wb_clusters
                 WHERE batch_id = :batch_id
            """),
            {"batch_id": batch_id},
        )
        rows = result.fetchall()
        logger.info(f"[normalize_wb_clusters] rows fetched: {len(rows)}")

        bronze = [
            {"content": c, "response_uuid": uuid, "response_dttm": dttm}
            for c, uuid, dttm in rows
        ]
        clusters: List[Dict[str, Any]] = transform_keyword_clusters_1d(bronze)
        total = len(clusters)
        logger.info(f"[normalize_wb_clusters] total records: {total}")

        if total:
            it = iter(clusters)
            chunk_idx = 0
            while True:
                chunk = list(islice(it, CLUSTER_CHUNK_SIZE))
                if not chunk:
                    break
                chunk_idx += 1
                logger.info(f"[normalize_wb_clusters] inserting chunk {chunk_idx} ({len(chunk)})")
                stmt = insert(SilverAdKeywordClusters1D.__table__).values(chunk)
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=[
                        "last_response_uuid", "campaign_id", "date", "keyword_cluster"
                    ]
                )
                await session.execute(stmt)
                await session.commit()

        logger.info("[normalize_wb_clusters] done")
