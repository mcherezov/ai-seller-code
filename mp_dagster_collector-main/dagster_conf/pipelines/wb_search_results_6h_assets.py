import json
import uuid
from datetime import datetime, timedelta, time, timezone as std_timezone
from zoneinfo import ZoneInfo
import requests
from dagster import asset, Failure
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from dagster_conf.resources.pg_resource import postgres_resource
from src.db.silver.models import WbSearchResults6h

MSK = ZoneInfo("Europe/Moscow")


def _parse_iso_any_tz(iso_str: str) -> datetime:
    # на вход приходит тег dagster/scheduled_execution_time
    # с таймзоной. парсим максимально терпимо:
    try:
        return datetime.fromisoformat(iso_str)
    except Exception:
        # если что-то совсем необычное — считаем это UTC
        return datetime.now(std_timezone.utc)


@asset(
    key_prefix=["bronze"],
    name="wb_search_results_6h",
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    description="Собирает сырые ответы поисковой выдачи WB (3 страницы) в bronze.wb_search_results_6h "
                "с заполнением run_schedule_dttm/receive_dttm/business_dttm.",
)
async def bronze_wb_search_results_6h(context):
    # 0) Мета запуска (как в Abstract миксине)
    run_uuid = context.run_id

    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    if not scheduled_iso:
        raise Failure("Missing tag 'dagster/scheduled_execution_time'")

    run_schedule_dttm = _parse_iso_any_tz(scheduled_iso).astimezone(MSK)  # плановое (MSK)
    run_dttm = datetime.now(MSK)                                           # фактический старт (MSK)
    # бизнес-период — час MSK, когда был сделан снэпшот
    business_dttm = run_dttm.replace(minute=0, second=0, microsecond=0)

    # 1) Получаем список поисковых фраз, по которым будем собирать данные
    async with context.resources.postgres() as session:

        # @TODO: убрать хардкод company_id 
        # (для этого придется сделать передачу api_token_id ниже необязательной)
        rows = await session.execute(text("""
            SELECT DISTINCT search_term, 1 as company_id
            FROM core.search_results_tracking
        """))

        pairs = rows.fetchall()

    base_url = "https://search.wb.ru/exactmatch/ru/common/v14/search"
    common_params = {
        "ab_testing": "false",
        "appType":    32,
        "curr":       "rub",
        "dest":       -1257786,
        "lang":       "ru",
        "resultset":  "catalog",
        "sort":       "popular",
        "uclusters":  0,
        "uiv":        0,
    }
    pages = [1, 2, 3]

    context.log.info(f"[bronze_wb_search_results_6h] start run_uuid={run_uuid} "
                     f"run_sched={run_schedule_dttm.isoformat()} business={business_dttm.isoformat()} "
                     f"keywords={len(pairs)}")

    async with context.resources.postgres() as session:
        for keyword, company_id in pairs:
            for page in pages:
                params = {**common_params, "query": keyword, "page": page}
                context.log.debug(f"[bronze] company_id={company_id} keyword={keyword!r} page={page} → HTTP GET")

                # HTTP-запрос
                try:
                    resp = requests.get(base_url, params=params, timeout=15)
                    code = resp.status_code
                    body = resp.text
                except requests.RequestException as e:
                    context.log.warning(f"[bronze] HTTP error for keyword={keyword!r} page={page}: {e}")
                    code = 0
                    body = ""

                # таймстемпы (MSK)
                request_dttm = datetime.now(MSK)
                receive_dttm = request_dttm
                response_dttm = request_dttm  # у этого API нет серверного ts — берём локальный
                request_uuid = uuid.uuid4()

                await session.execute(
                    text("""
                    INSERT INTO bronze.wb_search_results_6h (
                      api_token_id, run_uuid, run_dttm, run_schedule_dttm, business_dttm,
                      request_uuid, request_dttm,
                      request_parameters, request_body,
                      response_dttm, receive_dttm, response_code, response_body
                    )
                    VALUES (
                      :api_token_id, :run_uuid, :run_dttm, :run_schedule_dttm, :business_dttm,
                      :request_uuid, :request_dttm,
                      CAST(:request_parameters AS jsonb), CAST(:request_body AS jsonb),
                      :response_dttm, :receive_dttm, :response_code, :response_body
                    )
                    """),
                    {
                        "api_token_id": company_id,  # ← потом уйдёт в silver.company_id
                        "run_uuid": run_uuid,
                        "run_dttm": run_dttm,
                        "run_schedule_dttm": run_schedule_dttm,
                        "business_dttm": business_dttm,
                        "request_uuid": str(request_uuid),
                        "request_dttm": request_dttm,
                        "request_parameters": json.dumps(params, ensure_ascii=False),
                        "request_body": json.dumps(None),
                        "response_dttm": response_dttm,
                        "receive_dttm": receive_dttm,
                        "response_code": code,
                        "response_body": body,
                    },
                )
        await session.commit()

    context.log.info("[bronze_wb_search_results_6h] done")


def _chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def _s(val, default=""):
    return default if val is None else val


def _i(val, default=0):
    try:
        return int(val)
    except Exception:
        return default


def _f(val, default=0.0):
    try:
        return float(val)
    except Exception:
        return default


@asset(
    deps=[bronze_wb_search_results_6h],
    key_prefix=["silver"],
    name="wb_search_results_6h",
    resource_defs={"postgres": postgres_resource},
    required_resource_keys={"postgres"},
    description="Преобразует bronze.wb_search_results_6h → silver.wb_search_results_6h "
                "(PK: business_dttm, keyword, nm_id).",
)
async def silver_wb_search_results_6h(context):
    # Можем запустить материализацию конкретного нужного run_uuid из бронзы.
    # Иначе возьмем тот, который шёл в одном запуске с бронзой.
    run_uuid = context.dagster_run.tags.get("run_uuid") or context.run_id

    # берём ТОЛЬКО текущий прогон
    async with context.resources.postgres() as session:
        rows = await session.execute(text("""
            SELECT
                b.api_token_id     AS company_id,
                b.request_uuid     AS request_uuid,
                b.response_dttm    AS response_dttm,
                b.business_dttm    AS business_dttm,
                b.request_parameters AS params,
                b.response_body     AS raw_json
            FROM bronze.wb_search_results_6h b
            WHERE b.run_uuid = :run_uuid
        """), {"run_uuid": run_uuid})
        records = rows.fetchall()

    to_insert: list[dict] = []
    for company_id, request_uuid, response_dttm, business_dttm, params, raw in records:
        if isinstance(params, str):
            try:
                params = json.loads(params)
            except Exception:
                params = {}
        elif params is None:
            params = {}

        data = json.loads(raw)
        md = data.get("metadata", {}) or {}
        sr = md.get("search_result", {}) or {}

        keyword = params.get("query")
        app_type = _i(params.get("appType"))
        lang = _s(params.get("lang"))
        cur = _s(params.get("curr"))
        resultset = _s(params.get("resultset"))
        page = _i(params.get("page"), 1)

        catalog_type = _s(md.get("catalog_type"))
        catalog_value = _s(md.get("catalog_value"))
        normquery = md.get("normquery")

        search_result_name = _s(sr.get("name") or md.get("name"))
        search_result_rmi = _s(sr.get("rmi") or md.get("rmi"))
        search_result_title = _s(sr.get("title") or md.get("title"))
        search_result_rs = _i(sr.get("rs") or md.get("rs"))
        search_result_qv = _s(sr.get("qv") or md.get("qv"))

        products = data.get("products") or []
        for idx, prod in enumerate(products, start=1):
            nm_id_val = prod.get("id")
            if nm_id_val in (None, "", 0):  # ➜ скипаем пустой ID
                context.log.debug(
                    f"skip product without id; request_uuid={request_uuid} keyword={params.get('query')!r}")
                continue
            colors = prod.get("colors") or []
            color = colors[0] if colors else {}
            sizes = prod.get("sizes") or []
            size0 = sizes[0] if sizes else {}
            log = prod.get("log") or {}
            meta = prod.get("meta") or {}

            to_insert.append({
                # стандартные поля silver
                "company_id":     _i(company_id),
                "request_uuid":   request_uuid,
                "response_dttm":  response_dttm,
                "business_dttm":  business_dttm,

                # ключ и дата
                "keyword":        keyword,
                "date":           response_dttm,  # модель — timestamptz; оставляем как в бронзе
                "nm_position":    idx,

                # параметры запроса
                "app_type":       app_type,
                "lang":           lang,
                "cur":            cur,
                "resultset":      resultset,
                "page":           page,

                # metadata.search_result
                "catalog_type":        catalog_type,
                "catalog_value":       catalog_value,
                "normquery":           normquery,
                "search_result_name":  search_result_name,
                "search_result_rmi":   search_result_rmi,
                "search_result_title": search_result_title,
                "search_result_rs":    search_result_rs,
                "search_result_qv":    search_result_qv,

                # товар (nm_id вместо product_id)
                "nm_id":                 _i(prod.get("id")),
                "product_time_1":        _i(prod.get("time1")),
                "product_time_2":        _i(prod.get("time2")),
                "product_wh":            _i(prod.get("wh")),
                "product_d_type":        _i(prod.get("dtype")),
                "product_dist":          _i(prod.get("dist")),
                "product_root":          _i(prod.get("root")),
                "product_kind_id":       _i(prod.get("kindId")),

                "product_brand":         _s(prod.get("brand")),
                "product_brand_id":      _i(prod.get("brandId")),
                "product_site_brand_id": _i(prod.get("siteBrandId")),

                "product_colors_id":     _i(color.get("id")) if color else None,
                "product_colors_name":   color.get("name") if color else None,

                "product_subject_id":        _i(prod.get("subjectId")),
                "product_subject_parent_id": _i(prod.get("subjectParentId")),

                "product_name":         _s(prod.get("name")),
                "product_entity":       _s(prod.get("entity")),
                "product_match_id":     _i(prod.get("matchId")),

                "product_supplier":        _s(prod.get("supplier")),
                "product_supplier_id":     _i(prod.get("supplierId")),
                "product_supplier_rating": _f(prod.get("supplierRating")),
                "product_supplier_flags":  _i(prod.get("supplierFlags")),
                "product_pics":            _i(prod.get("pics")),

                "product_rating":          _i(prod.get("rating")),
                "product_review_rating":   _f(prod.get("reviewRating")),
                "product_nm_review_rating": _f(prod.get("nmReviewRating")),
                "product_feedbacks":       _i(prod.get("feedbacks")),
                "product_nm_feedbacks":    _i(prod.get("nmFeedbacks")),
                "product_panel_promo_id":  _i(prod.get("panelPromoId")) if prod.get("panelPromoId") is not None else None,
                "product_volume":          _i(prod.get("volume")),
                "product_view_flags":      _i(prod.get("viewFlags")),

                # размеры
                "product_sizes_name":            _s(size0.get("name")),
                "product_sizes_orig_name":       _s(size0.get("origName")),
                "product_sizes_rank":            _i(size0.get("rank")),
                "product_sizes_option_id":       _i(size0.get("optionId")),
                "product_sizes_wh":              _i(size0.get("wh")),
                "product_sizes_time_1":          _i(size0.get("time1")),
                "product_sizes_time_2":          _i(size0.get("time2")),
                "product_sizes_d_type":          _i(size0.get("dtype")),
                "product_sizes_price_basic":     _i((size0.get("price") or {}).get("basic")),
                "product_sizes_price_product":   _i((size0.get("price") or {}).get("product")),
                "product_sizes_price_logistics": _i((size0.get("price") or {}).get("logistics")),
                "product_sizes_price_return":    _i((size0.get("price") or {}).get("return")),
                "product_sizes_sale_conditions": _i(size0.get("saleConditions")),
                "product_sizes_payload":         _s(size0.get("payload")),

                "product_total_quantity": _i(prod.get("totalQuantity")) if prod.get("totalQuantity") is not None else None,

                # лог
                "product_log_cpm":            _i((log or {}).get("cpm")) if log else None,
                "product_log_promotion":      _i(log.get("promotion")) if log else None,
                "product_log_promo_position": _i(log.get("promoPosition")) if log else None,
                "product_log_position":       _i(log.get("position")) if log else None,
                "product_log_advert_id":      _i(log.get("advertId")) if log else None,
                "product_log_tp":             _s(log.get("tp")) if log else None,
                "product_logs":               prod.get("logs"),

                # meta
                "product_meta_tokens":   _s(json.dumps(meta.get("tokens", []), ensure_ascii=False)),
                "product_meta_preset_id": _i(meta.get("presetId")),
            })

    if not to_insert:
        context.log.info("Silver wb_search_results_6h: данных нет, пропускаем.")
        return

    num_columns = len(to_insert[0])
    MAX_PARAMS = 32767
    chunk_size = max(1, MAX_PARAMS // num_columns)

    async with context.resources.postgres() as session:
        for i in range(0, len(to_insert), chunk_size):
            chunk = to_insert[i: i + chunk_size]
            index_elements=["business_dttm", "keyword", "page", "nm_position"]
            upsert = pg_insert(WbSearchResults6h).values(chunk)
            # новый PK
            upsert = upsert.on_conflict_do_update(
                index_elements=index_elements,
                set_={
                    c.name: getattr(upsert.excluded, c.name)
                    for c in WbSearchResults6h.__table__.columns
                    if c.name not in index_elements
                }
            )
            await session.execute(upsert)
            await session.commit()
            context.log.info(f"[silver_wb_search_results_6h] inserted chunk #{i // chunk_size + 1} rows={len(chunk)}")

    context.log.info(f"Silver wb_search_results_6h: всего загружено {len(to_insert)} строк")
