# sync_api_assets_factory.py
from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from dagster import (
    asset,
    AssetsDefinition,
    Output,
    Failure,
    RetryPolicy,
    MultiPartitionsDefinition,
    MetadataValue,
)

# Партиции
from dagster_conf.lib.partitions import build_multipartitions

# Общие утилиты фабрик/бронзы
from dagster_conf.lib.asset_factories.factory_utils import (
    MSK,
    extract_partition,          # строгий разбор (business_dttm, company_id)
    merge_tags,
    bronze_output_metadata,
    default_resolve_auth,       # (token_id, token) по company_id
    default_persist_bronze,     # запись аудита/сырья в бронзу
    default_select_best_bronze, # выбор «лучшая» успешная бронза
    make_default_persist_silver,# upsert-функция для Silver
    normalize_wrapper,          # обёртка нормализатора (разные сигнатуры)
    coerce_row_types,           # приведение типов под SQLA-модель
    resolve_build_params,       # dotted-path → callable
    safe_json_loads,
)

# Единый адаптер вызова WB-клиента → bronze-dict (с логированием/протоколом)
from dagster_conf.lib.asset_factories.factory_utils import call_wb_method_and_wrap


# ---------------- Вспомогательное ----------------

def asyncify(fn: Callable):
    """Обернёт sync-функцию в async через threadpool (единоразово при сборке спека)."""
    if inspect.iscoroutinefunction(fn):
        return fn

    async def _wrapped(*args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    return _wrapped


def _iso(biz: datetime) -> str:
    return biz.date().isoformat()


def _iso_msk(biz: datetime) -> str:
    # 'YYYY-MM-DDTHH:MM:SS' в МСК без tz
    dt = (biz.astimezone(MSK) if biz.tzinfo else biz.replace(tzinfo=MSK)).astimezone(MSK)
    return dt.replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")


def _iso_plus(biz: datetime, delta: str) -> str:
    """
    Поддержка шаблона {{ business_dttm|iso_plus(1d) }} / (7d)/(1w)
    Возвращает YYYY-MM-DD.
    """
    s = delta.strip().lower().strip("()")
    if s.endswith("d"):
        n = int(s[:-1] or "0")
        return (biz.date() + timedelta(days=n)).isoformat()
    if s.endswith("w"):
        n = int(s[:-1] or "0")
        return (biz.date() + timedelta(days=7 * n)).isoformat()
    # фолбэк
    return (biz.date()).isoformat()


def _render_scalar(value: Any, *, biz: datetime, cid: int) -> Any:
    """
    Простейший рендерер выражений в стиле {{ business_dttm|... }} для pipe_cfg['api']['request'].
    Поддерживает:
      - {{ business_dttm|iso }}            -> 'YYYY-MM-DD'
      - {{ business_dttm|iso_msk }}        -> 'YYYY-MM-DDTHH:MM:SS'
      - {{ business_dttm|iso_plus(1d) }}   -> 'YYYY-MM-DD'
      - {{ company_id }}                   -> '123'
    Остальные типы возвращаем как есть.
    """
    if not isinstance(value, str):
        return value

    s = value
    if "{{" not in s:
        return s

    def _replace_token(token: str) -> str:
        token = token.strip()  # "business_dttm|iso_msk" / "company_id"
        if token.startswith("business_dttm"):
            if "|" in token:
                _, flt = token.split("|", 1)
                flt = flt.strip()
                if flt.startswith("iso_plus"):
                    return _iso_plus(biz, flt[len("iso_plus"):])
                if flt == "iso_msk":
                    return _iso_msk(biz)
                if flt == "iso":
                    return _iso(biz)
            return _iso(biz)
        if token == "company_id":
            return str(cid)
        return ""

    out = s
    # простой, но надёжный рендер без jinja
    while "{{" in out and "}}" in out:
        start = out.index("{{")
        end = out.index("}}", start + 2)
        expr = out[start + 2:end]
        repl = _replace_token(expr)
        out = out[:start] + repl + out[end + 2:]
    return out


def _render_request_block(req_cfg: Dict[str, Any] | None, *, biz: datetime, cid: int) -> Dict[str, Any]:
    """
    Превращает pipe_cfg['api']['request'] в dict параметров:
      {
        "query": {...},   # → именованные параметры метода (GET)
        "body":  {...},   # → payload/json_body (POST/PUT)
        "headers": {...}
      }
    С ГЛУБОКИМ рендерингом шаблонов {{ ... }}.
    """
    if not req_cfg:
        return {}

    def _render_any(x: Any) -> Any:
        if isinstance(x, dict):
            return {k: _render_any(v) for k, v in x.items()}
        if isinstance(x, list):
            return [_render_any(v) for v in x]
        return _render_scalar(x, biz=biz, cid=cid)

    out: Dict[str, Any] = {}
    for key in ("query", "body", "headers"):
        block = req_cfg.get(key)
        if isinstance(block, dict):
            out[key] = _render_any(block)
    return out


# =========================
#   Спецификации (dataclass)
# =========================

@dataclass
class BronzeApiContext:
    """Контекст вызова API для Bronze."""
    business_dttm: datetime
    company_id: int
    token_id: Optional[int]
    token: Optional[str]
    run_uuid: str


@dataclass
class BronzeSpec:
    """Спецификация Bronze-ассета."""
    name: str
    client_resource_key: str
    partitions_def: MultiPartitionsDefinition

    # async-колбэки
    resolve_auth: Callable[[Any, int], Awaitable[Tuple[Optional[int], Optional[str]]]]
    call_api: Callable[[Any, str, str, Callable, BronzeApiContext], Awaitable[Dict[str, Any]]]
    persist_bronze: Callable[[Any, str, BronzeApiContext, Dict[str, Any]], Awaitable[str]]

    # параметры вызова
    fetch_method: str
    build_request_params: Callable[[datetime, int], Dict[str, Any]]
    bronze_table: str

    # метаданные/ресурсы
    required_resource_keys: Sequence[str] = ("postgres",)
    op_retry_policy: RetryPolicy = field(default_factory=lambda: RetryPolicy(max_retries=3, delay=60))
    op_tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class SilverSpec:
    """Спецификация одного Silver-ассета (ветка от Bronze)."""
    name: str
    partitions_def: MultiPartitionsDefinition
    select_best_bronze: Callable[[Any, str, datetime, int], Awaitable[Optional[Dict[str, Any]]]]
    normalize: Callable[[Any, Dict[str, Any], BronzeApiContext], Awaitable[Iterable[Dict[str, Any]]]]
    persist_silver: Callable[[Any, Iterable[Dict[str, Any]]], Awaitable[int]]
    silver_model: Any
    pk_cols: Tuple[str, ...]
    op_tags: Dict[str, str] = field(default_factory=dict)
    required_resource_keys: Sequence[str] = ("postgres",)


@dataclass
class SyncApiPipelineSpec:
    """Полная спецификация пайплайна: один Bronze + N Silver."""
    pipe_name: str
    client_resource_key: str
    bronze: BronzeSpec
    silvers: List[SilverSpec]
    op_tags: Dict[str, str] = field(default_factory=dict)


# =========================
#   Внутренняя фабрика ассетов
# =========================

SENSITIVE_KEYS = {"token", "token_override", "authorization", "Authorization"}

def _scrub_params(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _scrub_params(v) for k, v in obj.items() if k not in SENSITIVE_KEYS}
    if isinstance(obj, list):
        return [_scrub_params(v) for v in obj]
    return obj


def make_sync_api_assets(spec: SyncApiPipelineSpec) -> List[AssetsDefinition]:
    """Создаёт 1 Bronze и N Silver ассетов. Порядок: [bronze, *silvers]."""
    base_tags = {"factory": "sync_api", "pipe": spec.pipe_name}

    # ---------------- Bronze ----------------
    bronze_tags = merge_tags(base_tags, merge_tags(spec.op_tags, spec.bronze.op_tags))

    @asset(
        name=f"bronze__{spec.bronze.name}",
        key_prefix=["bronze"],
        partitions_def=spec.bronze.partitions_def,
        required_resource_keys=set(spec.bronze.required_resource_keys) | {spec.client_resource_key},
        retry_policy=spec.bronze.op_retry_policy,
        op_tags=bronze_tags,
        description=(
            "Вызов WB API → запись аудита и сырых данных в Bronze. "
            "При статусе не 2xx: аудит пишется, затем ассет падает Failure (включаются ретраи Dagster)."
        ),
    )
    async def bronze_asset(context) -> Output[None]:
        business_dttm, company_id = extract_partition(context)
        run_dttm = datetime.now(MSK)
        # Аутентификация
        token_id, token = await spec.bronze.resolve_auth(context, company_id)
        api_ctx = BronzeApiContext(
            business_dttm=business_dttm, company_id=company_id,
            token_id=token_id, token=token, run_uuid=context.run_id,
        )

        if not token:
            md = bronze_output_metadata(api_ctx, request_uuid="", status=401, response_dttm=datetime.now(MSK))
            context.log.warning(
                f"[bronze__{spec.bronze.name}] no token for company_id={company_id}; writing audit with 401"
            )
            yield Output(value=None, metadata=md)
            raise Failure(f"No WB token for company_id={company_id}")

        # Предпросмотр параметров (диагностика)
        try:
            fn = spec.bronze.build_request_params
            sig = inspect.signature(fn)
            needs_ctx = len(sig.parameters) >= 3
            if inspect.iscoroutinefunction(fn):
                context.log.debug(f"[make_sync_api_assets][bronze_asset] build_request_params async: {fn}")
                req_params_raw = await (fn(context, business_dttm, company_id) if needs_ctx
                                        else fn(business_dttm, company_id)) or {}
            else:
                context.log.debug(f"[make_sync_api_assets][bronze_asset] build_request_params sync: {fn}")
                req_params_raw = (fn(context, business_dttm, company_id) if needs_ctx
                                  else fn(business_dttm, company_id)) or {}
        except Exception as e:
            context.log.warning(f"[make_sync_api_assets][bronze_asset] build_request_params error: {e!r}")
            req_params_raw = {}

        context.log.debug("[make_sync_api_assets][bronze_asset] req_params_raw=%s", req_params_raw)
        
        req_params_sanitized = _scrub_params(req_params_raw)
        body_preview = (req_params_raw.get("payload")
                        or req_params_raw.get("json_body")
                        or (req_params_raw.get("body") if isinstance(req_params_raw.get("body"), dict) else None))
        context.log.debug("[make_sync_api_assets][bronze_asset] body_preview=%s", body_preview)

        context.log.info(
            "[make_sync_api_assets][bronze_asset] %s.%s cid=%s biz=%s token=%s",
            spec.client_resource_key, spec.bronze.fetch_method, company_id,
            business_dttm.date().isoformat(),
            "YES" if token else "NO",
        )
        context.log.debug(
            "[make_sync_api_assets][bronze_asset] keys=%s; payload_present=%s; sample=%s",
            list(req_params_raw.keys()),
            ("payload" in req_params_raw) or ("json_body" in req_params_raw) or ("body" in req_params_raw),
            (str(body_preview)[:300] if body_preview is not None else "{}"),
        )

        # Вызов API и запись в бронзу
        try:
            req_started_at = datetime.now(MSK)
            resp = await spec.bronze.call_api(
                context,
                spec.client_resource_key,
                spec.bronze.fetch_method,
                spec.bronze.build_request_params,
                api_ctx,
            )
        except Exception as e:
            # Логируем детально до ретрая
            context.log.error(
                "[wb_call] ERROR during %s.%s: %r; req_keys=%s",
                spec.client_resource_key, spec.bronze.fetch_method, e, list(req_params_raw.keys()),
            )
            raise

        status = int(resp.get("status", 0))
        response_dttm = resp.get("response_dttm") or datetime.now(MSK)

        # Гарантируем корректные request_parameters/request_body из аудита транспорта
        # 1) Пробуем взять «как ушло» из WBResponse.request (audit уровня клиента)
        _audit = resp.get("request") or {}
        _query_from_audit = _audit.get("query") if isinstance(_audit, dict) else None
        _body_from_audit  = _audit.get("json_body") if isinstance(_audit, dict) else None

        # 2) Собираем финальные значения с умными fallback-ами
        final_request_parameters = (
            _query_from_audit
            or resp.get("request_parameters")
            or req_params_sanitized
            or {}
        )
        final_request_body = (
            _body_from_audit
            or resp.get("request_body")
        )

        # 3) Санитизация (уберём токены/секреты на всякий случай)
        final_request_parameters = _scrub_params(final_request_parameters)
        if final_request_body is not None:
            final_request_body = _scrub_params(final_request_body)

        # 4) Кладём в структуру, которую пойдёт в persist_bronze
        resp_to_persist = dict(resp)
        resp_to_persist["run_dttm"] = run_dttm
        resp_to_persist["request_parameters"] = final_request_parameters
        if final_request_body is not None:
            resp_to_persist["request_body"] = final_request_body
        else:
            resp_to_persist.pop("request_body", None)

        resp_to_persist["request_dttm"] = (req_started_at or
                resp.get("request_dttm") or resp.get("requested_at")
        )
        resp_to_persist["receive_dttm"] = datetime.now(MSK)

        context.log.debug(
            "[bronze persist] request_parameters=%s; request_body=%s; request_dttm=%s; receive_dttm=%s",
            str(final_request_parameters)[:1000],
            ("<present>" if final_request_body is not None else "<absent>"),
            resp_to_persist["request_dttm"],
            resp_to_persist["receive_dttm"],
        )

        request_uuid = await spec.bronze.persist_bronze(
            context, spec.bronze.bronze_table, api_ctx, resp_to_persist
        )
        md = bronze_output_metadata(api_ctx, request_uuid, status, response_dttm)

        if status < 200 or status >= 300:
            context.log.warning(
                f"[bronze__{spec.bronze.name}] non-2xx={status}; audit stored; raising Failure for retry"
            )
            yield Output(value=None, metadata=md)
            raise Failure(
                description=(
                    f"bronze__{spec.bronze.name}: статус ответа != 2xx (response_code={status}). "
                    f"Аудит записан; инициируем повтор согласно RetryPolicy."
                )
            )

        context.log.info(
            f"[bronze__{spec.bronze.name}] OK company_id={company_id} biz={business_dttm.date()} status={status}"
        )
        yield Output(value=None, metadata=md)

    assets: List[AssetsDefinition] = [bronze_asset]

    # ---------------- Silver fan-out ----------------
    for s in spec.silvers:
        silver_tags = merge_tags(base_tags, merge_tags(spec.op_tags, s.op_tags))

        def _make_silver_asset(s: SilverSpec, silver_tags: Dict[str, str]):
            @asset(
                name=f"silver__{s.name}",
                key_prefix=["silver"],
                partitions_def=s.partitions_def,
                required_resource_keys=set(s.required_resource_keys),
                # Оркестрационная зависимость от бронзы, без передачи значения
                deps=[bronze_asset.key],
                op_tags=silver_tags,
                description=(
                    "Берёт «лучшую» успешную запись из Bronze для партиции, нормализует и делает upsert в Silver. "
                    "Если успешной бронзы нет — пропускает без ошибки."
                ),
            )
            async def silver_asset(context) -> Optional[int]:
                business_dttm, company_id = extract_partition(context)

                # Для унификации контекста: резолвим токен (нормализатору обычно не нужен)
                token_id, token = await spec.bronze.resolve_auth(context, company_id)
                api_ctx = BronzeApiContext(
                    business_dttm=business_dttm,
                    company_id=company_id,
                    token_id=token_id,
                    token=token,
                    run_uuid=context.run_id,
                )

                best = await s.select_best_bronze(context, spec.bronze.bronze_table, business_dttm, company_id)
                if not best:
                    context.log.warning(
                        f"[silver__{s.name}] ещё нет успешной бронзы для company_id={company_id} "
                        f"biz={business_dttm.date()} — пропуск."
                    )
                    return None

                # нормализация → типизация → upsert
                try:
                    rows_iter = await s.normalize(context, best, api_ctx)
                except Exception as e:
                    context.log.error(f"[silver__{s.name}] normalize error: {e!r}")
                    raise

                prepared: List[Dict[str, Any]] = []
                for r in rows_iter or []:
                    if not r:
                        continue
                    base = {
                        "run_uuid": api_ctx.run_uuid,
                        "request_uuid": best.get("request_uuid"),
                        "company_id": company_id,
                        "receive_dttm": best.get("receive_dttm") or business_dttm,
                        "business_dttm": business_dttm,
                    }
                    merged = {**base, **dict(r)}
                    prepared.append(
                        coerce_row_types(
                            s.silver_model,
                            {k: v for k, v in merged.items() if k != "inserted_at"}
                        )
                    )

                # ── Диагностика нормализации и схемы ──
                model_cols = {c.name for c in getattr(s.silver_model, "__table__").columns}
                pk_cols    = set(s.pk_cols or ())
                norm_count = len(prepared)
                first_keys = sorted(list(prepared[0].keys())) if norm_count else []
                all_keys   = sorted({k for row in prepared for k in row.keys()})
                missing_pk_in_first = sorted([
                    c for c in pk_cols
                    if norm_count and (prepared[0].get(c) in (None, "")) and prepared[0].get(c) != 0
                ])
                unknown_cols   = sorted([c for c in all_keys if c not in model_cols])
                missing_in_model = sorted([c for c in pk_cols if c not in model_cols])  # должно быть пусто
                null_pk_stats = {
                    c: sum(1 for row in prepared if (row.get(c) in (None, "")) and row.get(c) != 0)
                    for c in pk_cols
                } if norm_count else {}

                context.log.info(
                    f"[silver__{s.name}] normalized={norm_count} "
                    f"first_keys={first_keys} "
                    f"pk={sorted(list(pk_cols))} "
                    f"missing_pk_in_first={missing_pk_in_first} "
                    f"unknown_cols={unknown_cols} "
                    f"missing_in_model={missing_in_model} "
                    f"null_pk_stats={null_pk_stats}"
                )

                if not prepared:
                    context.log.warning(f"[silver__{s.name}] пустой результат нормализации — пропуск.")
                    return 0

                try:
                    written = int(await s.persist_silver(context, prepared) or 0)
                except Exception as e:
                    context.log.error(f"[silver__{s.name}] persist error: {e!r}")
                    raise

                context.add_output_metadata({
                    "rows": MetadataValue.int(written),
                    "business_dttm": MetadataValue.text(business_dttm.isoformat()),
                    "company_id": MetadataValue.text(str(company_id)),
                    "request_uuid": MetadataValue.text(str(best.get("request_uuid", ""))),
                    "receive_dttm": MetadataValue.text(
                        (best.get("receive_dttm") or business_dttm).isoformat()
                    ),
                })
                return written

            return silver_asset

        assets.append(_make_silver_asset(s, silver_tags))


    return assets


# =========================
#   Публичный адаптер под билдер
# =========================

def build_assets(
    pipe_name: str,
    pipe_cfg: Dict[str, Any],
    model_bundle: Dict[str, Any],
    resources: Dict[str, Any],   # ресурсы подключаются Dagster'ом по именам
) -> List[AssetsDefinition]:
    """
    Собирает BronzeSpec + список SilverSpec из `model_bundle`/`pipe_cfg` и возвращает ассеты.
    Ничего кроме ассетов (jobs/schedules/sensors) не создаёт.

    Ожидаемые ключи:
    - pipe_cfg:
        * partitions_def: MultiPartitionsDefinition (если нет — построим из pipe_cfg)
        * client_resource_key: str = "wildberries_client"
        * api.method: str — имя метода WB-клиента
        * (опц.) build_request_params: callable|str               # если не задано — берём api.request
        * (опц.) api.request: { query/body/headers } с шаблонами  # {{ business_dttm|... }}, {{ company_id }}
        * (опц.) op_tags / bronze_op_tags / silver_op_tags: dict
    - model_bundle:
        * bronze_model: SQLAlchemy-модель
        * silver_models: dict[str, Model] — {silver_name -> модель}
        * silver_pk: dict[str, tuple[str, ...]]
        * normalizers: dict[str, Normalizer]
        * (опц.) resolve_auth / select_best_bronze / persist_bronze / persist_silver
    """
    # --- партиционирование ---
    partitions_def: Optional[MultiPartitionsDefinition] = pipe_cfg.get("partitions_def")
    if partitions_def is None:
        partitions_def = build_multipartitions(pipe_cfg)  # дефолт company_id из defaults/встроенного SQL

    # унифицируем дефолт клиента с task_poll_download
    client_resource_key = pipe_cfg.get("client_resource_key", "wildberries_client")

    # --- параметры вызова API ---
    api_cfg = pipe_cfg.get("api") or {}
    fetch_method = api_cfg.get("method") or pipe_cfg.get("fetch_method")
    if not fetch_method:
        raise ValueError("Не указан api.method (или pipe_cfg.fetch_method)")

    # resolve build_request_params:
    # 1) приоритет — явная функция из pipe_cfg/build_request_params
    # 2) fallback — dotted-path строка
    # 3) fallback — собрать из api.request (query/body/headers) с рендерингом шаблонов
    brp = pipe_cfg.get("build_request_params")
    if isinstance(brp, str):
        build_params_fn = resolve_build_params(brp)
    elif callable(brp):
        build_params_fn = brp
    else:
        req_block = api_cfg.get("request") or {}

        def build_params_fn(biz: datetime, cid: int) -> Dict[str, Any]:
            rendered = _render_request_block(req_block, biz=biz, cid=cid)
            # Соберём итоговые kwargs для метода клиента
            out: Dict[str, Any] = {}
            q = rendered.get("query")
            if isinstance(q, dict):
                out.update(q)
            body = rendered.get("body")
            if body is not None:
                out["payload"] = body
                out["json_body"] = body
            # headers (если нужны)
            h = rendered.get("headers")
            if isinstance(h, dict):
                out["headers"] = h
            return out

    # --- модели и колбэки ---
    bronze_model = model_bundle.get("bronze_model")
    silver_models: Dict[str, Any] = model_bundle.get("silver_models", {}) or {}
    silver_pk_map: Dict[str, Tuple[str, ...]] = model_bundle.get("silver_pk", {}) or {}
    normalizers: Dict[str, Any] = model_bundle.get("normalizers", {}) or {}

    if not bronze_model:
        raise ValueError("model_bundle.bronze_model обязателен")
    if not silver_models:
        raise ValueError("model_bundle.silver_models пуст — нужен хотя бы один Silver")

    # Полное имя бронзовой таблицы с учётом схемы
    tbl = bronze_model.__table__
    bronze_table = f"{tbl.schema}.{tbl.name}" if getattr(tbl, "schema", None) else tbl.name

    # Приводим колбэки к async один раз
    resolve_auth_cb = asyncify(model_bundle.get("resolve_auth") or default_resolve_auth)

    # поддержка форс-токена из конфига (если нужно)
    auth_cfg = (pipe_cfg.get("auth") or {})
    if auth_cfg.get("token_id"):
        _forced_token_id = int(auth_cfg["token_id"])
        async def _resolve_auth_override(context, company_id: int):
            return await default_resolve_auth(context, company_id, token_id_override=_forced_token_id)
        resolve_auth_cb = _resolve_auth_override

    select_best_bronze_cb = asyncify(model_bundle.get("select_best_bronze") or default_select_best_bronze)
    persist_bronze_cb = asyncify(model_bundle.get("persist_bronze") or default_persist_bronze)
    persist_silver_map: Dict[str, Callable] = model_bundle.get("persist_silver", {}) or {}

    # --- BronzeSpec ---
    bronze_spec = BronzeSpec(
        name=pipe_name,
        client_resource_key=client_resource_key,
        partitions_def=partitions_def,
        resolve_auth=resolve_auth_cb,
        call_api=asyncify(call_wb_method_and_wrap),
        persist_bronze=persist_bronze_cb,
        fetch_method=str(fetch_method),
        build_request_params=build_params_fn,
        bronze_table=bronze_table,
        required_resource_keys=("postgres", client_resource_key),
        op_tags=pipe_cfg.get("bronze_op_tags", {}) or {},
    )

    # --- SilverSpec list (fan-out) ---
    silvers: List[SilverSpec] = []
    silver_tables_cfg: Dict[str, Any] = (
        pipe_cfg.get("silver", {}).get("targets", {}) or pipe_cfg.get("silver_tables", {}) or {}
    )

    for silver_key, silver_model in silver_models.items():
        # нормализация имени ассета
        silver_name = silver_key[8:] if silver_key.startswith("silver__") else silver_key

        # PK: ищем по обоим ключам, затем в конфиге
        pk_cols = tuple(
            (silver_pk_map.get(silver_key) or silver_pk_map.get(silver_name) or ())
        )
        if not pk_cols:
            st = (silver_tables_cfg.get(silver_key) or silver_tables_cfg.get(silver_name) or {})
            pk_cols = tuple(st.get("primary_key") or st.get("pk") or ())
        if not pk_cols:
            raise ValueError(f"Не задан primary_key для silver '{silver_key}'")

        normalizer = normalizers.get(silver_key) or normalizers.get(silver_name)
        if normalizer is None:
            st = (silver_tables_cfg.get(silver_key) or silver_tables_cfg.get(silver_name) or {})
            n_path = st.get("normalizer")
            if isinstance(n_path, str):
                normalizer = resolve_build_params(n_path)

        if normalizer is None:
            raise ValueError(f"Не найден нормализатор для silver '{silver_name}'")

        normalize_cb = asyncify(normalize_wrapper(normalizer))

        # корректное имя таблицы с учётом схемы
        _tbl = silver_model.__table__
        _silver_table = f"{_tbl.schema}.{_tbl.name}" if getattr(_tbl, "schema", None) else _tbl.name

        persist_silver_cb = asyncify(
            persist_silver_map.get(silver_key)
            or persist_silver_map.get(silver_name)
            or make_default_persist_silver(_silver_table, pk_cols)
        )

        silvers.append(
            SilverSpec(
                name=silver_name,
                partitions_def=partitions_def,
                select_best_bronze=lambda ctx, table, biz, cid, _sb=select_best_bronze_cb: _sb(ctx, table, biz, cid),
                normalize=normalize_cb,
                persist_silver=persist_silver_cb,
                silver_model=silver_model,
                pk_cols=pk_cols,
                op_tags=pipe_cfg.get("silver_op_tags", {}) or {},
            )
        )

    pipeline_spec = SyncApiPipelineSpec(
        pipe_name=pipe_name,
        client_resource_key=client_resource_key,
        bronze=bronze_spec,
        silvers=silvers,
        op_tags=pipe_cfg.get("op_tags", {}) or {},
    )

    return make_sync_api_assets(pipeline_spec)
