from __future__ import annotations

import asyncio
import json
import inspect
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Awaitable
from contextlib import asynccontextmanager

from dagster import (
    asset,
    AssetsDefinition,
    AssetIn,
    Output,
    Failure,
    RetryPolicy,
    MultiPartitionsDefinition,
    MetadataValue,
)

# Партиции — если билдер не передал готовый объект, построим из pipe_cfg
from dagster_conf.lib.partitions import build_multipartitions

# Общие утилиты (те же, что и для sync_api фабрики)
from dagster_conf.lib.asset_factories.factory_utils import (
    MSK,
    extract_partition,                # строгий разбор MultiPartitionKey → (business_dttm, company_id)
    merge_tags,                       # объединение op_tags
    bronze_output_metadata,           # стандартные метаданные для Bronze materialization
    default_resolve_auth,             # (token_id, token) по company_id
    default_persist_bronze,           # запись аудита/сырья в бронзу
    default_select_best_bronze,       # выбор «лучшей» успешной бронзы
    make_default_persist_silver,      # фабрика upsert-функции для Silver
    normalize_wrapper,                # обёртка нормализатора (поддержка разных сигнатур)
    coerce_row_types,                 # приведение типов под модель
    resolve_build_params,             # dotted-path → callable
    as_unified_resp,
)

# ---------------------------
#   Спецификации (dataclass)
# ---------------------------

@dataclass
class BronzeApiContext:
    """Контекст вызова API для Bronze (передаётся в колбэки/нормализаторы)."""
    business_dttm: datetime
    company_id: int
    token_id: Optional[int]
    token: Optional[str]
    run_uuid: str


@dataclass
class TaskBronzeSpec:
    """
    Спецификация Bronze для паттерна submit → poll → download.
    При любом не-2xx статусе ассет должен упасть Failure (чтобы сработали ретраи Dagster).
    """
    name: str                                   # логическое имя → bronze__{name}
    client_resource_key: str                    # ключ ресурса-клиента (например, "wildberries_client")
    partitions_def: MultiPartitionsDefinition   # мультипартиции (business_dttm + company_id)

    # колбэки (async-контракт)
    resolve_auth: Callable[[Any, int], Awaitable[Tuple[Optional[int], Optional[str]]]]
    persist_bronze: Callable[[Any, str, BronzeApiContext, Dict[str, Any]], Awaitable[str]]

    # методы WB‑клиента
    submit_method: str
    status_method: str
    download_method: str

    # построитель параметров submit‑запроса
    build_request_params: Callable[[datetime, int], Dict[str, Any]]

    # опрос и завершение
    extract_status: Callable[[Dict[str, Any]], str]             # payload → status
    done_status_values: Sequence[str]
    error_status_values: Sequence[str]
    poll_interval_sec: int
    poll_timeout_sec: int

    # таблица бронзы
    bronze_table: str

    # метаданные/ресурсы
    required_resource_keys: Sequence[str] = ("postgres",)       # клиентский ресурс добавит декоратор
    op_retry_policy: RetryPolicy = field(default_factory=lambda: RetryPolicy(max_retries=3, delay=60))
    op_tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class TaskSilverSpec:
    """Спецификация одного Silver (ветвление от Bronze)."""
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
class TaskPollPipelineSpec:
    """Полная спецификация пайплайна: один Bronze + N Silver."""
    pipe_name: str
    client_resource_key: str
    bronze: TaskBronzeSpec
    silvers: List[TaskSilverSpec]
    op_tags: Dict[str, str] = field(default_factory=dict)


# ---------------------------
#   Внутренняя фабрика ассетов
# ---------------------------

class _AsyncNullContext:
    """Асинхронный no-op контекст для случаев, когда клиент не является контекст-менеджером."""
    def __init__(self, obj): self._obj = obj
    async def __aenter__(self): return self._obj
    async def __aexit__(self, exc_type, exc, tb): return False

@asynccontextmanager
async def _maybe_client_ctx(client):
    enter = getattr(client, "__aenter__", None)
    exit_ = getattr(client, "__aexit__", None)
    if enter and exit_:
        wb = await enter()
        try:
            yield wb
        finally:
            await exit_(None, None, None)
    else:
        yield client


def make_task_poll_download_assets(spec: TaskPollPipelineSpec) -> List[AssetsDefinition]:
    """
    Создаёт 1 Bronze (submit→poll→download) и N Silver ассетов.
    Возвращает список в порядке зависимостей: [bronze, *silvers].
    """
    SENSITIVE_KEYS = {"token", "token_override", "authorization", "Authorization"}

    def _scrub_params(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: (_scrub_params(v)) for k, v in obj.items() if k not in SENSITIVE_KEYS}
        if isinstance(obj, list):
            return [_scrub_params(v) for v in obj]
        return obj

    def _extract_from_resp(resp_obj: Any) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        try:
            audit = getattr(resp_obj, "request", None)
            if isinstance(audit, dict):
                q = audit.get("query") or {}
                jb = audit.get("json_body") if "json_body" in audit else None
                q = _scrub_params(q)
                jb = _scrub_params(jb) if jb is not None else None
                return q, jb
        except Exception:
            pass
        return {}, None

    base_tags = {"factory": "task_poll_download", "pipe": spec.pipe_name}

    # ---------- Bronze ----------
    bronze_tags = merge_tags(base_tags, merge_tags(spec.op_tags, spec.bronze.op_tags))

    @asset(
        name=f"bronze__{spec.bronze.name}",
        key_prefix=["bronze"],
        partitions_def=spec.bronze.partitions_def,
        required_resource_keys=set(spec.bronze.required_resource_keys) | {spec.client_resource_key},
        retry_policy=spec.bronze.op_retry_policy,
        op_tags=bronze_tags,
        description=(
            "WB submit → poll(status) → download; пишет аудит и сырые данные в Bronze. "
            "Если статус ответа не 2xx — ассет падает Failure (Dagster ретраит партицию)."
        ),
    )
    async def bronze_asset(context) -> Output[None]:
        business_dttm, company_id = extract_partition(context)
        token_id, token = await spec.bronze.resolve_auth(context, company_id)
        run_dttm = datetime.now(MSK)
        api_ctx = BronzeApiContext(
            business_dttm=business_dttm,
            company_id=company_id,
            token_id=token_id,
            token=token,
            run_uuid=context.run_id,
        )

        if not token:
            md = bronze_output_metadata(api_ctx, request_uuid="", status=401, response_dttm=datetime.now(MSK))
            yield Output(value=None, metadata=md)
            raise Failure(f"No WB token for company_id={company_id}")

        client_obj = getattr(context.resources, spec.client_resource_key)
        if not hasattr(client_obj, spec.bronze.submit_method):
            raise Failure(f"Client '{spec.client_resource_key}' has no method '{spec.bronze.submit_method}'")
        if not hasattr(client_obj, spec.bronze.status_method):
            raise Failure(f"Client '{spec.client_resource_key}' has no method '{spec.bronze.status_method}'")
        if not hasattr(client_obj, spec.bronze.download_method):
            raise Failure(f"Client '{spec.client_resource_key}' has no method '{spec.bronze.download_method}'")

        # строим submit-параметры
        submit_params = spec.bronze.build_request_params(business_dttm, company_id) or {}
        alias_map = {"dateFrom": "date_from", "dateTo": "date_to"}
        submit_params = {alias_map.get(k, k): v for k, v in submit_params.items()}
        if "token" in submit_params:
            submit_params["token_override"] = submit_params.pop("token")

        now_msk = datetime.now(MSK)
        flow_started_at = now_msk  # request_dttm всего потока

        # чтобы не ловить UnboundLocalError в except
        submit_params_from_audit: dict | None = None
        submit_body_from_audit: str | None = None
        poll_params_from_audit: dict | None = None

        async with _maybe_client_ctx(client_obj) as client:
            submit_fn = getattr(client, spec.bronze.submit_method)
            status_fn = getattr(client, spec.bronze.status_method)
            download_fn = getattr(client, spec.bronze.download_method)

            submit_sig = inspect.signature(submit_fn)
            status_sig = inspect.signature(status_fn)
            download_sig = inspect.signature(download_fn)

            # фильтруем kwargs по сигнатуре submit
            submit_params = {k: v for k, v in submit_params.items() if k in submit_sig.parameters}
            if "token_override" in submit_sig.parameters and "token_override" not in submit_params and token:
                submit_params["token_override"] = token

            try:
                # --- SUBMIT ---
                submit_resp = await submit_fn(**submit_params)
                submit_params_from_audit, submit_body_from_audit = _extract_from_resp(submit_resp)
                status_code, payload, headers, resp_dt = as_unified_resp(submit_resp, default_dt=now_msk)

                task_id = (
                    (payload.get("data") or {}).get("taskId")
                    if isinstance(payload, dict) and "data" in payload
                    else (payload.get("taskId") if isinstance(payload, dict) else None)
                )
                if not task_id:
                    request_uuid = await spec.bronze.persist_bronze(
                        context,
                        spec.bronze.bronze_table,
                        api_ctx,
                        {
                            "status": int(status_code),
                            "payload": {"error": "taskId not found in submit response", "payload": payload},
                            "headers": headers,
                            "request_dttm": flow_started_at,
                            "response_dttm": resp_dt,
                            "receive_dttm": resp_dt,
                            "request_parameters": submit_params_from_audit or _scrub_params(submit_params),
                            "request_body": submit_body_from_audit,
                        },
                    )
                    md = bronze_output_metadata(api_ctx, request_uuid, int(status_code), resp_dt)
                    yield Output(value=None, metadata=md)
                    raise Failure(f"taskId not found in submit response; company_id={company_id}")

                # kwargs для status/download
                status_kwargs = {"token_override": token} if "token_override" in status_sig.parameters else {}
                download_kwargs = {"token_override": token} if "token_override" in download_sig.parameters else {}

                # --- POLL ---
                deadline = now_msk.timestamp() + int(spec.bronze.poll_timeout_sec)
                while True:
                    poll_raw = await status_fn(task_id, **status_kwargs)
                    poll_params_from_audit, _ = _extract_from_resp(poll_raw)
                    poll_code, poll_payload, poll_headers, poll_dt = as_unified_resp(poll_raw, default_dt=now_msk)
                    state = spec.bronze.extract_status(poll_payload) or ""
                    context.log.info(f"[{spec.pipe_name}] task_id={task_id} status={state}")

                    if state in spec.bronze.done_status_values:
                        break
                    if state in spec.bronze.error_status_values:
                        request_uuid = await spec.bronze.persist_bronze(
                            context,
                            spec.bronze.bronze_table,
                            api_ctx,
                            {
                                "status": int(poll_code) if poll_code else 500,
                                "payload": {"error": f"WB task failed with status={state}", "payload": poll_payload},
                                "headers": poll_headers,
                                "request_dttm": flow_started_at,
                                "response_dttm": poll_dt,
                                "receive_dttm": poll_dt,
                                "request_parameters": (poll_params_from_audit or {"taskId": task_id}),
                            },
                        )
                        md = bronze_output_metadata(api_ctx, request_uuid, int(poll_code or 500), poll_dt)
                        yield Output(value=None, metadata=md)
                        raise Failure(f"WB task failed with status={state}")

                    if datetime.now(MSK).timestamp() > deadline:
                        request_uuid = await spec.bronze.persist_bronze(
                            context,
                            spec.bronze.bronze_table,
                            api_ctx,
                            {
                                "status": 504,
                                "payload": {"error": "polling timeout", "last_payload": poll_payload},
                                "headers": poll_headers,
                                "request_dttm": flow_started_at,
                                "response_dttm": poll_dt,
                                "receive_dttm": poll_dt,
                                "request_parameters": (poll_params_from_audit or {"taskId": task_id}),
                            },
                        )
                        md = bronze_output_metadata(api_ctx, request_uuid, 504, poll_dt)
                        yield Output(value=None, metadata=md)
                        raise Failure("WB task polling timeout")

                    await asyncio.sleep(int(spec.bronze.poll_interval_sec))

                # --- DOWNLOAD ---
                download_raw = await download_fn(task_id, **download_kwargs)
                download_params_from_audit, _ = _extract_from_resp(download_raw)
                dl_code, dl_payload, dl_headers, dl_dt = as_unified_resp(download_raw, default_dt=now_msk)
                dl_recv = getattr(download_raw, "received_at", None) or datetime.now(MSK)

                final_request_parameters = {
                    "submit": submit_params_from_audit or {},
                    "status": poll_params_from_audit or {"taskId": task_id},
                    "download": download_params_from_audit or {"taskId": task_id},
                }
                final_request_body = submit_body_from_audit

                request_uuid = await spec.bronze.persist_bronze(
                    context,
                    spec.bronze.bronze_table,
                    api_ctx,
                    {
                        "status": int(dl_code),
                        "payload": dl_payload,
                        "headers": dl_headers,
                        "run_dttm": run_dttm,
                        "request_dttm": flow_started_at,
                        "response_dttm": dl_dt,
                        "receive_dttm": dl_recv,
                        "request_parameters": final_request_parameters,
                        "request_body": final_request_body,
                    },
                )
                md = bronze_output_metadata(api_ctx, request_uuid, int(dl_code), dl_dt)

                if int(dl_code) < 200 or int(dl_code) >= 300:
                    yield Output(value=None, metadata=md)
                    raise Failure(
                        f"bronze__{spec.bronze.name}: статус download != 2xx (response_code={dl_code}). "
                        f"Аудит записан; инициируем повтор согласно RetryPolicy."
                    )

                context.log.info(
                    f"[bronze__{spec.bronze.name}] OK company_id={company_id} biz={business_dttm.date()} status={dl_code}"
                )
                yield Output(value=None, metadata=md)

            except Failure:
                raise
            except Exception as e:
                request_uuid = await spec.bronze.persist_bronze(
                    context,
                    spec.bronze.bronze_table,
                    api_ctx,
                    {
                        "status": 500,
                        "payload": {"error": f"unhandled task flow error: {e!s}"},
                        "headers": {},
                        "request_dttm": flow_started_at,
                        "response_dttm": now_msk,
                        "receive_dttm": now_msk,
                        "request_parameters": submit_params_from_audit or _scrub_params(submit_params),
                        "request_body": submit_body_from_audit,
                    },
                )
                raise Failure(f"Unhandled task flow error: {e!s}")

    assets: List[AssetsDefinition] = [bronze_asset]

    # ---------- Silver fan‑out ----------
    for s in spec.silvers:
        silver_tags = merge_tags(base_tags, merge_tags(spec.op_tags, s.op_tags))

        @asset(
            name=f"silver__{s.name}",
            key_prefix=["silver"],
            partitions_def=s.partitions_def,
            required_resource_keys=set(s.required_resource_keys),
            # оркестрационная зависимость от бронзы
            deps=[bronze_asset.key],
            op_tags=silver_tags,
            description=(
                "Берёт «лучшую» успешную запись из Bronze для партиции, нормализует и делает upsert в Silver. "
                "Если успешной бронзы нет — пропускает без ошибки."
            ),
        )
        async def silver_asset(context) -> Optional[int]:
            business_dttm, company_id = extract_partition(context)

            # Единый контекст (может понадобиться нормалайзеру)
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
                    f"[silver__{s.name}] нет успешной бронзы для company_id={company_id} biz={business_dttm.date()} — пропуск."
                )
                return None
            else:
                context.log.info(f"[silver__{s.name}] получена бронза {best}")
            rows_iter = await s.normalize(context, best, api_ctx)

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
            # ── Диагностика нормализации и схемы ──────────────────────────────
            model_cols = {c.name for c in getattr(s.silver_model, "__table__").columns}
            pk_cols    = set(s.pk_cols or ())
            norm_count = len(prepared)
            first_keys = sorted(list(prepared[0].keys())) if norm_count else []
            all_keys   = sorted({k for row in prepared for k in row.keys()})
            missing_pk_in_first = sorted([c for c in pk_cols if norm_count and not prepared[0].get(c) and prepared[0].get(c) != 0])
            unknown_cols = sorted([c for c in all_keys if c not in model_cols])
            missing_in_model = sorted([c for c in pk_cols if c not in model_cols])  # должно быть пусто

            context.log.info(
                f"[silver__{s.name}] normalized={norm_count} "
                f"first_keys={first_keys} "
                f"pk={sorted(list(pk_cols))} "
                f"missing_pk_in_first={missing_pk_in_first} "
                f"unknown_cols={unknown_cols} "
                f"missing_in_model={missing_in_model}"
            )
            # Небольшая подсветка частой причины: null-ы в PK на части строк
            if norm_count and pk_cols:
                null_pk_stats = {
                    c: sum(1 for row in prepared if (row.get(c) in (None, "")) and row.get(c) != 0)
                    for c in pk_cols
                }
                bad_rows = sum(1 for row in prepared if any((row.get(c) in (None, "")) and row.get(c) != 0 for c in pk_cols))
                context.log.info(
                    f"[silver__{s.name}] null_pk_stats={null_pk_stats} bad_rows={bad_rows}"
                )

            written = int(await s.persist_silver(context, prepared) or 0)
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

        assets.append(silver_asset)

    return assets


# ---------------------------
#   Публичный адаптер под билдер
# ---------------------------

def build_assets(
    pipe_name: str,
    pipe_cfg: Dict[str, Any],
    model_bundle: Dict[str, Any],
    resources: Dict[str, Any],
) -> List[AssetsDefinition]:
    """
    Собирает TaskBronzeSpec + список TaskSilverSpec из `model_bundle`/`pipe_cfg` и возвращает ассеты.

    Единые ключи конфига (в духе sync_api):
      - pipe_cfg.partitions_def ИЛИ описания партиций → через build_multipartitions
      - pipe_cfg.client_resource_key (по умолчанию "wildberries_client")
      - pipe_cfg.api.polling: { submit, status, download, status_path, done, error, interval_sec, timeout_sec }
      - pipe_cfg.api.request: { query: ... }  (опционально)
      - pipe_cfg.api.build_request_params: dotted‑path|callable (приоритет над .request.query)
      - model_bundle: те же поля, что и для sync_api (bronze_model, silver_models, silver_pk, normalizers, ...)
    """
    # --- партиционирование ---
    partitions_def: Optional[MultiPartitionsDefinition] = pipe_cfg.get("partitions_def")
    if partitions_def is None:
        partitions_def = build_multipartitions(pipe_cfg)

    client_resource_key = pipe_cfg.get("client_resource_key", "wildberries_client")

    # --- API‑секция: polling + request ---
    api_cfg: Dict[str, Any] = pipe_cfg.get("api") or {}
    polling_cfg: Dict[str, Any] = api_cfg.get("polling") or {}

    submit_method = str(polling_cfg.get("submit") or "").strip()
    status_method = str(polling_cfg.get("status") or "").strip()
    download_method = str(polling_cfg.get("download") or "").strip()
    if not (submit_method and status_method and download_method):
        raise ValueError("api.polling.{submit,status,download} — обязательны для task_poll_download")

    status_path = str(polling_cfg.get("status_path") or "").strip()
    done_values = list(polling_cfg.get("done") or [])
    error_values = list(polling_cfg.get("error") or [])
    interval_sec = int(polling_cfg.get("interval_sec") or 10)
    timeout_sec = int(polling_cfg.get("timeout_sec") or 1800)

    # построитель submit‑параметров:
    build_params_raw = api_cfg.get("build_request_params")
    if isinstance(build_params_raw, str):
        build_request_params = resolve_build_params(build_params_raw)
    elif callable(build_params_raw):
        build_request_params = build_params_raw
    else:
        req_cfg = (api_cfg.get("request") or {}).get("query") or {}

        def build_request_params(biz: datetime, cid: int) -> Dict[str, Any]:
            # Очень лёгкая интерпретация шаблонов: {{ business_dttm|iso }} / {{ business_dttm|iso_msk }} / {{ business_dttm|iso_plus(1d) }}
            def _render(v: Any) -> Any:
                if not isinstance(v, str):
                    return v
                s = v.strip()
                if "{{" in s and "}}" in s:
                    # поддержим три фильтра: iso, iso_msk, iso_plus(1d)
                    if "business_dttm" in s:
                        if "|iso_plus(1d)" in s:
                            dt = (biz if biz.tzinfo else biz.replace(tzinfo=MSK))
                            dt = dt.astimezone(MSK) + timedelta(days=1)
                            return dt.isoformat()
                        if "|iso_msk" in s:
                            dt = (biz if biz.tzinfo else biz.replace(tzinfo=MSK)).astimezone(MSK)
                            return dt.strftime("%Y-%m-%dT%H:%M:%S")
                        # по умолчанию — iso
                        return (biz if biz.tzinfo else biz.replace(tzinfo=MSK)).isoformat()
                return s

            return {k: _render(v) for k, v in dict(req_cfg).items()}

    # --- модели/нормализаторы/прикладные колбэки ---
    bronze_model = model_bundle.get("bronze_model")
    silver_models: Dict[str, Any] = model_bundle.get("silver_models", {}) or {}
    silver_pk_map: Dict[str, Tuple[str, ...]] = model_bundle.get("silver_pk", {}) or {}
    normalizers_map: Dict[str, Any] = model_bundle.get("normalizers", {}) or {}

    if not bronze_model:
        raise ValueError("model_bundle.bronze_model обязателен")
    if not silver_models:
        raise ValueError("model_bundle.silver_models пуст — нужен хотя бы один Silver")

    # Полное имя бронзовой таблицы (schema.table или table)
    tbl = bronze_model.__table__
    bronze_table = f"{tbl.schema}.{tbl.name}" if getattr(tbl, "schema", None) else tbl.name

    # Колбэк статуса — компилируем простую функцию извлечения поля (поддерживаем '$.a.b' и 'a.b')
    extract_status = _compile_status_extractor(status_path)

    # Колбэки Bronze/Silver (по умолчанию — общие, как и в sync_api)
    resolve_auth_cb = default_resolve_auth
    persist_bronze_cb = default_persist_bronze
    select_best_bronze_cb = default_select_best_bronze

    # Нормалайзеры + upsert‑персистеры для каждой витрины
    silvers: List[TaskSilverSpec] = []
    for silver_name, silver_model in silver_models.items():
        pk_cols = tuple(silver_pk_map.get(silver_name) or ())

        raw_norm = normalizers_map.get(silver_name)

        # 1) если нормализатор не указан
        if raw_norm is None:
            raise ValueError(
                f"[{pipe_name}] no normalizer configured for silver '{silver_name}'. "
                f"Add 'silver.targets.{silver_name}.normalizer' in the config."
            )

        # 2) если строка — резолвим dotted-path → callable
        if isinstance(raw_norm, str):
            try:
                raw_norm = resolve_build_params(raw_norm)
            except Exception as e:
                raise ValueError(
                    f"[{pipe_name}] failed to resolve normalizer for '{silver_name}': {normalizers_map.get(silver_name)!r} → {e}"
                )

        # 3) оборачиваем уже реальный callable
        normalizer = normalize_wrapper(raw_norm)
        if normalizers_map.get(silver_name) is None:
            context_str = f"{pipe_name}:{silver_name}"
            # при сборке ассетов лога нет, поэтому можно хотя бы через print:
            print(f"[WARN] no normalizer configured for {context_str}; will return []")

        persist_silver = make_default_persist_silver(
            table_name=(f"{silver_model.__table__.schema}.{silver_model.__table__.name}"
                        if getattr(silver_model.__table__, "schema", None)
                        else silver_model.__table__.name),
            pk_cols=pk_cols,
        )
        silvers.append(
            TaskSilverSpec(
                name=silver_name,
                partitions_def=partitions_def,
                select_best_bronze=select_best_bronze_cb,
                normalize=normalizer,
                persist_silver=persist_silver,
                silver_model=silver_model,
                pk_cols=pk_cols,
            )
        )

    bronze_spec = TaskBronzeSpec(
        name=pipe_name,
        client_resource_key=client_resource_key,
        partitions_def=partitions_def,
        resolve_auth=resolve_auth_cb,
        persist_bronze=persist_bronze_cb,
        submit_method=submit_method,
        status_method=status_method,
        download_method=download_method,
        build_request_params=build_request_params,
        extract_status=extract_status,
        done_status_values=done_values,
        error_status_values=error_values,
        poll_interval_sec=interval_sec,
        poll_timeout_sec=timeout_sec,
        bronze_table=bronze_table,
    )

    spec = TaskPollPipelineSpec(
        pipe_name=pipe_name,
        client_resource_key=client_resource_key,
        bronze=bronze_spec,
        silvers=silvers,
        op_tags=dict(pipe_cfg.get("op_tags") or {}),
    )

    return make_task_poll_download_assets(spec)


# ---------------------------
#   Вспомогательные функции
# ---------------------------

def _compile_status_extractor(status_path: str) -> Callable[[Any], str]:
    """
    Компилирует функцию извлечения статуса из payload.
    Сначала пробует указанный путь, затем — набор типичных альтернатив:
      $.data.status, $.result.status, $.task.status, $.payload.status,
      а также плоские 'status' или 'state' на верхнем уровне.
    Возвращает строку в UPPERCASE без лишних пробелов.
    """
    path = (status_path or "").strip()

    def _get_by_path(payload: Any, dotted: str) -> Any:
        if not dotted:
            return None
        keys = [p for p in dotted.split(".") if p]
        cur: Any = payload
        for k in keys:
            if isinstance(cur, dict):
                cur = cur.get(k)
            else:
                return None
        return cur

    # первичный путь: "$.a.b" или "a.b"
    primary_path = ""
    if path:
        primary_path = path[2:] if path.startswith("$.") else path

    # частые альтернативы
    fallback_paths = [
        "data.status",
        "result.status",
        "task.status",
        "payload.status",
        "meta.status",
    ]

    def _extract(payload: Any) -> str:
        val = None
        # 1) первичный путь
        if primary_path:
            val = _get_by_path(payload, primary_path)
        # 2) альтернативы
        if val in (None, "", []):
            for alt in fallback_paths:
                val = _get_by_path(payload, alt)
                if val not in (None, "", []):
                    break
        # 3) плоские ключи
        if val in (None, "", []) and isinstance(payload, dict):
            for k in ("status", "state"):
                if k in payload:
                    val = payload.get(k)
                    if val not in (None, "", []):
                        break

        try:
            s = str(val).strip()
            return s.upper() if s else ""
        except Exception:
            return ""

    return _extract
