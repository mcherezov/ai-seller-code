from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple
from datetime import datetime

from dagster import (
    asset,
    AssetsDefinition,
    MultiPartitionsDefinition,
    Output,
    RetryPolicy,
    MetadataValue,
)

from dagster_conf.lib.partitions import build_multipartitions
from dagster_conf.lib.asset_factories.factory_utils import (
    extract_partition,
    merge_tags,
    default_resolve_auth,
    default_persist_bronze,
    default_select_best_bronze,
    make_default_persist_silver,
    normalize_wrapper,
    coerce_row_types,
    MSK,
)
from dagster_conf.lib.asset_factories.task_asset_factory import BronzeApiContext


# ------------------------- specs -------------------------

@dataclass
class SeleniumBronzeSpec:
    name: str
    partitions_def: MultiPartitionsDefinition
    bronze_table: str
    # колбэки
    get_report_jobs: Callable[[Any, datetime, int], Awaitable[List[Dict[str, Any]]]]
    download_one: Callable[[Any, Dict[str, Any]], Awaitable[Dict[str, Any]]]
    resolve_auth: Callable[[Any, int], Awaitable[Tuple[Optional[int], Optional[str]]]] = default_resolve_auth
    persist_bronze: Callable[[Any, str, BronzeApiContext, Dict[str, Any]], Awaitable[str]] = default_persist_bronze
    required_resource_keys: Tuple[str, ...] = ("postgres", "selenium_remote")
    op_retry_policy: RetryPolicy = field(default_factory=lambda: RetryPolicy(max_retries=3, delay=60))
    op_tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class SeleniumSilverSpec:
    name: str
    partitions_def: MultiPartitionsDefinition
    silver_model: Any
    pk_cols: Tuple[str, ...]
    normalize: Callable[[Any, Dict[str, Any], BronzeApiContext], Awaitable[Iterable[Dict[str, Any]]]]
    select_best_bronze: Callable[[Any, str, datetime, int], Awaitable[Optional[Dict[str, Any]]]] = default_select_best_bronze
    persist_silver: Callable[[Any, Iterable[Dict[str, Any]]], Awaitable[int]] = None  # подставим make_default_persist_silver
    required_resource_keys: Tuple[str, ...] = ("postgres",)
    op_retry_policy: RetryPolicy = field(default_factory=lambda: RetryPolicy(max_retries=3, delay=30))
    op_tags: Dict[str, str] = field(default_factory=dict)


# ------------------------- factory -------------------------

def build_assets(
    pipe_name: str,
    pipe_cfg: Dict[str, Any],
    model_bundle: Dict[str, Any],
    res: Any = None,
) -> List[AssetsDefinition]:
    """Строим ассеты для Selenium-пайплайна (bronze + fan-out silver)."""

    def _resolve_callable(obj_or_path: Any) -> Callable:
        if callable(obj_or_path):
            return obj_or_path
        if isinstance(obj_or_path, str):
            if res is not None and hasattr(res, "resolve_callable"):
                return res.resolve_callable(obj_or_path)
            from importlib import import_module
            if ":" in obj_or_path:
                mod, obj = obj_or_path.split(":", 1)
            else:
                mod, obj = obj_or_path.rsplit(".", 1)
            return getattr(import_module(mod), obj)
        raise TypeError(f"Cannot resolve callable from {type(obj_or_path)}")

    # 1) партиции
    partitions_def = pipe_cfg.get("partitions_def") or build_multipartitions(pipe_cfg)

    # 2) бронза (таблица)
    bronze_model = model_bundle["bronze_model"]
    tbl = bronze_model.__table__
    bronze_table = f"{getattr(tbl, 'schema', None) + '.' if getattr(tbl, 'schema', None) else ''}{tbl.name}"

    # 3) колбэки Selenium из конфига
    selenium_cfg = pipe_cfg["selenium"]
    get_report_jobs = _resolve_callable(selenium_cfg["get_report_jobs"])
    download_one = _resolve_callable(selenium_cfg["download_one"])

    bronze_spec = SeleniumBronzeSpec(
        name=pipe_name,
        partitions_def=partitions_def,
        bronze_table=bronze_table,
        get_report_jobs=get_report_jobs,
        download_one=download_one,
        op_tags=(pipe_cfg.get("op_tags") or {}).get("bronze", {}),
    )

    # 4) silvers (fan-out)
    silvers: List[SeleniumSilverSpec] = []
    for s_name, s_model in (model_bundle.get("silver_models") or {}).items():
        normalizer_raw = (model_bundle.get("normalizers") or {}).get(s_name)
        if normalizer_raw is None:
            raise KeyError(f"Missing normalizer for silver target '{s_name}'")
        normalizer = normalize_wrapper(_resolve_callable(normalizer_raw))

        pk_cols = tuple((model_bundle.get("silver_pk") or {}).get(s_name) or ())
        if not pk_cols:
            raise KeyError(f"Missing silver_pk for '{s_name}'")

        s = SeleniumSilverSpec(
            name=s_name,
            partitions_def=partitions_def,
            silver_model=s_model,
            pk_cols=pk_cols,
            normalize=normalizer,
            op_tags=(pipe_cfg.get("op_tags") or {}).get("silver", {}),
        )
        silver_tbl = s_model.__table__
        silver_table = f"{silver_tbl.schema + '.' if getattr(silver_tbl, 'schema', None) else ''}{silver_tbl.name}"
        s.persist_silver = s.persist_silver or make_default_persist_silver(silver_table, pk_cols)
        silvers.append(s)

    base_tags = pipe_cfg.get("op_tags") or {}

    # ---------- bronze asset ----------
    @asset(
        name=f"bronze__{pipe_name}",
        key_prefix=["bronze"],
        partitions_def=bronze_spec.partitions_def,
        required_resource_keys=set(bronze_spec.required_resource_keys),
        retry_policy=bronze_spec.op_retry_policy,
        op_tags=merge_tags(base_tags, bronze_spec.op_tags),
        description="Selenium: собираем отчёты за день по всем профилям, скачиваем ZIP и пишем аудиторный след в бронзу",
    )
    async def bronze_asset(context):
        business_dttm, company_id = extract_partition(context)
        token_id, token = await bronze_spec.resolve_auth(context, company_id)
        api_ctx = BronzeApiContext(
            business_dttm=business_dttm,
            company_id=company_id,
            token_id=token_id,
            token=token,
            run_uuid=context.run_id,
        )

        jobs = await bronze_spec.get_report_jobs(context, business_dttm, company_id)
        ok, fail = 0, 0
        last_req_uuid: Optional[str] = None
        last_status: int = 0
        last_resp_dttm: datetime = business_dttm

        for j in (jobs or []):
            # метаданные job для аудита
            job_params = {
                "reportId": j.get("report_id") or j.get("reportId"),
                "reportDate": str(j.get("report_date") or j.get("reportDate") or ""),
                "profile": j.get("profile") or j.get("profile_name"),
            }
            req_started_at = datetime.now(MSK)

            try:
                # --- DOWNLOAD ---
                resp = await bronze_spec.download_one(context, j)  # твоя функция

                # Гарантируем dttm:
                resp = dict(resp or {})
                resp.setdefault("request_dttm", req_started_at)
                # response_dttm уже может быть проставлен download_one; если нет — поставим сейчас
                resp.setdefault("response_dttm", datetime.now(MSK))
                # receive_dttm: что пришло из download_one → иначе = response_dttm → иначе now
                resp.setdefault("receive_dttm", resp.get("response_dttm") or datetime.now(MSK))

                # request_parameters/request_body: дефолтами из job, если функция их не вернула
                rp = resp.get("request_parameters") or {}
                resp["request_parameters"] = {**job_params, **rp}

                req_uuid = await bronze_spec.persist_bronze(
                    context, bronze_spec.bronze_table, api_ctx, resp
                )

                last_req_uuid = req_uuid
                last_status = int(resp.get("status", 0))
                last_resp_dttm = resp.get("response_dttm") or business_dttm
                ok += 1

            except Exception as e:
                # Даже при ошибке пишем аудит с корректными метками времени
                resp_err = {
                    "status": 500,
                    "headers": {},
                    "payload": {"error": f"selenium download error: {e!s}"},
                    "request_parameters": job_params,
                    "request_dttm": req_started_at,
                    "response_dttm": datetime.now(MSK),
                    "receive_dttm": datetime.now(MSK),
                }
                try:
                    req_uuid = await bronze_spec.persist_bronze(
                        context, bronze_spec.bronze_table, api_ctx, resp_err
                    )
                    last_req_uuid = req_uuid
                    last_status = 500
                    last_resp_dttm = resp_err["response_dttm"]
                finally:
                    context.log.error(
                        f"[bronze__{pipe_name}] download/persist error for report {job_params['reportId']}: {e!r}"
                    )
                fail += 1

    assets: List[AssetsDefinition] = [bronze_asset]

    # ---------- silver assets ----------
    def make_silver_asset(s: SeleniumSilverSpec) -> AssetsDefinition:
        s_tags = merge_tags(base_tags, s.op_tags)

        @asset(
            name=f"silver__{s.name}",
            key_prefix=["silver"],
            partitions_def=s.partitions_def,
            required_resource_keys=set(s.required_resource_keys),
            deps=[bronze_asset.key],
            retry_policy=s.op_retry_policy,
            op_tags=s_tags,
            description="Берём лучшую бронзу по партиции, нормализуем и upsert в silver",
        )
        async def _silver(context) -> Optional[int]:
            business_dttm, company_id = extract_partition(context)
            token_id, token = await bronze_spec.resolve_auth(context, company_id)
            api_ctx = BronzeApiContext(
                business_dttm=business_dttm,
                company_id=company_id,
                token_id=token_id,
                token=token,
                run_uuid=context.run_id,
            )

            best = await s.select_best_bronze(
                context, bronze_spec.bronze_table, business_dttm, company_id
            )
            if not best:
                context.log.warning(
                    f"[silver__{s.name}] нет успешной бронзы для company_id={company_id} {business_dttm.date()} — пропуск"
                )
                return None

            rows_iter = await s.normalize(context, best, api_ctx)  # Iterable[dict]
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
                        {k: v for k, v in merged.items() if k != "inserted_at"},
                    )
                )

            written = await s.persist_silver(context, prepared)
            context.add_output_metadata(
                {
                    "rows": MetadataValue.int(written),
                    "business_dttm": MetadataValue.text(business_dttm.isoformat()),
                    "company_id": MetadataValue.text(str(company_id)),
                    "request_uuid": MetadataValue.text(str(best.get("request_uuid") or "")),
                    "receive_dttm": MetadataValue.text(
                        (best.get("receive_dttm") or business_dttm).isoformat()
                    ),
                }
            )
            return written

        return _silver

    for s in silvers:
        assets.append(make_silver_asset(s))

    return assets
