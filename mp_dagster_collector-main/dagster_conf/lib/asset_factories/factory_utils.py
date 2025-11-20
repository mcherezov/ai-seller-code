from __future__ import annotations
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, List
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
import json
import base64
import inspect
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from dagster import Failure, MultiPartitionKey, MetadataValue
from dagster_conf.lib.timeutils import scheduled_time_msk
from src.connectors.wb.wb_api_v2 import WildberriesAsyncClient, WBResponse
from dagster_conf.lib.timeutils import parse_http_date
MSK = ZoneInfo("Europe/Moscow")

def parse_business_dttm(raw: str) -> datetime:
    """Парсит бизнес-дату (дата или ISO) → timezone-aware datetime в MSK."""
    raw = (raw or "").strip()
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        dt = datetime.strptime(raw, "%Y-%m-%d")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MSK)
    return dt.astimezone(MSK)

def extract_partition(context) -> Tuple[datetime, int]:
    """Возвращает (business_dttm, company_id) из MultiPartitionKey — со строгими проверками."""
    pk = context.partition_key
    if not isinstance(pk, MultiPartitionKey):
        raise Failure(description="Ожидается MultiPartitionKey для (business_dttm, company_id)")
    try:
        biz_raw = pk.keys_by_dimension["business_dttm"]
        comp_raw = pk.keys_by_dimension["company_id"]
    except KeyError as e:
        raise Failure(description=f"В партиции отсутствует измерение: {e}")
    business_dttm = parse_business_dttm(biz_raw)
    try:
        company_id = int(comp_raw)
    except Exception:
        raise Failure(description=f"company_id должен приводиться к int, получено: {comp_raw}")
    return business_dttm, company_id

def merge_tags(base: Dict[str, str], extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    out = dict(base)
    if extra:
        out.update(extra)
    return out

def bronze_output_metadata(api_ctx, request_uuid: str, status: int, response_dttm: datetime) -> Dict[str, MetadataValue]:
    """Готовые метаданные для Materialization Bronze."""
    return {
        "business_dttm": MetadataValue.text(api_ctx.business_dttm.isoformat()),
        "company_id": MetadataValue.text(str(api_ctx.company_id)),
        "token_id": MetadataValue.text(str(api_ctx.token_id) if getattr(api_ctx, "token_id", None) is not None else ""),
        "status": MetadataValue.int(int(status)),
        "response_dttm": MetadataValue.text(response_dttm.isoformat()),
        "request_uuid": MetadataValue.text(str(request_uuid)),
    }

async def default_resolve_auth(context, company_id: int) -> Tuple[Optional[int], Optional[str]]:
    """Возвращает (token_id, token) из core.tokens по company_id."""
    try:
        pg = context.resources.postgres
        # async SQLAlchemy session
        if callable(pg):  # наш ресурс: async with postgres() as session
            async with pg() as session:
                res = await session.execute(
                    sa.text("""
                        SELECT token_id, token
                          FROM core.tokens
                         WHERE company_id = :cid AND is_active = TRUE
                         LIMIT 1
                    """),
                    {"cid": company_id},
                )
                row = res.first()
        else:
            # fallback: DB-API адаптер
            row = pg.fetchone(
                "SELECT token_id, token FROM core.tokens WHERE company_id = %s AND is_active = TRUE LIMIT 1",
                (company_id,),
            )

        if not row:
            context.log.warning(f"Не найден активный токен для company_id={company_id}")
            return None, None
        return int(row[0]), str(row[1])
    except Exception as e:
        context.log.warning(f"Ошибка при получении токена для company_id={company_id}: {e}")
        return None, None


def default_call_api(context, client_resource_key: str, fetch_method: str,
                     build_request_params: Callable[[datetime, int], Dict[str, Any]],
                     api_ctx) -> Dict[str, Any]:
    """Вызывает метод клиента-ресурса и приводит ответ к единому виду."""
    client = getattr(context.resources, client_resource_key)
    if not hasattr(client, fetch_method):
        raise RuntimeError(f"Клиент '{client_resource_key}' не содержит метод '{fetch_method}'")
    method = getattr(client, fetch_method)

    params = build_request_params(api_ctx.business_dttm, api_ctx.company_id) or {}
    # если токен нужен на уровне запроса — подставим его, если отсутствует
    if api_ctx.token is not None and "token" not in params:
        params = {**params, "token": api_ctx.token}

    # ожидаем кортеж (status, payload, headers, response_dttm)
    status, payload, headers, resp_dt = method(**params)
    return {
        "status": int(status),
        "payload": payload,
        "headers": headers or {},
        "response_dttm": resp_dt or datetime.now(MSK),
    }

def as_unified_resp(raw: Any, *, default_dt: datetime) -> Tuple[int, Any, Dict[str, Any], datetime]:
    try:
        from src.connectors.wb.wb_api_v2 import WBResponse as V2WBResponse
        if isinstance(raw, V2WBResponse):
            headers = dict(raw.headers or {})
            # 1) Если есть текст — всегда отдадим текст, пытаясь распарсить JSON вне зависимости от Content-Type
            if raw.body_text is not None:
                try:
                    payload = json.loads(raw.body_text)
                except Exception:
                    payload = raw.body_text
                return int(raw.status or 200), payload, headers, (raw.response_dttm or default_dt)

            # 2) Иначе, если есть байты — отдадим байты
            if raw.body_bytes is not None:
                return int(raw.status or 200), raw.body_bytes, headers, (raw.response_dttm or default_dt)
    except Exception:
        pass

    if isinstance(raw, dict) and "status" in raw:
        return int(raw.get("status", 200)), raw.get("payload"), dict(raw.get("headers") or {}), raw.get("response_dttm") or default_dt
    if isinstance(raw, (tuple, list)) and len(raw) >= 2:
        status = int(raw[0] if raw[0] is not None else 200)
        payload = raw[1]
        headers = dict(raw[2]) if len(raw) > 2 and isinstance(raw[2], dict) else {}
        dt = raw[3] if len(raw) > 3 and isinstance(raw[3], datetime) else default_dt
        return status, payload, headers, dt

    return 200, raw, {}, default_dt

def _to_jsonb_param(val: Any) -> Optional[str]:
    """Гарантированно приводим к JSON-строке для передачи в text() / asyncpg."""
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        try:
            # если это уже JSON в байтах
            json.loads(val.decode("utf-8", "replace"))
            return val.decode("utf-8", "replace")
        except Exception:
            return json.dumps({"_raw": val.decode("utf-8", "replace")}, ensure_ascii=False)
    if isinstance(val, str):
        # если это валидный JSON — оставим как есть; иначе обернём
        try:
            json.loads(val)
            return val
        except Exception:
            return json.dumps({"_raw": val}, ensure_ascii=False)
    # dict/list/прочее сериализуем
    try:
        return json.dumps(val, ensure_ascii=False)
    except Exception:
        return json.dumps({"_raw": str(val)}, ensure_ascii=False)

def _to_text_for_bronze(val):
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    if isinstance(val, (bytes, bytearray, memoryview)):
        try:
            return bytes(val).decode("utf-8")
        except Exception:
            return "__base64__:" + base64.b64encode(bytes(val)).decode("ascii")
    # всё прочее
    return str(val)

async def default_persist_bronze(context, table_name: str, api_ctx, resp: Dict[str, Any]) -> str:
    """Стандартная вставка в бронзу с typed bindparams для JSONB."""
    from uuid import uuid4

    request_uuid = str(uuid4())
    now_msk = datetime.now(MSK)

    # таймстемпы (при отсутствии — дефолтим текущим MSK)
    run_schedule_dttm = scheduled_time_msk(context, default_msk=api_ctx.business_dttm)
    run_dttm = resp.get("run_dttm")
    request_dttm = resp.get("request_dttm")
    response_dttm = resp.get("response_dttm")
    receive_dttm = resp.get("receive_dttm")

    status = int(resp.get("status", 0))

    response_body: Any = resp.get("response_body")
    if response_body is None:
        response_body = resp.get("payload")
    request_parameters: Any = resp.get("request_parameters")
    request_body: Any = resp.get("request_body")

    stmt = sa.text(f"""
        INSERT INTO {table_name} (
            request_uuid,
            company_id,
            api_token_id,
            business_dttm,
            response_dttm,
            receive_dttm,
            response_code,
            response_body,
            inserted_at,
            run_uuid,
            run_dttm,
            run_schedule_dttm,
            request_dttm,
            request_parameters,
            request_body
        ) VALUES (
            :request_uuid,
            :company_id,
            :api_token_id,
            :business_dttm,
            :response_dttm,
            :receive_dttm,
            :response_code,
            :response_body,
            now(),
            :run_uuid,
            :run_dttm,
            :run_schedule_dttm,
            :request_dttm,
            :request_parameters,
            :request_body
        )
    """).bindparams(
        sa.bindparam("request_parameters", type_=JSONB),
        sa.bindparam("request_body", type_=JSONB),
    )

    params = {
        "request_uuid": request_uuid,
        "company_id": api_ctx.company_id,
        "api_token_id": api_ctx.token_id,
        "business_dttm": api_ctx.business_dttm,
        "response_dttm": response_dttm,
        "receive_dttm": receive_dttm,
        "response_code": status,
        "response_body": _to_text_for_bronze(response_body),
        "run_uuid": api_ctx.run_uuid,
        "run_dttm": run_dttm,
        "run_schedule_dttm": run_schedule_dttm,
        "request_dttm": request_dttm,
        "request_parameters": request_parameters,
        "request_body": request_body,
    }

    try:
        async with context.resources.postgres() as session:
            await session.execute(stmt, params)
            await session.commit()
    except Exception as e:
        context.log.warning(f"Не удалось записать Bronze: {e}")

    return request_uuid


async def default_select_best_bronze(context, table_name: str, business_dttm: datetime, company_id: int) -> Optional[Dict[str, Any]]:
    """Возвращает последнюю успешную (2xx) бронзу по партиции или None."""
    try:
        async with context.resources.postgres() as session:
            res = await session.execute(
                sa.text(f"""
                    SELECT request_uuid, response_body, response_dttm, receive_dttm
                      FROM {table_name}
                     WHERE company_id = :cid
                       AND business_dttm = :biz
                       AND response_code BETWEEN 200 AND 299
                     ORDER BY response_dttm DESC
                     LIMIT 1
                """),
                {"cid": company_id, "biz": business_dttm},
            )
            row = res.first()
        if not row:
            return None

        req_uuid, body, resp_dt, recv_dt = row

        def preview(x: object, n: int = 20) -> str:
            if isinstance(x, bytes):
                s = x.decode("utf-8", errors="replace")
            else:
                s = str(x) if x is not None else ""
            return s[:n] + ("…" if len(s) > n else "")

        context.log.info(f"[select_best_bronze] body_type={type(body).__name__}")
        context.log.info("body: %s", preview(body, 20))

        # безопасно распарсим JSON, если это строка
        payload = safe_json_loads(body)
        context.log.info(f"[select_best_bronze] payload_type={type(payload).__name__}")
        context.log.info("payload: %s", preview(payload, 20))

        return {
            "request_uuid": req_uuid,
            "payload": payload,
            "response_dttm": resp_dt,
            "receive_dttm": recv_dt,
        }
    except Exception as e:
        context.log.warning(
            f"Ошибка выбора лучшей Bronze для company_id={company_id}, business_dttm={business_dttm}: {e}"
        )
        return None



def make_default_persist_silver(table_name: str, pk_cols: Tuple[str, ...]):
    """Возвращает async-функцию insert для Silver (без upsert)."""
    async def _persist(context, rows_iter: Iterable[Dict[str, Any]]) -> int:
        rows = list(rows_iter or [])
        if not rows:
            context.log.info(f"[persist:{table_name}] empty_rows")
            return 0

        cols = sorted({c for r in rows for c in r.keys()})
        sample = sorted(list(rows[0].keys()))
        null_pk_stats = {
            c: sum(1 for r in rows if (r.get(c) in (None, "")) and r.get(c) != 0)
            for c in (pk_cols or ())
        }
        context.log.info(
            f"[persist:{table_name}] rows={len(rows)} cols={cols} pk={list(pk_cols or ())} "
            f"first_row_keys={sample} null_pk_stats={null_pk_stats}"
        )

        insert_cols = ", ".join(cols)
        values_clause = "(" + ", ".join([f":{c}" for c in cols]) + ")"

        sql = sa.text(f"INSERT INTO {table_name} ({insert_cols}) VALUES {values_clause}")

        async with context.resources.postgres() as session:
            await session.execute(sql, rows)  # executemany
            await session.commit()
        return len(rows)

    return _persist


def normalize_wrapper(normalizer):
    if not normalizer:
        async def _noop(context, _best, _api_ctx):
            context.log.warning("[normalize] no normalizer configured → []")
            return []
        return _noop

    async def _call(context, best, api_ctx):
        payload = safe_json_loads(best.get("payload"))

        meta = {
            "company_id": api_ctx.company_id,
            "request_uuid": best.get("request_uuid"),
            "response_dttm": best.get("response_dttm"),
            "receive_dttm": best.get("receive_dttm"),
            "business_dttm": api_ctx.business_dttm,
            "run_uuid": api_ctx.run_uuid,
        }
        partition_ctx = {
            "business_dttm": api_ctx.business_dttm,
            "company_id": api_ctx.company_id,
        }

        import inspect, asyncio
        sig = None
        try:
            sig = inspect.signature(normalizer)
        except Exception:
            pass

        kwargs = {}
        if sig:
            params = sig.parameters
            if "meta" in params:
                kwargs["meta"] = meta
            if "partition_ctx" in params:
                kwargs["partition_ctx"] = partition_ctx
        else:
            # по умолчанию пробрасываем только meta
            kwargs["meta"] = meta

        if inspect.iscoroutinefunction(normalizer):
            return await normalizer(payload, **kwargs)
        return await asyncio.to_thread(normalizer, payload, **kwargs)

    return _call


def coerce_row_types(model, row: Dict[str, Any]) -> Dict[str, Any]:
    """Приводит значения к простейшим SQLAlchemy-типам столбцов модели."""
    types = {c.name: c.type for c in model.__table__.columns}
    out = {}
    for k, v in row.items():
        if k not in types:
            continue
        t = types[k]
        if v is None:
            out[k] = None
            continue
        try:
            if isinstance(t, sa.Date):
                if isinstance(v, datetime):
                    out[k] = v.date()
                elif isinstance(v, date):
                    out[k] = v
                elif isinstance(v, str):
                    out[k] = date.fromisoformat(v[:10])
                else:
                    out[k] = v
            elif isinstance(t, sa.Integer) and isinstance(v, str):
                out[k] = int(v)
            elif isinstance(t, (sa.Float, sa.Numeric)) and isinstance(v, str):
                out[k] = float(v)
            else:
                out[k] = v
        except Exception:
            out[k] = v
    return out

def resolve_build_params(func_name: str) -> Callable:
    """
    Разрешает dotted-path к вызываемому объекту.
    Поддерживает:
      - 'pkg.mod:func'
      - 'pkg.mod.func'
      - 'pkg.mod:Class.method'
      - 'pkg.mod.Class.method'
      - fallback: 'func' → ищем в paid_storage_partition_assets
    Возвращает callable, готовый к вызову (для методов — bound method).
    """
    import importlib
    import inspect

    if not isinstance(func_name, str) or not func_name.strip():
        raise RuntimeError("Пустой путь функции")

    func_name = func_name.strip()

    # Определяем модульную часть и атрибутную часть
    if ":" in func_name:
        mod_path, attr_path = func_name.split(":", 1)
    elif "." in func_name:
        # последний токен считаем атрибутом, всё слева — модуль
        mod_path, attr_path = func_name.rsplit(".", 1)
        # если слева нет точки (т.е. это не модуль), используем fallback
        if "." not in mod_path:
            mod_path = "paid_storage_partition_assets"
            attr_path = func_name
    else:
        mod_path = "paid_storage_partition_assets"
        attr_path = func_name

    module = importlib.import_module(mod_path)

    # Пройдёмся по атрибутам через точки (поддержка вложенности)
    parts = attr_path.split(".")
    cur = module
    trail = []  # копим путь, чтобы понять где класс
    for p in parts:
        trail.append(p)
        try:
            cur = getattr(cur, p)
        except AttributeError:
            raise RuntimeError(f"Не найдено: {mod_path}:{attr_path}")

    # Если это обычная функция или уже bound method — просто вернуть
    if callable(cur) and not inspect.isclass(cur):
        # Если это функция, но она "unbound" метод класса (доступ через класс),
        # то предыдущий объект — класс. Создадим инстанс и вернём bound.
        # Пример: module.WbUniversalNormalizer.norm__wb_supplier_incomes_1d
        if len(parts) >= 2:
            owner_name, meth_name = parts[-2], parts[-1]
            owner = getattr(module, owner_name, None)
            if inspect.isclass(owner) and hasattr(owner, meth_name):
                # Попробуем корректно сконструировать экземпляр.
                # Для WbUniversalNormalizer нужен обязательный target:str.
                # Возьмём его из имени метода после 'norm__' (или заглушку).
                sig = None
                try:
                    sig = inspect.signature(owner)
                except Exception:
                    pass

                kwargs = {}
                if sig:
                    params = sig.parameters
                    if "target" in params and params["target"].default is inspect._empty:
                        # обязательный target
                        if meth_name.startswith("norm__"):
                            kwargs["target"] = meth_name[len("norm__"):]
                        else:
                            kwargs["target"] = "__direct__"

                try:
                    instance = owner(**kwargs) if kwargs else owner()
                    bound = getattr(instance, meth_name, None)
                    if callable(bound):
                        return bound
                except TypeError:
                    pass

        return cur  # обычная функция/bound method

    # Если это класс — попробовать вернуть __call__
    if inspect.isclass(cur):
        # Если класс вызываемый (имеет __call__), вернём его инстанс
        try:
            instance = cur()
        except TypeError as e:
            raise RuntimeError(f"Невозможно инстанцировать класс {cur.__name__}: {e}")
        if callable(instance):
            return instance
        raise RuntimeError(f"Класс {cur.__name__} не является вызываемым объектом")

    # Иначе — нечто невызываемое
    raise RuntimeError(f"Объект по пути {mod_path}:{attr_path} не является вызываемым")

def resolve_client_method(client_cls: type, name: Optional[str]) -> Optional[str]:
    """Проверяет, что у клиентского класса есть метод с таким именем."""
    if name is None:
        return None
    if not hasattr(client_cls, name):
        raise RuntimeError(f"{client_cls.__name__} не содержит метод '{name}'")
    return name

def safe_json_loads(v):
    """Безопасно приводим к JSON-объекту (поддержка memoryview/bytes/str). Иначе возвращаем исходное значение."""
    try:
        if isinstance(v, memoryview):
            v = v.tobytes()
        if isinstance(v, (bytes, bytearray)):
            # попытаться декодировать как текстовый JSON; если это бинарь (zip), отдать как bytes
            try:
                s = v.decode("utf-8", "ignore")
            except Exception:
                return v
            try:
                return json.loads(s)
            except Exception:
                return s
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return v
        # dict/list и прочее уже нормальный Python → вернуть как есть
        return v
    except Exception:
        return v


def _aliases_to_snake(d: Dict[str, Any]) -> Dict[str, Any]:
    """Небольшой алиасинг параметров под сигнатуры клиента V2."""
    out = dict(d or {})
    # общие для WB V2:
    if "dateFrom" in out and "date_from" not in out:
        out["date_from"] = out.pop("dateFrom")
    if "dateTo" in out and "date_to" not in out:
        out["date_to"] = out.pop("dateTo")
    # на всякий случай: payload/json_body взаимозаменяемы
    if "payload" in out and "json_body" not in out:
        out["json_body"] = out["payload"]
    if "json_body" in out and "payload" not in out:
        out["payload"] = out["json_body"]
    return out

async def call_wb_method_and_wrap(
    context,
    client_resource_key: str,
    fetch_method: str,
    build_request_params,
    api_ctx,
) -> Dict[str, Any]:
    """
    Унифицированный вызов метода WB-клиента + приведение результата к бронзовому протоколу.
    • Сам подбирает имя аргумента для тела: payload/json_body
    • Прокидывает token_override, если метод его принимает
    • Возвращает также request_parameters (ИМЕННО в виде, как ушло в WB), чтобы писать в бронзу
    • На ClientResponseError НЕ бросаем исключение — возвращаем словарь с status/body,
      чтобы бронза всегда фиксировала неудачный ответ сервера.
    """
    import inspect
    import json
    from datetime import datetime
    from aiohttp import ClientResponseError
    from urllib.parse import urlparse, parse_qsl

    # вспомогалки для JSON
    def _maybe_parse_json(val):
        if isinstance(val, (dict, list)):
            return val
        if isinstance(val, (bytes, bytearray)):
            return bytes(val)
        if isinstance(val, str):
            s = val.strip()
            if s and s[0] in "{[":
                try:
                    return json.loads(val)
                except Exception:
                    return val
            return val
        return val

    # вычистим чувствительные поля
    def _scrub(x):
        SENSITIVE = {"token", "token_override", "authorization", "Authorization"}
        if isinstance(x, dict):
            return {k: _scrub(v) for k, v in x.items() if k not in SENSITIVE}
        if isinstance(x, list):
            return [_scrub(v) for v in x]
        return x

    # извлекаем request_parameters/request_body из WBResponse.request
    def _extract_params_from_audit(audit: dict | None):
        if not isinstance(audit, dict):
            return {}, None
        q = dict(audit.get("query") or {})
        jb = audit.get("json_body") if "json_body" in audit else None
        return _scrub(q), _scrub(jb)

    biz = api_ctx.business_dttm
    cid = api_ctx.company_id
    token = api_ctx.token

    # 1) Собираем параметры из pipe_cfg (это fallback, если не будет аудита)
    try:
        sig = inspect.signature(build_request_params)
        needs_ctx = len(sig.parameters) >= 3
        if inspect.iscoroutinefunction(build_request_params):
            req_raw = (await build_request_params(context, biz, cid) if needs_ctx
                       else await build_request_params(biz, cid)) or {}
        else:
            req_raw = (build_request_params(context, biz, cid) if needs_ctx
                       else build_request_params(biz, cid)) or {}
    except Exception as e:
        context.log.error(f"[wb_call] build_request_params error: {e!r}")
        req_raw = {}

    # это только запасной вариант; основной источник — audit ниже
    request_parameters_for_bronze = _scrub(req_raw)
    request_body_for_bronze = None

    context.log.info(
        f"[wb_call] {client_resource_key}.{fetch_method} cid={cid} biz={biz.date()} token={'YES' if token else 'NO'}"
    )

    try:
        payload_sample = req_raw.get("payload") or req_raw.get("json_body")
        context.log.debug(
            "[wb_call] build_params keys=%s; payload_present=%s; sample=%s",
            list(req_raw.keys()),
            "True" if payload_sample is not None else "False",
            json.dumps(payload_sample, ensure_ascii=False) if isinstance(payload_sample, (dict, list)) else payload_sample,
        )
    except Exception:
        pass

    # 2) Берём ресурс-клиент
    client = getattr(context.resources, client_resource_key, None)
    if client is None:
        now = datetime.now(MSK)
        return {
            "status": 500,
            "headers": {},
            "response_dttm": now,
            "response_body": f"Resource {client_resource_key!r} not available",
            "request_parameters": request_parameters_for_bronze,
            "request_body": request_body_for_bronze,
        }

    async def _invoke(_client):
        method = getattr(_client, fetch_method, None)
        if not callable(method):
            now = datetime.now(MSK)
            return {
                "status": 500,
                "headers": {},
                "response_dttm": now,
                "response_body": f"{client_resource_key}.{fetch_method} is not callable",
                "request_parameters": request_parameters_for_bronze,
                "request_body": request_body_for_bronze,
            }

        sig = inspect.signature(method)
        param_names = list(sig.parameters.keys())
        context.log.debug(
            f"[wb_call] method={client_resource_key}.{fetch_method} signature params={param_names}"
        )

        # Нормализуем имена для тела запроса
        params = dict(req_raw)
        body = params.get("payload", None)
        if body is None:
            body = params.get("json_body", None)

        if "payload" in sig.parameters:
            if body is not None:
                params["payload"] = body
            params.pop("json_body", None)
        elif "json_body" in sig.parameters:
            if body is not None:
                params["json_body"] = body
            params.pop("payload", None)
        else:
            params.pop("payload", None)
            params.pop("json_body", None)

        if "token_override" in sig.parameters:
            params["token_override"] = token

        # Оставим только то, что метод реально принимает
        filtered = {k: v for k, v in params.items() if k in sig.parameters}

        try:
            body_present = ("payload" in filtered and filtered["payload"] is not None) or \
                           ("json_body" in filtered and filtered["json_body"] is not None)
            body_sample = filtered.get("payload") or filtered.get("json_body")
            context.log.debug(
                f"[wb_call] final params keys={list(filtered.keys())}; "
                f"body_present={'YES' if body_present else 'NO'}; "
                f"body_sample={json.dumps(body_sample, ensure_ascii=False) if isinstance(body_sample, (dict, list)) else body_sample}"
            )
        except Exception:
            pass

        if ("payload" in sig.parameters or "json_body" in sig.parameters) and not (
            ("payload" in filtered and filtered["payload"] is not None) or
            ("json_body" in filtered and filtered["json_body"] is not None)
        ):
            context.log.warning("[wb_call] Method expects body ('payload'/'json_body'), but none provided after mapping!")

        now = datetime.now(MSK)

        try:
            resp = await method(**filtered)
        except ClientResponseError as e:
            status = int(getattr(e, "status", 0) or 0)
            headers = dict(getattr(e, "headers", {}) or {})
            msg = getattr(e, "message", "") or ""
            req_info = getattr(e, "request_info", None)
            url = str(getattr(req_info, "real_url", "")) if req_info else ""
            method_name = getattr(req_info, "method", "") if req_info else ""

            # Пытаемся достать query из URL даже при ошибке
            audit = None
            if url:
                parsed = urlparse(url)
                audit = {
                    "method": method_name or "GET",
                    "url": url,
                    "query": dict(parse_qsl(parsed.query, keep_blank_values=True)),
                }
                request_parameters_from_audit, request_body_from_audit = _extract_params_from_audit(audit)
            else:
                request_parameters_from_audit, request_body_from_audit = {}, None

            # Если получили audit — он главнее req_raw
            rp = request_parameters_from_audit or request_parameters_for_bronze
            rb = request_body_from_audit if request_body_from_audit is not None else request_body_for_bronze

            context.log.error(
                f"[wb_call] ERROR during {client_resource_key}.{fetch_method}: "
                f"{e!r}; params_keys={list(filtered.keys())}"
            )
            return {
                "status": status,
                "headers": headers,
                "response_dttm": now,
                "response_body": f"{method_name} {url} -> {msg}",
                "request_parameters": rp,
                "request_body": rb,
            }
        except Exception as e:
            context.log.error(
                f"[wb_call] ERROR during {client_resource_key}.{fetch_method}: {e!r}; params_keys={list(filtered.keys())}"
            )
            return {
                "status": 500,
                "headers": {},
                "response_dttm": now,
                "response_body": repr(e),
                "request_parameters": request_parameters_for_bronze,
                "request_body": request_body_for_bronze,
            }

        # --- Успешный путь: формируем тело и ПАРАМЕТРЫ ИЗ AUDIT ---
        if isinstance(resp, WBResponse):
            status = resp.status
            headers = resp.headers
            response_dttm = resp.response_dttm or now

            # тело
            if resp.body_bytes is not None:
                body_value = resp.body_bytes
            else:
                body_value = _maybe_parse_json(resp.body_text)

            # главное: берём «как ушло» из audit
            rp, rb = _extract_params_from_audit(resp.request)
        elif isinstance(resp, tuple) and len(resp) == 2:
            body_obj, hdrs = resp
            headers = dict(hdrs or {})
            status = int(headers.get("status", 200)) if headers else 200
            response_dttm = parse_http_date(headers.get("Date") or headers.get("date"), fallback=now)
            body_value = _maybe_parse_json(body_obj)
            rp, rb = request_parameters_for_bronze, request_body_for_bronze
        else:
            status = 200
            headers = {}
            response_dttm = now
            body_value = _maybe_parse_json(resp)
            rp, rb = request_parameters_for_bronze, request_body_for_bronze

        out: Dict[str, Any] = {
            "status": status,
            "headers": headers,
            "response_dttm": response_dttm,
            "response_body": body_value,   # dict/list/str or bytes
            "payload":        body_value,   # совместимость вверх по стеку
            "request_parameters": rp,
            "request_body": rb,
        }
        return out

    if hasattr(client, "__aenter__"):
        context.log.debug(f"[wb_call] entering async context for {client_resource_key}")
        async with client as c:
            return await _invoke(c)

    return await _invoke(client)
