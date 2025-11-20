from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
import re
import yaml
import uuid
from sqlalchemy import (
    Table,
    Column,
    Date,
    DateTime,
    Float,
    Numeric,
    Integer,
    SmallInteger,
    BigInteger,
    String,
    Text,
    Boolean,
    Index,
    UniqueConstraint,
    MetaData,
    ForeignKey,
    PrimaryKeyConstraint,
    text,
)
from sqlalchemy.sql import func
from sqlalchemy.orm import DeclarativeBase, declared_attr, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID, JSONB, BYTEA
from sqlalchemy.types import TIMESTAMP


# --------- БАЗА И МИКСИНЫ  ---------

class BronzeBase(DeclarativeBase):
    metadata = MetaData(schema="bronze_v2")


class SilverBase(DeclarativeBase):
    metadata = MetaData(schema="silver_v2")


# --------- КАРТА ТИПОВ ДЛЯ column_lineage ---------

_SQLA_TYPE_MAP = {
    "int": Integer,
    "integer": Integer,
    "float": Float,
    "double": Float,
    "str": String,
    "string": String,
    "text": Text,
    "bool": Boolean,
    "boolean": Boolean,
    "uuid": UUID,
    "date": Date,
    "datetime": DateTime(timezone=True),
    "timestamp": TIMESTAMP(timezone=False),
    "timestamptz": TIMESTAMP(timezone=True),
}


def _sa_type_from_name(type_name: str):
    t = (type_name or "").lower()
    if t not in _SQLA_TYPE_MAP:
        raise ValueError(f"Unknown SQLAlchemy type in config: {type_name}")
    return _SQLA_TYPE_MAP[t]


# --------- УТИЛИТЫ НАИМЕНОВАНИЙ ---------

def _camel(name: str) -> str:
    # wb_paid_storage_1d_new -> WbPaidStorage1DNew
    return "".join(part.capitalize() if part.isalpha() else part.upper()
                   for part in name.replace("-", "_").split("_"))

def _bronze_class_name(table_name: str) -> str:
    return f"Bronze{_camel(table_name)}"

def _silver_class_name(table_name: str) -> str:
    return f"Silver{_camel(table_name)}"


# --------- РАЗБОР КОНФИГА И ГЕНЕРАЦИЯ МОДЕЛЕЙ ---------

def _load_cfg(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _build_silver_columns(
    col_spec: List[dict],
) -> List[Column]:
    cols: List[Column] = []
    for item in col_spec:

        name = item["name"]
        sa_t = _sa_type_from_name(item["type"])
        nullable = bool(item.get("nullable", True))
        if sa_t is String:
            cols.append(Column(name, String(), nullable=nullable))
        elif sa_t is UUID:
            cols.append(Column(name, UUID(as_uuid=True), nullable=nullable))
        elif sa_t is DateTime(timezone=True):
            cols.append(Column(name, DateTime(timezone=True), nullable=nullable))
        else:
            cols.append(Column(name, sa_t, nullable=nullable))
    return cols


def _apply_indexes_and_pk(
    table_cls: type[DeclarativeBase],
    *,
    table_name: str,
    pk_cols: List[str] | None,
    indexes: List[dict] | None,
):
    if pk_cols:
        uc_name = f"ux_{table_name}_" + "_".join(pk_cols)
        UniqueConstraint(*pk_cols, name=uc_name, info={"generated": True})._set_parent(table_cls.__table__)

    for ix in (indexes or []):
        name = ix["name"]
        cols = ix["columns"]
        Index(name, *[table_cls.__table__.c[c] for c in cols])._set_parent(table_cls.__table__)


def build_models_from_config(
    *,
    config_path: Path,
    bronze_schema: str = "bronze_v2",
    silver_schema: str = "silver_v2",
) -> Tuple[Dict[str, Dict[str, Any]], MetaData, MetaData]:
    """
    Создаёт ORM‑классы (бронза/сильвер) для всех пайплайнов в config.yml.
    Возвращает: (registry, bronze_metadata, silver_metadata)
      registry = {pipe_name: {"bronze": BronzeModelClass, "silver": SilverModelClass}}
    """
    cfg = _load_cfg(config_path)

    registry: Dict[str, Dict[str, Any]] = {}
    pipelines: dict = cfg.get("pipelines", {}) or {}
    col_lineage: dict = cfg.get("column_lineage", {}) or {}

    # ---------- стандартизированные колонки миксинов ----------
    def _std_bronze_cols() -> List[Column]:
        return [
            Column("business_dttm", DateTime(timezone=True), nullable=False, doc="Бизнес‑дата/период"),
            Column("api_token_id", Integer, nullable=False, index=True, doc="Ссылка на токен из core.tokens"),
            Column("company_id", Integer, nullable=False, index=True, doc="ID компании"),
            Column("run_uuid", UUID(as_uuid=True), nullable=False, index=True, doc="Идентификатор DAG Run"),
            Column("run_dttm", DateTime(timezone=True), nullable=False, doc="Реальное время запуска/перезапуска (MSK)"),
            Column("run_schedule_dttm", DateTime(timezone=True), nullable=False, doc="Плановое время запуска (MSK)"),
            Column("request_uuid", UUID(as_uuid=True), primary_key=True, doc="Уникальный идентификатор запроса"),
            Column("request_dttm", DateTime(timezone=True), server_default=func.now(), nullable=False, doc="Время запроса (MSK)"),
            Column("request_parameters", JSONB, nullable=True, doc="Параметры GET"),
            Column("request_body", JSONB, nullable=True, doc="Тело запроса"),
            Column("response_code", Integer, nullable=False, server_default=text("0"), doc="HTTP‑код ответа (0 — если ответа не было)"),
            Column("response_dttm", DateTime(timezone=True), nullable=False, doc="timestamp в ответе API (TZ внешнего сервера)"),
            Column("receive_dttm", DateTime(timezone=True), nullable=False, doc="timestamp получения ответа (MSK)"),
            Column("response_body", Text, nullable=True, doc="Тело ответа API"),
            Column("inserted_at", DateTime(timezone=True), server_default=func.now(), nullable=False, doc="timestamp вставки (MSK)"),
        ]

    def _std_silver_cols() -> List[Column]:
        return [
            Column("business_dttm", DateTime(timezone=True), nullable=False),
            Column("run_uuid", UUID(as_uuid=True), nullable=False),
            Column("request_uuid", UUID(as_uuid=True), nullable=False),
            Column("company_id", Integer, nullable=False),
            Column("inserted_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
            Column("receive_dttm", DateTime(timezone=True), nullable=False),
        ]

    # ---------- маппинг типов для пользовательских колонок ----------
    TYPE_MAP = {
        "int": Integer,
        "integer": Integer,
        "int2": SmallInteger,  # PG smallint
        "int4": Integer,  # PG integer
        "int8": BigInteger,  # PG bigint
        "float": "float",
        "double": "float",
        "real": "float",  # PG real → Float
        "numeric": "numeric",  # numeric без точности
        "text": Text,
        "string": Text,  # по умолчанию text
        "varchar": "varchar",  # varchar без длины
        "json": JSONB,
        "jsonb": JSONB,
        "uuid": UUID(as_uuid=True),
        "timestamp_tz": DateTime(timezone=True),
        "timestamptz": DateTime(timezone=True),
        "datetime_tz": DateTime(timezone=True),
        "date": "date",
        "bool": "bool",
        "boolean": "bool",
    }

    _num_re = re.compile(r"^numeric\((\d+)\s*,\s*(\d+)\)$", re.I)
    _varchar_re = re.compile(r"^varchar\((\d+)\)$", re.I)

    def _resolve_type(t: str):
        t = (t or "").strip().lower()
        if not t:
            return Text

        if m := _num_re.match(t):
            p, s = int(m.group(1)), int(m.group(2))
            return Numeric(p, s)

        if m := _varchar_re.match(t):
            n = int(m.group(1))
            return String(n)

        v = TYPE_MAP.get(t)
        if v == "float":
            return Float
        if v == "numeric":
            return Numeric
        if v == "bool":
            return Boolean
        if v == "date":
            return Date
        if v == "varchar":
            return String
        return v or Text

    def _build_user_columns(spec_list: List[dict], *, skip_names: set[str] | None = None) -> List[Column]:
        """
        Строит пользовательские колонки из lineage.
        - Пропускает поля, которые входят в набор стандартных (skip_names)
        - Пропускает дубликаты внутри самого lineage (по имени)
        """
        skip_names = skip_names or set()
        seen: set[str] = set()
        cols: List[Column] = []

        for spec in spec_list:
            name = spec["name"]
            # пропускаем дубликаты стандартных и повторы в самом lineage
            if name in skip_names or name in seen:
                # опционально: можно логировать
                # print(f"[init_db_autogen] skip duplicate column: {name}")
                continue

            typ = _resolve_type(spec.get("type"))
            nullable = bool(spec.get("nullable", False))
            doc = spec.get("doc")
            cols.append(Column(name, typ, nullable=nullable, doc=doc))
            seen.add(name)

        return cols

    # ---------- основной цикл по пайпам ----------
    for pipe_name, pipe_cfg in pipelines.items():
        # ===== БРОНЗА =====
        bronze_table = pipe_cfg.get("bronze_table") or pipe_name
        bronze_model_name = _bronze_class_name(bronze_table)

        bronze_table_obj = Table(
            bronze_table,
            BronzeBase.metadata,
            *_std_bronze_cols(),
            schema=bronze_schema,
            *[
                Index(ix["name"], *[ixc for ixc in ix["columns"]])
                for ix in (pipe_cfg.get("bronze_indexes") or [])
            ],
        )

        BronzeModel = type(
            bronze_model_name,
            (BronzeBase, ),
            {
                "__table__": bronze_table_obj,
                "__tablename__": bronze_table,
            },
        )

        # ===== СИЛЬВЕР (fan-out) =====
        silver_section = pipe_cfg.get("silver") or {}
        silver_tables_cfg = (
                silver_section.get("targets")
                or pipe_cfg.get("silver_tables")
                or {}
        )

        silver_models_map: Dict[str, Any] = {}
        silver_pk_map: Dict[str, Tuple[str, ...]] = {}

        for silver_table, silver_tbl_cfg in silver_tables_cfg.items():
            silver_model_name = _silver_class_name(silver_table)

            # базовые служебные колонки silver
            base_cols: List[Column] = _std_silver_cols()
            std_silver_names: set[str] = {c.name for c in base_cols}

            # lineage: поддерживаем список ключей, поздние перекрывают ранние
            lineage_keys = silver_tbl_cfg.get("column_lineage", []) or []

            if not lineage_keys:
                # 1) прямые кандидаты
                for candidate in (pipe_name, silver_table):
                    if candidate in col_lineage:
                        lineage_keys = [candidate]
                        break

            # 2) fan-out эвристика: "{pipe_base}__{silver_base}"
            if not lineage_keys:
                def _base(s: str) -> str:
                    s = s.removeprefix("wb_")
                    for suf in ("_1h", "_1d"):
                        if s.endswith(suf):
                            s = s[: -len(suf)]
                    return s

                pipe_base = _base(pipe_name)
                silver_base = silver_table.removeprefix("wb_")
                candidate = f"{pipe_base}__{silver_base}"

                if candidate in col_lineage:
                    lineage_keys = [candidate]

            if not lineage_keys:
                raise RuntimeError(f"{pipe_name}: не найден column_lineage для {silver_table}")

            # сливаем спецификации по имени
            merged_by_name: Dict[str, dict] = {}
            for k in lineage_keys:
                for spec in (col_lineage.get(k) or []):
                    merged_by_name[spec["name"]] = spec  # overwrite
            merged_specs: List[dict] = list(merged_by_name.values())

            # генерим реальные SQLA-колонки, пропуская стандартные и дубли
            user_column_objs = _build_user_columns(merged_specs, skip_names=std_silver_names)

            pk_cols: List[str] = list(silver_tbl_cfg.get("primary_key") or [])
            table_args: List[Any] = []
            for ix in (silver_tbl_cfg.get("silver_indexes") or []):
                table_args.append(Index(ix["name"], *[c for c in ix["columns"]]))

            if pk_cols:
                table_args.append(PrimaryKeyConstraint(*pk_cols, name=f"pk_{silver_table}"))
            else:
                base_cols = [Column("id", BigInteger, primary_key=True, autoincrement=True)] + base_cols

            has_req_uuid_idx = any(
                "request_uuid" in (ix.get("columns") or [])
                for ix in (silver_tbl_cfg.get("silver_indexes") or [])
            )
            if not has_req_uuid_idx:
                table_args.append(Index(f"ix_{silver_table}__request_uuid", "request_uuid"))

            silver_table_obj = Table(
                silver_table,
                SilverBase.metadata,
                *(base_cols + user_column_objs),
                *table_args,
                schema=silver_schema,
            )

            all_cols = set(silver_table_obj.c.keys())
            missing_pk = [c for c in pk_cols if c not in all_cols]
            if missing_pk:
                raise RuntimeError(
                    f"{pipe_name}.{silver_table}: PK columns not found in schema: {missing_pk}. "
                    f"Check column_lineage merge and types."
                )

            SilverModel = type(
                silver_model_name,
                (SilverBase,),
                {
                    "__table__": silver_table_obj,
                    "__tablename__": silver_table,
                },
            )

            silver_models_map[silver_table] = SilverModel
            silver_pk_map[silver_table] = tuple(pk_cols)

        # Итоговая запись в реестр — уже в унифицированном формате
        registry[pipe_name] = {
            "bronze_model": BronzeModel,
            "silver_models": silver_models_map,
            "silver_pk": silver_pk_map,
        }

    return registry, BronzeBase.metadata, SilverBase.metadata



# --------- ХОЗЯЙСТВЕННЫЕ ФУНКЦИИ ---------

def create_all(
    engine,
    *,
    config_path: Path,
    bronze_schema: str = "bronze_v2",
    silver_schema: str = "silver_v2",
) -> Dict[str, Dict[str, Any]]:
    """
    Быстрая инициализация БД под тесты:
      - генерируем модели из конфига,
      - делаем create_all() для обеих схем.
    Возвращает registry (его можно затем скормить автосборщику пайпов).
    """
    registry, bronze_md, silver_md = build_models_from_config(
        config_path=config_path,
        bronze_schema=bronze_schema,
        silver_schema=silver_schema,
    )

    with engine.begin() as conn:
        conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{bronze_md.schema}"')
        conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{silver_md.schema}"')

    bronze_md.create_all(engine)
    silver_md.create_all(engine)
    return registry
