import os
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

from dagster_conf.lib.auto_model_builder import build_models_from_config


def _env(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    if val is None:
        print(f"[init_db_autogen] ERROR: env {name} is not set and no default provided", file=sys.stderr)
        sys.exit(1)
    return val


def main() -> None:
    database_url = _env("DATABASE_URL")
    config_path = _env("AUTOGEN_CONFIG_PATH", "dagster_conf/lib/config.yml")
    bronze_schema = _env("BRONZE_SCHEMA", "bronze_v2")
    silver_schema = _env("SILVER_SCHEMA", "silver_v2")

    cfg_path = Path(config_path)
    if not cfg_path.exists():
        print(f"[init_db_autogen] ERROR: config not found: {cfg_path}", file=sys.stderr)
        sys.exit(1)

    print(f"[init_db_autogen] Using config: {cfg_path}")
    print(f"[init_db_autogen] Schemas: bronze={bronze_schema}, silver={silver_schema}")

    registry, bronze_meta, silver_meta = build_models_from_config(
        config_path=cfg_path,
        bronze_schema=bronze_schema,
        silver_schema=silver_schema,
    )
    print(f"[init_db_autogen] Models generated for pipelines: {', '.join(registry.keys()) or '(none)'}")

    engine = create_engine(database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for schema in {bronze_schema, silver_schema}:
            print(f"[init_db_autogen] Ensuring schema exists: {schema}")
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

        # создаём таблицы (если их нет)
        print("[init_db_autogen] Creating bronze tables if not exist…")
        bronze_meta.create_all(bind=conn)

        print("[init_db_autogen] Creating silver tables if not exist…")
        silver_meta.create_all(bind=conn)

        print("[init_db_autogen] Ensuring default indexes on silver.request_uuid …")

        def _ensure_silver_request_uuid_indexes(conn, silver_meta, silver_schema: str):
            for tbl in silver_meta.tables.values():
                if getattr(tbl, "schema", None) != silver_schema:
                    continue
                table_name = tbl.name

                exists = conn.execute(text("""
                    SELECT 1
                    FROM pg_class t
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    JOIN pg_index i ON i.indrelid = t.oid
                    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
                    WHERE n.nspname = :schema
                      AND t.relname  = :table
                      AND a.attname  = 'request_uuid'
                    LIMIT 1
                """), {"schema": silver_schema, "table": table_name}).scalar() is not None

                if not exists:
                    idx_name = f'ix_{table_name}__request_uuid'
                    conn.execute(text(
                        f'CREATE INDEX IF NOT EXISTS "{idx_name}" '
                        f'ON "{silver_schema}"."{table_name}" ("request_uuid")'
                    ))
                    print(f"[init_db_autogen]   + created {idx_name} on {silver_schema}.{table_name}")
                else:
                    print(
                        f"[init_db_autogen]   = index on request_uuid already exists for {silver_schema}.{table_name}")

        _ensure_silver_request_uuid_indexes(conn, silver_meta, silver_schema)

    engine.dispose()
    print("[init_db_autogen] Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[init_db_autogen] FAILED: {e}", file=sys.stderr)
        sys.exit(2)
