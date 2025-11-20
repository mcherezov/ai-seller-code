# src/connectors/wb/resources.py
from dagster import resource, Field
from typing import Optional
from src.connectors.wb.wb_api_v2 import WildberriesAsyncClient  # v2 клиент

@resource(
    config_schema={
        "token": Field(str, description="WB API token для текущей партиции"),
        "token_id": Field(int, description="ID токена в core.tokens"),
        "rps": Field(int, default_value=5, description="Requests per second"),
        "timeout": Field(int, default_value=120, description="HTTP timeout, sec"),
        "retries": Field(int, default_value=3, description="Retries"),
        "backoff": Field(float, default_value=0.5, description="Exponential backoff base, sec"),
    }
)
def wildberries_client_v2(init_context) -> WildberriesAsyncClient:
    cfg = init_context.resource_config
    return WildberriesAsyncClient(
        token=cfg["token"],
        token_id=cfg["token_id"],
        rps=cfg.get("rps", 5),
        timeout=cfg.get("timeout", 120),
        retries=cfg.get("retries", 3),
        backoff=cfg.get("backoff", 0.5),
    )
