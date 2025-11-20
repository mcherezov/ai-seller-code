import os
from dagster import resource, String, Field
from src.connectors.ozon.ozon_api import OzonAsyncClient


@resource(
    config_schema={
        "client_id": String,
        "api_key": String,
        "client_secret": Field(String, is_required=False),
    }
)
def ozon_client(init_context) -> OzonAsyncClient:
    """
    Ресурс для OzonAsyncClient.
    Параметры передаются через run_config:
      resources.ozon_client.config.client_id
      resources.ozon_client.config.api_key
      (опционально) resources.ozon_client.config.client_secret
    """
    cfg = init_context.resource_config
    return OzonAsyncClient(
        client_id=cfg["client_id"],
        api_key=cfg["api_key"],
        client_secret=cfg.get("client_secret"),
    )
