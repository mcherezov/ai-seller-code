import os
from dagster import resource, String, Int
from src.connectors.wb.wb_api import WildberriesAsyncClient

@resource(config_schema={"token": String, "token_id": Int})
def wildberries_client(init_context) -> WildberriesAsyncClient:
    """
    Ресурс для WildberriesAsyncClient.
    Токен и token_id передаются через run_config:
    resources.wildberries_client.config.token
    resources.wildberries_client.config.token_id
    """
    token = init_context.resource_config["token"]
    token_id = init_context.resource_config["token_id"]
    return WildberriesAsyncClient(token=token, token_id=token_id)
