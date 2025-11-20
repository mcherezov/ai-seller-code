# dagster_conf/resources/http_client.py

import aiohttp
from aiohttp import ClientSession
from dagster import resource

@resource
def http_client(_context) -> ClientSession:
    """
    Асинхронный HTTP-клиент для aiohttp.
    """
    return aiohttp.ClientSession()
