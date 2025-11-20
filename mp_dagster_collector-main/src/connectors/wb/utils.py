from typing import Union, Tuple, Dict, List
from aiohttp import ClientResponseError
import asyncio


async def retry_download(session, url: str, max_retries: int = 5, base_delay: float = 1.0) -> Tuple[bytes, Dict[str, str]]:
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, timeout=120) as resp:
                if resp.status == 429:
                    raise ClientResponseError(
                        resp.request_info, resp.history,
                        status=429, message='Too Many Requests',
                        headers=resp.headers
                    )
                resp.raise_for_status()
                return await resp.read(), dict(resp.headers)

        except ClientResponseError as e:
            if e.status == 429:
                print(f"[retry_download] 429 Too Many Requests, попытка {attempt}/{max_retries}")
                await asyncio.sleep(base_delay * (2 ** (attempt - 1)))
            else:
                raise

    raise RuntimeError(f"[retry_download] Превышено число попыток при 429 Too Many Requests: {url}")
