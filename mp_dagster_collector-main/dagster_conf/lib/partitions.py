from __future__ import annotations

from datetime import date, time, datetime
from typing import Any, Iterable, List, Tuple, Dict

from dagster import (
    DailyPartitionsDefinition,
    WeeklyPartitionsDefinition,
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
    TimeWindowPartitionsDefinition,
)
import sqlalchemy as sa
from sqlalchemy import text as sql_text

from dagster_conf.lib.timeutils import MSK

# ---------------------------------------------------------------------------
# Конфиг‑драйв партиций: business_dttm × company_id
# ---------------------------------------------------------------------------

def build_multipartitions(cfg: Dict[str, Any]) -> MultiPartitionsDefinition:
    """
    Создаёт MultiPartitionsDefinition на основе cfg[pipeline]['partitions'].
    """
    tz = cfg.get("timezone", "Europe/Moscow")
    schedule = cfg["schedule"]
    part_cfg = (cfg.get("partitions") or {})
    biz_cfg = (part_cfg.get("business_dttm") or {})

    start_raw = biz_cfg.get("start")
    if not start_raw:
        raise RuntimeError("config.partitions.business_dttm.start is required")

    # @TODO: Зачем вообще нужна эта функция? А если нужна, то надо ее вынести в utils
    # как и было — нормализуем start в 'YYYY-MM-DD'
    from datetime import datetime, date, timedelta
    def _normalize_start(x) -> str:
        if isinstance(x, date):
            return x.strftime("%Y-%m-%d")
        if isinstance(x, str):
            s = x.strip()
            try:
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"           # @TODO: похоже на костыль который может сломаться
                dt = datetime.fromisoformat(s)
                return dt.date().strftime("%Y-%m-%d")
            except Exception:
                return s[:10]
        return str(x)[:10]

    start_norm = _normalize_start(start_raw)

    kind = str(biz_cfg.get("kind") or "daily").lower()

    # @TODO: Частоту партиций стоит получать прямо из "schedule". Тогда можно избавиться от "kind" в конфиге.
    if kind == "weekly":
        d_mon = datetime.fromisoformat(start_norm)
        business = WeeklyPartitionsDefinition(
            start_date=d_mon.date().strftime("%Y-%m-%d"),
            timezone=tz,
            day_offset=1
        )
    elif kind == "intradaily":
        business = TimeWindowPartitionsDefinition(
            cron_schedule=schedule,
            start=start_norm,
            timezone=tz,
            fmt="%Y-%m-%d"
        )
    else:
        business = DailyPartitionsDefinition(
            start_date=start_norm, 
            timezone=tz
        )

    companies = DynamicPartitionsDefinition(name="company_id")
    return MultiPartitionsDefinition({
        "business_dttm": business,
        "company_id": companies,
    })

# ---------------------------------------------------------------------------
# Синхронизация динамической партиции company_id из БД (с дефолтами)
# ---------------------------------------------------------------------------

async def refresh_company_partitions(instance, postgres_resource, cfg: Dict[str, Any]) -> List[str]:
    """
    Подтягивает список company_id из БД и публикует в DynamicPartitionsDefinition("company_id").

    Источники параметров (приоритет сверху вниз):
      1) pipelines.<name>.partitions.company_id.query / allowlist
      2) defaults.partitions.company_id.query / allowlist  (из корня config.yml)
      3) встроенный дефолт SQL:
         SELECT DISTINCT company_id FROM core.tokens WHERE is_active = TRUE
    """
    # Конфиг пайплайна и корневой конфиг
    pipe_part = (cfg.get("partitions") or {}).get("company_id") or {}
    root_cfg = (cfg.get("__root_cfg__") or {})
    root_defaults = (root_cfg.get("defaults") or {}).get("partitions") or {}
    root_company = (root_defaults.get("company_id") or {})

    # SQL-запрос (pipeline → root defaults → built-in default)
    query = (
        pipe_part.get("query")
        or root_company.get("query")
        or "SELECT DISTINCT company_id FROM core.tokens WHERE is_active = TRUE"
    )

    # allowlist (pipeline override → root defaults → пусто)
    allow = set(pipe_part.get("allowlist") or root_company.get("allowlist") or [])

    values: List[str] = []
    async with postgres_resource() as session:  # ожидается async Session
        res = await session.execute(sql_text(query))
        for row in res.fetchall():
            try:
                cid = int(row[0])
            except Exception:
                continue
            if allow and cid not in allow:
                continue
            values.append(str(cid))

    if values:
        instance.add_dynamic_partitions("company_id", values)
    return values


# ---------------------------------------------------------------------------
# Парсинг ключа партиции (совместим со строками и MultiPartitionKey)
# ---------------------------------------------------------------------------

def parse_partition_key(pk: Any) -> Tuple[datetime, int]:
    """
    Возвращает: (business_dttm@MSK start-of-day, company_id)
    Понимает MultiPartitionKey, строку 'YYYY-MM-DD|123', JSON-строку, ISO-датавремя.
    """
    # 1) Достаём сырье
    try:
        from dagster import MultiPartitionKey
        if isinstance(pk, MultiPartitionKey):
            biz_raw = pk.keys_by_dimension["business_dttm"]
            comp_raw = pk.keys_by_dimension["company_id"]
        else:
            raise Exception()
    except Exception:
        import json
        s = str(pk or "")
        if s.startswith("{"):
            d = json.loads(s)
            biz_raw = d["business_dttm"]
            comp_raw = d["company_id"]
        else:
            parts = s.split("|", 1)
            if len(parts) != 2:
                raise ValueError(f"Bad partition key format: {s!r}")
            biz_raw, comp_raw = parts[0], parts[1]

    # 2) Разбираем дату/датавремя с максимальной терпимостью
    from datetime import datetime
    from zoneinfo import ZoneInfo
    MSK_TZ = ZoneInfo("Europe/Moscow")

    def _parse_biz(raw: Any) -> datetime:
        if isinstance(raw, datetime):
            dt = raw
        else:
            s = str(raw).strip()
            # поддержка '...Z'
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            # сначала пробуем ISO (с или без времени/таймзоны)
            try:
                dt = datetime.fromisoformat(s)
            except Exception:
                # затем 'YYYY-MM-DD'
                try:
                    dt = datetime.strptime(s[:10], "%Y-%m-%d")
                except Exception as e:
                    raise ValueError(f"Cannot parse business_dttm from {raw!r}") from e

        # приводим к МСК и ставим полночь
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MSK_TZ)
        else:
            dt = dt.astimezone(MSK_TZ)
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    biz = _parse_biz(biz_raw)
    return biz, int(str(comp_raw).strip())


def parse_context_partition(context) -> Tuple[datetime, int]:
    """Достаём (business_dttm, company_id) из контекста ассета/опа."""
    return parse_partition_key(getattr(context, "partition_key", None))
