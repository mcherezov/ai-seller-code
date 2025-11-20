from __future__ import annotations

from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")


def scheduled_time_msk(context, default_msk: datetime) -> datetime:
    """
    Плановое время запуска:
      - если есть тег dagster/scheduled_execution_time — парсим его (ISO)
      - иначе возвращаем default_msk (обычно business_dttm при бэкфилле)
    """
    tag = getattr(context, "dagster_run", None)
    tag = getattr(tag, "tags", {}) if tag else {}
    iso = tag.get("dagster/scheduled_execution_time")
    if not iso:
        return default_msk

    s = iso.strip()
    # 'Z' → '+00:00' для fromisoformat
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MSK)
        return dt.astimezone(MSK)
    except Exception:
        return default_msk


def parse_http_date(value: str | None, fallback: datetime) -> datetime:
    """Разбираем HTTP Date (RFC 7231). Без TZ считаем MSK."""
    if not value:
        return fallback
    try:
        dt = parsedate_to_datetime(value)
        return dt if dt.tzinfo else dt.replace(tzinfo=MSK)
    except Exception:
        return fallback


def yesterday_msk_date_iso(ctx_time_utc: datetime | None) -> str:
    """Берём «вчера» относительно расписания в МСК (или текущего времени)."""
    base_utc = ctx_time_utc or datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
    base_msk = base_utc.astimezone(MSK)
    y_msk = (base_msk - timedelta(days=1)).date()
    return y_msk.isoformat()
