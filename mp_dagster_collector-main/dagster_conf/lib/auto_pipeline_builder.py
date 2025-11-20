from __future__ import annotations
import re, json, datetime as dt
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple, Optional
from zoneinfo import ZoneInfo
import yaml
from dagster import (
    AssetsDefinition,
    AssetSelection,
    ScheduleDefinition,
    SensorDefinition,
    RunRequest,
    SkipReason,
    define_asset_job,
    with_resources,
    build_schedule_from_partitioned_job,
)
from dagster_conf.lib.asset_factories.factory_utils import resolve_build_params
DEFAULT_TZ = "Europe/Moscow"


# =========================
#   Импорт фабрик (унифицированный интерфейс build_assets)
# =========================

try:
    from dagster_conf.lib.asset_factories.sync_api_assets_factory import build_assets as build_sync_api_assets
except Exception:
    from dagster_conf.lib.asset_factories.sync_api_assets_factory import (
        build_assets as build_sync_api_assets,
    )

try:
    from dagster_conf.lib.asset_factories.task_asset_factory import build_assets as build_task_assets
except Exception:
    from dagster_conf.lib.asset_factories.task_asset_factory import (
        build_assets as build_task_assets,
    )

try:
    from dagster_conf.lib.asset_factories.selenium_factory import build_assets as build_selenium_assets
except Exception:
    from dagster_conf.lib.asset_factories.selenium_factory import (
        build_assets as build_selenium_assets,
    )

class UnknownFactoryError(RuntimeError):
    """Неизвестный тип фабрики (factory_type) в config.yml."""


FactoryFn = Callable[[str, dict, dict, dict], List[AssetsDefinition]]

FACTORY_DISPATCH: Dict[str, FactoryFn] = {
    "sync_api": build_sync_api_assets,
    "task_poll_download": build_task_assets,
    "selenium": build_selenium_assets,
}


# =========================
#   Вспомогательные функции
# =========================

def _load_cfg(config_path: Path) -> dict:
    """Читает YAML-конфиг. Бросает FileNotFoundError, если файл отсутствует."""
    if not config_path.exists():
        raise FileNotFoundError(f"config.yml не найден: {config_path}")
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _factory_for(pipe_name: str, pipe_cfg: dict) -> FactoryFn:
    """Возвращает подходящую фабрику по полю factory_type (по умолчанию task_poll_download)."""
    ftype = (pipe_cfg or {}).get("factory_type", "task_poll_download")
    factory = FACTORY_DISPATCH.get(ftype)
    if not factory:
        raise UnknownFactoryError(f"{pipe_name}: неизвестный factory_type={ftype!r}")
    return factory

def _resolve_normalizer(spec: str):
    """
    Поддерживает dotted path вида:
      - package.module:callable
      - package.module:Class.method  (для WbUniversalNormalizer метод norm__*)
    Возвращает вызываемый объект (callable).
    """
    fn = resolve_build_params(spec)
    if not callable(fn):
        raise ValueError(f"Normalizer '{spec}' must be callable, got {type(fn)}")
    return fn
def _model_bundle_for(pipe_name: str, model_registry: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Достаёт model_bundle из реестра. Бросает понятную ошибку, если нет записи."""
    bundle = model_registry.get(pipe_name)
    if not bundle:
        raise RuntimeError(
            f"{pipe_name}: отсутствует model_bundle в model_registry. "
            f"Ожидается словарь с ключами: bronze_model, silver_models, silver_pk, normalizers (и др.)."
        )
    return bundle


def _resources_for(resources: Dict[str, Any]) -> Dict[str, Any]:
    """Простая проверка ресурсов: нужен хотя бы postgres (как минимум обе фабрики на него опираются)."""
    if "postgres" not in resources:
        # Не валим сразу программу — отдаём подсказку; фабрики сами бросят ошибку, если ресурс критичен
        pass
    return resources


def _build_job_for(pipe_name: str, assets: List[AssetsDefinition], job_tags: Optional[Dict[str, str]] = None):
    """Создаёт AssetJob, выделяя только ассеты данного пайплайна.
    Если все ассеты разделяют один и тот же partitions_def — протягиваем его в job,
    чтобы расписание запускало партиционированные ран(ы).
    """
    selection = AssetSelection.keys(*[a.key for a in assets])

    shared_pd = None
    if assets:
        first_pd = getattr(assets[0], "partitions_def", None)
        if all(getattr(a, "partitions_def", None) == first_pd for a in assets):
            shared_pd = first_pd

    return define_asset_job(
        name=f"{pipe_name}_job",
        selection=selection,
        partitions_def=shared_pd,
        tags=job_tags or {},
    )

_DOW_ALIASES = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]  # 0..6 == Sun..Sat

def _sanitize_name(s: str) -> str:
    """Оставляем только [A-Za-z0-9_], остальное -> '_'."""
    return re.sub(r"[^A-Za-z0-9_]+", "_", s)

def _schedule_name(pipe_name: str, kw: Dict) -> str:
    base = f"{pipe_name}_schedule_{_combo_suffix(kw)}"
    return _sanitize_name(base)

def _is_hourly_pattern(cron: str) -> bool:
    """
    True для «часовых» кронов, где поле часа — любой:
      - '*/N * * * *'
      - 'M * * * *'
      - 'M1,M2 * * * *'
    False для дневных/недельных: 'M H * * *', 'M H * * DOW', и т.п.
    """
    cron = (cron or "").strip()
    parts = re.split(r"\s+", cron)
    if len(parts) != 5:
        return False
    _m, h, _dom, _mon, _dow = parts
    return h in ("*", "*/1")

def _parse_cron_for_partitioned(cron: str) -> List[Dict]:
    """
    Разбирает cron для partitioned-job и возвращает список kwargs:
      [{'minute_of_hour': 0, 'hour_of_day': 4, 'day_of_week': 1}, ...]
    Поддерживаемые случаи:
      1) 'M H * * *'                      -> одна комбинация
      2) 'M H * * DOW'                    -> одна комбинация
      3) '*/N * * * *'                    -> N-комбинаций минут (каждый час)
      4) 'M * * * *'                      -> одна комбинация (каждый час в :M)
      5) 'M1,M2 * * * *'                  -> несколько минут (каждый час)
      6) 'M H * * DOW1,DOW2'              -> несколько DOW
    Не поддерживаем сложные выражения в часах (типа '*/2' в поле часа).
    """
    cron = cron.strip()
    fields = re.split(r"\s+", cron)
    if len(fields) != 5:
        return []

    m_str, h_str, _dom, _mon, dow_str = fields

    def parse_minutes(s: str) -> List[int]:
        s = s.strip()
        if s.startswith("*/"):
            step = int(s[2:])
            if step <= 0 or step > 60:
                raise ValueError(f"Invalid minute step: {s}")
            return list(range(0, 60, step))
        result: List[int] = []
        for part in s.split(","):
            part = part.strip()
            if part == "*":
                # '*' в минутах для partitioned не используем (частота = каждую минуту)
                raise ValueError("Unsupported '*' in minute field for partitioned job")
            result.append(int(part) % 60)
        return result

    def parse_hour(s: str) -> Optional[int]:
        s = s.strip()
        if s in ("*", "*/1"):
            return None  # каждый час
        if s.startswith("*/"):
            # пока не поддерживаем "каждые N часов"
            raise ValueError("Unsupported '*/N' in hour field for partitioned job")
        return int(s) % 24

    def parse_dows(s: str) -> List[Optional[int]]:
        s = s.strip()
        if s == "*":
            return [None]  # без ограничения по дню недели
        out: List[Optional[int]] = []
        for part in s.split(","):
            p = part.strip().lower()
            if p.isdigit():
                v = int(p)
                if v == 7:  # cron-совместимость: 7 == Sunday
                    v = 0
                out.append(v % 7)  # 0..6 (0=Sun)
            else:
                try:
                    out.append(_DOW_ALIASES.index(p))  # 'sun'..'sat' -> 0..6
                except ValueError:
                    raise ValueError(f"Unsupported day_of_week token: {part}")
        return out

    minutes = parse_minutes(m_str)
    hour = parse_hour(h_str)            # None = каждый час
    dows = parse_dows(dow_str)          # [None] или список int (0..6)

    combos: List[Dict] = []
    for m in minutes:
        for d in dows:
            kw: Dict = {"minute_of_hour": int(m)}
            if hour is not None:
                kw["hour_of_day"] = int(hour)
            if d is not None:
                kw["day_of_week"] = int(d)  # ВАЖНО: int, не 'mon'
            combos.append(kw)
    return combos

def _combo_suffix(kw: Dict) -> str:
    """Суффикс для имени (без недопустимых символов)."""
    m = kw.get("minute_of_hour")
    h = kw.get("hour_of_day")
    d = kw.get("day_of_week")  # int 0..6 или None
    mpart = f"m{int(m):02d}"
    hpart = f"h{int(h):02d}" if h is not None else "hxx"
    dpart = f"d{_DOW_ALIASES[int(d)]}" if d is not None else "dany"
    return "_".join([mpart, hpart, dpart])

def _build_sensor_for(pipe_name: str, job, pipe_cfg: dict):
    """
    Сенсор для «часовых» кронов (минутные/шаговые), тикает раз в минуту и
    запускает ран, когда совпала минута и день недели.
    """
    cron: str = pipe_cfg["schedule"]
    tz = pipe_cfg.get("timezone") or DEFAULT_TZ
    schedule_tags = (pipe_cfg.get("schedule_tags") or {})

    # разложим cron и оставим только варианты "каждый час"
    combos = [kw for kw in _parse_cron_for_partitioned(cron) if kw.get("hour_of_day") is None]
    if not combos:
        return None

    name = _sanitize_name(f"{pipe_name}_sensor")

    def _eval(context):
        cursor = json.loads(context.cursor) if context.cursor else {}
        now = dt.datetime.now(ZoneInfo(tz))
        py_dow = now.weekday()  # Mon=0..Sun=6

        emitted = False
        for kw in combos:
            minute = int(kw["minute_of_hour"])
            d = kw.get("day_of_week")  # 0..6 (Sun=0) или None
            # сопоставление: cron Sun=0 -> python Mon=0
            if d is not None and ((d - 1) % 7) != py_dow:
                continue
            if now.minute != minute:
                continue

            combo_key = f"m{minute:02d}_d{(d if d is not None else 'any')}"
            minute_key = now.strftime("%Y%m%d%H%M")
            if cursor.get(combo_key) == minute_key:
                continue  # дедуп в рамках минуты

            run_key = f"{pipe_name}:{minute_key}:{combo_key}"

            if getattr(job, "partitions_def", None) is not None:
                ts = now.timestamp()
                pk = job.partitions_def.get_partition_key_for_timestamp(ts)
                yield RunRequest(run_key=run_key, partition_key=pk, tags=schedule_tags)
            else:
                yield RunRequest(run_key=run_key, tags=schedule_tags)

            cursor[combo_key] = minute_key
            emitted = True

        if not emitted:
            yield SkipReason("no matching minute/DOW for this tick")

        context.update_cursor(json.dumps(cursor))

    return SensorDefinition(
        name=name,
        evaluation_fn=_eval,
        minimum_interval_seconds=60,
        description=f"Auto sensor for hourly pattern: {cron} ({tz})",
    )

def _build_schedule_for(pipe_name: str, job, pipe_cfg: dict):
    """
    Возвращает:
      - SensorDefinition для «часовых» кронов (*/N, M,*),
      - либо один/несколько ScheduleDefinition для конкретного часа/DOW,
      - либо один ScheduleDefinition для непартиционированных джобов.
    ВАЖНО: для partitioned-schedule НЕ передавать cron_schedule/execution_timezone,
    только minute_of_hour/hour_of_day/day_of_week (int).
    """
    cron: Optional[str] = pipe_cfg.get("schedule")
    if not cron:
        return None

    # 1) Часовые паттерны -> сенсор
    if _is_hourly_pattern(cron):
        return _build_sensor_for(pipe_name, job, pipe_cfg)

    # 2) Нечасовые
    tz = pipe_cfg.get("timezone") or DEFAULT_TZ
    schedule_tags = (pipe_cfg.get("schedule_tags") or {})

    # 2a) Partitioned job: строим расписание(я) через разложенные kwargs
    if getattr(job, "partitions_def", None) is not None:
        combos = _parse_cron_for_partitioned(cron)
        if not combos:
            raise ValueError(
                f"[{pipe_name}] Unsupported cron for partitioned job: '{cron}'. "
                "Supported: 'M H * * *', 'M H * * DOW', '*/N * * * *', 'M * * * *', "
                "'M1,M2 * * * *', 'M H * * DOW1,DOW2'."
            )
        schedules = []
        for kw in combos:
            schedules.append(
                build_schedule_from_partitioned_job(
                    job=job,
                    name=_schedule_name(pipe_name, kw),
                    tags=schedule_tags,
                    **kw  # minute_of_hour/hour_of_day/day_of_week (int!); БЕЗ cron/TZ
                )
            )
        return schedules

    # 2b) Non-partitioned: обычный cron + TZ
    return ScheduleDefinition(
        name=_sanitize_name(f"{pipe_name}_schedule"),
        job=job,
        cron_schedule=cron,
        execution_timezone=tz,
        tags=schedule_tags,
    )

# =========================
#   Публичный билдер
# =========================

def build_all_from_config(
    *,
    config_path: Path,
    resources: Dict[str, Any],
    model_registry: Dict[str, Dict[str, Any]],
) -> Tuple[List[AssetsDefinition], List[Any], List[ScheduleDefinition]]:
    """
    Строит ВСЕ пайплайны из config.yml:

      • ассеты (bronze + N silver) на каждый pipe через соответствующую фабрику;
      • job (AssetJob) на каждый pipe;
      • schedule (ScheduleDefinition) на каждый pipe, если указан cron.

    Возвращает тройку списков: (assets, jobs, schedules).

    Ожидаемый формат model_registry[pipe_name] — унифицированный под обе фабрики:
      {
        "bronze_model": <SQLA модель>,
        "silver_models": { "<silver_name>": <SQLA модель>, ... },
        "silver_pk": { "<silver_name>": (<pk_col1>, ...), ... },
        "normalizers": { "<silver_name>": <Normalizer>, ... },
        # опционально: кастомные колбэки — см. описания фабрик
      }
    """
    cfg = _load_cfg(config_path)
    pipelines = cfg.get("pipelines", {}) or {}

    assets_all: List[AssetsDefinition] = []
    jobs_all: List[Any] = []
    schedules_all: List[ScheduleDefinition] = []

    if not pipelines:
        return assets_all, jobs_all, schedules_all

    for pipe_name, pipe_cfg in pipelines.items():
        factory = _factory_for(pipe_name, pipe_cfg)
        model_bundle = _model_bundle_for(pipe_name, model_registry)
        res = _resources_for(resources)

        # 1) ассеты
        pipe_cfg_with_root = dict(pipe_cfg)
        pipe_cfg_with_root["__root_cfg__"] = cfg

        # --- ПОДТЯГИВАЕМ НОРМАЛИЗАТОРЫ ИЗ КОНФИГА ---
        cfg_targets = ((pipe_cfg.get("silver") or {}).get("targets") or {})
        cfg_norm_map = {}
        for s_name, t_cfg in cfg_targets.items():
            spec = (t_cfg or {}).get("normalizer")
            if spec:
                fn = _resolve_normalizer(spec)
                if fn:
                    cfg_norm_map[s_name] = fn

        # сливаем в model_bundle (что пришло из model_registry — не ломаем, а дополняем/переопределяем)
        model_bundle = dict(model_bundle)  # копия
        mb_norm = dict(model_bundle.get("normalizers") or {})
        mb_norm.update(cfg_norm_map)
        model_bundle["normalizers"] = mb_norm

        assets = factory(pipe_name, pipe_cfg_with_root, model_bundle, res)
        if not assets:
            # Защита от пустой сборки
            raise RuntimeError(f"{pipe_name}: фабрика не вернула ни одного ассета")
        assets_bound = with_resources(assets, resource_defs=res)
        assets_all.extend(assets_bound)

        # 2) job для ассетов пайплайна
        job_tags: Dict[str, str] = pipe_cfg.get("job_tags") or {}
        job = _build_job_for(pipe_name, assets_bound, job_tags=job_tags)
        jobs_all.append(job)

        # 3) расписание (если задано)
        sch = _build_schedule_for(pipe_name, job, pipe_cfg)
        if sch:
            if isinstance(sch, list):
                schedules_all.extend(sch)
            else:
                schedules_all.append(sch)

    return assets_all, jobs_all, schedules_all
