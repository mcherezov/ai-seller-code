from __future__ import annotations

from typing import Any, Iterable, Mapping, Protocol, TypeVar, runtime_checkable

# Базовый тип "строки" после нормализации: dict-подобная мапа полей
Row = Mapping[str, Any]

# Ковариантный тип строки, чтобы конкретные пайплайны могли указывать свои TypedDict
R_co = TypeVar("R_co", bound=Row, covariant=True)


@runtime_checkable
class Normalizer(Protocol[R_co]):
    """
    Контракт нормализатора для пайплайнов.

    Любая реализация должна быть чистой функцией (без I/O и сайд-эффектов),
    принимать "raw" (ответ API/распарсенный JSON и т.п.) и возвращать итерируемую
    коллекцию строк (dict-подобных мап), совместимых со схемой соответствующей таблицы.

    Пример использования:
        from dagster_conf.lib.normalizer_base import Normalizer
        from .normalize import normalize_paid_storage  # -> Callable[[Any], Iterable[PaidStorageRow]]

        def use(n: Normalizer[PaidStorageRow], raw: Any) -> list[PaidStorageRow]:
            return list(n(raw))
    """

    def __call__(self, raw: Any, **kwargs) -> Iterable[R_co]: ...


__all__ = ["Normalizer", "Row"]
