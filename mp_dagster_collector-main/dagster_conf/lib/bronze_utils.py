from __future__ import annotations

from typing import List, Optional


def pick_best_bronze_row(rows: List[dict]) -> Optional[dict]:
    """
    Выбираем «лучшую» запись попыток:
      - приоритет 200-ответам
      - затем по receive_dttm (последняя — правее)
    """
    if not rows:
        return None
    return sorted(
        rows,
        key=lambda r: ((r.get("response_code") == 200), r.get("receive_dttm")),
        reverse=True,
    )[0]
