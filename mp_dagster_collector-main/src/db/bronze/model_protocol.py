from typing import Protocol, Any, Mapping


class TableModelProtocol(Protocol):
    """
    Protocol для SQLAlchemy-модели, у которой обязательно есть:
      - __tablename__: str
      - __table_args__: Mapping[str, Any]  (обычно {'schema': 'bronze'})
    """
    __tablename__: str
    __table_args__: Mapping[str, Any]
