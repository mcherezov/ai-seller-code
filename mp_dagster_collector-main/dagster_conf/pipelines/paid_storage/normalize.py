from __future__ import annotations
from datetime import date, datetime
from typing import Any, Iterable, Optional, TypedDict, cast


# Строка после нормализации (имена полей — как в silver-модели)
class PaidStorageRow(TypedDict, total=False):
    date: Optional[date]
    warehouse_id: Optional[int]
    warehouse_name: Optional[str]
    warehouse_coef: Optional[float]
    income_id: Optional[int]
    chrt_id: Optional[int]
    tech_size: Optional[str]
    barcode: Optional[str]
    supplier_article: Optional[str]
    nm_id: Optional[int]
    volume: Optional[float]
    calc_type: Optional[str]
    warehouse_price: Optional[float]
    barcodes_count: Optional[int]
    pallet_place_code: Optional[int]
    pallet_count: Optional[float]
    loyalty_discount: Optional[float]


def _to_int(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        # самый частый формат от WB — YYYY-MM-DD
        s = v.strip()
        try:
            return date.fromisoformat(s[:10])
        except Exception:
            return None
    return None


def normalize_paid_storage(raw: Any) -> Iterable[PaidStorageRow]:
    data = raw if isinstance(raw, list) else (raw.get("data") or [])
    if not isinstance(data, list):
        data = []

    for rec in data:
        row: PaidStorageRow = {
            "date": _to_date(rec.get("date")),
            "warehouse_id": _to_int(rec.get("warehouseId") or rec.get("officeId")),
            "warehouse_name": cast(Optional[str], rec.get("warehouseName") or rec.get("warehouse")),
            "warehouse_coef": _to_float(rec.get("warehouseCoef") or rec.get("logWarehouseCoef")),
            "income_id": _to_int(rec.get("incomeId") or rec.get("giId")),
            "chrt_id": _to_int(rec.get("chrtId")),
            "tech_size": cast(Optional[str], rec.get("techSize") or rec.get("size")),
            "barcode": cast(Optional[str], rec.get("barcode")),
            "supplier_article": cast(Optional[str], rec.get("supplierArticle") or rec.get("vendorCode")),
            "nm_id": _to_int(rec.get("nmId")),
            "volume": _to_float(rec.get("volume")),
            "calc_type": cast(Optional[str], rec.get("calcType")),
            "warehouse_price": _to_float(rec.get("warehousePrice")),
            "barcodes_count": _to_int(rec.get("barcodesCount")),
            "pallet_place_code": _to_int(rec.get("palletPlaceCode")),
            "pallet_count": _to_float(rec.get("palletCount")),
            "loyalty_discount": _to_float(rec.get("loyaltyDiscount")),
        }
        yield row

__all__ = ["PaidStorageRow", "normalize_paid_storage"]