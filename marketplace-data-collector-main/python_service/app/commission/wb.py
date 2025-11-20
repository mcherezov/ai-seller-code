import pandas as pd
from datetime import date
from api_clients.wb import WildberriesAPI

def fetch_commission_data(
    config: dict,
    entities_meta: dict[str, dict]
) -> pd.DataFrame:
    """
    Получить общий отчет по комиссиям Wildberries для всех юрлиц в виде одного DataFrame
    """
    sellers = list(config["wb_marketplace_keys"].keys())
    dfs = []

    for seller_legal in sellers:
        wb = WildberriesAPI(seller_legal)
        data = wb.get_commission()

        report_data = data.get("report", [])
        if not report_data:
            print(f"⚠️ Нет данных для {seller_legal}, пропускаем.")
            continue

        df = pd.json_normalize(report_data)

        # → приводим к float + округляем
        float_cols = [
            "kgvpMarketplace", "kgvpSupplier", "kgvpSupplierExpress",
            "paidStorageKgvp", "kgvpBooking", "kgvpPickup"
        ]
        for col in float_cols:
            if col in df.columns:
                df[col] = df[col].astype(float).round(4)

        # остальные типы
        df["parentID"] = df["parentID"].astype(int)
        df["subjectID"] = df["subjectID"].astype(int)
        df["parentName"] = df["parentName"].astype(str)
        df["subjectName"] = df["subjectName"].astype(str)

        # → переименование в snake_case
        df.rename(columns={
            "kgvpMarketplace":       "kgvp_marketplace",
            "kgvpSupplier":          "kgvp_supplier",
            "kgvpSupplierExpress":   "kgvp_supplier_express",
            "paidStorageKgvp":       "paid_storage_kgvp",
            "kgvpBooking":           "kgvp_booking",
            "kgvpPickup":            "kgvp_pickup",
            "parentID":              "parent_id",
            "parentName":            "parent_name",
            "subjectID":             "subject_id",
            "subjectName":           "subject_name"
        }, inplace=True)

        # дата и мета
        df["date"] = date.today().strftime("%Y-%m-%d")
        meta = entities_meta.get(seller_legal)
        if meta:
            df["legal_entity"] = meta["display_name"]
        else:
            df["legal_entity"] = seller_legal

        dfs.append(df)

    if not dfs:
        raise Exception("Нет данных ни для одного продавца")

    return pd.concat(dfs, ignore_index=True)
