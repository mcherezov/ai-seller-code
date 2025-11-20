from wildberries_funnel import fetch_sales_funnel
from wildberries_orders import fetch_orders
from google_sheets_utils import add_data_to_sheet
import pandas as pd

def funnel_and_orders_pipeline(date_from: str, date_to: str, seller_legal: str, spreadsheet_id: str):
    """
    Выполняет логику:
    1) Получение воронки (fetch_sales_funnel) с записью в GSheet (SalesFunnel(ext)).
    2) Получение заказов (fetch_orders) без записи.
    3) Преобразование полученных заказов на основе данных воронки.
    4) Запись финального DataFrame в GSheet (OrdersReport(ext)).
    """

    # Воронка с записью
    funnel_df = fetch_sales_funnel(
        date_from=date_from,
        date_to=date_to,
        seller_legal=seller_legal,
        spreadsheet_id=spreadsheet_id,
        write_to_sheet=True
    )

    # Заказы без записи
    orders_df = fetch_orders(
        date_from=date_from,
        date_to=date_to,
        seller_legal=seller_legal,
        spreadsheet_id=spreadsheet_id,
        write_to_sheet=False
    )

    if funnel_df.empty:
        print("⚠ funnel_df пустой, дальнейшие операции бессмысленны.")
    if orders_df.empty:
        print("⚠ orders_df пустой, возможны пропуски в расчётах.")

    # --- Подготовка данных из воронки ---
    df_result = funnel_df[["vendorCode", "statistics_selectedPeriod_ordersCount"]].copy()
    df_result.rename(columns={"statistics_selectedPeriod_ordersCount": "ordersCount"}, inplace=True)
    df_result = df_result.groupby("vendorCode", as_index=False)["ordersCount"].sum()

    # --- Группировка заказов по артикулу ---
    if orders_df.empty or "supplierArticle" not in orders_df.columns:
        print("⚠ orders_df пустой или не содержит колонку 'supplierArticle'. Пропускаем группировку.")
        counts_df = pd.DataFrame(columns=["supplierArticle", "orders_funnel_count"])
    else:
        counts_df = (
            orders_df
            .groupby("supplierArticle")
            .size()
            .reset_index(name="orders_funnel_count")
        )

    # --- Объединение двух источников ---
    df_result = df_result.merge(
        counts_df,
        left_on="vendorCode",
        right_on="supplierArticle",
        how="left"
    )

    # Удаление вспомогательного поля и заполнение NaN
    if "supplierArticle" in df_result.columns:
        df_result.drop(columns=["supplierArticle"], inplace=True)
    df_result["orders_funnel_count"] = df_result["orders_funnel_count"].fillna(0)

    # --- Расчёт разницы ---
    df_result["difference"] = df_result["orders_funnel_count"] - df_result["ordersCount"]

    # --- Удаление отменённых заказов, если difference > 0 ---
    if not orders_df.empty and "supplierArticle" in orders_df.columns and "isCancel" in orders_df.columns:
        for row in df_result.itertuples(index=False):
            vendor = row.vendorCode
            diff_value = int(row.difference)

            if diff_value > 0:
                mask = (orders_df["supplierArticle"] == vendor) & (orders_df["isCancel"] == True)
                matching_indexes = orders_df[mask].index
                indexes_to_drop = matching_indexes[:diff_value]
                orders_df.drop(index=indexes_to_drop, inplace=True)

    orders_df.reset_index(drop=True, inplace=True)

    # --- Финальная запись ---
    if not orders_df.empty:
        add_data_to_sheet(spreadsheet_id, orders_df, "OrdersReport(ext)")
        print("✅ Заказы: Финальный DataFrame записан в Google Sheets в OrdersReport(ext)")
    else:
        print("⚠ Финальный DataFrame пуст, запись в Google Sheets пропущена.")

# Пример запуска
if __name__ == "__main__":
    funnel_and_orders_pipeline(
        date_from="20.01.2025",
        date_to="20.01.2025",
        seller_legal="inter",
        spreadsheet_id="1oOVF7PvIezny-RnzT_0_g8ml7YQ_r4hTtS7XKdE_19I"
    )
