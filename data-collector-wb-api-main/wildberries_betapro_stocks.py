import requests
import pandas as pd
from xml.etree import ElementTree as ET
from time import sleep
from datetime import datetime
from google_sheets_utils import add_data_to_sheet
from config_loader import load_config

CONFIG = load_config()

def fetch_betapro_data(spreadsheet_id: str):
    """
    Получает данные из API Betapro и добавляет их в Google Sheets.

    :param spreadsheet_id: Идентификатор Google Sheets.
    """
    def get_xml_data(partner_id: str, password: str):
        """
        Отправляет запрос в API Betapro и получает XML-данные.
        """
        url = "http://api.betapro.ru:8080/bp/hs/wsrv"
        xml_body = f'<request partner_id="{partner_id}" password="{password}" request_type="151"></request>'
        headers = {
            "Content-Type": "text/xml",
            "Content-Length": str(len(xml_body)),
            "Connection": "close"
        }

        response = requests.post(url, headers=headers, data=xml_body, timeout=60, verify=False)
        if response.status_code != 200:
            raise Exception(f"Ошибка API Betapro: {response.status_code}, {response.text}")
        return response.text

    def parse_xml_to_dataframe(xml_data: str, partner_id: str) -> pd.DataFrame:
        """
        Парсит XML-ответ API Betapro в DataFrame.
        """
        root = ET.fromstring(xml_data)
        ns = {'ns': 'FBox/XMLSchema'}
        data = []

        # Парсинг <good>
        for good in root.findall('ns:good', ns):
            data.append(good.attrib)

        # Парсинг <doc>
        for doc in root.findall('ns:doc', ns):
            doc_data = {
                "doc_type": doc.get("doc_type"),
                "doc_type_descrip": doc.get("doc_type_descrip"),
                "doc_date": doc.get("doc_date"),
                "doc_datetime": doc.get("doc_datetime"),
                "zdoc_id": doc.get("zdoc_id"),
                "partner_id": partner_id
            }
            data.append(doc_data)

        df = pd.DataFrame(data)

        # Если есть данные о запасах (qnt), добавляем расчет "Свободного остатка"
        if "qnt" in df.columns:
            qt_len = len([col for col in df.columns if col.startswith("qnt")])
            df["Свободный остаток"] = df.apply(
                lambda row: float(row["qnt"]) - sum([float(row.get(f"qnt{i}", 0)) for i in range(2, qt_len)]),
                axis=1
            )
        df["Дата Изменения"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return df

    # Список партнеров из config
    partners = CONFIG.get("betapro_partners", [])
    if not partners:
        raise Exception("Партнеры Betapro не указаны в конфигурации.")

    df_list = []
    for partner in partners:
        partner_id = partner["partner_id"]
        password = partner["password"]

        print(f"Загрузка данных для партнера {partner_id}...")
        xml_data = get_xml_data(partner_id, password)
        df = parse_xml_to_dataframe(xml_data, partner_id)
        df_list.append(df)
        sleep(3)  # Задержка между запросами

    # Объединение данных всех партнеров
    final_df = pd.concat(df_list, ignore_index=True)

    # Удаляем дубликаты
    final_df = final_df.drop_duplicates()

    # Загрузка данных в Google Sheets
    try:
        add_data_to_sheet(spreadsheet_id, final_df, "BetaproReport(ext)")
        print("Данные успешно добавлены на вкладку BetaproReport(ext).")
    except Exception as e:
        raise Exception(f"Ошибка при добавлении данных в Google Sheets: {e}")

if __name__ == "__main__":
    # Тестовая функция
    def test_fetch_betapro():
        try:
            fetch_betapro_data("1Gjx1UFXW0nZPUiDIrzNedBiJad1B__YQmTjH0uTSnws")
            print("Тест успешно завершен.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    # Запуск теста
    test_fetch_betapro()