import os
from typing import Dict, List
import uuid
import requests
import pandas as pd
from datetime import datetime
import pytz
from xml.etree import ElementTree as ET
from time import sleep
from config_loader import load_config
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Загрузка конфигурации
CONFIG = load_config()

# Google Sheets утилиты

def connect_to_google_sheets():
    creds_cfg = CONFIG.get("google_sheets", {})
    json_file = creds_cfg.get("credentials_file")
    if json_file:
        if not os.path.isabs(json_file):
            base = os.path.dirname(__file__)
            alt = os.path.join(base, json_file)
            if os.path.isfile(alt):
                json_file = alt
        if os.path.isfile(json_file):
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                json_file,
                ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            )
            return gspread.authorize(creds)
    # Попытка загрузить из dict в конфиге
    json_dict = creds_cfg.get("credentials_json")
    if json_dict:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json_dict,
            ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        )
        return gspread.authorize(creds)
    raise FileNotFoundError(f"Не найден файл учетных данных Google Sheets: {creds_cfg.get('credentials_file')}")


def clear_range(spreadsheet_id: str, sheet_name: str, range_to_clear: str):
    client = connect_to_google_sheets()
    ws = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    ws.batch_clear([range_to_clear])


def add_data_to_sheet_without_headers_with_format(
    spreadsheet_id: str,
    data: pd.DataFrame,
    sheet_name: str
):
    client = connect_to_google_sheets()
    ws = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    df = data.copy()
    # Преобразуем даты в строки и попытку конвертаций для чисел
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%d.%m.%Y')
        else:
            try:
                df[col] = df[col].astype(str).str.replace(',', '.').astype(float)
            except Exception:
                pass
    df = df.fillna("")
    existing = ws.col_values(1)
    start_row = len(existing) + 1
    ws.update(f'A{start_row}', df.values.tolist(), value_input_option='USER_ENTERED')


def fetch_betapro_data(betapro_partners: List[Dict[str, str]]) -> pd.DataFrame:
    """
    Получает данные из API Betapro и сразу выгружает результаты в Google Sheets.
    """
    def get_xml_data(pid: str, pwd: str) -> str:
        url = "http://api.betapro.ru:8080/bp/hs/wsrv"
        xml = f'<request partner_id="{pid}" password="{pwd}" request_type="151"/>'
        headers = {"Content-Type": "text/xml"}
        resp = requests.post(url, headers=headers, data=xml, verify=False)
        if resp.status_code != 200:
            raise Exception(f"API Betapro ошибка {resp.status_code}")
        return resp.text

    def parse_xml(xml_data: str, pid: str) -> pd.DataFrame:
        root = ET.fromstring(xml_data)
        ns = {'ns': 'FBox/XMLSchema'}
        rows = []
        for good in root.findall('ns:good', ns):
            attrib = good.attrib.copy()
            attrib['partner_id'] = pid
            rows.append(attrib)
        for doc in root.findall('ns:doc', ns):
            rows.append({
                'partner_id': pid,
                'doc_date': doc.get('doc_date'),
                'zdoc_id': doc.get('zdoc_id')
            })
        df = pd.DataFrame(rows)
        df['uuid'] = [str(uuid.uuid4()) for _ in range(len(df))]
        # Корректный расчет свободного остатка
        qnt_cols = [c for c in df.columns if c.startswith('qnt')]
        if qnt_cols:
            # Индексы для всех qnt: 'qnt' -> 1, 'qnt2' -> 2 и т.д.
            qnt_indices = []
            for c in qnt_cols:
                if c == 'qnt':
                    qnt_indices.append(1)
                else:
                    suffix = c[len('qnt'):]
                    if suffix.isdigit():
                        qnt_indices.append(int(suffix))
            max_q = max(qnt_indices)
            def free_qty(row):
                base = float(row.get('qnt', 0))
                others = sum(float(row.get(f'qnt{i}', 0)) for i in qnt_indices if i != 1)
                return base - others
            df['Свободный остаток'] = df.apply(free_qty, axis=1)
        return df

    if not betapro_partners:
        raise ValueError("Не указаны партнеры Betapro.")

    frames = []
    for p in betapro_partners:
        xml_text = get_xml_data(p['partner_id'], p['password'])
        df = parse_xml(xml_text, p['partner_id'])
        frames.append(df)
        sleep(3)
    final_df = pd.concat(frames, ignore_index=True).drop_duplicates()

    msk_now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')

    # Выгрузка: "Остатки Беты"
    sheet2_id = "1e3KkBxPEAMg1cfNMZVm8BLDMGzkHYIQVk9Sa9fffcLg"
    sheet2_name = 'Остатки Беты'
    df2 = pd.DataFrame({
        'Номер кабинета': final_df['partner_id'],
        'EAN': final_df.get('good_id', ''),
        'qual_type': final_df.get('qual_type', ''),
        'резерв': final_df.get('qnt', 0)
    })
    for i in range(2, 9):
        df2[f'резерв {i}'] = final_df.get(f'qnt{i}', 0)
    df2['Свободный остаток'] = final_df.get('Свободный остаток', 0)
    df2['Номер кабинета.1'] = final_df['partner_id']
    df2['Дата Изменения'] = msk_now
    order = [
        'Номер кабинета', '__empty1__', '__empty2__', 'EAN', 'qual_type',
        'резерв', 'резерв 2', 'резерв 3', 'резерв 4', 'резерв 5', 'резерв 6', 'резерв 7', 'резерв 8',
        'Свободный остаток', 'Номер кабинета.1', 'Дата Изменения'
    ]
    for col in order:
        if col not in df2.columns:
            df2[col] = ''
    df2 = df2[order]
    clear_range(sheet2_id, sheet2_name, 'A2:P')
    add_data_to_sheet_without_headers_with_format(sheet2_id, df2, sheet2_name)

    return final_df


if __name__ == '__main__':
    partners = CONFIG.get('betapro_partners', [])
    records = fetch_betapro_data(partners)
    print(f"Загружено записей: {len(records)}")
