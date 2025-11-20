import requests
import pandas as pd
import xml.etree.ElementTree as ET
from time import sleep
from d_loger import tg_logger
from datetime import datetime
from art_utils import get_right_art, prepare_art
from make_google import make_google_table

betapro_tokens = [
    {
        'password': 'LKi4y97rIGRYgbFF',
        'partner_id': '1040',
    },
    {
        'password': 'LKo8796TIRJGkuyFF',
        'partner_id': '1134',
    },
    {
        'password': 'LKuy86543q4tFF',
        'partner_id': '1160',
    },
    {
        'password': 'LKou8783irgUTHFF',
        'partner_id': '1159',
    },
]

def get_xml_data(partner_id, password):
    tg_logger.info('Load xml data')
        
    # Константы
    CFG_REQUEST_HTTP = 'http://'
    CFG_REQUEST_HOST = 'api.betapro.ru'
    CFG_REQUEST_PORT = 8080
    CFG_REQUEST_URL = '/bp/hs/wsrv'
    CFG_REQUEST_FULLURL = f"{CFG_REQUEST_HTTP}{CFG_REQUEST_HOST}:{CFG_REQUEST_PORT}{CFG_REQUEST_URL}"
    CFG_REQUEST_TIMEOUT = 60
    CFG_CONTENT_TYPE = 'text/xml'
    CFG_TEST_XMLBODY = f'<request partner_id="{partner_id}" password="{password}" request_type="151"></request>'
    # Заголовки
    headers = {
        'Content-Type': CFG_CONTENT_TYPE,
        'Content-Length': str(len(CFG_TEST_XMLBODY)),
        'Connection': 'close'
    }

    response = requests.post(
        CFG_REQUEST_FULLURL,
        headers=headers,
        data=CFG_TEST_XMLBODY,
        timeout=CFG_REQUEST_TIMEOUT,
        verify=False  
    )
    print('response')
    print(response)
    print('response')
    # Вывод ответа
    return response.text

def read_xml(partner_id, password):
    tg_logger.info(f'Reading XML for partner_id: {partner_id}')
    xml_data = get_xml_data(partner_id, password)
    print('xml_data')
    print(xml_data)
    print('xml_data')
    root = ET.fromstring(xml_data)

    ns = {'ns': 'FBox/XMLSchema'}

    data = []
    for good in root.findall('ns:good', ns):
        row = good.attrib
        data.append(row)

    for doc in root.findall('ns:doc', ns):
        doc_data = {
            'doc_type': doc.get('doc_type'),
            'doc_type_descrip': doc.get('doc_type_descrip'),
            'doc_date': doc.get('doc_date'),
            'doc_datetime': doc.get('doc_datetime'),
            'zdoc_id': doc.get('zdoc_id')
            }
        data.append(doc_data)

    df = pd.DataFrame(data)
     
    qt_len = len(df.columns) - 1

    df['Свободный остаток'] = df.apply( lambda S: float(S['qnt']) - sum([float(S[f'qnt{i}']) for i in range(2, qt_len)]), axis=1)
    df['Номер кабинета'] = partner_id
    df['Дата Изменения'] = str(datetime.today())
    return df

def get_name(x, reprt, name, partner_id):
    reprt = reprt.copy()
    if partner_id:
        reprt = reprt[reprt.partner_id == int(partner_id)]
    else:
        x = get_right_art(x)
        reprt = reprt.set_index('Артикул продавца')
        reprt = reprt.rename(columns={'Штрихкод': "EAN"})
    x = str(x)
    if x in reprt.index:
        res = reprt.loc[x, name]
        if isinstance(res, pd.Series):
            res = res.tolist()[0]
        if name == 'EAN':
            if res==res:
                if ',' in res:
                    res = res.split(',')[0]
            else:
                if  x.isnumeric():
                    res = x
                elif res==res and ',' in res:
                    res = res.split(',')[0]
                else:
                    res = float('nan')
        return res
    if name == 'EAN':
        if  x.isnumeric():
            res = x
        elif x==x and ',' in x:
            res = x.split(',')[0]
        else:
            res = float('nan')

        return res
    return float('nan')

def get_d_name(x, reprt, name, partner_id, cross):
    y = get_name(x, reprt, name, partner_id)
    if y == y:
        return y
    y = get_name(x, cross, name, 0)
    return y
 
def add_mata(df, reprt, cross):
    tg_logger.info('Adding metadata to DataFrame')

    reprt = reprt.set_index('Артикул') 
    rename_columns = {
        'good_id': 'Артикул',
    }
    df = df.rename(columns=rename_columns)
    for name in ['EAN', 'Наименование']:
        name_line = df.apply(lambda S: get_d_name(S.Артикул, reprt, name, S['Номер кабинета'], cross), axis=1)
        df.insert(1, name, name_line)
    return df

def make_total_ostatok(raw_report, cross):
    df_list = []
    for token in betapro_tokens:
        try:
            print(token)
            tg_logger.info(f'Processing token for partner_id: {token["partner_id"]}')
            xml_df = read_xml(**token)
            print('xml_df')
            print(xml_df)
            print('xml_df')
            raise Exception('')
            df = add_mata(xml_df, raw_report, cross)
            df_list.append(df)
            sleep(10)
        except Exception as e:
            tg_logger.info(f'Error processing token for partner_id: {token["partner_id"]}')
            tg_logger.error(f'{e}')
    DF = pd.concat(df_list)
    DF.Артикул = DF.Артикул.apply(prepare_art)
    DF.columns = [name.replace('qnt', 'резерв ') for name in DF.columns ]

    tg_logger.info('Concatenated all DataFrames')
    return DF

def reb_df(a):
    if isinstance(a, str):
        if a.isnumeric():
            a = float(a)
    return a

def main():
    tg_logger.info('Starting main function')
    RELOAD_API = True
    try:
        if RELOAD_API:
            raw_report = pd.read_csv('data/input/total_report.csv')
            cross = pd.read_csv('data/input/cross.csv')

            DF = make_total_ostatok(raw_report, cross)
            if DF is not None:
                tg_logger.info('Finished processing, resulting DataFrame:')
                DF.to_csv('data/output/DF.csv', index=False)
            else:
                tg_logger.error('Failed to create the final DataFrame')
        else:
            DF = pd.read_csv('data/output/DF.csv')

        DF['Дата Изменения'] = pd.to_datetime(DF['Дата Изменения']).dt.strftime('%Y-%m-%d %H:%M:%S')
        DF.to_csv('data/output/stok_11_25.csv', index=False)
        DF.to_clipboard('data/output/stok_11_25.csv', index=False)
        make_google_table(DF, 'ОстаткиБета')
        tg_logger.warning('Бета про обновлено')
        
    except Exception as e:
        tg_logger.error(f'An error occurred in the main function: {e}')


if __name__ == '__main__':
    main()
