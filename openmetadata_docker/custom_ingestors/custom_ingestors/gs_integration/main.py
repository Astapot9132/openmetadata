import datetime
from typing import Any
from zoneinfo import ZoneInfo

import requests

from .gs_collector import GoogleSheetsCollector



class ClickhouseGSInfo:
    
    MAX_LAST_UPDATE_DAYS = 0
    
    
    def __init__(self, file_id: str):
        self._collector = GoogleSheetsCollector(file_id)

        self.tables = self.get_all_tables()
        self.tables_data = {}

    
    @property
    def metadata_need_update(self) -> bool:
        last_update_datetime = self._collector.get_last_modified_time() or datetime.datetime.now(ZoneInfo('Europe/Moscow'))
        if (datetime.datetime.now(ZoneInfo('Europe/Moscow')) - last_update_datetime).days > self.MAX_LAST_UPDATE_DAYS:
            return False
        return True
    
    def get_all_tables(self) -> list[str]:
        try:
            list_with_tables = self._collector.get_data_from_original_source(list_name='Список таблиц')
            if list_with_tables.empty:
                print('Лист с таблицами пуст. Завершаем работу')
                return []
            if 'Таблица' not in list_with_tables.columns:
                print('На листе с названиями таблиц отсутствует колонка "Таблица"')
                return []
            tables = list_with_tables['Таблица']
            return list(tables)
        except KeyError:
            print('Лист "Список таблиц" отсутствует в ресурсе. Завершаем работу.')
            return []

        

    @staticmethod
    def update_info_about_table_in_metadata(table: dict[str, Any]):
        
        url = f"http://localhost:8585/api/v1/tables/{table['id']}"
        headers = {
            "Authorization": "Bearer ваш_токен",
            "Content-Type": "application/json"
        }
    
        response = requests.put(url, headers=headers, json=table)
        response.raise_for_status()
    
        return response.json()

    
    def get_info_about_tables_in_gs(self):
        for table_name in self.tables:
            try:
                table_df = self._collector.get_data_from_original_source(table_name)
            except KeyError:
                print(f'Отсутствует лист "{table_name}", хотя он был в списке таблиц, продолжаем обработку')
                continue
            if table_df.empty:
                continue
                
            self.tables_data[table_name] = {row[table_name]: row['Описание'] for _, row in table_df.iterrows()}
        self.tables_data = {k: v for k, v in self.tables_data.items() if v}
        return self.tables_data

def get_all_info_for_ch_tables() -> ClickhouseGSInfo:
    gs_info = ClickhouseGSInfo('1W_WLS0chOvaxl_Cejt8pgF6cnUm8-4QkUdlYWPn1KGA')
    # 
    # gs_info.get_info_about_table_in_gs()
    # pprint(gs_info.tables_data)
    
    # 
    # get_info_about_table_in_metadata(
    #     service_name=CLICKHOUSE_SERVICE_NAME,
    #     db_name=CLICKHOUSE_DB_NAME,
    #     schema_name=CLICKHOUSE_DB_NAME,
    #     table_name='dim_users'
    # )

    return gs_info


if __name__ == '__main__':
    get_all_info_for_ch_tables()
