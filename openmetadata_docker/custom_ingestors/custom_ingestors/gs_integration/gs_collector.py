import datetime
from typing import Optional

import pandas as pd
from googleapiclient.discovery import build

from .google_api import BaseGoogleApi


class GoogleSheetsCollector(BaseGoogleApi):
    def __init__(self, google_sheet_id):
        """
        Args:
            google_sheet_id (str): ID таблицы google sheet. Например 1y_gqMZ6ZKyunhmnusbJ9pdUFHvim-aJmUhGqGD4AF7Y
        """
        self.google_sheet_id = google_sheet_id
        super().__init__()
        self.service = self.authenticate()
        self._existing_sheets = None
        self._spreadsheet_info = None

    def authenticate(self):
        """
        Аутентифицируется в Google API и создаёт сервис для взаимодействия с Google Sheets API.

        Returns: Сервис для работы с Google Sheets API.
        """
        creds = self.get_credentials()
        return build('sheets', 'v4',  credentials=creds)

    def get_spreadsheet_info(self) -> dict:
        """
        Получает полную информацию о таблице.
        """
        if self._spreadsheet_info is None:
            self._spreadsheet_info = self.service.spreadsheets().get(
                spreadsheetId=self.google_sheet_id,
                fields="properties,sheets.properties,revisionId"
            ).execute()
        return self._spreadsheet_info

    def get_last_modified_time(self) -> Optional[datetime.datetime]:
        """
        Возвращает время последнего изменения файла.
        """
        try:
            # Получаем информацию о драйве (через Google Drive API)
            drive_service = build('drive', 'v3', credentials=self.get_credentials())
            file_info = drive_service.files().get(
                fileId=self.google_sheet_id,
                fields="modifiedTime"
            ).execute()

            modified_time_str = file_info.get('modifiedTime')
            if modified_time_str:
                return datetime.datetime.fromisoformat(modified_time_str.replace('Z', '+00:00'))
        except Exception as e:
            print(f"Ошибка при получении времени изменения: {e}")

        return None

    def get_revision_history(self) -> list[dict]:
        """
        Получает историю ревизий файла через Google Drive API.
        """
        try:
            drive_service = build('drive', 'v3', credentials=self.get_credentials())
            revisions = drive_service.revisions().list(
                fileId=self.google_sheet_id,
                fields="revisions(id, modifiedTime, lastModifyingUser)"
            ).execute()

            return revisions.get('revisions', [])
        except Exception as e:
            print(f"Ошибка при получении истории ревизий: {e}")
            return []

    @property
    def existing_sheets(self):
        """
        Возвращает список существующих листов.
        Если значение ещё не загружено, вызывает метод для загрузки.
        """
        if self._existing_sheets is None:
            self._existing_sheets = self.get_sheets_names_from_table()
        return self._existing_sheets

    @existing_sheets.setter
    def existing_sheets(self, value):
        """
        Устанавливает значение для списка существующих листов.
        """
        if not isinstance(value, list):
            raise ValueError("existing_sheets должно быть списком.")
        self._existing_sheets = value

    def get_data_from_original_source(self, list_name: str, *args, **kwargs) -> pd.DataFrame:
        """
        Функция идёт в переданный лист текущей таблицы, возвращает Pandas data frame с данными.
        """
        if not list_name:
            raise ValueError("Missing required argument list_name.")

        # Проверяем наличие переданного листа в таблице
        if list_name not in self.existing_sheets:
            raise KeyError(f'Лист {list_name} не найден в таблице {self.google_sheet_id}')

        # Получаем данные из переданного листа
        data_from_given_list = self.service.spreadsheets().values().get(spreadsheetId=self.google_sheet_id, range=list_name).execute()
        values = data_from_given_list.get('values', [])
        if not values:
            return pd.DataFrame()  # Пустой DataFrame, если нет значений

        # Определяем максимальное количество колонок
        max_columns = max(len(row) for row in values)

        # Добавляем недостающие заголовки
        header = values[0]
        if len(header) < max_columns:
            header += [f"Column_{i}" for i in range(len(header), max_columns)]

        # Приводим все строки к одинаковой длине, добавляя пустые значения
        normalized_values = [row + [''] * (max_columns - len(row)) for row in values]

        return pd.DataFrame(normalized_values[1:], columns=header)

    def get_sheets_names_from_table(self):
        """
        Функция возвращает список названий всех листов таблицы.
        """
        table = self.service.spreadsheets().get(spreadsheetId=self.google_sheet_id).execute()
        sheets_names = [sheet['properties']['title'] for sheet in table['sheets']]
        return sheets_names
