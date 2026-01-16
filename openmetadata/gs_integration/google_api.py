import os.path

from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow


class BaseGoogleApi:
    """
    Базовый класс для работы с сервисами Google, такими как:
      - Google Drive;
      - Google sheets;
    etc.
    Все возможные скоупы для google API перечислены тут: https://developers.google.com/identity/protocols/oauth2/scopes?hl=ru
    """
    def __init__(self):
        self.token_path = 'token.json'
        self.SCOPES = [
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/spreadsheets'
        ]

    def get_credentials(self):
        """
        Получает токен авторизации из локального файла или инициирует процесс авторизации.

        Returns:
            google.oauth2.credentials.Credentials: Объект с авторизационными данными.
        """
        creds = self._load_token()
        if not creds or not creds.valid:  # токен не найден, либо не валидный.
            if creds and creds.expired and creds.refresh_token: # токен найден, но истёк срок жизни.
                creds = self._refresh_token(creds)
            else: # токен не найден, делаем новый.
                creds = self._authorize()
            self._save_token(creds)
        return creds

    def _load_token(self):
        """
        Загружает токен авторизации из локального файла.

        Returns:
            google.oauth2.credentials.Credentials | None: Объект с токеном или None, если файл отсутствует.
        """
        if os.path.exists(self.token_path):
            return Credentials.from_authorized_user_file(self.token_path, self.SCOPES)
        return None

    def _refresh_token(self, creds):
        """
        Обновляет истёкший токен.

        Args:
            creds (google.oauth2.credentials.Credentials): Объект с устаревшими данными.

        Returns:
            google.oauth2.credentials.Credentials: Обновлённый токен.
        """

        try:
            creds.refresh(Request())
            return creds
        except RefreshError as e:
            if 'invalid_scope' in str(e):  # Если ошибка о неправильном скоупе
                print("Ошибка invalid_scope. Удаляем старый токен и генерируем новый.")
                os.remove(self.token_path)  # Удаляем старый токен
                return self._authorize()  # Генерируем новый токен через процесс авторизации
            else:
                raise e

    def _save_token(self, creds):
        """
        Сохраняет токен авторизации в локальный файл.

        Args:
            creds (google.oauth2.credentials.Credentials): Объект с токеном.
        """
        with open(self.token_path, 'w') as token:
            token.write(creds.to_json())

    def _authorize(self):
        """
        Инициирует процесс авторизации через OAuth2.

        Returns:
            google.oauth2.credentials.Credentials: Новый токен авторизации.
        """
        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', self.SCOPES)
        return flow.run_local_server(port=0)


