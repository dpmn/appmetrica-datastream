import json
from pathlib import Path

from .exceptions import AppmetricaConfigError, AppmetricaApiError

from requests import Session


API_BASE_URL = 'https://api.appmetrica.yandex.ru/{resource_name}/v1/application/{application_id}'
REQUIRED_CONFIG_KEYS = (
    'access_token',
    'application_id',
)


class AppmetricaDatastream:
    def __init__(self, config_path: str | Path):
        self._config = self._read_config(config_path)
        self._http_session = Session()
        self._http_session.headers.update({
            'Authorization': f'OAuth {self._config["access_token"]}',
            'Accept-Encoding': 'gzip'
        })

    @staticmethod
    def _read_config(config_path) -> dict:
        with open(config_path, 'r', encoding='utf-8') as config_file:
            config = json.loads(config_file.read())

        for key in REQUIRED_CONFIG_KEYS:
            try:
                config[key]
            except KeyError:
                raise AppmetricaConfigError(key)

        return config

    def set_settings(self, datastream_settings_file: str | Path):
        url = '/'.join([
            API_BASE_URL.format(resource_name='management', application_id=self._config['application_id']),
            'datastream',
            'settings'
        ])

        with open(datastream_settings_file, 'r', encoding='utf-8') as file:
            datastream_settings = file.read()

        response = self._http_session.post(url=url, json=datastream_settings)

        if response.status_code == 200:
            return response.json()
        else:
            raise AppmetricaApiError('Request exception occurred', response.json())

    def get_settings(self):
        url = '/'.join([
            API_BASE_URL.format(resource_name='management', application_id=self._config['application_id']),
            'datastream',
            'settings'
        ])

        response = self._http_session.get(url)

        if response.status_code == 200:
            return response.json()
        else:
            raise AppmetricaApiError('Request exception occurred', response.json())

    def get_status(self):
        url = '/'.join([
            API_BASE_URL.format(resource_name='datastream', application_id=self._config['application_id']),
            'status'
        ])

        response = self._http_session.get(url)

        if response.status_code == 200:
            return response.json()
        else:
            raise AppmetricaApiError('Request exception occurred', response.json())

    def fetch_data(self, data_type: str, stream_window_ts: str):
        url = '/'.join([
            API_BASE_URL.format(resource_name='datastream', application_id=self._config['application_id']),
            'data'
        ])
        params = {
            'data_type': data_type,
            'stream_window_timestamp': stream_window_ts
        }

        response = self._http_session.get(url=url, params=params)

        if response.status_code == 200:
            return response.content
        else:
            raise AppmetricaApiError('Request exception occurred', response.json())
