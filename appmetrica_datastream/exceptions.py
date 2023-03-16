class AppmetricaClientError(Exception):
    pass


class AppmetricaApiError(Exception):
    pass


class AppmetricaConfigError(AppmetricaClientError):
    pass
