from django.conf import settings

class APISettings:  # TODO: config validation check
    def __init__(self):
        self.Settings = getattr(settings, 'API_SETTINGS', {})

    def __getattr__(self, item):
        try:
            return self.Settings[item]
        except KeyError:
            return None

api_settings = APISettings()
