from .base import *
import raven
import environ

env = environ.Env()

DEBUG = False

ALLOWED_HOSTS += env.list('ALLOWED_HOSTS', default=['*', ])


# Sentry
# TODO: version file or tag?
version = env.str('PORTAL_API_VERSION', None)

RAVEN_CONFIG = {
    'dsn': env.str('API_SENTRY_DSN', None),
    'environment': env.str('SENTRY_ENVIRONMENT', 'Staging')
}

if version:
    RAVEN_CONFIG['release'] = version
