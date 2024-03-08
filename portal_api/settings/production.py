from .base import *
import environ

env = environ.Env()

DEBUG = False

ALLOWED_HOSTS += ['10.70.{}.{}'.format(i, j)
                  for i in range(256) for j in range(256)]

# ALLOWED_HOSTS += ['172.16.144.{}'.format(i)
#                   for i in range(256)]
# ALLOWED_HOSTS += ['172.16.6.{}'.format(i)
#                   for i in range(256)]

ALLOWED_HOSTS += ["172.16.6.%s" % s for s in range(2, 255)]
ALLOWED_HOSTS += ["172.16.7.%s" % s for s in range(2, 255)]
ALLOWED_HOSTS += ['172.16.{}.{}'.format(i, j)
                  for i in range(144, 159) for j in range(256)]


ALLOWED_HOSTS += [
    "localhost", "test.sentinelportal.com", "https://gcp-catv-consumer-service.api.sentinelprotocol.io","https://catv-consumer-service.api.sentinelprotocol.io"
]

# TODO: version file or tag?
version = env.str('PORTAL_API_VERSION', None)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {process:d} {thread:d} {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
        'logstash': {
            'level': 'WARNING',
            'class': 'logstash.TCPLogstashHandler',
            'host': env.str('API_LOGSTASH_SERVER', 'localhost'),
            'port': 5959,
            'version': 1,
            'message_type': 'logstash',
            'fqdn': True,
            'tags': ['django.request', 'django-portalapi'],
        },
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'propagate': True,
        },
        'django.request': {
            'handlers': ['logstash'],
            'level': 'WARNING',
            'propagate': False
        },
    }
}
