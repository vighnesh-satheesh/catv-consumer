from django.utils.translation import gettext_lazy as _

from rest_framework import status
from rest_framework import exceptions


#####
# 400
#####
class ValidationError(exceptions.APIException):
    status_code = status.HTTP_400_BAD_REQUEST
    default_detail = _('Invalid input.')
    default_code = 'invalid'


#####
# 401
#####
class AuthenticationCheckError(exceptions.AuthenticationFailed):
    default_detail = _('user not exist or password wrong.')


#####
# 404
#####
class FileNotFound(exceptions.NotFound):
    default_detail = _('file not found')

#####
# 409
#####
class DataIntegrityError(exceptions.APIException):
    status_code = status.HTTP_409_CONFLICT
    default_detail = _('data conflict')

class BitqueryFetchTimedOut(exceptions.APIException):
    status_code = status.HTTP_200_OK
    default_detail = _('Fetching data from bitquery timed out')
