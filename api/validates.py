from django.core.exceptions import ValidationError
import django.contrib.auth.password_validation as password_validator

from rest_framework import exceptions

from . import models


def validate_pattern_type_subtype(pattern_type, pattern_subtype, model=False):
    exc_class = ValidationError if model is True else exceptions.ValidationError

    if pattern_type == models.IndicatorPatternType.NETWORKADDR:
        if pattern_subtype not in models.IndicatorPatternSubtype.networkaddr_subtypes():
            raise exc_class({"pattern_subtype": "pattern_subtype should be one of network address subtype"})
    elif pattern_type == models.IndicatorPatternType.CRYPTOADDR:
        if pattern_subtype not in models.IndicatorPatternSubtype.cryptoaddr_subtypes():
            raise exc_class({"pattern_subtype": "pattern_subtype should be one of crypto address subtype"})
    elif pattern_type == models.IndicatorPatternType.OTHER:
        if pattern_subtype != models.IndicatorPatternSubtype.OTHER:
            raise exc_class({"pattern_subtype": "pattern_subtype should be other when pattern_type is other"})


def validate_security_type_tag(security_category, security_tag, model=False):
    exc_class = ValidationError if model is True else exceptions.ValidationError

    if security_category == models.IndicatorSecurityCategory.WHITELIST:
        if not (security_tag is None or (isinstance(security_tag, list) and len(security_tag) == 0)):
            raise exc_class({"security_tags": "whitelist cannot have security_tags"})
    elif security_category == models.IndicatorSecurityCategory.BLACKLIST:
        if security_tag is None:
            return
        elif isinstance(security_tag, list) and len(security_tag) == 0:
            return
        elif not isinstance(security_tag, list):
            raise exc_class({"security_tags": "list of string is required."})
        else:
            for item in security_tag:
                if str(item).lower() not in ["phishing", "scam", "malware", "hacks", "exploits"]:
                    raise exc_class({"security_tags": "tag item should be one of  Phishing, Scam, Malware, Hacks, Exploits. Or empty list"})


def validate_max_length(text, model=False, limit=128, field_name="text"):
    exc_class = ValidationError if model is True else exceptions.ValidationError

    if len(text) > limit:
        raise exc_class({field_name: "The length of {0} should be less than {1}".format(field_name, limit)})


def validate_password(user, password, model=False):
    exc_class = ValidationError if model is True else exceptions.ValidationError
    try:
        password_validator.validate_password(password, user=user)
    except ValidationError as err:
        message = "minimum 8 charater. common or numeric password not allowed."
        if isinstance(err.messages, list):
            message = " ".join(err.messages)
        else:
            message = str(err.messages)
        raise exc_class({"password": message})
