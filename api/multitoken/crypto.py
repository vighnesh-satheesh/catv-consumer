import os
from datetime import datetime
import secrets
import base64
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from Crypto.Hash import SHA384

from ..settings import api_settings
from ..exceptions import AuthenticationCheckError


CIPHER = None
try:
    buf = None
    if os.path.exists(api_settings.TOKEN_ENCRYPT_PRIVATE_KEY):
        with open(api_settings.TOKEN_ENCRYPT_PRIVATE_KEY, 'r') as f:
            buf = f.read()
    else:
        buf = api_settings.TOKEN_ENCRYPT_PRIVATE_KEY
    key = RSA.importKey(buf.encode('utf-8').decode('unicode_escape'))
    CIPHER = PKCS1_OAEP.new(key, hashAlgo=SHA384)
except Exception:  # TODO: what kind of errors?
    raise


def decrypt_message(encrypted_msg, timestamp):
    try:
        if timestamp is None:
            return None

        if not timestamp.isnumeric():
            return None

        current_utc_dt = datetime.utcnow()
        sent_utc_dt = datetime.utcfromtimestamp(int(timestamp))

        if abs((current_utc_dt - sent_utc_dt).total_seconds()) > api_settings.DECRYPT_TIMESTAMP_LEEWAY:
            return None

        timestamp = int(timestamp)

        decoded_msg = base64.b64decode(encrypted_msg)
        plain_ts = CIPHER.decrypt(decoded_msg)
        items = plain_ts.decode("utf-8").split("\n")
        if len(items) != 2:
            return None

        ts = items[1]

        if not ts.isnumeric():
            return None

        if int(ts) != timestamp:
            return None

        return items[0]
    except Exception as err:
        print(">>>", err)
        return None


def generate_new_token():
    token = secrets.token_hex(32)
    return token


def verify_token(encrypted_token, timestamp):
    token = decrypt_message(encrypted_token, timestamp)
    if token is None:
        raise AuthenticationCheckError("invalid token")
    return token
