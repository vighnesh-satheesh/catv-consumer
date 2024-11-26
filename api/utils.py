import hashlib
import mimetypes
import random
import re
from datetime import datetime, timedelta

from django.core.files.storage import default_storage

from .models import (
    CatvTokens
)


def validate_dateformat(value, date_format):
    datetime.strptime(value, date_format)


def validate_dateformat_and_randomize_seconds(value, input_format, output_format):
    random_seconds = random.randint(1, 59)
    date_obj = datetime.strptime(value, input_format)
    date_obj += timedelta(seconds=random_seconds)
    return date_obj.strftime(output_format)


def create_tracking_cache_pattern(data):
    wallet_address = data.get("wallet_address", "")
    source_depth = data.get("source_depth", 0)
    distribution_depth = data.get("distribution_depth", 0)
    transaction_limit = data.get("transaction_limit", 0)
    from_date = data.get("from_date", "")
    to_date = data.get("to_date", "")
    token_address = data.get("token_address", "")

    return 'w{0}s{1}d{2}tx{3}fd{4}td{5}tk{6}'.format(wallet_address, source_depth, distribution_depth,
                                                     transaction_limit, from_date, to_date, token_address)


def create_path_cache_pattern(data):
    address_from = data.get("address_from", "")
    address_to = data.get("address_to", '')
    token_address = data.get("token_address", "")
    depth = data.get("depth", "")
    from_date = data.get("from_date", "")
    to_date = data.get("to_date", "")

    return f"af{address_from}at{address_to}d{depth}fd{from_date}td{to_date}tk{token_address}"


def determine_wallet_type(token_type):
    address_mapping = {
        "ETH": "Ethereum/ERC20",
        "TRX": "Tron",
        "BTC": "Bitcoin",
        "LTC": "Litecoin",
        "BCH": "Bitcoin Cash",
        "XLM": "Stellar",
        "EOS": "EOS",
        "XRP": "Ripple",
        "BNB": "Binance Coin",
        "ADA": "Cardano",
        "BSC": "Binance Smart Chain",
        "KLAY": "Klaytn",
        "LUNC": "LUNC",
        "FTM": "Fantom",
        "POL": "Matic",
        "AVAX": "Avalanche",
        "DOGE": "Doge Coin",
        "ZEC": "Zcash",
        "DASH": "DASH"
    }

    if address_mapping.__contains__(token_type.value):
        return address_mapping[token_type.value]

    return "Ethereum/ERC20"


def pattern_matches_token(address, token_type):
    token_regex_map = {
        CatvTokens.ETH.value: "^0x[a-fA-F0-9]{40}$",
        CatvTokens.TRON.value: "^T[a-zA-Z0-9]{21,34}$",
        CatvTokens.BTC.value: "^([13]|bc1).*[a-zA-Z0-9]{26,35}$",
        CatvTokens.LTC.value: "^[LM3][a-km-zA-HJ-NP-Z1-9]{26,33}$",
        CatvTokens.BCH.value: "^([13][a-km-zA-HJ-NP-Z1-9]{25,34})|^((bitcoincash:)?(q|p)[a-z0-9]{41})|^((BITCOINCASH:)?(Q|P)[A-Z0-9]{41})$",
        CatvTokens.XRP.value: "^r[0-9a-zA-Z]{24,34}$",
        CatvTokens.EOS.value: "^[1-5a-z.]{12}$",
        CatvTokens.XLM.value: "^[0-9a-zA-Z]{56}$",
        CatvTokens.BNB.value: "^(bnb1)[0-9a-z]{38}$",
        CatvTokens.ADA.value: "^[0-9a-zA-Z]+$",
        CatvTokens.BSC.value: "^0x[a-fA-F0-9]{40}$",
        CatvTokens.KLAY.value: "^0x[a-fA-F0-9]{40}$",
        CatvTokens.LUNC.value: "^(terra1)[0-9a-z]{38}$",
        CatvTokens.FTM.value: "^0x[a-fA-F0-9]{40}$",
        CatvTokens.POL.value: "^0x[a-fA-F0-9]{40}$",
        CatvTokens.AVAX.value: "^0x[a-fA-F0-9]{40}$",
        CatvTokens.DOGE.value: "^(D|A|9)[a-km-zA-HJ-NP-Z1-9]{33,34}$",
        CatvTokens.ZEC.value: "^(t)[A-Za-z0-9]{34}$",
        CatvTokens.DASH.value: "^[X|7][0-9A-Za-z]{33}$"
    }
    pattern = token_regex_map.get(token_type, None)
    if not pattern:
        return False
    return re.compile(pattern).match(address)


# def upload_content_file_to_s3(content_file):
#     # default_storage is configured as S3Storage in base.py
#     return default_storage.save(content_file.name, content_file)

def upload_content_file_to_gcs(content_file):
    # default_storage is configured as GCS Storage in base.py
    return default_storage.save(content_file.name, content_file)


def get_file_meta(file, file_name):
    hasher = hashlib.md5()
    size = file.size
    mime_type, _ = mimetypes.guess_type(file_name)
    # Read the content of the ContentFile and update the hasher
    content = file.read()
    hasher.update(content)
    return hasher.hexdigest(), size, mime_type
