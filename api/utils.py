import ast
import hashlib
import json
import mimetypes
import random
import re
from datetime import datetime, timedelta

from django.core.files.storage import default_storage
from requests import ReadTimeout
from google.cloud import storage
from django.core.exceptions import SuspiciousOperation
from google.cloud.exceptions import NotFound

from .exceptions import BitqueryConcurrentRequestError, BitqueryNetworkTimeoutError, BitqueryDataNotFoundError, \
    BitqueryMemoryLimitExceeded, TracerBaseException
from .models import (
    CatvTokens
)
from .settings import api_settings


def validate_dateformat(value, date_format):
    datetime.strptime(value, date_format)


def validate_dateformat_and_randomize_seconds(value, output_format):
    random_seconds = random.randint(1, 59)
    try:
        date_obj = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        try:
            date_obj = datetime.strptime(value, '%Y-%m-%d')
            date_obj = date_obj.replace(hour=0, minute=0, second=0)
        except ValueError:
            raise ValueError(f"Time data '{value}' does not match format '%Y-%m-%d %H:%M:%S' or '%Y-%m-%d'")
    date_obj += timedelta(seconds=random_seconds)
    return date_obj.strftime(output_format)


def format_tx_time(tx_time):
    """Handle multiple timestamp formats from different vendors"""
    try:
        # Split the timestamp string to handle different formats
        parts = tx_time.replace('Z', '').replace('+00:00', '')

        # Handle format with 'T' separator
        if 'T' in parts:
            date_part, time_part = parts.split('T')
            # Return format: "YYYY-MM-DD HH:MM"
            return f"{date_part} {time_part[:5]}"
        else:
            # If the format is unexpected, return the original
            return tx_time
    except Exception:
        # If any parsing error occurs, return the original
        return tx_time


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


def upload_content_file_to_gcs(content_file):
    # default_storage is configured as GCS Storage in base.py
    return default_storage.save(content_file.name, content_file)


def get_gcs_file(filename):
    client = storage.Client()
    bucket = client.bucket(api_settings.ATTACHED_FILE_S3_BUCKET_NAME)

    try:
        blob = bucket.blob(filename)
        body = blob.download_as_text()
        results = json.loads(body)
        if isinstance(results, str):
            results = ast.literal_eval(results)
        return results
    except NotFound:
        raise SuspiciousOperation(f"The file '{filename}' does not exist in the GCS bucket.")


def get_file_meta(file, file_name):
    hasher = hashlib.md5()
    size = file.size
    mime_type, _ = mimetypes.guess_type(file_name)
    # Read the content of the ContentFile and update the hasher
    content = file.read()
    hasher.update(content)
    return hasher.hexdigest(), size, mime_type


def get_user_error_message(exception: Exception, error_source: str = None) -> str:
    """
    Convert exceptions to user-friendly error messages, including error source when available.

    Args:
        exception: The caught exception
        error_source: Source of the error ("source" for incoming, "distribution" for outgoing transactions)

    Returns:
        A user-friendly error message string
    """

    error_messages = {
        BitqueryConcurrentRequestError: "The query took too long to complete. This may be because the number of transactions are too high to provide a response in a short time. Try reducing the time range or depth if this issue persists.",

        BitqueryNetworkTimeoutError: "There are too many transactions to display at once. Try narrowing down your search to a smaller date range.",

        BitqueryMemoryLimitExceeded: "There are too many transactions to process. Please try: 1) Selecting a shorter time period 2) Reducing the search depth.",

        BitqueryDataNotFoundError: "We couldn't find any transactions for this wallet in the time period you selected. Please check the wallet address and dates.",

        ReadTimeout: "The query took too long to complete. This may be because the number of transactions are too high to provide a response in a short time. Try reducing the time range or depth if this issue persists.",

        TracerBaseException: "Tracer API returned no data.",
    }

    # Get base message
    base_message = error_messages.get(type(exception), "Not able to fetch results at this time. Please try again.")

    # If error_source is provided, prepend it to the message
    if error_source:
        transaction_type = "Incoming" if error_source == "source" else "Outgoing"
        return f"Sorry, {transaction_type.lower()} transactions missing: {base_message}"

    # Otherwise return the message as is
    return base_message


def safe_get(dict_obj, *keys, default=None):
    """
    Safely get nested values from dictionaries and lists
    Args:
        dict_obj: The object to traverse (can be dict or list)
        *keys: Keys/indices to access nested values
        default: Default value if path doesn't exist
    """
    try:
        result = dict_obj
        for key in keys:
            if isinstance(result, dict):
                if key not in result:
                    return default
                result = result[key]
            elif isinstance(result, list):
                if not isinstance(key, int) or key >= len(result):
                    return default
                result = result[key]
            else:
                return default

            if result in [None, "None"]:
                return default

        return result
    except Exception:
        return default
