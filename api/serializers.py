import traceback

from django.utils import timezone
from requests.exceptions import ReadTimeout
from rest_framework import serializers

from . import exceptions
from . import models
from . import utils
from .catvutils.tracking_results import (
    TrackingResults, BTCCoinpathTrackingResults, EthPathResults,
    BtcPathResults
)
from .exceptions import BitqueryFetchTimedOut


class CATVETHSerializer(serializers.Serializer):
    wallet_address = serializers.CharField(required=True)
    source_depth = serializers.IntegerField(
        required=False, min_value=1, max_value=10)
    distribution_depth = serializers.IntegerField(
        required=False, min_value=1, max_value=10)
    transaction_limit = serializers.IntegerField(
        required=True, min_value=100, max_value=100000)
    from_date = serializers.CharField(required=True)
    to_date = serializers.CharField(required=True)
    token_address = serializers.CharField(
        required=False, default='0x0000000000000000000000000000000000000000')
    force_lookup = serializers.BooleanField(required=False, default=False)

    def validate(self, data):
        if 'source_depth' in data or 'distribution_depth' in data:
            return data
        else:
            raise serializers.ValidationError(
                "Either of source_depth or distribution_depth is needed.")

    def validate_wallet_address(self, value):
        if not utils.pattern_matches_token(value, self._token_type):
            raise serializers.ValidationError(
                "Token address is not a valid ethereum address.")
        return value

    def validate_from_date(self, value):
        try:
            utils.validate_dateformat(value, '%Y-%m-%d')
            return value
        except ValueError:
            raise serializers.ValidationError(
                "Incorrect date format, should be YYYY-MM-DD.")

    def validate_to_date(self, value):
        try:
            utils.validate_dateformat(value, '%Y-%m-%d')
            return value
        except ValueError:
            raise serializers.ValidationError(
                "Incorrect date format, should be YYYY-MM-DD.")

    def get_tracking_results(self, tx_limit=10000, limit=10000, save_to_db=True, build_lossy_graph=True):
        tracking_results = TrackingResults(**self.data, chain=self._token_type)
        try:
            tracking_results.get_tracking_data(tx_limit, limit, save_to_db)
            tracking_results.create_graph_data(build_lossy_graph)
            tracking_results.set_annotations_from_db(
                token_type=models.CatvTokens.ETH.value)
            return {
                "graph": tracking_results.make_graph_dict(),
                "api_calls": tracking_results.ext_api_calls,
                "messages": tracking_results.error_messages
            }
        except ReadTimeout:
            raise exceptions.FileNotFound("Timeout exceeded while fetching/processing data.")
        except Exception:
            raise


class CATVETHPathSerializer(serializers.Serializer):
    address_from = serializers.CharField(required=True)
    address_to = serializers.CharField(required=True)
    token_address = serializers.CharField(
        required=False, default='0x0000000000000000000000000000000000000000')
    depth = serializers.IntegerField(
        required=False, min_value=1, max_value=10, default=5)
    from_date = serializers.CharField(
        required=False, default=timezone.datetime(2015, 1, 1).strftime('%Y-%m-%d'))
    to_date = serializers.CharField(
        required=False, default=timezone.now().strftime('%Y-%m-%d'))
    min_tx_amount = serializers.FloatField(required=False, default=0.0)
    limit_address_tx = serializers.IntegerField(required=False, default=100000)
    force_lookup = serializers.BooleanField(required=False, default=False)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._tracker = EthPathResults

    def validate_address_from(self, value):
        if not utils.pattern_matches_token(value, self._token_type):
            raise serializers.ValidationError(
                "Wallet address 'address_form' is not a valid ethereum address.")
        return value

    def validate_address_to(self, value):
        if not utils.pattern_matches_token(value, self._token_type):
            raise serializers.ValidationError(
                "Wallet address 'address_to' is not a valid ethereum address.")
        return value

    def validate_from_date(self, value):
        try:
            utils.validate_dateformat(value, '%Y-%m-%d')
            return value
        except ValueError:
            raise serializers.ValidationError(
                "Incorrect date format, should be YYYY-MM-DD.")

    def validate_to_date(self, value):
        try:
            utils.validate_dateformat(value, '%Y-%m-%d')
            return value
        except ValueError:
            raise serializers.ValidationError(
                "Incorrect date format, should be YYYY-MM-DD.")

    def validate(self, data):
        if data['address_from'].lower() == data['address_to'].lower():
            raise serializers.ValidationError("Source and destination addresses cannot be same. Perhaps you meant to "
                                              "use the '/catv' resource?")
        return data

    def get_tracking_results(self, save_to_db=False):
        tracking_instance = self._tracker(**self.data, chain=self._token_type)
        try:
            tracking_instance.get_tracking_data()
            tracking_instance.create_graph_data()
            tracking_instance.set_annotations_from_db(
                token_type=self._token_type)
            return {
                "graph": tracking_instance.make_graph_dict(),
                "api_calls": tracking_instance.ext_api_calls,
                "messages": tracking_instance.error_messages
            }
        except ReadTimeout:
            raise exceptions.FileNotFound("Timeout exceeded while fetching/processing data.")
        except Exception as e:
            print(e)
            traceback.print_exc()
            err_msg = "Incorrect or missing transactions. Please try adjusting your search criteria."
            if tracking_instance.error:
                err_msg = tracking_instance.error
            elif e:
                err_msg = "Oops! Something went wrong while getting results for this address. Please try again later."
            raise exceptions.FileNotFound(err_msg)


class CATVBTCSerializer(CATVETHSerializer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def validate_wallet_address(self, value):
        if not utils.pattern_matches_token(value, self._token_type):
            raise serializers.ValidationError(
                "Wallet address is an invalid Bitcoin address")
        return value

    def get_tracking_results(self, tx_limit=10000, limit=10000, save_to_db=True, build_lossy_graph=True):
        serializer_data = self.data
        tracking_results = BTCCoinpathTrackingResults(**serializer_data, chain=self._token_type)
        try:
            tracking_results.get_tracking_data(tx_limit, limit, save_to_db)
            tracking_results.create_graph_data(build_lossy_graph)
            tracking_results.set_annotations_from_db(
                token_type=models.CatvTokens.BTC.value)
            return {
                "graph": tracking_results.make_graph_dict(),
                "api_calls": tracking_results.ext_api_calls,
                "messages": tracking_results.error_messages
            }
        except ReadTimeout:
            raise exceptions.FileNotFound(tracking_results.error_messages["source"])
        except Exception:
            traceback.print_exc()
            raise


class CATVBTCPathSerializer(CATVETHPathSerializer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._tracker = BtcPathResults

    def validate_address_from(self, value):
        if not utils.pattern_matches_token(value, self._token_type):
            raise serializers.ValidationError(
                "Wallet address 'address_form' is not a valid bitcoin address.")
        return value

    def validate_address_to(self, value):
        if not utils.pattern_matches_token(value, self._token_type):
            raise serializers.ValidationError(
                "Wallet address 'address_to' is not a valid bitcoin address.")
        return value
