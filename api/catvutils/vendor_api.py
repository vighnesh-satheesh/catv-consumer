from datetime import datetime
import requests
import traceback
from django.conf import settings

__all__ = ('BloxyEthAPIInterface',)


class BloxyEthAPIInterface:
    def __init__(self, key, api_url=settings.BLOXY_ETHCOINPATH_ENDPOINT):
        self.__key = key
        self.__coinpath_endpoint = api_url

    def fetch_api_response(self, api_url, data, timeout=600):
        response = requests.get(api_url, params=data, timeout=timeout)
        if response.status_code != 200:
            print(response)
            return []
        response_list = response.json()
        return response_list

    def get_path_transactions(self, path_tracker):
        api_url = self.__coinpath_endpoint
        payload = {
            'key': self.__key,
            'address1': path_tracker.address_from,
            'address2': path_tracker.address_to,
            'chain': path_tracker.chain.lower(),
            'token': path_tracker.token_address,
            'depth_limit': path_tracker.depth_limit,
            'min_tx_amount': path_tracker.min_tx_amount,
            'from_date': path_tracker.from_date,
            'till_date': path_tracker.to_date,
            'limit_address_tx_count': path_tracker.limit_address_tx,
            'format': 'json',
            'stop': 'nearest'
        }
        if path_tracker.token_address:
            payload.update({'token': path_tracker.token_address})
        updated_chain_map = {
            'trx': 'tron',
            'xrp': 'ripple',
            'xlm': 'stellar',
            'bnb': 'binance',
            'ada': 'cardano',
            'bsc': 'binance smart chain',
            'klay': 'klaytn',
            'ftm': 'fantom',
            'pol': 'matic',
            'avax': 'avax'
        }
        updated_chain = path_tracker.chain.lower()
        if updated_chain in updated_chain_map.keys():
            updated_chain = updated_chain_map[updated_chain]
        payload.update({'chain': updated_chain})
        r = self.fetch_api_response(api_url, payload)
        return r
