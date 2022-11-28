from datetime import datetime
import requests
import traceback
from django.conf import settings

__all__ = ('LyzeAPIInterface', 'BloxyBTCAPIInterface', 'BloxyEthAPIInterface', )


class LyzeAPIInterface:
    def __init__(self, key):
        self.__key = key
        self.__source_endpoint = settings.LYZE_SRC_ENDPOINT
        self.__distribution_endpoint = settings.LYZE_DIST_ENDPOINT
        self.__txlist_endpoint = settings.LYZE_TXLIST_ENDPOINT

    def fetch_api_response(self, api_url, data, timeout=600):
        header = {
            'x-api-key': self.__key
        }
        response = requests.get(api_url, params=data, timeout=timeout, headers=header)
        if response.status_code != 200:
            print(response)
            return []
        response_list = response.json()
        return response_list["body"].get("result", [])

    def get_transactions(self, address, limit, tx_hash, depth_limit=2, source=True):
        api_url = self.__source_endpoint if source else self.__distribution_endpoint

        payload = {
            "address": address,
            "limit": limit,
            "depth_limit": depth_limit,
            "tx_id": tx_hash
        }
        r = self.fetch_api_response(api_url, payload)
        return r

    def get_txlist(self, address, from_date=None, to_date=None):
        from_time = datetime.now().strftime('%Y%m%d_000000')
        to_time = datetime.now().strftime('%Y%m%d_235959')
        if from_date is not None:
            from_time = datetime.strptime(from_date, '%Y-%m-%d')
            from_time = from_time.strftime('%Y%m%d_000000')
        if to_date is not None:
            to_time = datetime.strptime(to_date, '%Y-%m-%d')
            to_time = to_time.strftime('%Y%m%d_235959')

        payload = {
            "wallet_address": address,
            "from_time": from_time,
            "to_time": to_time
        }
        r = self.fetch_api_response(self.__txlist_endpoint, payload)
        return r


class BloxyBTCAPIInterface:
    def __init__(self, key):
        self.__key = key
        self.__source_endpoint = settings.BLOXY_BTC_SRC_ENDPOINT
        self.__distribution_endpoint = settings.BLOXY_BTC_DIST_ENDPOINT

    def fetch_api_response(self, api_url, data, timeout=600):
        response = requests.get(api_url, params=data, timeout=timeout, verify=False)
        if response.status_code != 200:
            print(response)
            return []
        response_list = response.json()
        return response_list

    def get_transactions(self, address, tx_limit, limit, depth_limit=2, from_time=datetime(2015, 1, 1, 0, 0),
                         till_time=datetime.now(), source=True, chain='BTC'):
        if chain == 'BCH':
            grahql_bch_interface = GraphQLInterfaceBCH(source, address, depth_limit, from_time, till_time, limit, chain)
            results = grahql_bch_interface.call_bch_endpoint()
            return results
        else:
            if chain is not 'BTC':
                self.__source_endpoint = settings.BLOXY_LTC_SRC_ENDPOINT
                self.__distribution_endpoint = settings.BLOXY_LTC_DIST_ENDPOINT
            api_url = self.__source_endpoint if source else self.__distribution_endpoint
            depth = depth_limit
            updated_chain_map = {
                'trx': 'tron',
                'xrp': 'ripple',
                'xlm': 'stellar',
                'bnb': 'binance',
                'ada': 'cardano',
                'bsc': 'binance smart chain',
                'klay': 'klaytn'
            }
            updated_chain = chain.lower()
            if updated_chain in updated_chain_map.keys():
                updated_chain = updated_chain_map[updated_chain]
            if updated_chain == 'ripple' or updated_chain == 'stellar':
                api_url = api_url.replace('coinpath', 'ripple:sentinel')
            payload = {'key': self.__key, 'address': address, 'depth_limit': depth,
                    'from_date': from_time, 'till_date': till_time, 'snapshot_time': from_time if source else till_time,
                    'limit_address_tx_count': tx_limit, 'limit': limit, 'format': 'json',
                    'chain': updated_chain}
            r = self.fetch_api_response(api_url, payload)
            return r

class GraphQLInterfaceBCH:
    def __init__(self, source, address, depth_limit, from_time, till_time, limit, chain):
        self._bch_key = settings.GRAPHQL_X_API_KEY
        self._bch_endpoint = "https://graphql.bitquery.io"
        self._headers = {'X-API-KEY': self._bch_key}
        self.source = source
        self.address = address
        self.depth = depth_limit
        self.from_time = from_time
        self.till_time = till_time
        self.chain = chain
        self.limit = int(limit)

    def _define_query(self):
        if self.source:
            direction = "inbound"
        else:
            direction = "outbound"
        GRAPHQL_BCH_QUERY = f"""
            query sentinel_bch {{
                bitcoin(network: bitcash) {{
                    coinpath(
                        options: {{ direction: {direction}, asc: "depth", limit: {self.limit} }}
                        initialAddress: {{ is: "{self.address}" }}
                        depth: {{ lteq: {self.depth} }}
                        date: {{ since: "{self.from_time}", till: "{self.till_time}" }}
                        receiver: {{ not: "" }}
                        sender: {{ not: "" }}
                    ) {{
                      receiver {{
                        address
                        annotation
                        firstTxAt {{
                            time
                        }}
                        lastTxAt {{
                            time
                        }}
                        type
                      }}
                      sender {{
                        address
                        annotation
                        firstTxAt {{
                            time
                        }}
                        lastTxAt {{
                            time
                        }}
                        type
                      }}
                      transaction {{
                        hash
                        valueIn
                        valueOut
                      }}
                      depth
                      amount
                      currency {{
                        symbol
                      }}
                      transactions {{
                        timestamp
                      }}
                    }}
                  }}
                }}   
            """
        return GRAPHQL_BCH_QUERY

    def call_bch_endpoint(self):
        query = self._define_query()
        try:
            flattened_response = []
            r = requests.post(self._bch_endpoint, json={
                              'query': query}, headers=self._headers)
            response = r.json()
            if response["data"]["bitcoin"]["coinpath"] is None:
                response["data"]["bitcoin"]["coinpath"] = []
            for item in response["data"]["bitcoin"]["coinpath"]:
                sender_annotation = item["sender"]["annotation"]
                receiver_annotation = item["receiver"]["annotation"]
                
                flattened_response.append(
                    {
                        "depth": item["depth"],
                        "tx_time": item["transactions"][0]["timestamp"],
                        "tx_hash": item["transaction"]["hash"],
                        "sender": item["sender"]["address"],
                        "receiver": item["receiver"]["address"],
                        "amount": item["amount"],
                        "sender_type": item["sender"]["type"],
                        "sender_annotation": sender_annotation if sender_annotation not in [None, "None"] else "",
                        "receiver_type": item["receiver"]["type"],
                        "receiver_annotation": receiver_annotation if receiver_annotation not in [None, "None"] else "",
                        "symbol": item["currency"]["symbol"],
                        "tx_value_in": item["transaction"]["valueIn"],
                        "tx_value_out": item["transaction"]["valueOut"],
                    }
                )
            print('GraphQl Response', len(flattened_response))
            return flattened_response
        except Exception as e:
            traceback.print_exc()
            return []

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
            'klay': 'klaytn'
        }
        updated_chain = path_tracker.chain.lower()
        if updated_chain in updated_chain_map.keys():
            updated_chain = updated_chain_map[updated_chain]
        payload.update({'chain': updated_chain})
        r = self.fetch_api_response(api_url, payload)
        return r

