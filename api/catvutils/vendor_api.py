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
        # The verify flag is set to false because of an issue with sending requests to this endpoint
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
        elif chain == 'ADA':
            grahql_cardano_interface = GraphQLInterfaceCARDANO(source, address, depth_limit, from_time, till_time, limit, chain)
            results = grahql_cardano_interface.call_cardano_endpoint()
            return results
        elif chain == 'LTC':
            grahql_ltc_interface = GraphQLInterfaceLTC(source, address, depth_limit, from_time, till_time, limit, chain)
            results = grahql_ltc_interface.call_ltc_endpoint()
            return results
        else:
            if chain != 'BTC':
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
        self._bch_endpoint = settings.GRAPHQL_ENDPOINT
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

class GraphQLInterfaceCARDANO:
    def __init__(self, source, address, depth_limit, from_time, till_time, limit, chain):
        self._cardano_key = settings.GRAPHQL_X_API_KEY
        self._cardano_endpoint = settings.GRAPHQL_ENDPOINT
        self._headers = {'X-API-KEY': self._cardano_key}
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
        GRAPHQL_CARDANO_QUERY = f"""
            query sentinel_cardano {{
                cardano(network: cardano) {{
                    coinpath(
                        options: {{ direction: {direction}, asc: "depth", limit: {self.limit} }}
                        initialAddress: {{ is: "{self.address}" }}
                        depth: {{ lteq: {self.depth} }}
                        date: {{ since: "{self.from_time}", till: "{self.till_time}" }}
                    ) {{
                      receiver {{
                        address
                        annotation
                      }}
                      sender {{
                        address
                        annotation
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
        return GRAPHQL_CARDANO_QUERY

    def call_cardano_endpoint(self):
        query = self._define_query()
        try:
            flattened_response = []
            r = requests.post(self._cardano_endpoint, json={
                              'query': query}, headers=self._headers)
            response = r.json()
            if response["data"]["cardano"]["coinpath"] is None:
                response["data"]["cardano"]["coinpath"] = []
            for item in response["data"]["cardano"]["coinpath"]:
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
                        "sender_type": "unknown",
                        "sender_annotation": sender_annotation if sender_annotation not in [None, "None"] else "",
                        "receiver_type": "unknown",
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

class GraphQLInterfaceLTC:
    def __init__(self, source, address, depth_limit, from_time, till_time, limit, chain):
        self._ltc_key = settings.GRAPHQL_X_API_KEY
        self._ltc_endpoint = settings.GRAPHQL_ENDPOINT
        self._headers = {'X-API-KEY': self._ltc_key}
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
        GRAPHQL_LTC_QUERY = f"""
            query sentinel_ltc {{
                bitcoin(network: litecoin) {{
                    coinpath(
                        options: {{ direction: {direction}, asc: "depth", limit: {self.limit} }}
                        initialAddress: {{ is: "{self.address}" }}
                        depth: {{ lteq: {self.depth} }}
                        date: {{ since: "{self.from_time}", till: "{self.till_time}" }}
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
        return GRAPHQL_LTC_QUERY

    def call_ltc_endpoint(self):
        query = self._define_query()
        try:
            flattened_response = []
            r = requests.post(self._ltc_endpoint, json={
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


# class GraphQLInterfaceGeneral:
#     def __init__(self):
#         self._graphql_key = settings.GRAPHQL_X_API_KEY
#         self._graphql_endpoint = settings.GRAPHQL_ENDPOINT
#         self._headers = {'X-API-KEY': self._graphql_key}
#
#     def format_results(self, res):
#         if res['data'].get("ethereum") and res['data']["ethereum"].get("coinpath"):
#             data = res['data']["ethereum"]["coinpath"]
#             print(data)
#             first_depth_result = self.get_first_depth(receiver_address=data[0]['sender']['address'])
#             final_res = [
#                 {
#                     "symbol": data[0]['currency']['symbol'],
#                     "tokenId": data[0]['currency']['tokenId'],
#                     "token_type": data[0]['currency']['tokenType'],
#                     "direction": "outbound",
#                     "path": [first_depth_result]
#                 }
#             ]
#
#             for path in data:
#                 path_item = {
#                     "depth": path['depth'],
#                     "block": path['transactions'][0]['height'],
#                     "tx_time": path['transactions'][0]['timestamp'],
#                     "amount": path['transactions'][0]['amount'],
#                     "tx_value": path['transaction']['value'],
#                     "sender": path['sender']['address'],
#                     "receiver": path['receiver']['address'],
#                     "tx_hash": path['transaction']['hash']
#                 }
#                 final_res[0]["path"].append(path_item)
#
#             return final_res
#         else:
#             return []
#
#     def get_first_depth(self, receiver_address):
#         query = f"""query MyQuery {{
#         ethereum(network: { self.chain } ) {{
#             coinpath(
#               date: {{ after: "{ self.path_tracker.from_date }", till: "{ self.path_tracker.to_date }" }}
#               options: {{asc: "depth"}}
#               depth: {{is: 1}}
#               receiver: {{in: "{ receiver_address }"}}
#               sender: {{in: "{ self.path_tracker.address_from }"}}
#             ) {{
#               sender {{
#                 address
#                 smartContract {{ contractType }}
#               }}
#               depth
#               transactions {{ timestamp amount height }}
#               transaction {{ hash value }}
#               receiver {{
#                 address
#                 smartContract {{ contractType }}
#               }}
#               currency {{ address name symbol tokenId tokenType }}
#             }}
#           }}
#         }}"""
#         print(query)
#         result = self.fetch_api_response(query)
#         print("first depth")
#         path = result['data']["ethereum"]["coinpath"][0]
#         first_path = {
#             "depth": path['depth'],
#             "block": path['transactions'][0]['height'],
#             "tx_time": path['transactions'][0]['timestamp'],
#             "amount": path['transactions'][0]['amount'],
#             "tx_value": path['transaction']['value'],
#             "sender": path['sender']['address'],
#             "receiver": path['receiver']['address'],
#             "tx_hash": path['transaction']['hash']
#         }
#         print(first_path)
#         return first_path
#
#
#
#     def fetch_api_response(self, query):
#         res = requests.post(self._graphql_endpoint,
#                             json={'query': query},
#                             headers=self._headers)
#         if res.status_code != 200:
#             print(res)
#             return []
#
#         return res.json()
#
#     def get_path_transactions(self, path_tracker):
#         updated_chain_map = {
#             'trx': 'tron',
#             'xrp': 'ripple',
#             'xlm': 'stellar',
#             'bnb': 'binance',
#             'ada': 'cardano',
#             'klay': 'klaytn',
#             'ftm': 'fantom'
#         }
#
#         chain = path_tracker.chain.lower()
#         if chain in updated_chain_map:
#             chain = updated_chain_map[chain]
#
#         self.path_tracker = path_tracker
#         self.chain = chain
#         query = f"""
#         query MyQuery {{
#               ethereum(network: { chain }) {{
#                 coinpath(
#                   depth: {{lteq: {path_tracker.depth_limit} }}
#                   date: {{after: "{ path_tracker.from_date }", till: "{ path_tracker.to_date }"}}
#                   initialAddress: {{in: "{ path_tracker.address_from }"}}
#                   receiver: {{in: "{ path_tracker.address_to }"}}
#                   options: {{ minimumTxAmount: {path_tracker.min_tx_amount} }}
#                 ) {{
#                   sender {{ address annotation
#                     smartContract {{ contractType }}
#                   }}
#                   depth
#                   currency {{ address name symbol tokenId tokenType }}
#                   receiver {{ address annotation
#                     smartContract {{ contractType }}
#                   }}
#                   transactions {{ timestamp txHash txValue amount height }}
#                   transaction {{ hash value }}
#                 }}
#               }}
#             }}
#             """
#
#         response = self.fetch_api_response(query)
#         print('api response')
#         print(response)
#
#         result = self.format_results(response)
#         print('result json')
#         print(result)
#
#         return result


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
            'matic': 'matic',
            'avax': 'avax'
        }
        updated_chain = path_tracker.chain.lower()
        if updated_chain in updated_chain_map.keys():
            updated_chain = updated_chain_map[updated_chain]
        payload.update({'chain': updated_chain})
        r = self.fetch_api_response(api_url, payload)
        return r

