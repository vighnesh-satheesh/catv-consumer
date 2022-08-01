import requests
import traceback
from datetime import datetime

from django.conf import settings


class BloxyAPIInterface:
    def __init__(self, key):
        self._key = key
        self._source_endpoint = settings.BLOXY_SRC_ENDPOINT
        self._distribution_endpoint = settings.BLOXY_DIST_ENDPOINT
        self._terra_key = settings.GRAPHQL_X_API_KEY
        self._terra_endpoint = settings.GRAPHQL_ENDPOINT

    def call_bloxy_api(self, api_url, data, timeout=600):
        print(api_url)
        print(data)
        response = requests.get(api_url, params=data, timeout=timeout)
        if response.status_code != 200:
            print(response)
            return []
        response_list = response.json()
        return response_list

    def get_transactions(self, address, tx_limit, limit, depth_limit=2,
                        from_time=datetime(2015, 1, 1, 0, 0),
                        till_time=datetime.now(),
                        token_address=None, source=True, chain='ETH'):
        if chain == 'LUNC':
            grahql_terra_interface = GraphQLInterfaceTerra(source, address, depth_limit, from_time, till_time, limit)
            results = grahql_terra_interface.call_terra_endpoint()
            return results

        else:
            if source:
                api_url = settings.BLOXY_ETH_SRC_ENDPOINT if (
                                                chain == 'ETH' or
                                                chain == 'BSC' or
                                                chain == 'KLAY'
                                            ) else self._source_endpoint
                depth = depth_limit
            else:
                api_url = settings.BLOXY_ETH_DIST_ENDPOINT if (
                                                chain == 'ETH' or
                                                chain == 'BSC' or
                                                chain == 'KLAY'
                                            ) else self._distribution_endpoint
                depth = depth_limit

            updated_chain_map = {
                'trx': 'tron',
                'xrp': 'ripple',
                'xlm': 'stellar',
                'bnb': 'binance',
                'ada': 'cardano',
                'bsc': 'bsc',
                'klay': 'klaytn'
            }

            updated_chain = chain.lower()
            if updated_chain in updated_chain_map.keys():
                updated_chain = updated_chain_map[updated_chain]

            if updated_chain == 'ripple' or updated_chain == 'stellar':
                api_url = api_url.replace('coinpath', 'ripple:sentinel')

            payload = {'key': self._key, 'address': address, 'depth_limit': depth,
                       'from_date': from_time, 'till_date': till_time, 'snapshot_time': from_time if source else till_time,
                       'limit_address_tx_count': tx_limit, 'limit': limit, 'chain': updated_chain}
            if token_address:
                if chain == 'ETH' or chain == 'BSC' or chain == 'KLAY':
                    payload['token_address'] = token_address
                else:
                    payload['token'] = token_address
            print("Payload: ", payload)
            r = self.call_bloxy_api(api_url, payload)
            return r


class GraphQLInterfaceTerra:
    def __init__(self, source, address, depth_limit, from_time, till_time, limit):
        self._terra_key = settings.GRAPHQL_X_API_KEY
        self._terra_endpoint = "https://graphql.bitquery.io"
        self._headers = {'X-API-KEY': self._terra_key}
        self.source = source
        self.address = address
        self.depth = depth_limit
        self.from_time = from_time
        self.till_time = till_time
        self.limit = int(limit)

    def _define_query(self):
        if self.source:
            direction = "inbound"
        else:
            direction = "outbound"
        GRAPHQL_TERRA_QUERY = f"""
            query sentinel_terra {{
                  cosmos(network: terra) {{
                    coinpath(
                      options: {{ direction: {direction}, asc: "depth", limit: {self.limit} }}
                      initialAddress: {{ is: "{self.address}" }}
                      depth: {{ lteq: {self.depth} }}
                      date: {{ between: ["{self.from_time.split("T")[0]}","{self.till_time.split("T")[0]}"] }}
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
                        value
                      }}
                      block {{
                        timestamp {{
                          time(format: "%Y-%m-%d")
                        }}
                      }}
                      depth
                      amount
                      currency {{
                        symbol
                      }}
                    }}
                  }}
                }}   
            """
        return GRAPHQL_TERRA_QUERY

    def call_terra_endpoint(self):
        query = self._define_query()
        try:
            flattened_response = []
            r = requests.post(self._terra_endpoint, json={'query': query}, headers=self._headers)
            response = r.json()
            for item in response["data"]["cosmos"]["coinpath"]:
                sender_annotation = item["sender"]["annotation"]
                receiver_annotation = item["receiver"]["annotation"]
                flattened_response.append(
                    {
                        "depth": item["depth"],
                        "tx_time": item["block"]["timestamp"]["time"],
                        "sender": item["sender"]["address"],
                        "receiver": item["receiver"]["address"],
                        "tx_hash": item["transaction"]["hash"],
                        "tx_value": item["transaction"]["value"],
                        "amount": item["amount"],
                        "symbol": item["currency"]["symbol"],
                        "sender_annotation": sender_annotation if sender_annotation not in [None, "None"] else "",
                        "receiver_annotation": receiver_annotation if receiver_annotation not in [None, "None"] else ""
                    }
                )
            return flattened_response
        except Exception as e:
            traceback.print_exc()
            return []