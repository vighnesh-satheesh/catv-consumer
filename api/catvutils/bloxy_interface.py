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
        print('api_url:', api_url)
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
        elif chain == 'KLAY':
            grahql_klaytn_interface = GraphQLInterfaceKlaytn(source, address, depth_limit, from_time, till_time, limit, chain, token_address)
            results = grahql_klaytn_interface.call_klaytn_endpoint()
            return results
        elif chain == 'BSC':
            grahql_bsc_interface = GraphQLInterfaceBSC(source, address, depth_limit, from_time, till_time, limit, chain, token_address, self._key)
            results = grahql_bsc_interface.call_bsc_endpoint()
            return results
        elif chain == 'BNB':
            grahql_bnb_interface = GraphQLInterfaceBNB(source, address, depth_limit, from_time, till_time, limit, chain, token_address, self._key)
            results = grahql_bnb_interface.call_bnb_endpoint()
            return results
        elif chain == 'TRX':
            grahql_trx_interface = GraphQLInterfaceTRX(source, address, depth_limit, from_time, till_time, limit, chain, token_address, self._key)
            results = grahql_trx_interface.call_trx_endpoint()
            return results
        elif chain == 'EOS':
            grahql_eos_interface = GraphQLInterfaceEOS(source, address, depth_limit, from_time, till_time, limit, chain)
            results = grahql_eos_interface.call_eos_endpoint()
            return results
        elif chain == 'XRP':
            grahql_ripple_interface = GraphQLInterfaceRipple(source, address, depth_limit, from_time, till_time, limit, chain)
            results = grahql_ripple_interface.call_ripple_endpoint()
            return results
        elif chain == 'XLM':
            grahql_stellar_interface = GraphQLInterfaceStellar(source, address, depth_limit, from_time, till_time, limit, chain)
            results = grahql_stellar_interface.call_stellar_endpoint()
            return results
        else:
            if source:
                if chain == 'ETH':
                    api_url = settings.BLOXY_ETH_SRC_ENDPOINT
                elif chain in ['BSC', 'KLAY']:
                    api_url = settings.BLOXY_KLAY_SRC_ENDPOINT
                else:
                    api_url = self._source_endpoint
                depth = depth_limit
            else:
                if chain == 'ETH':
                    api_url = settings.BLOXY_ETH_DIST_ENDPOINT
                elif chain in ['BSC', 'KLAY']:
                    api_url = settings.BLOXY_KLAY_DIST_ENDPOINT
                else:
                    api_url = self._distribution_endpoint
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
        self._terra_endpoint = settings.GRAPHQL_ENDPOINT
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

class GraphQLInterfaceKlaytn:
    def __init__(self, source, address, depth_limit, from_time, till_time, limit, chain, token_address):
        self._klaytn_key = settings.GRAPHQL_X_API_KEY
        self._klaytn_endpoint = settings.GRAPHQL_ENDPOINT
        self._headers = {'X-API-KEY': self._klaytn_key}
        self.source = source
        self.address = address
        self.depth = depth_limit
        self.from_time = from_time
        self.till_time = till_time
        self.chain = chain
        self.token_address = token_address
        self.limit = int(limit)

    def _define_query(self):
        if self.source:
            direction = "inbound"
        else:
            direction = "outbound"
        currency_value = self.chain
        if self.token_address is not None and self.token_address != '0x0000000000000000000000000000000000000000':
            currency_value = self.token_address
        GRAPHQL_KLAYTN_QUERY = f"""
            query sentinel_klaytn {{
                  ethereum(network: klaytn) {{
                    coinpath(
                      options: {{ direction: {direction}, asc: "depth", limit: {self.limit} }}
                      initialAddress: {{ is: "{self.address}" }}
                      depth: {{ lteq: {self.depth} }}
                      date: {{ since: "{self.from_time}", till: "{self.till_time}" }}
                      currency: {{ is: "{currency_value}" }}
                    ) {{
                      receiver {{
                        address
                        annotation
                        smartContract {{
                            contractType
                        }}
                        firstTxAt {{
                            time
                        }}
                        lastTxAt {{
                            time
                        }}
                        amountOut
                        amountIn
                        balance
                        receiversCount
                        sendersCount
                        type
                      }}
                      sender {{
                        address
                        annotation
                        smartContract {{
                            contractType
                        }}
                        type
                      }}
                      transaction {{
                        hash
                        value
                      }}
                      transactions {{
                        timestamp
                        txHash
                        txValue
                        amount
                        height
                      }}
                      depth
                      amount
                      currency {{
                        address
                        name
                        symbol
                        tokenId
                        tokenType
                      }}
                    }}
                  }}
                }}   
            """
        return GRAPHQL_KLAYTN_QUERY

    def call_klaytn_endpoint(self):
        query = self._define_query()
        try:
            flattened_response = []
            r = requests.post(self._klaytn_endpoint, json={'query': query}, headers=self._headers)
            response = r.json()
            if response["data"]["ethereum"]["coinpath"] is None:
                response["data"]["ethereum"]["coinpath"] = []
            for item in response["data"]["ethereum"]["coinpath"]:
                sender_annotation = item["sender"]["annotation"]
                receiver_annotation = item["receiver"]["annotation"]
                sender_type = item["sender"]["smartContract"]["contractType"]
                receiver_type = item["receiver"]["smartContract"]["contractType"]
                
                flattened_response.append(
                    {
                        "depth": item["depth"],
                        "tx_time": item["transactions"][0]["timestamp"],
                        "tx_hash": item["transactions"][0]["txHash"],
                        "sender": item["sender"]["address"],
                        "receiver": item["receiver"]["address"],
                        "amount": item["amount"],
                        "sender_type": sender_type if sender_type not in [None, "None"] else "Wallet",
                        "sender_annotation": sender_annotation if sender_annotation not in [None, "None"] else "",
                        "receiver_type": receiver_type if receiver_type not in [None, "None"] else "Wallet",
                        "receiver_annotation": receiver_annotation if receiver_annotation not in [None, "None"] else "",
                        "symbol": item["currency"]["symbol"],
                        "token": self.token_address, 
                        "token_id": item["currency"]["tokenId"],
                        "token_type": item["currency"]["tokenType"],
                        "receiver_receivers_count": item["receiver"]["receiversCount"],
                        "receiver_senders_count": item["receiver"]["sendersCount"],
                        "receiver_first_tx_at": item["receiver"]["firstTxAt"]["time"],
                        "receiver_last_tx_at": item["receiver"]["lastTxAt"]["time"],
                        "receiver_amount_out": float(item["receiver"]["amountOut"]),
                        "receiver_amount_in": float(item["receiver"]["amountIn"]),
                        "receiver_balance": float(item["receiver"]["balance"])
                    }
                )
            print('GraphQl Response', len(flattened_response))
            return flattened_response
        except Exception as e:
            traceback.print_exc()
            return []

class GraphQLInterfaceBSC:
    def __init__(self, source, address, depth_limit, from_time, till_time, limit, chain, token_address, key):
        self._bsc_key = settings.GRAPHQL_X_API_KEY
        self._bsc_endpoint = settings.GRAPHQL_ENDPOINT
        self._headers = {'X-API-KEY': self._bsc_key}
        self.source = source
        self.address = address
        self.depth = depth_limit
        self.from_time = from_time
        self.till_time = till_time
        self.chain = chain
        self.token_address = token_address
        self.limit = int(limit)

    def _define_query(self):
        if self.source:
            direction = "inbound"
        else:
            direction = "outbound"
        if self.token_address is not None and self.token_address != '0x0000000000000000000000000000000000000000':
            GRAPHQL_BSC_QUERY = f"""
                query sentinel_bsc {{
                      ethereum(network: bsc) {{
                        coinpath(
                          options: {{ direction: {direction}, asc: "depth", limit: {self.limit} }}
                          initialAddress: {{ is: "{self.address}" }}
                          depth: {{ lteq: {self.depth} }}
                          date: {{ since: "{self.from_time}", till: "{self.till_time}" }}
                          currency: {{ is: "{self.token_address}" }}
                        ) {{
                          receiver {{
                            address
                            annotation
                            smartContract {{
                                contractType
                            }}
                            firstTxAt {{
                                time
                            }}
                            lastTxAt {{
                                time
                            }}
                            amountOut
                            amountIn
                            balance
                            receiversCount
                            sendersCount
                            type
                          }}
                          sender {{
                            address
                            annotation
                            smartContract {{
                                contractType
                            }}
                            type
                          }}
                          transaction {{
                            hash
                            value
                          }}
                          transactions {{
                            timestamp
                            txHash
                            txValue
                            amount
                            height
                          }}
                          depth
                          amount
                          currency {{
                            address
                            name
                            symbol
                            tokenId
                            tokenType
                          }}
                        }}
                      }}
                    }}   
                """
        else:
            GRAPHQL_BSC_QUERY = f"""
              query sentinel_bsc {{
                    ethereum(network: bsc) {{
                      coinpath(
                        options: {{ direction: {direction}, asc: "depth", limit: {self.limit} }}
                        initialAddress: {{ is: "{self.address}" }}
                        depth: {{ lteq: {self.depth} }}
                        date: {{ since: "{self.from_time}", till: "{self.till_time}" }}
                      ) {{
                        receiver {{
                          address
                          annotation
                          smartContract {{
                              contractType
                          }}
                          firstTxAt {{
                              time
                          }}
                          lastTxAt {{
                              time
                          }}
                          amountOut
                          amountIn
                          balance
                          receiversCount
                          sendersCount
                          type
                        }}
                        sender {{
                          address
                          annotation
                          smartContract {{
                              contractType
                          }}
                          type
                        }}
                        transaction {{
                          hash
                          value
                        }}
                        transactions {{
                          timestamp
                          txHash
                          txValue
                          amount
                          height
                        }}
                        depth
                        amount
                        currency {{
                          address
                          name
                          symbol
                          tokenId
                          tokenType
                        }}
                      }}
                    }}
                  }}   
              """
        return GRAPHQL_BSC_QUERY

    def call_bsc_endpoint(self):
        query = self._define_query()
        try:
            flattened_response = []
            r = requests.post(self._bsc_endpoint, json={'query': query}, headers=self._headers)
            response = r.json()
            if response["data"]["ethereum"]["coinpath"] is None:
                response["data"]["ethereum"]["coinpath"] = []
            for item in response["data"]["ethereum"]["coinpath"]:
                sender_annotation = item["sender"]["annotation"]
                receiver_annotation = item["receiver"]["annotation"]
                sender_type = item["sender"]["smartContract"]["contractType"]
                receiver_type = item["receiver"]["smartContract"]["contractType"]
                
                flattened_response.append(
                    {
                        "depth": item["depth"],
                        "tx_time": item["transactions"][0]["timestamp"],
                        "tx_hash": item["transactions"][0]["txHash"],
                        "sender": item["sender"]["address"],
                        "receiver": item["receiver"]["address"],
                        "amount": item["amount"],
                        "sender_type": sender_type if sender_type not in [None, "None"] else "Wallet",
                        "sender_annotation": sender_annotation if sender_annotation not in [None, "None"] else "",
                        "receiver_type": receiver_type if receiver_type not in [None, "None"] else "Wallet",
                        "receiver_annotation": receiver_annotation if receiver_annotation not in [None, "None"] else "",
                        "symbol": item["currency"]["symbol"],
                        "token": self.token_address, 
                        "token_id": item["currency"]["tokenId"],
                        "token_type": item["currency"]["tokenType"],
                        "receiver_receivers_count": item["receiver"]["receiversCount"],
                        "receiver_senders_count": item["receiver"]["sendersCount"],
                        "receiver_first_tx_at": item["receiver"]["firstTxAt"]["time"],
                        "receiver_last_tx_at": item["receiver"]["lastTxAt"]["time"],
                        "receiver_amount_out": float(item["receiver"]["amountOut"]),
                        "receiver_amount_in": float(item["receiver"]["amountIn"]),
                        "receiver_balance": float(item["receiver"]["balance"])
                    }
                )
            print('GraphQl Response', len(flattened_response))
            return flattened_response
        except Exception as e:
            traceback.print_exc()
            return []

class GraphQLInterfaceBNB:
    def __init__(self, source, address, depth_limit, from_time, till_time, limit, chain, token_address, key):
        self._bnb_key = settings.GRAPHQL_X_API_KEY
        self._bnb_endpoint = settings.GRAPHQL_ENDPOINT
        self._headers = {'X-API-KEY': self._bnb_key}
        self.source = source
        self.address = address
        self.depth = depth_limit
        self.from_time = from_time
        self.till_time = till_time
        self.chain = chain
        self.token_address = token_address
        self.limit = int(limit)

    def _define_query(self):
        if self.source:
            direction = "inbound"
        else:
            direction = "outbound"
        GRAPHQL_BNB_QUERY = f"""
            query sentinel_bnb {{
                  binance {{
                    coinpath(
                      options: {{ direction: {direction}, asc: "depth", limit: {self.limit} }}
                      initialAddress: {{ is: "{self.address}" }}
                      depth: {{ lteq: {self.depth} }}
                      date: {{ since: "{self.from_time}", till: "{self.till_time}" }}
                      currency: {{ is: "{self.chain}" }}
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
                        amountOut
                        amountIn
                        balance
                        receiversCount
                        sendersCount
                        type
                      }}
                      sender {{
                        address
                        annotation
                        type
                      }}
                      transaction {{
                        hash
                        value
                        time {{
                            time
                        }}
                      }}
                      depth
                      amount
                      currency {{
                        address
                        name
                        symbol
                        tokenId
                        tokenType
                      }}
                    }}
                  }}
                }}   
            """
        return GRAPHQL_BNB_QUERY

    def call_bnb_endpoint(self):
        query = self._define_query()
        try:
            flattened_response = []
            r = requests.post(self._bnb_endpoint, json={'query': query}, headers=self._headers)
            response = r.json()
            if response["data"]["binance"]["coinpath"] is None:
                response["data"]["binance"]["coinpath"] = []
            for item in response["data"]["binance"]["coinpath"]:
                sender_annotation = item["sender"]["annotation"]
                receiver_annotation = item["receiver"]["annotation"]
                
                flattened_response.append(
                    {
                        "depth": item["depth"],
                        "tx_time": item["transaction"]["time"]["time"],
                        "tx_hash": item["transaction"]["hash"],
                        "sender": item["sender"]["address"],
                        "receiver": item["receiver"]["address"],
                        "amount": item["amount"],
                        "sender_type": item["sender"]["type"],
                        "sender_annotation": sender_annotation if sender_annotation not in [None, "None"] else "",
                        "receiver_type": item["receiver"]["type"],
                        "receiver_annotation": receiver_annotation if receiver_annotation not in [None, "None"] else "",
                        "symbol": item["currency"]["symbol"],
                        "token": self.token_address, 
                        "token_id": item["currency"]["tokenId"],
                        "token_type": item["currency"]["tokenType"],
                        "receiver_receivers_count": item["receiver"]["receiversCount"],
                        "receiver_senders_count": item["receiver"]["sendersCount"],
                        "receiver_first_tx_at": item["receiver"]["firstTxAt"]["time"],
                        "receiver_last_tx_at": item["receiver"]["lastTxAt"]["time"],
                        "receiver_amount_out": float(item["receiver"]["amountOut"]),
                        "receiver_amount_in": float(item["receiver"]["amountIn"]),
                        "receiver_balance": float(item["receiver"]["balance"])
                    }
                )
            print('GraphQl Response', len(flattened_response))
            return flattened_response
        except Exception as e:
            traceback.print_exc()
            return []

class GraphQLInterfaceTRX:
    def __init__(self, source, address, depth_limit, from_time, till_time, limit, chain, token_address, key):
        self._trx_key = settings.GRAPHQL_X_API_KEY
        self._trx_endpoint = settings.GRAPHQL_ENDPOINT
        self._headers = {'X-API-KEY': self._trx_key}
        self.source = source
        self.address = address
        self.depth = depth_limit
        self.from_time = from_time
        self.till_time = till_time
        self.chain = chain
        self.token_address = token_address
        self.limit = int(limit)

    def _define_query(self):
        if self.source:
            direction = "inbound"
        else:
            direction = "outbound"
        currency_value = self.chain
        if self.token_address is not None and self.token_address != '0x0000000000000000000000000000000000000000':
            currency_value = self.token_address
        GRAPHQL_TRX_QUERY = f"""
            query sentinel_trx {{
                  tron {{
                    coinpath(
                      options: {{ direction: {direction}, asc: "depth", limit: {self.limit} }}
                      initialAddress: {{ is: "{self.address}" }}
                      depth: {{ lteq: {self.depth} }}
                      date: {{ since: "{self.from_time}", till: "{self.till_time}" }}
                      currency: {{ is: "{currency_value}" }}
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
                        amountOut
                        amountIn
                        balance
                        receiversCount
                        sendersCount
                        type
                      }}
                      sender {{
                        address
                        annotation
                        type
                      }}
                      transaction {{
                        hash
                        value
                        time {{
                            time
                        }}
                      }}
                      depth
                      amount
                      currency {{
                        address
                        name
                        symbol
                        tokenId
                        tokenType
                      }}
                    }}
                  }}
                }}   
            """
        return GRAPHQL_TRX_QUERY

    def call_trx_endpoint(self):
        query = self._define_query()
        try:
            flattened_response = []
            r = requests.post(self._trx_endpoint, json={'query': query}, headers=self._headers)
            response = r.json()
            if response["data"]["tron"]["coinpath"] is None:
                response["data"]["tron"]["coinpath"] = []
            for item in response["data"]["tron"]["coinpath"]:
                sender_annotation = item["sender"]["annotation"]
                receiver_annotation = item["receiver"]["annotation"]
                
                flattened_response.append(
                    {
                        "depth": item["depth"],
                        "tx_time": item["transaction"]["time"]["time"],
                        "tx_hash": item["transaction"]["hash"],
                        "sender": item["sender"]["address"],
                        "receiver": item["receiver"]["address"],
                        "amount": item["amount"],
                        "sender_type": item["sender"]["type"],
                        "sender_annotation": sender_annotation if sender_annotation not in [None, "None"] else "",
                        "receiver_type": item["receiver"]["type"],
                        "receiver_annotation": receiver_annotation if receiver_annotation not in [None, "None"] else "",
                        "symbol": item["currency"]["symbol"],
                        "token": self.token_address, 
                        "token_id": item["currency"]["tokenId"],
                        "token_type": item["currency"]["tokenType"],
                        "receiver_receivers_count": item["receiver"]["receiversCount"],
                        "receiver_senders_count": item["receiver"]["sendersCount"],
                        "receiver_first_tx_at": item["receiver"]["firstTxAt"]["time"],
                        "receiver_last_tx_at": item["receiver"]["lastTxAt"]["time"],
                        "receiver_amount_out": float(item["receiver"]["amountOut"]),
                        "receiver_amount_in": float(item["receiver"]["amountIn"]),
                        "receiver_balance": float(item["receiver"]["balance"])
                    }
                )
            print('GraphQl Response', len(flattened_response))
            return flattened_response
        except Exception as e:
            traceback.print_exc()
            return []

class GraphQLInterfaceEOS:
    def __init__(self, source, address, depth_limit, from_time, till_time, limit, chain):
        self._eos_key = settings.GRAPHQL_X_API_KEY
        self._eos_endpoint = settings.GRAPHQL_ENDPOINT
        self._headers = {'X-API-KEY': self._eos_key}
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
        GRAPHQL_EOS_QUERY = f"""
            query sentinel_eos {{
                eos(network: eos) {{
                    coinpath(
                        options: {{ direction: {direction}, asc: "depth", limit: {self.limit} }}
                        initialAddress: {{ is: "{self.address}" }}
                        depth: {{ lteq: {self.depth} }}
                        date: {{ since: "{self.from_time}", till: "{self.till_time}" }}
                    ) {{
                      receiver {{
                        address
                        annotation
                        balance
                        amountOut
                        amountIn
                        type
                        receiversCount
                        sendersCount
                        firstTxAt {{
                            time
                        }}
                        lastTxAt {{
                            time
                        }}
                      }}
                      sender {{
                        address
                        annotation
                        type
                        firstTxAt {{
                            time
                        }}
                        lastTxAt {{
                            time
                        }}
                      }}
                      transaction {{
                        hash
                        time {{
                            time
                        }}
                      }}
                      depth
                      amount
                      currency {{
                        symbol
                        tokenType
                        tokenId
                        name
                      }}
                    }}
                  }}
                }}   
            """
        return GRAPHQL_EOS_QUERY

    def call_eos_endpoint(self):
        query = self._define_query()
        try:
            flattened_response = []
            r = requests.post(self._eos_endpoint, json={
                              'query': query}, headers=self._headers)
            response = r.json()
            if response["data"]["eos"]["coinpath"] is None:
                response["data"]["eos"]["coinpath"] = []
            for item in response["data"]["eos"]["coinpath"]:
                sender_annotation = item["sender"]["annotation"]
                receiver_annotation = item["receiver"]["annotation"]
                
                flattened_response.append(
                    {
                        "depth": item["depth"],
                        "tx_time": item["transaction"]["time"]["time"],
                        "tx_hash": item["transaction"]["hash"],
                        "sender": item["sender"]["address"],
                        "receiver": item["receiver"]["address"],
                        "amount": item["amount"],
                        "sender_type": item["sender"]["type"],
                        "sender_annotation": sender_annotation if sender_annotation not in [None, "None"] else "",
                        "receiver_type": item["receiver"]["type"],
                        "receiver_annotation": receiver_annotation if receiver_annotation not in [None, "None"] else "",
                        "symbol": item["currency"]["symbol"],
                        "token": item["currency"]["name"],
                        "token_id": item["currency"]["tokenId"],
                        "token_type": item["currency"]["tokenType"],
                        "receiver_receivers_count": item["receiver"]["receiversCount"],
                        "receiver_senders_count": item["receiver"]["sendersCount"],
                        "receiver_first_tx_at": item["receiver"]["firstTxAt"]["time"],
                        "receiver_last_tx_at": item["receiver"]["lastTxAt"]["time"],
                        "receiver_amount_out": float(item["receiver"]["amountOut"]),
                        "receiver_amount_in": float(item["receiver"]["amountIn"]),
                        "receiver_balance": float(item["receiver"]["balance"])
                    }
                )
            print('GraphQl Response - EOS', len(flattened_response))
            return flattened_response
        except Exception as e:
            traceback.print_exc()
            return []

class GraphQLInterfaceStellar:
    def __init__(self, source, address, depth_limit, from_time, till_time, limit, chain):
        self._stellar_key = settings.GRAPHQL_X_API_KEY
        self._stellar_endpoint = settings.GRAPHQL_ENDPOINT
        self._headers = {'X-API-KEY': self._stellar_key}
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
        GRAPHQL_STELLAR_QUERY = f"""
            query sentinel_stellar {{
                stellar(network: stellar) {{
                    coinpath(
                        options: {{ direction: {direction}, asc: "depth", limit: {self.limit} }}
                        initialAddress: {{ is: "{self.address}" }}
                        depth: {{ lteq: {self.depth} }}
                        date: {{ since: "{self.from_time}", till: "{self.till_time}" }}
                    ) {{
                      receiver {{
                        address
                        annotation
                        receiversCount
                        sendersCount
                        firstTransferAt {{
                            time
                        }}
                        lastTransferAt {{
                            time
                        }}
                      }}
                      sender {{
                        address
                        annotation
                        receiversCount
                        sendersCount
                        firstTransferAt {{
                            time
                        }}
                        lastTransferAt {{
                            time
                        }}
                      }}
                      transaction {{
                        hash
                        time {{
                            time
                        }}
                        valueFrom
                        valueTo
                      }}
                      depth
                      amountFrom
                      amountTo
                      operation
                      currencyFrom {{
                        symbol
                        name
                      }}
                      currencyTo {{
                        symbol
                        name
                      }}
                    }}
                  }}
                }}   
            """
        return GRAPHQL_STELLAR_QUERY

    def call_stellar_endpoint(self):
        query = self._define_query()
        try:
            flattened_response = []
            r = requests.post(self._stellar_endpoint, json={
                              'query': query}, headers=self._headers)
            response = r.json()
            if response["data"]["stellar"]["coinpath"] is None:
                response["data"]["stellar"]["coinpath"] = []
            for item in response["data"]["stellar"]["coinpath"]:
                sender_annotation = item["sender"]["annotation"]
                receiver_annotation = item["receiver"]["annotation"]
                
                flattened_response.append(
                    {
                        "depth": item["depth"],
                        "tx_time": item["transaction"]["time"]["time"],
                        "sender": item["sender"]["address"],
                        "receiver": item["receiver"]["address"],
                        "sent_amount": item["amountFrom"],
                        "sent_tx_value": item["transaction"]["valueFrom"],
                        "sent_currency": item["currencyFrom"]["symbol"],
                        "received_amount": item["amountTo"],
                        "received_tx_value": item["transaction"]["valueTo"],
                        "received_currency": item["currencyTo"]["symbol"],
                        "operation_type": item["operation"],
                        "tx_hash": item["transaction"]["hash"],
                        "sender_annotation": sender_annotation if sender_annotation not in [None, "None"] else "",
                        "receiver_annotation": receiver_annotation if receiver_annotation not in [None, "None"] else "",
                        "receiver_receive_from_count": item["receiver"]["receiversCount"],
                        "receiver_send_to_count": item["receiver"]["sendersCount"],
                        "receiver_first_transfer_at": item["receiver"]["firstTransferAt"]["time"],
                        "receiver_last_transfer_at": item["receiver"]["lastTransferAt"]["time"],
                        "sender_receive_from_count": item["sender"]["receiversCount"],
                        "sender_send_to_count": item["sender"]["sendersCount"],
                        "sender_first_transfer_at": item["sender"]["firstTransferAt"]["time"],
                        "sender_last_transfer_at": item["sender"]["lastTransferAt"]["time"],
                    }
                )
            print('GraphQl Response - Stellar', len(flattened_response))
            return flattened_response
        except Exception as e:
            traceback.print_exc()
            return []

class GraphQLInterfaceRipple:
    def __init__(self, source, address, depth_limit, from_time, till_time, limit, chain):
        self._ripple_key = settings.GRAPHQL_X_API_KEY
        self._ripple_endpoint = settings.GRAPHQL_ENDPOINT
        self._headers = {'X-API-KEY': self._ripple_key}
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
        GRAPHQL_RIPPLE_QUERY = f"""
            query sentinel_ripple {{
                ripple(network: ripple) {{
                    coinpath(
                        options: {{ direction: {direction}, asc: "depth", limit: {self.limit} }}
                        initialAddress: {{ is: "{self.address}" }}
                        depth: {{ lteq: {self.depth} }}
                        date: {{ since: "{self.from_time}", till: "{self.till_time}" }}
                    ) {{
                      receiver {{
                        address
                        annotation
                        receiversCount
                        sendersCount
                        firstTransferAt {{
                            time
                        }}
                        lastTransferAt {{
                            time
                        }}
                      }}
                      sender {{
                        address
                        annotation
                        receiversCount
                        sendersCount
                        firstTransferAt {{
                            time
                        }}
                        lastTransferAt {{
                            time
                        }}
                      }}
                      transaction {{
                        hash
                        time {{
                            time
                        }}
                        valueFrom
                        valueTo
                      }}
                      depth
                      amountFrom
                      amountTo
                      operation
                      currencyFrom {{
                        symbol
                        name
                      }}
                      currencyTo {{
                        symbol
                        name
                      }}
                    }}
                  }}
                }}   
            """
        return GRAPHQL_RIPPLE_QUERY

    def call_ripple_endpoint(self):
        query = self._define_query()
        try:
            flattened_response = []
            r = requests.post(self._ripple_endpoint, json={
                              'query': query}, headers=self._headers)
            response = r.json()
            if response["data"]["ripple"]["coinpath"] is None:
                response["data"]["ripple"]["coinpath"] = []
            for item in response["data"]["ripple"]["coinpath"]:
                sender_annotation = item["sender"]["annotation"]
                receiver_annotation = item["receiver"]["annotation"]
                
                flattened_response.append(
                    {
                        "depth": item["depth"],
                        "tx_time": item["transaction"]["time"]["time"],
                        "sender": item["sender"]["address"],
                        "receiver": item["receiver"]["address"],
                        "sent_amount": item["amountFrom"],
                        "sent_tx_value": item["transaction"]["valueFrom"],
                        "sent_currency": item["currencyFrom"]["symbol"],
                        "received_amount": item["amountTo"],
                        "received_tx_value": item["transaction"]["valueTo"],
                        "received_currency": item["currencyTo"]["symbol"],
                        "operation_type": item["operation"],
                        "tx_hash": item["transaction"]["hash"],
                        "sender_annotation": sender_annotation if sender_annotation not in [None, "None"] else "",
                        "receiver_annotation": receiver_annotation if receiver_annotation not in [None, "None"] else "",
                        "receiver_receive_from_count": item["receiver"]["receiversCount"],
                        "receiver_send_to_count": item["receiver"]["sendersCount"],
                        "receiver_first_transfer_at": item["receiver"]["firstTransferAt"]["time"],
                        "receiver_last_transfer_at": item["receiver"]["lastTransferAt"]["time"],
                        "sender_receive_from_count": item["sender"]["receiversCount"],
                        "sender_send_to_count": item["sender"]["sendersCount"],
                        "sender_first_transfer_at": item["sender"]["firstTransferAt"]["time"],
                        "sender_last_transfer_at": item["sender"]["lastTransferAt"]["time"],
                    }
                )
            print('GraphQl Response - Ripple', len(flattened_response))
            return flattened_response
        except Exception as e:
            traceback.print_exc()
            return []