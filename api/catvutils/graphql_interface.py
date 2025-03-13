import json
import os
import traceback
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import Optional, Any, Dict, List
from django.conf import settings

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from api.catvutils.tracer_interface import TracerAPIInterface
from api.constants import Constants
from api.exceptions import BitqueryConcurrentRequestError, BitqueryNetworkTimeoutError, \
    BitqueryMemoryLimitExceeded
from api.exceptions import BitqueryServerError, BitqueryBaseException, BitqueryDataNotFoundError, InvalidGraphqlQuery
from api.utils import validate_dateformat_and_randomize_seconds, safe_get


class GraphQLClient:
    """Handles GraphQL HTTP communications with connection pooling and retries."""

    def __init__(self, endpoint: str, headers: Dict[str, str], timeout: tuple = (60, 300)):
        self.endpoint = endpoint
        self.headers = headers
        self.timeout = timeout
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=10,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
            respect_retry_after_header=True,  # Honor server's retry guidance
            raise_on_redirect=True,  # Fail fast on redirects
            raise_on_status=True
        )

        # Configure connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def execute_query(self, query: str) -> Dict[str, Any]:
        try:
            print(f"query: {query}")
            response = self.session.post(
                self.endpoint,
                json={'query': query},
                headers=self.headers,
                timeout=self.timeout
            )

            print(f"X-Graphql-Query-Id: {response.headers['x-graphql-query-id']} status: {response.status_code}")

            # Raise for any HTTP error status codes
            response.raise_for_status()

            # Parse and check for GraphQL-level errors
            json_response = response.json()
            if 'errors' in json_response:
                error = json_response.get('errors')[0]
                error_message = error.get('message', '')
                error_type = error.get('error_type', '')
                query_id = error.get('query_id', '')

                print(f"GraphQL Error: {error_message} (Type: {error_type}, Query ID: {query_id})")

                # Handle specific error cases
                if "simultaneous requests" in error_message.lower():
                    raise BitqueryConcurrentRequestError(f"Concurrent request limit exceeded: {error_message}")
                elif "timeout" in error_message.lower() or "net::readtimeout" in error_message.lower():
                    raise BitqueryNetworkTimeoutError(f"Network timeout occurred: {error_message}")
                elif "ActiveRecord::ActiveRecordError" in error_message.lower():
                    raise BitqueryMemoryLimitExceeded(f"Network timeout occurred: {error_message}")
                elif error_type == 'server':
                    raise BitqueryServerError(f"Server error occurred: {error_message}")
                else:
                    raise BitqueryBaseException(f"GraphQL error: {error_message}")

            # Even if we don't have explicit errors, check if we got a valid response structure
            if not json_response.get('data'):
                raise BitqueryDataNotFoundError("No data returned from API")

            return json_response
        except requests.exceptions.Timeout:
            print("Request timed out")
            raise BitqueryNetworkTimeoutError("Request timed out")
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {str(e)}")
            raise BitqueryServerError(f"Request failed: {str(e)}")
        except (BitqueryConcurrentRequestError, BitqueryNetworkTimeoutError, BitqueryMemoryLimitExceeded,
                BitqueryServerError, BitqueryBaseException, BitqueryDataNotFoundError):
            raise
        except Exception as e:
            print(f"Unexpected error during query execution: {str(e)}")
            raise BitqueryServerError(f"Unexpected error: {str(e)}")


class GraphQLInterface:
    def __init__(
            self,
            chain: str,
            source: bool,
            address: str,
            token_address: Optional[str],
            depth_limit: int,
            from_time: datetime,
            till_time: datetime,
            limit: int,
            graphql_client: GraphQLClient
    ):
        self.chain = chain
        self.source = source
        self.address = address
        self.token_address = token_address
        self.depth = depth_limit
        self.from_time = from_time
        self.till_time = str(till_time).replace(" ", "T")
        self.limit = int(limit)
        self._graphql_client = graphql_client

    def call_graphql_endpoint(self) -> List[Dict[str, Any]]:
        """Execute GraphQL query and process response."""
        try:
            request_body = self._graphql_query_builder()
            if not request_body:
                raise InvalidGraphqlQuery(f"Error while forming query")
            response = self._graphql_client.execute_query(request_body)
            return self._process_response(response)
        except Exception:
            raise

    """Process and flatten GraphQL response."""

    def _process_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            response_data = response["data"][Constants.NETWORK_CHAIN_MAPPING_FOR_RESPONSE[self.chain]]["coinpath"]
            if not response_data:
                raise BitqueryDataNotFoundError("No data found for the specified date range.")
            flattened_response = []
            possible_swaps = []
            for item in response_data:
                self._flatten_node(item, flattened_response, possible_swaps, self.token_address)
            if len(possible_swaps) > 0:
                flattened_response = self.update_swap_info(possible_swaps, flattened_response)
            return flattened_response
        except KeyError as e:
            if response and "errors" in response:
                print(f"Bitquery error response: {response['errors']}")
                raise BitqueryServerError(f"API returned errors: {response['errors']}")
            raise BitqueryBaseException("Invalid response structure from API")

    def is_swaps(self, item):
        try:

            dex_keywords = [
                'dex', 'swap', 'exchange', 'uniswap', 'sushiswap',
                'pancakeswap', 'generic'
            ]

            receiver = item.get('receiver', {})
            annotation = receiver.get('annotation', '').lower() if receiver.get('annotation') else ''
            contract_type = (
                receiver.get('smartContract', {}).get('contractType', '') if receiver.get('smartContract') else '')

            if contract_type:
                for keyword in dex_keywords:
                    if keyword in annotation or keyword in contract_type.lower():
                        return True

            return False
        except Exception:
            print("ERROR : is_swaps")
            traceback.print_exc()
            return False

    def _flatten_node(self, item, flattened_response, possible_swaps, token_address):
        """Process and flatten blockchain transaction data"""
        try:
            # Base transaction details common to all chains
            current_iter_dict = {
                "depth": int(safe_get(item, "depth", default=0)),
                "tx_hash": safe_get(item, "transaction", "hash", default=""),
                "sender": safe_get(item, "sender", "address", default=""),
                "receiver": safe_get(item, "receiver", "address", default=""),
                "sender_annotation": safe_get(item, "sender", "annotation", default=""),
                "receiver_annotation": safe_get(item, "receiver", "annotation", default="")
            }

            # XRP and XLM processing
            if self.chain in ["XRP", "XLM"]:
                current_iter_dict.update({
                    "tx_time": safe_get(item, "transaction", "time", "time", default=""),
                    "sent_amount": safe_get(item, "amountFrom", default=0),
                    "sent_tx_value": safe_get(item, "transaction", "valueFrom", default=0),
                    "sent_currency": safe_get(item, "currencyFrom", "symbol", default=""),
                    "received_amount": safe_get(item, "amountTo", default=0),
                    "received_tx_value": safe_get(item, "transaction", "valueTo", default=0),
                    "received_currency": safe_get(item, "currencyTo", "symbol", default=""),
                    "operation_type": safe_get(item, "operation", default=""),
                    "receiver_receive_from_count": safe_get(item, "receiver", "receiversCount", default=0),
                    "receiver_send_to_count": safe_get(item, "receiver", "sendersCount", default=0),
                    "receiver_first_transfer_at": safe_get(item, "receiver", "firstTransferAt", "time"),
                    "receiver_last_transfer_at": safe_get(item, "receiver", "lastTransferAt", "time")
                })

                if self.chain == "XRP":
                    dest_tag = safe_get(item, "destinationTag")
                    if dest_tag:
                        current_iter_dict["destination_tag"] = dest_tag
                    source_tag = safe_get(item, "sourceTag")
                    if source_tag:
                        current_iter_dict["source_tag"] = source_tag

                flattened_response.append(current_iter_dict)
                return

            # Common fields for non-XRP/XLM chains
            current_iter_dict.update({
                "symbol": safe_get(item, "currency", "symbol", default=""),
                "amount": safe_get(item, "amount", default=0),
                "amount_usd": safe_get(item, "amount_usd", default=0)
            })

            # LUNC processing
            if self.chain == "LUNC":
                current_iter_dict.update({
                    "tx_time": safe_get(item, "block", "timestamp", "time", default=""),
                    "tx_value": safe_get(item, "transaction", "value", default=0)
                })
                flattened_response.append(current_iter_dict)
                return

            # Bitcoin-like chains processing
            if self.chain in ["BTC", "BCH", "LTC", "ADA", "DOGE", "ZEC", "DASH"]:
                if self.chain in ["BTC", "DOGE", "DASH"]:
                    if safe_get(item, "receiver", "type") == "coinbase" and not safe_get(item, "receiver", "address"):
                        return

                current_iter_dict.update({
                    "tx_time": safe_get(item, "transactions", 0, "timestamp", default=""),
                    "tx_value_in": safe_get(item, "transaction", "valueIn", default=0),
                    "tx_value_out": safe_get(item, "transaction", "valueOut", default=0)
                })

                if self.chain in ["BTC", "BCH", "LTC", "DOGE", "ZEC", "DASH"]:
                    current_iter_dict.update({
                        "sender_type": safe_get(item, "sender", "type", default="unknown"),
                        "receiver_type": safe_get(item, "receiver", "type", default="unknown")
                    })

                    if self.chain == "ZEC":
                        if current_iter_dict["sender"] == "" and current_iter_dict["sender_type"]:
                            current_iter_dict["sender"] = current_iter_dict["sender_type"]
                        if current_iter_dict["sender"] == "<shielded>" and current_iter_dict[
                            "sender_type"] == "shielded":
                            current_iter_dict["sender"] = "shielded"
                        if current_iter_dict["receiver"] == "" and current_iter_dict["receiver_type"]:
                            current_iter_dict["receiver"] = current_iter_dict["receiver_type"]
                        if current_iter_dict["receiver"] == "<shielded>" and current_iter_dict[
                            "receiver_type"] == "shielded":
                            current_iter_dict["receiver"] = "shielded"

                    flattened_response.append(current_iter_dict)
                    return

                elif self.chain == "ADA":
                    current_iter_dict.update({
                        "sender_type": "unknown",
                        "receiver_type": "unknown"
                    })
                    flattened_response.append(current_iter_dict)
                    return

            # Smart contract chains common fields
            current_iter_dict.update({
                "token_id": safe_get(item, "currency", "tokenId", default=""),
                "token_type": safe_get(item, "currency", "tokenType", default=""),
                "receiver_receivers_count": safe_get(item, "receiver", "receiversCount", default=0),
                "receiver_senders_count": safe_get(item, "receiver", "sendersCount", default=0),
                "receiver_first_tx_at": safe_get(item, "receiver", "firstTxAt", "time"),
                "receiver_last_tx_at": safe_get(item, "receiver", "lastTxAt", "time"),
                "receiver_amount_out": float(safe_get(item, "receiver", "amountOut", default=0)),
                "receiver_amount_in": float(safe_get(item, "receiver", "amountIn", default=0)),
                "receiver_balance": float(safe_get(item, "receiver", "balance", default=0))
            })

            # Ethereum-like chains
            if self.chain in ["ETH", "KLAY", "BSC", "FTM", "POL", "AVAX"]:
                current_iter_dict.update({
                    "token": token_address,
                    "tx_time": safe_get(item, "transactions", 0, "timestamp", default=""),
                    "sender_type": safe_get(item, "sender", "smartContract", "contractType", default="Wallet"),
                    "receiver_type": safe_get(item, "receiver", "smartContract", "contractType", default="Wallet")
                })

                if self.is_swaps(item):
                    # swap_dict = {"flattened_swap_txn": current_iter_dict, "bq_swap_txn": item}
                    # swap_dict = {"flattened_swap_txn": current_iter_dict, "bq_swap_txn": item}
                    possible_swaps.append(current_iter_dict)
                    return

                flattened_response.append(current_iter_dict)
                return

            # BNB, TRX, EOS chains
            current_iter_dict.update({
                "tx_time": safe_get(item, "transaction", "time", "time", default=""),
                "sender_type": safe_get(item, "sender", "type", default="unknown"),
                "receiver_type": safe_get(item, "receiver", "type", default="unknown")
            })

            if self.chain in ["BNB", "TRX"]:
                current_iter_dict["token"] = token_address
                flattened_response.append(current_iter_dict)
                return

            if self.chain == "EOS":
                current_iter_dict["token"] = safe_get(item, "currency", "name", default="")
                flattened_response.append(current_iter_dict)
                return

        except Exception as e:
            print(f"Error in flatten_node for chain {self.chain}: {str(e)}")

    # def _flatten_response1(self, response_data: List[Dict[str, Any]]) -> List[Dict]:
    #     """Flatten response data into desired format."""
    #     flattened_response = []
    #
    #     for item in response_data:
    #         current_iter_dict = {
    #             "depth": item["depth"],
    #             "tx_hash": item["transaction"]["hash"],
    #             "sender": item["sender"]["address"],
    #             "receiver": item["receiver"]["address"],
    #             "sender_annotation": item["sender"]["annotation"] if item["sender"]["annotation"] not in [None,
    #                                                                                                       "None"] else "",
    #             "receiver_annotation": item["receiver"]["annotation"] if item["receiver"]["annotation"] not in [
    #                 None, "None"] else ""
    #         }
    #         # XRP and XLM have the same parameters so they are grouped together
    #         if self.chain in ["XRP", "XLM"]:
    #             current_iter_dict["tx_time"] = item["transaction"]["time"]["time"]
    #             current_iter_dict["sent_amount"] = item["amountFrom"]
    #             current_iter_dict["sent_tx_value"] = item["transaction"]["valueFrom"]
    #             current_iter_dict["sent_currency"] = item["currencyFrom"]["symbol"]
    #             current_iter_dict["received_amount"] = item["amountTo"]
    #             current_iter_dict["received_tx_value"] = item["transaction"]["valueTo"]
    #             current_iter_dict["received_currency"] = item["currencyTo"]["symbol"]
    #             current_iter_dict["operation_type"] = item["operation"]
    #             current_iter_dict["receiver_receive_from_count"] = item["receiver"]["receiversCount"]
    #             current_iter_dict["receiver_send_to_count"] = item["receiver"]["sendersCount"]
    #             current_iter_dict["receiver_first_transfer_at"] = item["receiver"]["firstTransferAt"]["time"]
    #             current_iter_dict["receiver_last_transfer_at"] = item["receiver"]["lastTransferAt"]["time"]
    #             if self.chain == "XRP" and item.get("destinationTag"):
    #                 current_iter_dict["destination_tag"] = item["destinationTag"]
    #             if self.chain == "XRP" and item.get("sourceTag"):
    #                 current_iter_dict["source_tag"] = item["sourceTag"]
    #
    #             flattened_response.append(current_iter_dict)
    #             continue
    #         else:
    #             # The symbol and amount/amount_usd parameters are common to all except XRP and XLM so they are assigned here itself
    #             current_iter_dict["symbol"] = item["currency"]["symbol"]
    #             current_iter_dict["amount"] = item["amount"]
    #             current_iter_dict["amount_usd"] = item["amount_usd"]
    #             if self.chain == "LUNC":
    #                 current_iter_dict["tx_time"] = item["block"]["timestamp"]["time"]
    #                 current_iter_dict["tx_value"] = item["transaction"]["value"]
    #                 flattened_response.append(current_iter_dict)
    #                 continue
    #             else:
    #                 # BTC, BCH, LTC, DOGE, ZEC, DASH and ADA have almost all parameters in common
    #                 # except sender_type and receiver_type
    #                 if self.chain in ["BTC", "BCH", "LTC", "DOGE", "ZEC", "DASH", "ADA"]:
    #                     if self.chain in ["BTC", "DOGE", "DASH"]:
    #                         if item["receiver"]["type"] == "coinbase" and item["receiver"]["address"] == "":
    #                             continue
    #                     current_iter_dict["tx_time"] = item["transactions"][0]["timestamp"]
    #                     current_iter_dict["tx_value_in"] = item["transaction"]["valueIn"]
    #                     current_iter_dict["tx_value_out"] = item["transaction"]["valueOut"]
    #                     if self.chain in ["BTC", "BCH", "LTC", "DOGE", "ZEC", "DASH"]:
    #                         current_iter_dict["sender_type"] = item["sender"]["type"]
    #                         current_iter_dict["receiver_type"] = item["receiver"]["type"]
    #                         if self.chain == "ZEC":
    #                             if current_iter_dict["sender"] == "" and current_iter_dict["sender_type"]:
    #                                 current_iter_dict["sender"] = current_iter_dict["sender_type"]
    #                             if current_iter_dict["sender"] == "<shielded>" and current_iter_dict[
    #                                 "sender_type"] == "shielded":
    #                                 current_iter_dict["sender"] = "shielded"
    #                             if current_iter_dict["receiver"] == "" and current_iter_dict["receiver_type"]:
    #                                 current_iter_dict["receiver"] = current_iter_dict["receiver_type"]
    #                             if current_iter_dict["receiver"] == "<shielded>" and current_iter_dict[
    #                                 "receiver_type"] == "shielded":
    #                                 current_iter_dict["receiver"] = "shielded"
    #                         flattened_response.append(current_iter_dict)
    #                         continue
    #                     elif self.chain == "ADA":
    #                         current_iter_dict["sender_type"] = "unknown"
    #                         current_iter_dict["receiver_type"] = "unknown"
    #                         flattened_response.append(current_iter_dict)
    #                         continue
    #                 else:
    #                     # the parameters below are common to all the following blockchains
    #                     current_iter_dict["token_id"] = item["currency"]["tokenId"]
    #                     current_iter_dict["token_type"] = item["currency"]["tokenType"]
    #                     current_iter_dict["receiver_receivers_count"] = item["receiver"]["receiversCount"]
    #                     current_iter_dict["receiver_senders_count"] = item["receiver"]["sendersCount"]
    #                     current_iter_dict["receiver_first_tx_at"] = item["receiver"]["firstTxAt"]["time"]
    #                     current_iter_dict["receiver_last_tx_at"] = item["receiver"]["lastTxAt"]["time"]
    #                     current_iter_dict["receiver_amount_out"] = float(item["receiver"]["amountOut"])
    #                     current_iter_dict["receiver_amount_in"] = float(item["receiver"]["amountIn"])
    #                     current_iter_dict["receiver_balance"] = float(item["receiver"]["balance"])
    #                     if self.chain in ["ETH", "KLAY", "BSC", "FTM", "POL", "AVAX"]:
    #                         current_iter_dict["token"] = self.token_address
    #                         current_iter_dict["tx_time"] = item["transactions"][0]["timestamp"]
    #                         current_iter_dict["sender_type"] = item["sender"]["smartContract"]["contractType"] if \
    #                             item["sender"]["smartContract"]["contractType"] not in [None, "None"] else "Wallet"
    #                         current_iter_dict["receiver_type"] = item["receiver"]["smartContract"][
    #                             "contractType"] if item["receiver"]["smartContract"]["contractType"] not in [None,
    #                                                                                                          "None"] else "Wallet"
    #                         flattened_response.append(current_iter_dict)
    #                         continue
    #                     else:
    #                         current_iter_dict["tx_time"] = item["transaction"]["time"]["time"]
    #                         current_iter_dict["sender_type"] = item["sender"]["type"]
    #                         current_iter_dict["receiver_type"] = item["receiver"]["type"]
    #                         if self.chain in ["BNB", "TRX"]:
    #                             current_iter_dict["token"] = self.token_address
    #                             flattened_response.append(current_iter_dict)
    #                             continue
    #                         if self.chain == "EOS":
    #                             current_iter_dict["token"] = item["currency"]["name"]
    #                             flattened_response.append(current_iter_dict)
    #                             continue
    #                             # Once the loop has run its course, the flattened response array is returned
    #
    #     return flattened_response

    # def _graphql_query_builder1(self):
    #     # define the direction of transaction flow:
    #     direction = "inbound" if self.source else "outbound"
    #     # define starter query parameter modules (these will be modified based on the chain)
    #     amount_details = " amountOut amountIn balance "
    #     smart_contract = " smartContract { contractType } "
    #     common_receiver_query = " receiver { address annotation receiversCount sendersCount "
    #     # Adding the params common to most blockchains first, these are modified later
    #     currency = " "
    #     receiver = "receiver { address annotation } "
    #     sender = receiver.replace("receiver", "sender")
    #     extra_params = " depth amount amount_usd: amount(in: USD) currency { symbol } "
    #     time = " var { time } "
    #     if self.token_address is not None and self.token_address != "" and self.token_address != '0x0000000000000000000000000000000000000000':
    #         currency_value = self.token_address
    #     else:
    #         currency_value = Constants.GRAPHQL_CURRENCY_MAPPING.get(self.chain, None)
    #     network = Constants.NETWORK_CHAIN_MAPPING_FOR_RESPONSE[self.chain] + \
    #               " (network: " + Constants.NETWORK_CHAIN_MAPPING_FOR_QUERY[self.chain] + " ) "
    #     destination_tag = " "
    #     source_tag = " "
    #     try:
    #         # Cardano or ADA
    #         if self.chain == "ADA":
    #             transaction = " transaction { hash valueIn valueOut } transactions { timestamp } "
    #         #  TERRA or LUNC
    #         elif self.chain == "LUNC":
    #             transaction = """ transaction { hash value } block { timestamp { time ( format: "%Y-%m-%d" ) } } """
    #         # Ripple/Stellar or XRP/XLM
    #         elif self.chain in ["XRP", "XLM"]:
    #             receiver = common_receiver_query + time.replace("var", "firstTransferAt") + " " + \
    #                        time.replace("var", "lastTransferAt") + " } "
    #             sender = " sender { address annotation " + time.replace("var", "firstTransferAt") + " " + \
    #                      time.replace("var", "lastTransferAt") + " } "
    #             transaction = " transaction { hash " + time.replace("var", "time") + " valueFrom valueTo  }"
    #             extra_params = " depth  amountFrom amountTo operation currencyFrom { name symbol } currencyTo { name symbol } "
    #             if self.chain == "XRP":
    #                 destination_tag = " destinationTag"
    #                 source_tag = " sourceTag"
    #         # Bitcoin Cash/Litecoin or BCH/LTC
    #         elif self.chain in ["BTC", "BCH", "LTC", "DOGE", "ZEC", "DASH"]:
    #             receiver = common_receiver_query + time.replace("var", "firstTxAt") + \
    #                        " " + time.replace("var", "lastTxAt") + " type } "
    #             sender = " sender { address annotation type " + \
    #                      time.replace("var", "firstTxAt") + \
    #                      " " + time.replace("var", "lastTxAt") + " } "
    #             transaction = " transaction { hash  valueIn valueOut } transactions { timestamp } "
    #         # EOS
    #         elif self.chain == "EOS":
    #             receiver = common_receiver_query + time.replace("var", "firstTxAt") + \
    #                        " " + time.replace("var", "lastTxAt") + " type " + amount_details + " } "
    #             sender = " sender { address annotation type } "
    #             transaction = " transaction { hash value " + time.replace("var", "time") + " } "
    #             extra_params = " depth amount amount_usd: amount(in: USD) currency { name symbol tokenId tokenType } "  # Klaytn/Binance Smart Chain or KLAY/BSC
    #         elif self.chain in ["ETH", "KLAY", "BSC", "FTM", "POL", "AVAX"]:
    #             currency = f""" currency: {{ is: "{currency_value}" }} """ if currency_value else " "
    #             receiver = common_receiver_query + amount_details + \
    #                        time.replace("var", "firstTxAt") + " " + \
    #                        time.replace("var", "lastTxAt") + \
    #                        " type " + smart_contract + " } "
    #             sender = " sender { address annotation type " + amount_details + smart_contract + " }"
    #             transaction = " transaction { hash value } " + \
    #                           " transactions { timestamp txHash txValue amount height } "
    #             extra_params = " depth amount amount_usd: amount(in: USD) currency { name symbol tokenId tokenType address } "
    #
    #             # Binance Coin/Tron or BNB/TRX
    #         elif self.chain in ["BNB", "TRX"]:
    #             currency = f""" currency: {{ is: "{currency_value}" }} """
    #             receiver = common_receiver_query + time.replace("var", "firstTxAt") + \
    #                        " " + time.replace("var", "lastTxAt") + " type " + amount_details + " } "
    #             sender = " sender { address annotation type } "
    #             network = network if self.chain == "TRX" else Constants.NETWORK_CHAIN_MAPPING_FOR_RESPONSE[self.chain]
    #             transaction = " transaction { hash value " + time.replace("var", "time") + " } "
    #             extra_params = " depth amount amount_usd: amount(in: USD) currency { name symbol tokenId tokenType } "
    #
    #         # building final GraphQL query
    #         graphql_query = f"""
    #             query sentinel_query {{
    #                 {network} {{
    #                     coinpath(
    #                     options: {{ direction: {direction}, asc: "depth", limit: {self.limit} }}
    #                     initialAddress: {{ is: "{self.address}" }}
    #                     depth: {{ lteq: {self.depth} }}
    #                     date: {{ since: "{self.from_time}", till: "{self.till_time}" }}
    #                     {currency}
    #                     ) {{
    #                         {destination_tag}
    #                         {source_tag}
    #                         {receiver}
    #                         {sender}
    #                         {transaction}
    #                         {extra_params}
    #                     }}
    #                 }}
    #             }}
    #             """
    #         return graphql_query
    #     except Exception:
    #         traceback.print_exc()
    #         return None

    def _graphql_query_builder(self) -> str:
        """Build GraphQL query using templates"""
        try:
            from_time = validate_dateformat_and_randomize_seconds(self.from_time, "%Y-%m-%dT%H:%M:%S")
            from_time = str(from_time).replace(" ", "T")

            template, params = self._get_template_and_params(from_time)
            return template.safe_substitute(params)
        except Exception:
            traceback.print_exc()
            return None

    def _get_template_and_params(self, from_time: str) -> tuple:
        """Determine which template to use and prepare its parameters"""
        template_key = Constants.CHAIN_TEMPLATE_MAPPING[self.chain]
        template = Constants.CATV_QUERY_TEMPLATES[template_key]

        params = {
            'network': f"{Constants.NETWORK_CHAIN_MAPPING_FOR_RESPONSE[self.chain]} (network: {Constants.NETWORK_CHAIN_MAPPING_FOR_QUERY[self.chain]})",
            'direction': "inbound" if self.source else "outbound",
            'address': self.address,
            'depth': self.depth,
            'from_time': from_time,
            'till_time': self.till_time,
            'limit': self.limit,
            'currency': "",
            'tags': ""
        }

        # Handle currency parameter for token-supporting chains
        if template_key in ["ETHEREUM_LIKE", "BINANCE_TRON"]:
            if self.token_address and self.token_address != '0x0000000000000000000000000000000000000000':
                params['currency'] = f'currency: {{ is: "{self.token_address}" }}'
            elif self.chain not in ["FTM", "POL", "AVAX"]:  # Only set default currency for specific chains
                currency_value = Constants.GRAPHQL_CURRENCY_MAPPING.get(self.chain)
                params['currency'] = f'currency: {{ is: "{currency_value}" }}' if currency_value else ""
            # Handle special tags for XRP
        elif template_key == "RIPPLE_STELLAR":
            params['tags'] = "destinationTag sourceTag" if self.chain == "XRP" else ""

        return template, params

    def update_swap_info(self, possible_swaps, flattened_response):
        try:
            # Get logical CPU count for optimal threading
            cpu_count = os.cpu_count()
            # Limit workers to a reasonable number (min of CPU count and max 8)
            worker_count = min(cpu_count or 4, 8)

            print(f"{len(possible_swaps)=}")
            # Process in batches rather than creating a thread per swap
            with ThreadPool(processes=worker_count) as pool:
                results = pool.map(self.process_swap, possible_swaps)

            # Use a list comprehension with filtering in one pass
            valid_swaps = []
            for result in results:
                if result is not None:  # Not None check
                    valid_swaps.extend(result)  # Add all items from the result list
            reverse_swap_txns = TracerAPIInterface.create_reverse_swap_transactions(valid_swaps)
            flattened_response.extend(valid_swaps)
            flattened_response.extend(reverse_swap_txns)
            return flattened_response
        except Exception as e:
            print(f"ERROR : get_tx_with_swaps: {str(e)}")
            traceback.print_exc()
            return None

    def process_swap(self, incoming_swap_txn):
        tx_hash = incoming_swap_txn["tx_hash"]
        request_body = self._graphql_dex_trades_query_builder(tx_hash)
        if request_body is None or len(request_body) == 0:
            return []
        try:
            r = requests.post(settings.GRAPHQL_ENDPOINT, json={'query': request_body}, headers={'X-API-KEY': settings.GRAPHQL_X_API_KEY},
                              timeout=(60, 300))
            response_object = r.json()
            dex_trades = response_object["data"][Constants.NETWORK_CHAIN_MAPPING_FOR_RESPONSE[self.chain]]["dexTrades"]
            if len(dex_trades) < 1:
                return [incoming_swap_txn]
            # swap_aggregator_node = dex_trades[0]['smartContract']['address']['address']
            # # check if receiver is the
            # if swap_aggregator_node != incoming_swap_txn["receiver"]:
            #     print("Inside if swap_aggregator_node != incoming_swap_txn ")
            #     return None
            # token_address = dex_trades[-1]['sellCurrency']['address']
            # from_time = dex_trades[-1]['block']['timestamp']['time']
            # new_amount = dex_trades[-1]['sellAmount']
            # new_amount_usd = dex_trades[-1]['sell_amount_usd']
            #
            # new_currency = {
            #     "name": dex_trades[-1]['sellCurrency']['name'],
            #     "symbol": dex_trades[-1]['sellCurrency']['symbol'],
            #     "address": token_address
            # }

            # add swap info into the incoming swap txn
            updated_incoming_swap_txn = self.add_swap_info(incoming_swap_txn, dex_trades)
            return [updated_incoming_swap_txn]
        except Exception:
            print("ERROR : process_swap")
            traceback.print_exc()
            return [incoming_swap_txn]

    def add_swap_info(self, incoming_swap_txn: dict, dex_trades: list) -> dict:
        """
        Adds swap_info to the transaction object based on dex trades data.

        Args:
            swap_txn: Flattened transaction object
            dex_trades: List of dex trades from the API response

        Returns:
            Updated transaction object with swap_info
        """
        print(f"DEX TRADES: {json.dumps(dex_trades, indent=2)}")
        try:
            # Get first and last trade
            first_trade = dex_trades[0]
            last_trade = dex_trades[-1]

            # Create swap_info object
            swap_info = {
                "protocol": first_trade["protocol"],
                "amount_in": str(first_trade("buyAmount")),
                "amount_out": str(last_trade["sellAmount"]),
                "token_in": {
                    "address": first_trade["buyCurrency"]["address"],
                    "symbol": first_trade["buyCurrency"]["symbol"]
                },
                "token_out": {
                    "address": last_trade["sellCurrency"]["address"],
                    "symbol": last_trade["sellCurrency"]["symbol"]
                },
                "pool_address": first_trade["smartContract"]["address"]["address"]
            }

            # Add swap_info to incoming swap transaction
            incoming_swap_txn["swap_info"] = swap_info
            incoming_swap_txn["is_swap"] = True
            return incoming_swap_txn
        except Exception as e:
            print(f"Error creating swap info: {str(e)}")
            return incoming_swap_txn

    def _graphql_dex_trades_query_builder(self, tx_hash):

        network = Constants.NETWORK_CHAIN_MAPPING_FOR_RESPONSE[self.chain] + \
                  " (network: " + Constants.NETWORK_CHAIN_MAPPING_FOR_QUERY[self.chain] + " ) "

        try:
            GRAPHQL_DEX_QUERY = f"""
                query sentinel_query {{
                    {network} {{
                        dexTrades(
                        txHash: {{ is: "{tx_hash}" }}
                        ) {{
                            block {{
                                timestamp {{
                                time(format: "%Y-%m-%d %H:%M:%S")
                                }}
                                height
                            }}
                            tradeIndex
                            protocol
                            exchange {{
                                fullName
                            }}
                            smartContract {{
                                address {{
                                address
                                annotation
                                }}
                            }}
                            buyAmount
                            buy_amount_usd: buyAmount(in: USD)
                            buyCurrency {{
                                address
                                symbol
                            }}
                            sellAmount
                            sell_amount_usd: sellAmount(in: USD)
                            sellCurrency {{
                                address
                                name
                                symbol
                            }}
                        }}
                    }}
                }}   
                """
            return GRAPHQL_DEX_QUERY
        except Exception as e:
            traceback.print_exc()
            return None
