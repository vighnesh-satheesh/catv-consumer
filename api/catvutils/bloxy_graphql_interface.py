import time
import traceback
from datetime import datetime
from typing import Optional, Any, Dict, List

import requests
from django.conf import settings
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from api.constants import Constants
from api.exceptions import BitqueryFetchTimedOut
from api.utils import validate_dateformat_and_randomize_seconds


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
            backoff_factor=1,
            status_forcelist=[502, 503, 504],
            allowed_methods=["POST"]
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

    def execute(self, query: str) -> Dict[str, Any]:
        """Execute GraphQL query with retry logic for gateway errors."""
        max_retries = 2
        initial_delay = 1
        max_delay = 13
        retriable_status_codes = {502, 503, 504}

        for attempt in range(max_retries):
            try:
                print(f"query: {query}")
                response = self.session.post(
                    self.endpoint,
                    json={'query': query},
                    headers=self.headers,
                    timeout=self.timeout
                )
                print(f"X-Graphql-Query-Id: {response.headers['x-graphql-query-id']}")
                # For gateway errors, implement retry
                if response.status_code in retriable_status_codes:
                    if attempt == max_retries - 1:  # Last attempt
                        response.raise_for_status()

                    delay = min(initial_delay * (2 ** attempt), max_delay)  # exponential backoff
                    print(
                        f"Gateway error (status: {response.status_code}) on attempt {attempt + 1}/{max_retries}. Retrying in {delay} seconds")
                    time.sleep(delay)
                    continue

                # Raise for other error status codes
                response.raise_for_status()

                # Check for GraphQL-level errors
                json_response = response.json()
                if 'errors' in json_response:
                    error_message = str(json_response.get('errors'))
                    print(f"GraphQL returned errors: {error_message}", )
                    raise BitqueryFetchTimedOut

                return json_response

            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:  # Last attempt
                    print(f"Request failed after {max_retries} retries: {str(e)}", max_retries, str(e))
                    raise

                # Only retry gateway errors
                if isinstance(e,
                              requests.exceptions.HTTPError) and e.response.status_code not in retriable_status_codes:
                    raise

                delay = min(initial_delay * (2 ** attempt), max_delay)
                print(f"Request failed on attempt {attempt + 1}/{max_retries}: {str(e)}. Retrying in {delay} seconds...")
                time.sleep(delay)
            except Exception as e:
                raise BitqueryFetchTimedOut

        # If we get here somehow, raise the last exception
        raise BitqueryFetchTimedOut


class BloxyAPIInterface:
    def __init__(self):
        self._graphql_client = GraphQLClient(
            endpoint=settings.GRAPHQL_ENDPOINT,
            headers={'X-API-KEY': settings.GRAPHQL_X_API_KEY},
            timeout=(60, 300)
        )

    def get_transactions(
            self,
            address: str,
            tx_limit: int,
            depth_limit: int = 2,
            from_time: datetime = datetime(2015, 1, 1, 0, 0),
            till_time: datetime = datetime.now(),
            token_address: Optional[str] = None,
            source: bool = True,
            chain: str = 'ETH'
    ) -> List[Dict[str, Any]]:
        """
        Get transaction data with automatic retries and error handling.
        """
        try:
            graphql_interface = GraphQLInterface(
                chain=chain,
                source=source,
                address=address,
                token_address=token_address,
                depth_limit=depth_limit,
                from_time=from_time,
                till_time=till_time,
                limit=tx_limit,
                graphql_client=self._graphql_client
            )
            return graphql_interface.call_graphql_endpoint()
        except Exception as e:
            print(f"Failed to get transactions: {str(e)}")
            raise


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
        self.from_time = str(validate_dateformat_and_randomize_seconds(
            from_time, '%Y-%m-%d', "%Y-%m-%dT%H:%M:%S"
        )).replace(" ", "T")
        self.till_time = str(till_time).replace(" ", "T")
        self.limit = int(limit)
        self._graphql_client = graphql_client

    def call_graphql_endpoint(self) -> List[Dict[str, Any]]:
        """Execute GraphQL query and process response."""
        request_body = self._graphql_query_builder()
        if not request_body:
            print("Error while forming query")
            return []
        try:
            response = self._graphql_client.execute(request_body)
            return self._process_response(response)
        except Exception as e:
            print(f"Failed to execute GraphQL query: {str(e)}")
            raise

    def _process_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process and flatten GraphQL response."""
        try:
            response_data = response["data"][Constants.NETWORK_CHAIN_MAPPING_FOR_RESPONSE[self.chain]]["coinpath"]
            return self._flatten_response(response_data)
        except KeyError as e:
            if response and "errors" in response:
                print(f"Bitquery error response: {response['errors']}")
            return []

    def _flatten_response(self, response_data: List[Dict[str, Any]]) -> List[Dict]:
        """Flatten response data into desired format."""
        flattened_response = []

        for item in response_data:
            current_iter_dict = {
                "depth": item["depth"],
                "tx_hash": item["transaction"]["hash"],
                "sender": item["sender"]["address"],
                "receiver": item["receiver"]["address"],
                "sender_annotation": item["sender"]["annotation"] if item["sender"]["annotation"] not in [None,
                                                                                                          "None"] else "",
                "receiver_annotation": item["receiver"]["annotation"] if item["receiver"]["annotation"] not in [
                    None, "None"] else ""
            }
            # XRP and XLM have the same parameters so they are grouped together
            if self.chain in ["XRP", "XLM"]:
                current_iter_dict["tx_time"] = item["transaction"]["time"]["time"]
                current_iter_dict["sent_amount"] = item["amountFrom"]
                current_iter_dict["sent_tx_value"] = item["transaction"]["valueFrom"]
                current_iter_dict["sent_currency"] = item["currencyFrom"]["symbol"]
                current_iter_dict["received_amount"] = item["amountTo"]
                current_iter_dict["received_tx_value"] = item["transaction"]["valueTo"]
                current_iter_dict["received_currency"] = item["currencyTo"]["symbol"]
                current_iter_dict["operation_type"] = item["operation"]
                current_iter_dict["receiver_receive_from_count"] = item["receiver"]["receiversCount"]
                current_iter_dict["receiver_send_to_count"] = item["receiver"]["sendersCount"]
                current_iter_dict["receiver_first_transfer_at"] = item["receiver"]["firstTransferAt"]["time"]
                current_iter_dict["receiver_last_transfer_at"] = item["receiver"]["lastTransferAt"]["time"]
                if self.chain == "XRP" and item.get("destinationTag"):
                    current_iter_dict["destination_tag"] = item["destinationTag"]
                if self.chain == "XRP" and item.get("sourceTag"):
                    current_iter_dict["source_tag"] = item["sourceTag"]

                flattened_response.append(current_iter_dict)
                continue
            else:
                # The symbol and amount parameters are common to all except XRP and XLM so they are assigned here itself
                current_iter_dict["symbol"] = item["currency"]["symbol"]
                current_iter_dict["amount"] = item["amount"]
                if self.chain == "LUNC":
                    current_iter_dict["tx_time"] = item["block"]["timestamp"]["time"]
                    current_iter_dict["tx_value"] = item["transaction"]["value"]
                    flattened_response.append(current_iter_dict)
                    continue
                else:
                    # BTC, BCH, LTC, DOGE, ZEC, DASH and ADA have almost all parameters in common
                    # except sender_type and receiver_type
                    if self.chain in ["BTC", "BCH", "LTC", "DOGE", "ZEC", "DASH", "ADA"]:
                        if self.chain in ["BTC", "DOGE", "DASH"]:
                            if item["receiver"]["type"] == "coinbase" and item["receiver"]["address"] == "":
                                continue
                        current_iter_dict["tx_time"] = item["transactions"][0]["timestamp"]
                        current_iter_dict["tx_value_in"] = item["transaction"]["valueIn"]
                        current_iter_dict["tx_value_out"] = item["transaction"]["valueOut"]
                        if self.chain in ["BTC", "BCH", "LTC", "DOGE", "ZEC", "DASH"]:
                            current_iter_dict["sender_type"] = item["sender"]["type"]
                            current_iter_dict["receiver_type"] = item["receiver"]["type"]
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
                            continue
                        elif self.chain == "ADA":
                            current_iter_dict["sender_type"] = "unknown"
                            current_iter_dict["receiver_type"] = "unknown"
                            flattened_response.append(current_iter_dict)
                            continue
                    else:
                        # the parameters below are common to all the following blockchains
                        current_iter_dict["token_id"] = item["currency"]["tokenId"]
                        current_iter_dict["token_type"] = item["currency"]["tokenType"]
                        current_iter_dict["receiver_receivers_count"] = item["receiver"]["receiversCount"]
                        current_iter_dict["receiver_senders_count"] = item["receiver"]["sendersCount"]
                        current_iter_dict["receiver_first_tx_at"] = item["receiver"]["firstTxAt"]["time"]
                        current_iter_dict["receiver_last_tx_at"] = item["receiver"]["lastTxAt"]["time"]
                        current_iter_dict["receiver_amount_out"] = float(item["receiver"]["amountOut"])
                        current_iter_dict["receiver_amount_in"] = float(item["receiver"]["amountIn"])
                        current_iter_dict["receiver_balance"] = float(item["receiver"]["balance"])
                        if self.chain in ["ETH", "KLAY", "BSC", "FTM", "POL", "AVAX"]:
                            current_iter_dict["token"] = self.token_address
                            current_iter_dict["tx_time"] = item["transactions"][0]["timestamp"]
                            current_iter_dict["sender_type"] = item["sender"]["smartContract"]["contractType"] if \
                                item["sender"]["smartContract"]["contractType"] not in [None, "None"] else "Wallet"
                            current_iter_dict["receiver_type"] = item["receiver"]["smartContract"][
                                "contractType"] if item["receiver"]["smartContract"]["contractType"] not in [None,
                                                                                                             "None"] else "Wallet"
                            flattened_response.append(current_iter_dict)
                            continue
                        else:
                            current_iter_dict["tx_time"] = item["transaction"]["time"]["time"]
                            current_iter_dict["sender_type"] = item["sender"]["type"]
                            current_iter_dict["receiver_type"] = item["receiver"]["type"]
                            if self.chain in ["BNB", "TRX"]:
                                current_iter_dict["token"] = self.token_address
                                flattened_response.append(current_iter_dict)
                                continue
                            if self.chain == "EOS":
                                current_iter_dict["token"] = item["currency"]["name"]
                                flattened_response.append(current_iter_dict)
                                continue
                                # Once the loop has run its course, the flattened response array is returned

        return flattened_response

    def _graphql_query_builder(self):
        # define the direction of transaction flow:
        direction = "inbound" if self.source else "outbound"
        # define starter query parameter modules (these will be modified based on the chain)
        amount_details = " amountOut amountIn balance "
        smart_contract = " smartContract { contractType } "
        common_receiver_query = " receiver { address annotation receiversCount sendersCount "
        # Adding the params common to most blockchains first, these are modified later
        currency = " "
        receiver = "receiver { address annotation } "
        sender = receiver.replace("receiver", "sender")
        extra_params = " depth amount currency { symbol } "
        time = " var { time } "
        if self.token_address is not None and self.token_address != "" and self.token_address != '0x0000000000000000000000000000000000000000':
            currency_value = self.token_address
        else:
            currency_value = Constants.GRAPHQL_CURRENCY_MAPPING.get(self.chain, None)
        network = Constants.NETWORK_CHAIN_MAPPING_FOR_RESPONSE[self.chain] + \
                  " (network: " + Constants.NETWORK_CHAIN_MAPPING_FOR_QUERY[self.chain] + " ) "
        destination_tag = " "
        source_tag = " "
        try:
            # Cardano or ADA
            if self.chain == "ADA":
                transaction = " transaction { hash valueIn valueOut } transactions { timestamp } "
            #  TERRA or LUNC
            elif self.chain == "LUNC":
                transaction = """ transaction { hash value } block { timestamp { time ( format: "%Y-%m-%d" ) } } """
            # Ripple/Stellar or XRP/XLM
            elif self.chain in ["XRP", "XLM"]:
                receiver = common_receiver_query + time.replace("var", "firstTransferAt") + " " + \
                           time.replace("var", "lastTransferAt") + " } "
                sender = " sender { address annotation " + time.replace("var", "firstTransferAt") + " " + \
                         time.replace("var", "lastTransferAt") + " } "
                transaction = " transaction { hash " + time.replace("var", "time") + " valueFrom valueTo  }"
                extra_params = " depth  amountFrom amountTo operation currencyFrom { name symbol } currencyTo { name symbol } "
                if self.chain == "XRP":
                    destination_tag = " destinationTag"
                    source_tag = " sourceTag"
            # Bitcoin Cash/Litecoin or BCH/LTC
            elif self.chain in ["BTC", "BCH", "LTC", "DOGE", "ZEC", "DASH"]:
                receiver = common_receiver_query + time.replace("var", "firstTxAt") + \
                           " " + time.replace("var", "lastTxAt") + " type } "
                sender = " sender { address annotation type " + \
                         time.replace("var", "firstTxAt") + \
                         " " + time.replace("var", "lastTxAt") + " } "
                transaction = " transaction { hash  valueIn valueOut } transactions { timestamp } "
            # EOS
            elif self.chain == "EOS":
                receiver = common_receiver_query + time.replace("var", "firstTxAt") + \
                           " " + time.replace("var", "lastTxAt") + " type " + amount_details + " } "
                sender = " sender { address annotation type } "
                transaction = " transaction { hash value " + time.replace("var", "time") + " } "
                extra_params = " depth amount  currency { name symbol tokenId tokenType } "
                # Klaytn/Binance Smart Chain or KLAY/BSC
            elif self.chain in ["ETH", "KLAY", "BSC", "FTM", "POL", "AVAX"]:
                currency = f""" currency: {{ is: "{currency_value}" }} """ if currency_value else " "
                receiver = common_receiver_query + amount_details + \
                           time.replace("var", "firstTxAt") + " " + \
                           time.replace("var", "lastTxAt") + \
                           " type " + smart_contract + " } "
                sender = " sender { address annotation type " + amount_details + smart_contract + " }"
                transaction = " transaction { hash value } " + \
                              " transactions { timestamp txHash txValue amount height } "
                extra_params = " depth amount  currency { name symbol tokenId tokenType address } "

                # Binance Coin/Tron or BNB/TRX
            elif self.chain in ["BNB", "TRX"]:
                currency = f""" currency: {{ is: "{currency_value}" }} """
                receiver = common_receiver_query + time.replace("var", "firstTxAt") + \
                           " " + time.replace("var", "lastTxAt") + " type " + amount_details + " } "
                sender = " sender { address annotation type } "
                network = network if self.chain == "TRX" else Constants.NETWORK_CHAIN_MAPPING_FOR_RESPONSE[self.chain]
                transaction = " transaction { hash value " + time.replace("var", "time") + " } "
                extra_params = " depth amount  currency { name symbol tokenId tokenType } "

            # building final GraphQL query
            graphql_query = f"""
                query sentinel_query {{
                    {network} {{
                        coinpath(
                        options: {{ direction: {direction}, asc: "depth", limit: {self.limit} }}
                        initialAddress: {{ is: "{self.address}" }}
                        depth: {{ lteq: {self.depth} }}
                        date: {{ since: "{self.from_time}", till: "{self.till_time}" }}
                        {currency}
                        ) {{
                            {destination_tag}
                            {source_tag}
                            {receiver}
                            {sender}
                            {transaction}
                            {extra_params}
                        }}
                    }}
                }}   
                """
            return graphql_query
        except Exception as e:
            traceback.print_exc()
            return None
