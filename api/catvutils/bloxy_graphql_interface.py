import traceback
from datetime import datetime
from typing import Optional, Any, Dict, List

import requests
from django.conf import settings
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from api.constants import Constants
from api.exceptions import BitqueryConcurrentRequestError, BitqueryNetworkTimeoutError, \
    BitqueryServerError, BitqueryBaseException, BitqueryDataNotFoundError, BitqueryMemoryLimitExceeded, \
    InvalidGraphqlQuery
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
            backoff_factor=5,
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

    def execute(self, query: str) -> Dict[str, Any]:
        try:
            print(f"query: {query}")
            response = self.session.post(
                self.endpoint,
                json={'query': query},
                headers=self.headers,
                timeout=self.timeout
            )

            print(f"X-Graphql-Query-Id: {response.headers['x-graphql-query-id']} {response}")
            print(f"{response.json() =}")

            # Raise for any HTTP error status codes
            response.raise_for_status()

            # Parse and check for GraphQL-level errorsexc
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
        except Exception:
            traceback.print_exc()
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
        try:
            request_body = self._graphql_query_builder()
            if not request_body:
                raise InvalidGraphqlQuery(f"Error while forming query")
            response = self._graphql_client.execute(request_body)
            return self._process_response(response)
        except Exception:
            raise

    """Process and flatten GraphQL response."""

    def _process_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            response_data = response["data"][Constants.NETWORK_CHAIN_MAPPING_FOR_RESPONSE[self.chain]]["coinpath"]
            print(f"{response_data=}")
            if not response_data:
                raise BitqueryDataNotFoundError("No data found for the specified date range.")
            return self._flatten_response(response_data)
        except KeyError as e:
            if response and "errors" in response:
                print(f"Bitquery error response: {response['errors']}")
                raise BitqueryServerError(f"API returned errors: {response['errors']}")
            raise BitqueryBaseException("Invalid response structure from API")

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
                # The symbol and amount/amount_usd parameters are common to all except XRP and XLM so they are assigned here itself
                current_iter_dict["symbol"] = item["currency"]["symbol"]
                current_iter_dict["amount"] = item["amount"]
                current_iter_dict["amount_usd"] = item["amount_usd"]
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
        extra_params = " depth amount amount_usd: amount(in: USD) currency { symbol } "
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
                extra_params = " depth amount amount_usd: amount(in: USD) currency { name symbol tokenId tokenType } "  # Klaytn/Binance Smart Chain or KLAY/BSC
            elif self.chain in ["ETH", "KLAY", "BSC", "FTM", "POL", "AVAX"]:
                currency = f""" currency: {{ is: "{currency_value}" }} """ if currency_value else " "
                receiver = common_receiver_query + amount_details + \
                           time.replace("var", "firstTxAt") + " " + \
                           time.replace("var", "lastTxAt") + \
                           " type " + smart_contract + " } "
                sender = " sender { address annotation type " + amount_details + smart_contract + " }"
                transaction = " transaction { hash value } " + \
                              " transactions { timestamp txHash txValue amount height } "
                extra_params = " depth amount amount_usd: amount(in: USD) currency { name symbol tokenId tokenType address } "

                # Binance Coin/Tron or BNB/TRX
            elif self.chain in ["BNB", "TRX"]:
                currency = f""" currency: {{ is: "{currency_value}" }} """
                receiver = common_receiver_query + time.replace("var", "firstTxAt") + \
                           " " + time.replace("var", "lastTxAt") + " type " + amount_details + " } "
                sender = " sender { address annotation type } "
                network = network if self.chain == "TRX" else Constants.NETWORK_CHAIN_MAPPING_FOR_RESPONSE[self.chain]
                transaction = " transaction { hash value " + time.replace("var", "time") + " } "
                extra_params = " depth amount amount_usd: amount(in: USD) currency { name symbol tokenId tokenType } "

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
        except Exception:
            traceback.print_exc()
            return None
