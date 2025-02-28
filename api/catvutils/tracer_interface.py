import json
import traceback
from typing import List, Dict, Any, Optional

import requests
# from django.conf import settings

from api.catvutils.transactions_api_interface import TransactionAPIInterface


class TracerAPIInterface(TransactionAPIInterface):
    """Interface for Tracer API"""

    def __init__(self):
        # self._api_url = settings.TRACER_ENDPOINT
        self._api_url = "https://stgtracer-api.sentinelprotocol.io/"
        self._timeout = (60, 300)

    def get_transactions(
            self,
            address: str,
            tx_limit: int,
            depth_limit: int = 2,
            from_time: str = None,
            till_time: str = None,
            token_address: Optional[str] = None,
            source: bool = True,
            chain: str = 'ETH'
    ) -> List[Dict[str, Any]]:
        """
        Get transaction data from Tracer API.
        """
        try:
            # Convert chain to chain_id
            chain_id = self._get_chain_id(chain)

            # Format the start_datetime if needed
            # Assuming from_time is in format "YYYY-MM-DD" and needs to be converted
            # to "YYYY-MM-DDT00:00:00.000Z" format
            if from_time and 'T' not in from_time:
                start_datetime = f"{from_time}T00:00:00.000Z"
            else:
                start_datetime = from_time
            till_time = f"{till_time}Z"
            # Prepare request body
            request_body = {
                "chain_type": "evm",
                "chain_id": chain_id,
                "start_address": address,
                "start_datetime": start_datetime,
                "end_datetime": till_time,
                "max_hops": depth_limit,
                "max_workers": 5,
                "tokens": [
                    token_address] if token_address and token_address != "0x0000000000000000000000000000000000000000" else []
            }
            endpoint = 'trace-inbound' if source else "trace-outbound"
            self._api_url += endpoint
            print(f"Calling Tracer API: {self._api_url} body: {json.dumps(request_body)}")
            # Make API call
            response = requests.post(
                self._api_url,
                json=request_body,
                timeout=self._timeout
            )
            response.raise_for_status()  # Raise exception for HTTP errors

            # Process and return data in the same format as BitqueryAPIInterface
            return self._process_response(response.json(), source)

        except Exception:
            traceback.print_exc()
            raise

    def _get_chain_id(self, chain: str) -> int:
        """
        Convert chain name to chain_id.
        """
        chain_mapping = {
            'ETH': 1,  # Ethereum Mainnet
            'BSC': 56,  # Binance Smart Chain
            'FTM': 250,  # Fantom
            'POL': 137,  # Polygon
            'ETC': 61,  # Ethereum Classic
            'AVAX': 43114,  # Avalanche
            'KLAY': 8217,  # Klaytn (not used by Tracer but included for completeness)
            # Add other chains as needed
        }
        return chain_mapping.get(chain, 1)  # Default to Ethereum

    def _process_response(self, response_data: Dict, source: bool) -> List[Dict[str, Any]]:
        """
        Process the response from Tracer API to match the format expected by TrackingResults.
        """
        transactions = response_data.get('transactions', [])

        # process swaps here -> convert swap_info into a txn item

        return transactions