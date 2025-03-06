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
            if from_time and 'T' not in from_time:
                start_datetime = f"{from_time}T00:00:00.000Z"
            else:
                start_datetime = from_time

            end_datetime = f"{till_time}Z"
            # Prepare request body
            request_body = {
                "chain_type": "evm",
                "chain_id": chain_id,
                "start_address": address,
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
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

        # offsetting depth to -(depth) for source transactions
        if source:
            for transaction in transactions:
                if 'depth' in transaction:
                    transaction['depth'] = -transaction['depth']

        # Process swaps to create reverse transactions
        swap_transactions = [tx for tx in transactions if tx.get('is_swap') and tx.get('swap_info')]
        reverse_swap_transactions = self._create_reverse_swap_transactions(swap_transactions)

        # Add the reverse swap transactions to the original list
        if reverse_swap_transactions:
            transactions.extend(reverse_swap_transactions)
            print(f"Added {len(reverse_swap_transactions)} reverse swap transactions")

        return transactions

    def _create_reverse_swap_transactions(self, swap_transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Create reverse transactions for swaps to visualize token flow from router back to sender.

        Args:
            swap_transactions: List of transactions with is_swap=true and valid swap_info

        Returns:
            List of new transaction objects representing the reverse swap flow
        """
        reverse_transactions = []

        for tx in swap_transactions:
            swap_info = tx.get('swap_info', {})
            token_out = swap_info.get('token_out', {})

            # Skip if token_out is missing or invalid
            if not token_out or not isinstance(token_out, dict):
                continue

            # Create reverse transaction (from router to original sender)
            reverse_tx = {
                # Keep same identification fields
                "chain_id": tx.get('chain_id'),
                "depth": tx.get('depth'),
                "direction": tx.get('direction'),
                "tx_hash": tx.get('tx_hash'),
                "block_height": tx.get('block_height'),
                "tx_time": tx.get('tx_time'),

                # Swap addresses
                "sender": tx.get('receiver'),  # Router address is now sender
                "receiver": tx.get('sender'),  # Original sender is now receiver

                # Swap annotations and security categories
                "sender_annotation": tx.get('receiver_annotation', ''),
                "receiver_annotation": tx.get('sender_annotation', ''),
                "sender_security_category": tx.get('receiver_security_category', ''),
                "receiver_security_category": tx.get('sender_security_category', ''),

                # Token details from token_out
                "symbol": token_out.get('symbol', ''),
                "token": {
                    "address": token_out.get('address', ''),
                    "symbol": token_out.get('symbol', ''),
                    "decimals": token_out.get('decimals')
                },
                "token_type": "ERC20",  # Assuming all swap tokens are ERC20
                "token_id": "",

                # Amount from swap_info.amount_out
                "amount": swap_info.get('amount_out', 0),
                "amount_usd": 0,  # Empty as specified

                # Swap sender/receiver types
                "sender_type": tx.get('receiver_type', 'Wallet'),
                "receiver_type": tx.get('sender_type', 'Wallet'),
                "is_swap": True
            }

            reverse_transactions.append(reverse_tx)

        return reverse_transactions