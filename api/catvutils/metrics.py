from operator import gt

from django.utils.timezone import now

from api.catvutils.tracking_results import chunks
from api.models import IndicatorExtraAnnotation
from api.settings import api_settings
from decimal import Decimal, ROUND_DOWN
from collections import defaultdict

__all__ = ('CatvMetrics',)


class CatvMetrics:
    def __init__(self, data, search_params, token_type):
        self.item_list = data.get("item_list", [])
        self.node_list = data.get("node_list", [])
        self.seg_item_list = []
        self.seg_node_list = []
        self.search_params = search_params
        self.origin = search_params['wallet_address']
        self._category_metrics = {
            'outbound': {
                'blacklisted': {'count': 0, 'total_amount': 0},
                'exchanges': {'count': 0, 'total_amount': 0},
                'annotated': {'count': 0, 'total_amount': 0}
            },
            'inbound': {
                'blacklisted': {'count': 0, 'total_amount': 0},
                'exchanges': {'count': 0, 'total_amount': 0},
                'annotated': {'count': 0, 'total_amount': 0}
            }
        }
        self.symbol = self.find_main_token(token_type)

    def find_main_token(self, token_type):
        # Determine the main token by finding the most frequent symbol in a subset
        if self.item_list:
            # Use a sample of up to 20 transactions to determine main token
            sample_size = min(20, len(self.item_list))
            sample_items = self.item_list[:sample_size]

            # Count occurrences of each token symbol
            token_counts = {}
            for item in sample_items:
                symbol = item.get('symbol')
                if symbol:
                    token_counts[symbol] = token_counts.get(symbol, 0) + 1

            # Find the symbol with highest count
            if token_counts:
                return max(token_counts.items(), key=lambda x: x[1])[0]
            else:
                return token_type
        else:
            return token_type

    def generate_metrics(self, compare_operator):
        # Filter items and nodes based on depth/level
        self.seg_item_list = list(filter(lambda item: compare_operator(item["depth"], 0), self.item_list))
        self.seg_node_list = list(filter(lambda node: compare_operator(node["level"], 0), self.node_list))

        if not self.seg_node_list:
            return {
                "legacy_metrics": {},
                "enhanced_metrics": {}
            }

        # Calculate legacy metrics
        legacy_metrics = self._calculate_legacy_metrics(compare_operator)

        # Calculate enhanced metrics
        enhanced_metrics = self._calculate_enhanced_metrics(compare_operator)
        enhanced_metrics['depth_breakdown'] = legacy_metrics['depth_breakdown']
        enhanced_metrics['max_receiver'] = legacy_metrics['max_receiver']
        enhanced_metrics['max_sender'] = legacy_metrics['max_sender']
        return {
            "legacy_metrics": legacy_metrics,
            "enhanced_metrics": enhanced_metrics
        }

    def _calculate_legacy_metrics(self, compare_operator):
        """Calculate legacy metrics using seg_item_list and seg_node_list"""

        # Only use main token transactions for calculations
        main_token_items = [item for item in self.seg_item_list if
                            item['symbol'] == self.symbol]

        # Top 10 blacklisted wallets by balance
        black_wallets = list(filter(lambda node: node["group"] == 'Blacklist', self.seg_node_list))
        black_wallets_top = sorted(black_wallets, key=lambda wallet: wallet["balance"], reverse=True)
        black_wallets_top = self._pick_n_unique(black_wallets_top, "address", 10)
        black_wallets_top = [{"address": wallet["address"], "balance": wallet["balance"]} for wallet in
                             black_wallets_top]

        # Top 10 exchange wallets by balance
        exchange_wallets = list(filter(lambda node: node["group"] == 'Exchange/DEX/Bridge/Mixer', self.seg_node_list))
        exchange_wallets_top = sorted(exchange_wallets, key=lambda wallet: wallet["amount_in"], reverse=True)[:15]
        exchange_wallets_clean = {}
        skip_words = ["exchange", "wallet", "exchange wallet", "user wallet", "fiat gateway",
                      "proxy contract", "defi", "dex"]

        for wallet in exchange_wallets_top:
            word_list = wallet["annotation"].split(",")
            word_list = [w.strip() for w in word_list]
            clean_name = next((word for word in word_list
                               if word.lower() not in skip_words),
                              "Generic")
            clean_name = clean_name.replace("_", " ")
            clean_name = clean_name.split(" ")[0]
            if clean_name != "Generic":
                exchange_wallets_clean[clean_name] = clean_name

        # Calculate depth breakdown
        grouped_by_depth = self._group_by(main_token_items, lambda item: str(item["depth"]))
        highest_by_depth = defaultdict(dict)

        # Calculate highest sent/received per level
        self._calculate_highest_by_depth(main_token_items, grouped_by_depth, highest_by_depth, compare_operator)

        # Calculate max sender/receiver
        max_sender, max_receiver = self._calculate_max_senders_receivers(
            main_token_items) if self.symbol != "BTC" else self._calculate_max_senders_receivers_btc(main_token_items)

        return {
            "blacklisted": black_wallets_top,
            "exchange": list(exchange_wallets_clean.keys())[:10],
            "depth_breakdown": dict(highest_by_depth),
            "max_sender": max_sender,
            "max_receiver": max_receiver
        }

    def _calculate_highest_by_depth(self, main_token_items, grouped_by_depth, highest_by_depth, compare_operator):
        """Calculate highest sent/received per depth level with special handling for BTC"""

        # Check if it's BTC
        is_btc = self.symbol == "BTC"

        if not is_btc:
            # Original logic for non-BTC tokens
            for level, items in grouped_by_depth.items():
                max_sent_item = max(items, key=lambda item: item["amount"])
                highest_by_depth[level]["sent"] = {"tx_hash": max_sent_item["tx_hash"],
                                                   "amount": max_sent_item["amount"]}

                if abs(int(level)) > 1:
                    depth_key = str(abs(int(level)) - 1)
                    depth_key = depth_key if compare_operator == gt else f"-{depth_key}"
                    highest_by_depth[depth_key]["received"] = {"tx_hash": max_sent_item["tx_hash"],
                                                               "amount": max_sent_item["amount"]}
        else:
            # BTC-specific logic using from_amount and to_amount
            for level, items in grouped_by_depth.items():
                # Track unique transaction hashes to avoid duplicates
                processed_hashes = set()

                # Transform items to include only unique tx_hashes with correct amounts
                unique_items = []
                for item in items:
                    tx_hash = item["tx_hash"]
                    if tx_hash not in processed_hashes:
                        # Use amount here not from/to amount
                        unique_item = item.copy()
                        unique_item["btc_amount"] = item["amount"]  # Fallback
                        unique_items.append(unique_item)
                        processed_hashes.add(tx_hash)

                # Find max sent item based on from_amount (or fallback to amount)
                if unique_items:
                    max_sent_item = max(unique_items, key=lambda item: item["btc_amount"])
                    highest_by_depth[level]["sent"] = {
                        "tx_hash": max_sent_item["tx_hash"],
                        "amount": max_sent_item["btc_amount"]
                    }

                    # For received at the previous level, use to_amount from the same transaction
                    if abs(int(level)) > 1:
                        depth_key = str(abs(int(level)) - 1)
                        depth_key = depth_key if compare_operator == gt else f"-{depth_key}"

                        # If to_amount is available, use it, otherwise fallback to amount
                        receive_amount = max_sent_item.get("to_amount", max_sent_item["amount"])

                        highest_by_depth[depth_key]["received"] = {
                            "tx_hash": max_sent_item["tx_hash"],
                            "amount": receive_amount
                        }

    def _calculate_enhanced_metrics(self, compare_operator):
        """Calculate enhanced metrics using seg_item_list and seg_node_list"""
        is_outbound = compare_operator(1, 0)

        # Calculate wallet metrics
        wallet_metrics = self._calculate_wallet_metrics(is_outbound)

        # Calculate swap metrics
        if self.symbol in ['ETH', 'BSC', 'FTM', 'POL', 'ETC', 'AVAX'] and is_outbound:
            swap_metrics = self._calculate_swap_metrics()
            wallet_metrics["swap_metrics"] = swap_metrics

        # Add overview section for enhanced metrics
        wallet_metrics["overview"] = self._generate_flow_overview(is_outbound)

        main_token_items = [item for item in self.seg_item_list if item['symbol'] == self.symbol]

        if is_outbound:
            # Distribution side - top 20 receivers
            top_receivers = self._calculate_top_receivers(
                main_token_items) if self.symbol not in ["BTC", "LTC"] else self._calculate_top_receivers_btc(main_token_items)
            wallet_metrics["top_receivers"] = top_receivers
        else:
            # Source side - top 20 senders
            top_senders = self._calculate_top_senders(
                main_token_items) if self.symbol not in ["BTC", "LTC"] else self._calculate_top_senders_btc(main_token_items)
            wallet_metrics["top_senders"] = top_senders

        return wallet_metrics

    def _generate_flow_overview(self, is_outbound):
        """Generate flow-specific (inbound/outbound) overview metrics"""
        flow_type = 'outbound' if is_outbound else 'inbound'

        return {
            'blacklisted_wallets': {
                'count': self._category_metrics[flow_type]['blacklisted']['count'],
                'total_amount': self._category_metrics[flow_type]['blacklisted']['total_amount']
            },
            'annotated_wallets': {
                'count': self._category_metrics[flow_type]['annotated']['count'],
                'total_amount': self._category_metrics[flow_type]['annotated']['total_amount']
            },
            'exchanges': {
                'count': self._category_metrics[flow_type]['exchanges']['count'],
                'total_amount': self._category_metrics[flow_type]['exchanges']['total_amount']
            }
        }

    def generate_overview_metrics(self):
        """Generate total overview metrics"""
        blacklisted_addresses = {node['address'] for node in self.node_list if node['group'] == 'Blacklist'}
        annotated_addresses = {node['address'] for node in self.node_list if node['group'] == 'Annotated'}
        exchange_addresses = {node['address'] for node in self.node_list if
                              node['group'] == 'Exchange/DEX/Bridge/Mixer'}

        main_token_from_origin = [item for item in self.item_list
                                  if item['sender'].lower() == self.origin.lower()
                                  and item['symbol'] == self.symbol]

        main_token_to_origin = [item for item in self.item_list
                                if item['receiver'].lower() == self.origin.lower()
                                and item['symbol'] == self.symbol]

        is_btc = self.symbol == "BTC"
        if not is_btc:
            # Calculate sums for main token
            transactions_from_origin_sum = Decimal(sum(item['amount'] for item in main_token_from_origin))
            if transactions_from_origin_sum >= 1:
                transactions_from_origin_sum = transactions_from_origin_sum.quantize(Decimal('0.01'),
                                                                                     rounding=ROUND_DOWN)
            elif transactions_from_origin_sum > 0:
                transactions_from_origin_sum = transactions_from_origin_sum.quantize(Decimal('0.000001'),
                                                                                     rounding=ROUND_DOWN)
            transactions_to_origin_sum = Decimal(sum(item['amount'] for item in main_token_to_origin))
            if transactions_to_origin_sum >= 1:
                transactions_to_origin_sum = transactions_to_origin_sum.quantize(Decimal('0.01'), rounding=ROUND_DOWN)
            elif transactions_to_origin_sum > 0:
                transactions_to_origin_sum = transactions_to_origin_sum.quantize(Decimal('0.000001'), rounding=ROUND_DOWN)
        else:
            # For BTC, handle UTXO model with deduplication

            # For transactions from origin
            from_origin_amount = 0
            processed_from_hashes = set()

            for item in main_token_from_origin:
                tx_hash = item["tx_hash"]
                if tx_hash not in processed_from_hashes:
                    if "from_amount" in item:
                        from_origin_amount += item["from_amount"]
                    else:
                        from_origin_amount += item["amount"]  # Fallback to amount if from_amount is missing
                    processed_from_hashes.add(tx_hash)

            # For transactions to origin
            to_origin_amount = 0
            processed_to_hashes = set()

            for item in main_token_to_origin:
                tx_hash = item["tx_hash"]
                if tx_hash not in processed_to_hashes:
                    if "to_amount" in item:
                        to_origin_amount += item["to_amount"]
                    else:
                        to_origin_amount += item["amount"]  # Fallback to amount if to_amount is missing
                    processed_to_hashes.add(tx_hash)

            # Convert to Decimal with conditional rounding
            transactions_from_origin_sum = Decimal(from_origin_amount)
            if transactions_from_origin_sum >= 1:
                transactions_from_origin_sum = transactions_from_origin_sum.quantize(Decimal('0.01'),
                                                                                     rounding=ROUND_DOWN)
            elif transactions_from_origin_sum > 0:
                transactions_from_origin_sum = transactions_from_origin_sum.quantize(Decimal('0.000001'),
                                                                                     rounding=ROUND_DOWN)
            transactions_to_origin_sum = Decimal(to_origin_amount)
            if transactions_to_origin_sum >= 1:
                transactions_to_origin_sum = transactions_to_origin_sum.quantize(Decimal('0.01'), rounding=ROUND_DOWN)
            elif transactions_to_origin_sum > 0:
                transactions_to_origin_sum = transactions_to_origin_sum.quantize(Decimal('0.000001'), rounding=ROUND_DOWN)

        tokens_involved = {item['symbol'] for item in self.item_list}

        return {
            'transactions_from_origin': sum(1 for item in self.item_list if item['sender'].lower() == self.origin.lower()),
            'transactions_to_origin': sum(1 for item in self.item_list if item['receiver'].lower() == self.origin.lower()),
            'transactions_from_origin_sum': f"{transactions_from_origin_sum} {self.symbol}",
            'transactions_to_origin_sum': f"{transactions_to_origin_sum} {self.symbol}",
            'blacklisted_wallets': len(blacklisted_addresses),
            'annotated_wallets': len(annotated_addresses),
            'exchanges': len(exchange_addresses),
            'tokens_involved': list(tokens_involved),
        }

    def _calculate_wallet_metrics(self, is_outbound):
        """Calculate enhanced wallet metrics using seg_item_list"""

        # Filter for main token transactions
        main_token_items = [item for item in self.seg_item_list
                            if item['symbol'] == self.symbol and not item.get('is_swap', False)]

        wallet_amounts = defaultdict(lambda: {'total_amount': 0, 'depth': None})

        # Calculate total amounts in a single pass
        for item in main_token_items:
            wallet = item['receiver'] if is_outbound else item['sender']
            wallet_amounts[wallet]['total_amount'] += item['amount']
            wallet_amounts[wallet]['depth'] = item['depth']

        # Initialize wallet categories
        wallet_metrics = {
            'blacklisted': {'wallets': []},
            'exchanges': {'wallets': []},
            'annotated': {'wallets': []}
        }
        processed_addresses = {
            'blacklisted': set(),
            'exchanges': set(),
            'annotated': set()
        }

        # Process nodes and categorize wallets
        for node in self.seg_node_list:
            addr = node['address']
            if addr not in wallet_amounts:
                continue

            wallet_info = {
                'id': node['id'],
                'address': addr,
                'total_amount': wallet_amounts[addr]['total_amount'],
                'depth': wallet_amounts[addr]['depth']
            }

            if node['group'] == 'Blacklist' and addr not in processed_addresses['blacklisted']:
                wallet_info['label'] = node.get('label', '')
                wallet_info['annotation'] = node.get('annotation', '')
                wallet_metrics['blacklisted']['wallets'].append(wallet_info)
                processed_addresses['blacklisted'].add(addr)
            elif node['group'] == 'Exchange/DEX/Bridge/Mixer' and addr not in processed_addresses['exchanges']:
                wallet_info['label'] = node.get('label', '')
                wallet_info['annotation'] = node.get('annotation', '')
                wallet_metrics['exchanges']['wallets'].append(wallet_info)
                processed_addresses['exchanges'].add(addr)
            elif node['group'] == 'Annotated' and addr not in processed_addresses['annotated']:
                wallet_info['label'] = node.get('label', '')
                wallet_info['annotation'] = node.get('annotation', '')
                wallet_metrics['annotated']['wallets'].append(wallet_info)
                processed_addresses['annotated'].add(addr)

        flow_type = 'outbound' if is_outbound else 'inbound'
        # Calculate category totals
        for category in wallet_metrics:
            total = sum(w['total_amount'] for w in wallet_metrics[category]['wallets'])
            count = len(wallet_metrics[category]['wallets'])
            wallet_metrics[category]['total_amount'] = total
            self._category_metrics[flow_type][category]['total_amount'] = total
            self._category_metrics[flow_type][category]['count'] = count

        return wallet_metrics

    def _calculate_swap_metrics(self):
        """Calculate metrics specifically for swap transactions"""
        # Filter only swap transactions
        swap_items = [item for item in self.seg_item_list if item.get('is_swap', False)]
        if not swap_items:
            return []

        # Create simplified swap information
        swap_details = []
        # Define mapping of wrapped token addresses to native token symbols
        WRAPPED_TO_NATIVE = {
            # Ethereum
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "ETH",  # WETH -> ETH

            # Binance Smart Chain
            "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": "BNB",  # WBNB -> BNB

            # Polygon
            "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270": "MATIC",  # WMATIC -> MATIC

            # Avalanche
            "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7": "AVAX",  # WAVAX -> AVAX

            # Fantom
            "0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83": "FTM",  # WFTM -> FTM
        }

        # Symbol mapping (alternative approach)
        WRAPPED_SYMBOLS = {
            "WETH": "ETH",
            "WBNB": "BNB",
            "WMATIC": "MATIC",
            "WAVAX": "AVAX",
            "WFTM": "FTM"
        }

        for item in swap_items:
            if 'swap_info' in item:
                # Determine amount of main token involved

                token_in_address = item['swap_info']['token_in']['address'].lower()
                token_in_symbol = item['swap_info']['token_in']['symbol']
                token_in_amount = item['swap_info']['amount_in']

                # Check if token_in is a wrapped token and replace with native symbol
                if token_in_address in WRAPPED_TO_NATIVE:
                    token_in_symbol = WRAPPED_TO_NATIVE[token_in_address]
                # Alternative: Check by symbol
                elif token_in_symbol in WRAPPED_SYMBOLS:
                    token_in_symbol = WRAPPED_SYMBOLS[token_in_symbol]

                token_out_symbol = item['swap_info']['token_out']['symbol']
                token_out_amount = item['swap_info']['amount_out']

                swap_details.append({
                    'depth': item['depth'],
                    'tx_hash': item['tx_hash'],
                    'amount_in': {
                        'value': token_in_amount,
                        'symbol': token_in_symbol
                    },
                    'amount_out': {
                        'value': token_out_amount,
                        'symbol': token_out_symbol
                    },
                    'protocol': item['swap_info'].get('protocol', 'Unknown'),
                    'edge_id': item['edge_id']
                })

        return {
            'swaps': swap_details
        }

    def _calculate_max_senders_receivers(self, main_token_items):
        """Calculate maximum senders and receivers from main_token_items"""
        grouped_by_sender = defaultdict(list)
        grouped_by_receiver = defaultdict(list)

        for item in main_token_items:
            grouped_by_sender[item["sender"]].append(item)
            grouped_by_receiver[item["receiver"]].append(item)

        # Calculate max sender
        grouped_by_sender = [{
            "address": sender,
            "amount": sum([item["amount"] if abs(item["depth"]) >= 1 else 0 for item in items])}
            for sender, items in grouped_by_sender.items()]
        max_sender = max(grouped_by_sender, key=lambda sender: sender["amount"])

        # Calculate max receiver
        grouped_by_receiver = [{
            "address": receiver,
            "amount": sum([item["amount"] if abs(item["depth"]) >= 1 else 0 for item in items])}
            for receiver, items in grouped_by_receiver.items()]
        max_receiver = max(grouped_by_receiver, key=lambda receiver: receiver["amount"])

        return max_sender, max_receiver

    def _calculate_max_senders_receivers_btc(self, main_token_items):
        # For BTC, handle UTXO model properly
        sender_amounts = defaultdict(float)
        receiver_amounts = defaultdict(float)

        # Track processed transaction hashes for each sender
        processed_sender_hashes = defaultdict(set)
        # Track processed transaction hashes for each receiver
        processed_receiver_hashes = defaultdict(set)

        for item in main_token_items:
            # Handle sender (use from_amount)
            sender = item["sender"]
            tx_hash = item["tx_hash"]

            # Only count each transaction hash once per sender
            if tx_hash not in processed_sender_hashes[sender]:
                if "from_amount" in item:
                    sender_amounts[sender] += item["from_amount"]
                else:
                    sender_amounts[sender] += item["amount"]  # Fallback to amount if from_amount is missing
                processed_sender_hashes[sender].add(tx_hash)

            # Handle receiver (use to_amount)
            receiver = item["receiver"]

            # Only count each transaction hash once per receiver
            if tx_hash not in processed_receiver_hashes[receiver]:
                if "to_amount" in item:
                    receiver_amounts[receiver] += item["to_amount"]
                else:
                    receiver_amounts[receiver] += item["amount"]  # Fallback to amount if to_amount is missing
                processed_receiver_hashes[receiver].add(tx_hash)

        # Convert to the expected format
        grouped_by_sender = [{"address": sender, "amount": amount} for sender, amount in sender_amounts.items()]
        grouped_by_receiver = [{"address": receiver, "amount": amount} for receiver, amount in receiver_amounts.items()]

        # Find max sender and receiver
        max_sender = max(grouped_by_sender, key=lambda sender: sender["amount"]) if grouped_by_sender else {
            "address": "", "amount": 0}
        max_receiver = max(grouped_by_receiver, key=lambda receiver: receiver["amount"]) if grouped_by_receiver else {
            "address": "", "amount": 0}

        return max_sender, max_receiver

    def _calculate_top_receivers(self, main_token_items):
        """Calculate top 20 receivers from main_token_items (distribution side, depth > 0)"""
        # Group by receiver and sum amounts
        grouped_by_receiver = defaultdict(float)
        receiver_depths = {}  # Track depth for each receiver

        for item in main_token_items:
            if abs(item["depth"]) >= 1:
                receiver = item["receiver"]
                grouped_by_receiver[receiver] += item["amount"]
                # Store the depth (use the first occurrence or update if needed)
                if receiver not in receiver_depths:
                    receiver_depths[receiver] = item["depth"]

        # Convert to list format
        receiver_list = [
            {"address": receiver, "total_amount": amount}
            for receiver, amount in grouped_by_receiver.items()
        ]

        # Sort by total_amount descending and take top 20
        receiver_list.sort(key=lambda x: x["total_amount"], reverse=True)
        top_20_receivers = receiver_list[:20]

        # Enrich with node details
        node_map = {node["address"]: node for node in self.seg_node_list}

        enriched_receivers = []
        for receiver in top_20_receivers:
            address = receiver["address"]
            node = node_map.get(address, {})

            enriched_receivers.append({
                "id": node.get("id", None),
                "address": address,
                "total_amount": receiver["total_amount"],
                "depth": receiver_depths.get(address, 0),
                "label": node.get("label", ""),
                "annotation": node.get("annotation", "")
            })

        return enriched_receivers

    def _calculate_top_senders(self, main_token_items):
        """Calculate top 20 senders from main_token_items (source side, depth < 0)"""
        # Group by sender and sum amounts
        grouped_by_sender = defaultdict(float)
        sender_depths = {}  # Track depth for each sender

        for item in main_token_items:
            if abs(item["depth"]) >= 1:
                sender = item["sender"]
                grouped_by_sender[sender] += item["amount"]
                # Store the depth (use the first occurrence or update if needed)
                if sender not in sender_depths:
                    sender_depths[sender] = item["depth"]

        # Convert to list format
        sender_list = [
            {"address": sender, "total_amount": amount}
            for sender, amount in grouped_by_sender.items()
        ]

        # Sort by total_amount descending and take top 20
        sender_list.sort(key=lambda x: x["total_amount"], reverse=True)
        top_20_senders = sender_list[:20]

        # Enrich with node details
        node_map = {node["address"]: node for node in self.seg_node_list}

        enriched_senders = []
        for sender in top_20_senders:
            address = sender["address"]
            node = node_map.get(address, {})

            enriched_senders.append({
                "id": node.get("id", None),
                "address": address,
                "total_amount": sender["total_amount"],
                "depth": sender_depths.get(address, 0),
                "label": node.get("label", ""),
                "annotation": node.get("annotation", "")
            })

        return enriched_senders

    def _calculate_top_receivers_btc(self, main_token_items):
        """Calculate top 20 receivers for BTC (distribution side, depth > 0)
        Based on aggregated from_amount for unique (tx_hash, receiver) pairs
        Excludes leaf nodes (terminal nodes with no outgoing transactions)"""
        # First, identify all addresses that have outgoing transactions (non-leaf nodes)
        addresses_with_outgoing = set()
        for item in main_token_items:
            if abs(item["depth"]) >= 1:
                addresses_with_outgoing.add(item["sender"])

        receiver_amounts = defaultdict(float)
        receiver_depths = {}
        # Track processed (transaction_hash, receiver) pairs
        processed_receiver_txs = defaultdict(set)

        for item in main_token_items:
            if abs(item["depth"]) >= 1:
                receiver = item["receiver"]
                tx_hash = item["tx_hash"]

                # Skip leaf nodes - only include receivers that also appear as senders
                if receiver not in addresses_with_outgoing:
                    continue

                # Only count each (tx_hash, receiver) pair once
                if tx_hash not in processed_receiver_txs[receiver]:
                    if "from_amount" in item:
                        receiver_amounts[receiver] += item["from_amount"]
                    else:
                        receiver_amounts[receiver] += item["amount"]  # Fallback
                    processed_receiver_txs[receiver].add(tx_hash)

                    # Store the depth (use the first occurrence)
                    if receiver not in receiver_depths:
                        receiver_depths[receiver] = item["depth"]

        # Convert to list format
        receiver_list = [
            {"address": receiver, "total_amount": amount}
            for receiver, amount in receiver_amounts.items()
        ]

        # Sort by total_amount descending and take top 20
        receiver_list.sort(key=lambda x: x["total_amount"], reverse=True)
        top_20_receivers = receiver_list[:20]

        # Enrich with node details
        node_map = {node["address"]: node for node in self.seg_node_list}

        enriched_receivers = []
        for receiver in top_20_receivers:
            address = receiver["address"]
            node = node_map.get(address, {})

            enriched_receivers.append({
                "id": node.get("id", None),
                "address": address,
                "total_amount": receiver["total_amount"],
                "depth": receiver_depths.get(address, 0),
                "label": node.get("label", ""),
                "annotation": node.get("annotation", "")
            })

        return enriched_receivers

    def _calculate_top_senders_btc(self, main_token_items):
        """Calculate top 20 senders for BTC (source side, depth < 0)
        Based on aggregated from_amount for unique (tx_hash, sender) pairs"""
        sender_amounts = defaultdict(float)
        sender_depths = {}
        # Track processed (transaction_hash, sender) pairs
        processed_sender_txs = defaultdict(set)

        for item in main_token_items:
            if abs(item["depth"]) >= 1:
                sender = item["sender"]
                tx_hash = item["tx_hash"]

                # Only count each (tx_hash, sender) pair once
                if tx_hash not in processed_sender_txs[sender]:
                    if "from_amount" in item:
                        sender_amounts[sender] += item["from_amount"]
                    else:
                        sender_amounts[sender] += item["amount"]  # Fallback
                    processed_sender_txs[sender].add(tx_hash)

                    # Store the depth (use the first occurrence)
                    if sender not in sender_depths:
                        sender_depths[sender] = item["depth"]

        # Convert to list format
        sender_list = [
            {"address": sender, "total_amount": amount}
            for sender, amount in sender_amounts.items()
        ]

        # Sort by total_amount descending and take top 20
        sender_list.sort(key=lambda x: x["total_amount"], reverse=True)
        top_20_senders = sender_list[:20]

        # Enrich with node details
        node_map = {node["address"]: node for node in self.seg_node_list}

        enriched_senders = []
        for sender in top_20_senders:
            address = sender["address"]
            node = node_map.get(address, {})

            enriched_senders.append({
                "id": node.get("id", None),
                "address": address,
                "total_amount": sender["total_amount"],
                "depth": sender_depths.get(address, 0),
                "label": node.get("label", ""),
                "annotation": node.get("annotation", "")
            })

        return enriched_senders

    def _pick_n_unique(self, iterable, key, n):
        """Helper function to pick N unique items based on a key"""
        seen = []
        n_list = []
        for item in iterable:
            if len(n_list) == n:
                return n_list
            if item[key] not in seen:
                n_list.append(item)
                seen.append(item[key])
        return n_list

    def calculate_total_amounts(self):
        """Calculate total amounts in main token currency and USD value"""
        # Default values
        total_amount = 0
        total_amount_usd = 0

        # Check if we have valid items with amount data
        if not self.item_list:
            return total_amount, total_amount_usd

        # Check if this is BTC
        is_btc = self.symbol == "BTC"

        if not is_btc:
            # Original logic for non-BTC tokens
            for item in self.item_list:
                if item['symbol'] == self.symbol:
                    total_amount += item.get("amount", 0)
                    total_amount_usd += item.get("amount_usd", 0)
        else:
            # For BTC, handle deduplication based on transaction hash
            processed_tx_hashes = set()

            for item in self.item_list:
                if item['symbol'] == self.symbol:
                    tx_hash = item.get("tx_hash")

                    # Skip if we've already processed this hash
                    if tx_hash in processed_tx_hashes:
                        continue

                    # Add to totals and mark as processed
                    total_amount += item.get("amount", 0)
                    total_amount_usd += item.get("amount_usd", 0)
                    processed_tx_hashes.add(tx_hash)

        return total_amount, total_amount_usd

    def _group_by(self, items, key_func):
        """Helper function to group items by a key function"""
        groups = defaultdict(list)
        for item in items:
            groups[key_func(item)].append(item)
        return groups

    def save_annotations(self):
        all_nodes = {node["address"]: node for node in self.node_list}
        all_node_keys = list(all_nodes.keys())
        if api_settings.SAVE_EXTRA_ANNOTATE:
            for node_chunk in chunks(all_node_keys, api_settings.QUERY_CHUNK_SIZE):
                bulk_indicators = []
                matched_nodes = IndicatorExtraAnnotation.objects.filter(pattern__in=node_chunk)
                matched_nodes_addr = [node.pattern for node in matched_nodes]
                missing_nodes_addr = set(node_chunk) - set(matched_nodes_addr)
                for matched_node in matched_nodes:
                    matched_node.annotation = all_nodes[matched_node.pattern]["annotation"]
                    matched_node.updated = now()
                for missing in missing_nodes_addr:
                    bulk_indicators.append(
                        IndicatorExtraAnnotation(pattern=missing, annotation=all_nodes[missing]["annotation"])
                    )
                IndicatorExtraAnnotation.objects.bulk_create(bulk_indicators)
                IndicatorExtraAnnotation.objects.bulk_update(matched_nodes, update_fields=['annotation', 'updated'])
