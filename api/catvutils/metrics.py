from collections import defaultdict
from operator import gt

from django.utils.timezone import now

from api.catvutils.tracking_results import chunks
from api.models import IndicatorExtraAnnotation
from api.settings import api_settings
from decimal import Decimal, ROUND_DOWN

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
        self.symbol = self.item_list[0]['symbol'] if self.item_list else token_type

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
        main_token_items = [item for item in self.seg_item_list if item['symbol'] == self.symbol]

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
        for level, items in grouped_by_depth.items():
            max_sent_item = max(items, key=lambda item: item["amount"])
            highest_by_depth[level]["sent"] = {"tx_hash": max_sent_item["tx_hash"], "amount": max_sent_item["amount"]}

            if abs(int(level)) > 1:
                depth_key = str(abs(int(level)) - 1)
                depth_key = depth_key if compare_operator == gt else f"-{depth_key}"
                highest_by_depth[depth_key]["received"] = {"tx_hash": max_sent_item["tx_hash"],
                                                           "amount": max_sent_item["amount"]}

        # Calculate max sender/receiver
        max_sender, max_receiver = self._calculate_max_senders_receivers(main_token_items)

        return {
            "blacklisted": black_wallets_top,
            "exchange": list(exchange_wallets_clean.keys())[:10],
            "depth_breakdown": dict(highest_by_depth),
            "max_sender": max_sender,
            "max_receiver": max_receiver
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

        # Calculate sums for main token
        transactions_from_origin_sum = Decimal(sum(item['amount'] for item in main_token_from_origin)).quantize(
            Decimal('0.01'), rounding=ROUND_DOWN)

        transactions_to_origin_sum = Decimal(sum(item['amount'] for item in main_token_to_origin)).quantize(
            Decimal('0.01'), rounding=ROUND_DOWN)

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
                wallet_info['label'] = node.get('annotation', '')
                wallet_metrics['blacklisted']['wallets'].append(wallet_info)
                processed_addresses['blacklisted'].add(addr)
            elif node['group'] == 'Exchange/DEX/Bridge/Mixer' and addr not in processed_addresses['exchanges']:
                wallet_info['label'] = node.get('annotation', '').split(',')[0]
                wallet_metrics['exchanges']['wallets'].append(wallet_info)
                processed_addresses['exchanges'].add(addr)
            elif node['group'] == 'Annotated' and addr not in processed_addresses['annotated']:
                wallet_info['label'] = node.get('annotation', '')
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
        total_main_token_amount = 0

        for item in swap_items:
            if 'swap_info' in item:
                # Determine amount of main token involved
                token_in_symbol = item['swap_info']['token_in']['symbol']
                token_out_symbol = item['swap_info']['token_out']['symbol']

                token_in_amount = item['swap_info']['amount_in']
                token_out_amount = item['swap_info']['amount_out']

                # Add to main token total if involved
                if token_in_symbol == self.symbol:
                    total_main_token_amount += token_in_amount
                elif token_out_symbol == self.symbol:
                    total_main_token_amount += token_out_amount

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
                    'protocol': item['swap_info'].get('protocol', 'Unknown')
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
