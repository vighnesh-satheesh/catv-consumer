from collections import defaultdict
from itertools import chain, islice
from math import ceil

from api.settings import api_settings
from api.utils import format_tx_time

MULTIPLIER = 0.4
EDGE_WIDTH_MAX = 4
EDGE_WIDTH_MIN = 1
DIST_DEPTH_OFFSET = 0
SOURCE_DEPTH_OFFSET = 1
BTC_DIST_DEPTH_OFFSET = 1
BTC_SOURCE_DEPTH_OFFSET = 1


class Node:
    SKIP_ANNOTATIONS = ["exchange", "wallet", "exchange wallet", "user wallet", "fiat gateway",
                        "proxy contract", "defi", "dex", "smart contract", "token contract",
                        "router", "contract", "v1", "v2", "v3"]

    def __init__(self, id, address, annotation, type, depth, balance=None,
                 amount_in=None, amount_out=None, trdb_info=None):
        self.id = id
        self.address = address
        self.annotation = annotation
        self.type = type
        self.level = depth
        self.label = self.generate_label()
        self.balance = balance
        self.amount_in = amount_in
        self.amount_out = amount_out
        self.trdb_info = trdb_info
        self.group = ""
        self.set_group_from_annotation()

    def generate_label(self):
        """
        Generate a meaningful label for the node based on its address and annotation.
        Uses a simplified approach that preserves multi-word entity names.

        Returns:
            A string representing the most meaningful label for the node
        """
        if not self.annotation:
            return self.address[:8]

        # Split annotations by comma and use the first one as starting point
        first_annotation = self.annotation.split(",")[0].strip()

        # Handle domain-style project names
        if "." in first_annotation:
            if first_annotation.startswith("DEX."):
                pass  # Keep DEX.AG intact
            elif "inch" in first_annotation.lower():
                first_annotation = "1inch"  # Special case for 1inch.exchange
            else:
                first_annotation = first_annotation.split(".")[0]

        # Remove parts after colon or parentheses
        if ":" in first_annotation:
            first_annotation = first_annotation.split(":")[0].strip()
        if "(" in first_annotation:
            first_annotation = first_annotation.split("(")[0].strip()

        # Replace underscores with spaces
        first_annotation = first_annotation.replace("_", " ")

        # KEY SIMPLIFICATION: If the cleaned annotation contains multiple words,
        # keep the entire thing rather than just the first word
        words = first_annotation.split()
        if len(words) >= 2:
            # If there's a trailing number, remove it
            if words[-1].isdigit():
                return " ".join(words[:-1])
            return first_annotation

        # If it's a single word annotation, return it
        # (this preserves single word names like "Uniswap", "Binance", etc.)
        if first_annotation.lower() not in self.SKIP_ANNOTATIONS:
            return first_annotation

        # Fallback: If result would be in the skip list or empty, use the entire annotation
        return self.annotation

    def set_group_from_annotation(self):
        # Initialize default
        self.group = ""

        # Use casefold() for case-insensitive comparison
        if not self.annotation:
            annotation_list = []
        else:
            # Use casefold() for case-insensitive comparison
            annotation_list = self.annotation.casefold().split(", ")

        if annotation_list:
            # Define term sets for faster lookups
            suspicious_terms = {'scamming', 'phishing'}
            exchange_terms = {'dex', 'exchange', 'bridge', 'mixer'}
            contract_terms = {'smart', 'contract'}

            # Process annotations
            for annotation in annotation_list:
                # Check for suspicious terms
                if any(term in annotation for term in suspicious_terms):
                    self.group = 'Suspicious'
                    break

                # Check for exchange-related terms
                elif 'non-exchange' not in annotation and any(term in annotation for term in exchange_terms):
                    self.group = 'Exchange/DEX/Bridge/Mixer'
                    break

                # Check for smart contract terms
                elif any(term in annotation for term in contract_terms):
                    self.group = 'Smart Contract'
                    break

                # Default case for this annotation
                elif annotation:
                    self.group = 'Annotated'

        # Node type check after annotation processing
        if self.group == "" and self.type != 'Wallet':
            self.group = 'Smart Contract'

        # Final fallback to ensure group is never empty
        if self.group == "":
            self.group = 'No Tag'

    def update(self, **kwargs):
        # Update all attributes from kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

        # If annotation was updated, update the label
        # except for blacklist or whitelist security categories
        if 'annotation' in kwargs and kwargs['annotation']:
            # Only update label if not blacklist or whitelist
            if not (self.group == 'Blacklist' or self.group == 'Whitelist'):
                self.label = self.generate_label()
            else:
                # For blacklist or whitelist, keep the default label
                self.label = self.address[:8]


class BTCNode(Node):
    def __init__(self, **kwargs):
        super(BTCNode, self).__init__(**kwargs)

    def set_group_from_annotation(self):
        annotation_list = self.annotation.casefold().split(", ")

        # Initialize default
        self.group = ""

        # Case-insensitive terms to check
        suspicious_terms = ['scamming', 'phishing', 'Scamming', 'Phishing']
        exchange_terms = {'dex', 'exchange', 'bridge', 'mixer'}

        if annotation_list:
            for annotation in annotation_list:
                # Check for suspicious terms
                if any(term in annotation for term in suspicious_terms):
                    self.group = 'Suspicious'
                    break

                # Check for exchange-related terms
                elif 'non-exchange' not in annotation and any(term in annotation for term in exchange_terms):
                    self.group = 'Exchange/DEX/Bridge/Mixer'
                    break

                # Default case for this annotation
                elif annotation:
                    self.group = 'Annotated'

        # Final fallback to ensure group is never empty
        if self.group == "":
            self.group = 'No Tag'


class NodesCollection:
    def __init__(self):
        self.__nodes = {}
        self.__node_enum = {}
        self.__edge_keys = set()
    
    @property
    def count(self):
        return len(self.__nodes.items())
        
    @property
    def edge_keys(self):
        return self.__edge_keys
    
    @edge_keys.setter
    def edge_keys(self, value):
        self.__edge_keys = value

    def get_nodes(self):
        return self.__nodes

    def get_nodes_as_dict(self):
        return {k: v.__dict__ for k, v in self.__nodes.items()}

    def get_node(self, address):
        return self.get_nodes().get(address, None)

    def add_node(self, Node):
        self.__nodes[Node.address] = Node

    def get_node_enum(self):
        return {k: v.id for k, v in self.__nodes.items()}

    def update_node(self, node_key, node_value):
        self.__nodes[node_key] = node_value
        
    def filter_update_nodes(self):
        if self.__edge_keys:
            self.__nodes = {k:v for k, v in self.__nodes.items() if k in self.__edge_keys}


def take(n, iterable):
    return list(islice(iterable, n))


def group_by_depth(edge_dict: dict, mode: int) -> dict:
    grouped_by_depth: dict = defaultdict(dict)
    for key, value in edge_dict.items():
        shifted_depth = value["depth"] + 1 if mode == -1 else value["depth"]
        grouped_by_depth[shifted_depth][key] = value
    return grouped_by_depth


def sort_per_depth(grouped_by_depth: dict) -> dict:
    for key, value in grouped_by_depth.items():
        grouped_by_depth[key] = {k: v for k, v in sorted(value.items(), key=lambda item: item[1]["sum"], reverse=True)}
    return grouped_by_depth


def limit_connected_edges(sorted_grouped_edges: dict, scaling_factor: float) -> dict:
    limited_edges = {}
    ratio_depth = [0.01, 0.03, 0.05, 0.07, 0.09, 0.11, 0.13, 0.15, 0.17, 0.19]
    max_depth = max(sorted_grouped_edges.keys())
    if max_depth != 10:
        slice_depth = ratio_depth[0:max_depth]
        slice_sum = sum(slice_depth)
        ratio_depth = list(map(lambda x: x / slice_sum, slice_depth))
    for depth, edge_dict in sorted_grouped_edges.items():
        node_count = ceil(ratio_depth[depth - 1] * api_settings.CATV_MAX_SCALED_NODES)
        print(f"Node count for depth {depth}: {node_count}")
        if depth != 1:
            receivers_prev = list(limited_edges.keys())
            receivers_prev = set([k for key_pair in receivers_prev for k in key_pair])
            modified_edge_dict = take(node_count, filter(
                lambda item: item[0][0] in receivers_prev or item[0][1] in receivers_prev, edge_dict.items()))
        else:
            modified_edge_dict = take(node_count, edge_dict.items())
        modified_edge_dict = {dict_tuple[0]: dict_tuple[1] for dict_tuple in modified_edge_dict}
        limited_edges = {**limited_edges, **modified_edge_dict}
    return limited_edges


def make_lossy_graph(nc, edge_dict, mode):
    print("Grouping by depth...")
    grouped_by_depth = group_by_depth(edge_dict, mode)
    print("Sorting by depth...")
    sorted_per_depth = sort_per_depth(grouped_by_depth)
    node_count = nc.count
    max_scaled_nodes = api_settings.CATV_MAX_SCALED_NODES
    scaling_factor = max_scaled_nodes / node_count
    print(f"Shedding edges with scale of: {scaling_factor}")
    limited_conn_edges = limit_connected_edges(sorted_per_depth, scaling_factor)
    edge_keys = set(chain.from_iterable(limited_conn_edges.keys()))
    all_nodes = list(nc.get_nodes_as_dict().values())
    limited_nodes = filter(lambda node: node["address"] in edge_keys, all_nodes)
    nc.edge_keys = edge_keys
    return limited_conn_edges, limited_nodes


def uniqfy_generator(seq, addr_key, exclusions):
    temp_dict = exclusions
    for e in seq:
        try:
            temp_dict[e[addr_key]]
        except KeyError:
            temp_dict[e[addr_key]] = e
            yield e


def create_edge(id, tx, node_enum):
    amount = tx['amount']
    symbol = tx.get('symbol', '')
    sum_dict = {symbol: abs(amount)}
    sum_list = [f"{abs(amount)} {symbol}"]

    edge = {
        'id': id,
        'arrows': 'to',
        'sum': abs(tx['amount']),
        'sum_dict': sum_dict,  # Add new field for symbol-based sums
        'sum_list': sum_list,
        'from': node_enum[tx['sender']],
        'to': node_enum[tx['receiver']],
        'data': [{
            'amount': abs(tx['amount']),
            'from_amount': tx['from_amount'] if 'from_amount' in tx else 0,
            'to_amount': tx['to_amount'] if 'to_amount' in tx else 0,
            'tx_hash': tx['tx_hash'],
            'depth': tx['depth'],
            'tx_time': format_tx_time(tx['tx_time']),
            'symbol': symbol
        }],
        'depth': tx['depth'],
        'is_swap': tx['is_swap'] if 'is_swap' in tx else False,
    }
    return edge


def create_edge_btc(id, tx, node_enum):
    edge = {
        'id': id,
        'arrows': 'to',
        'sum': tx['sender_amount'],
        'from': node_enum[tx['sender']],
        'to': node_enum[tx['receiver']],
        'data': [{
            "ref_tx_id": tx["ref_tx_id"],
            "block_num": tx["block_num"],
            "vin_number": tx["vin_number"],
            'amount': tx['sender_amount'],
            'tx_hash': tx['ref_tx_id'],
            'depth': tx['depth'],
            'tx_time': tx['tx_time'],
            'tx_fees': tx['sender_amount'] - tx['receiver_amount']
        }]
    }
    return edge

def add_edge_ids_to_item_list(edge_list, result):
    # Create a dictionary that maps tx_hash to edge_id from edge_list
    edge_map = {}
    for edge in edge_list:
        for transaction in edge['data']:
            edge_map[transaction['tx_hash']] = edge['id']

    # Update the result with the corresponding edge_id
    for entry in result:
        tx_hash = entry.get('tx_hash')
        if tx_hash in edge_map:
            entry['edge_id'] = edge_map[tx_hash]
        else:
            entry['edge_id'] = None  # If no match is found, set edge_id to None
    return result


def assign_edges(result, mode, node_enum):
    edge_dict = {}
    counter = 0
    bidirectional_pairs = {}  # Track potential bidirectional pairs

    for item in result:
        try:
            amount = float(item['amount']) if isinstance(item['amount'], str) else item['amount']
            amount_usd = 0
            if 'amount_usd' in item:
                amount_usd = float(item['amount_usd']) if isinstance(item['amount_usd'], str) else item['amount_usd']

            symbol = item.get('symbol', '')
            if symbol == "BTC" or symbol == "LTC":
                amount = item['from_amount'] if 'from_amount' in item else amount

            formatted_tx_time = format_tx_time(item['tx_time'])
            item['amount'] = amount
            item['tx_time'] = formatted_tx_time
            item['amount_usd'] = amount_usd

            edge_dict[(item['sender'], item['receiver'])]['data'].append({
                'amount': abs(amount),
                'from_amount': item['from_amount'] if 'from_amount' in item else 0, # for BTC
                'to_amount': item['to_amount'] if 'to_amount' in item else 0, # for BTC
                'tx_hash': item['tx_hash'],
                'depth': item['depth'],
                'tx_time': formatted_tx_time,
                'amount_usd': abs(amount_usd) if amount_usd is not None else 0,
                'symbol': symbol
            })
            edge_dict[(item['sender'], item['receiver'])]['sum'] += abs(item['amount'])

            # Update the sum_dict - internal tracking of amounts by symbol
            if symbol in edge_dict[(item['sender'], item['receiver'])]['sum_dict']:
                edge_dict[(item['sender'], item['receiver'])]['sum_dict'][symbol] += abs(amount)
            else:
                edge_dict[(item['sender'], item['receiver'])]['sum_dict'][symbol] = abs(amount)

            if 'depth' not in edge_dict[(item['sender'], item['receiver'])]:
                edge_dict[(item['sender'], item['receiver'])]['depth'] = item['depth']
        except KeyError:
            edge_id = '{}_{}'.format(counter, mode)
            edge_dict[(item['sender'], item['receiver'])] = create_edge(edge_id, item, node_enum)
            # Track this direction
            if (item['receiver'], item['sender']) not in bidirectional_pairs:
                bidirectional_pairs[(item['sender'], item['receiver'])] = edge_id
            counter += 1

        width = edge_dict[(item['sender'], item['receiver'])]["sum"] * MULTIPLIER
        if width > EDGE_WIDTH_MAX:
            edge_dict[(item['sender'], item['receiver'])]['width'] = EDGE_WIDTH_MAX
        elif width < EDGE_WIDTH_MIN:
            edge_dict[(item['sender'], item['receiver'])]['width'] = EDGE_WIDTH_MIN
        else:
            edge_dict[(item['sender'], item['receiver'])]['width'] = width

    # Process all edges after the main loop to set sum_list and is_swap flags
    for key_tuple, edge in edge_dict.items():
        # Update sum_list from sum_dict with proper formatting
        edge['sum_list'] = [
            f"{amount} {symbol}" for symbol, amount in edge['sum_dict'].items()
        ]
        # Check for bidirectional relationships
        reverse_tuple = (key_tuple[1], key_tuple[0])
        if reverse_tuple in edge_dict:
            # Both edges are part of a bidirectional relationship
            edge['is_bidirectional'] = True
            edge_dict[reverse_tuple]['is_bidirectional'] = True

            # Determine which is original and which is reverse
            # We use the bidirectional_pairs map which recorded the first seen edge ID for each direction
            if key_tuple in bidirectional_pairs:
                # This direction was seen first, so it's the original
                # The reverse should reference this edge
                edge_dict[reverse_tuple]['original_edge_id'] = edge['id']
            else:
                # The reverse direction was seen first, so current edge is the reverse
                # It should reference the original
                edge['original_edge_id'] = edge_dict[reverse_tuple]['id']

    return edge_dict


def assign_edges_btc(result, mode, node_enum):
    edge_dict = {}
    counter = 0
    for item in result:
        try:
            edge_dict[(item['sender'], item['receiver'])]['data'].append({
                "ref_tx_id": item["ref_tx_id"],
                "block_num": item["block_num"],
                "vin_number": item["vin_number"],
                'amount': item['sender_amount'],
                'tx_hash': item['ref_tx_id'],
                'depth': item['depth'],
                'tx_time': item['tx_time'],
                'tx_fees': item['sender_amount'] - item['receiver_amount']
            })
            edge_dict[(item['sender'], item['receiver'])]['sum'] += item['sender_amount']
        except KeyError:
            try:
                edge_dict[(item['sender'], item['receiver'])] = create_edge_btc('{}_{}'.format(counter, mode), item,
                                                                                node_enum)
                counter += 1
            except KeyError:
                pass
        try:
            width = edge_dict[(item['sender'], item['receiver'])]["sum"] * MULTIPLIER
            if width > EDGE_WIDTH_MAX:
                edge_dict[(item['sender'], item['receiver'])]['width'] = EDGE_WIDTH_MAX
            elif width < EDGE_WIDTH_MIN:
                edge_dict[(item['sender'], item['receiver'])]['width'] = EDGE_WIDTH_MIN
            else:
                edge_dict[(item['sender'], item['receiver'])]['width'] = width
        except KeyError:
            pass
    return edge_dict


def assign_nodes(result, mode, token_type='ETH'):
    # mode = 1: distribution
    # mode = -1: source
    nc = NodesCollection()
    volume_count = {}
    counter = 1

    if mode == 1:
        outer = 'receiver'
        inner = 'sender'
        depth_offset = DIST_DEPTH_OFFSET
    elif mode == -1:
        outer = 'sender'
        inner = 'receiver'
        depth_offset = SOURCE_DEPTH_OFFSET

    temp_node = Node(
        id=0,
        address=result[0][inner],
        annotation=result[0].get(inner + '_annotation', ''),
        type=result[0].get(inner + '_type', 'Wallet'),
        depth=0
    )

    nc.add_node(temp_node)

    exclusions = {result[0][inner]: result[0]}
    for item in uniqfy_generator(result, outer, exclusions):
        item_depth = (item['depth'] + depth_offset) * mode
        temp_node = Node(
            id=mode*counter,
            address=item[outer],
            annotation=item.get(outer + '_annotation', ''),
            type=item.get(outer + '_type', 'Wallet'),
            depth=item_depth,
            balance=abs(item.get(outer + '_balance', 0)),
            amount_in=abs(item.get(outer + '_amount_in', 0)),
            amount_out=abs(item.get(outer + '_amount_out', 0)),
        )
        if mode == -1:
            temp_node.level += 1
        nc.add_node(temp_node)
        try:
            volume_count[item[inner]] += 1
        except KeyError:
            volume_count[item[inner]] = 1
        counter += 1
    return nc, volume_count


def assign_nodes_btc(result, mode, wallet_address):
    # mode = 1: distribution
    # mode = -1: source
    nc = NodesCollection()
    volume_count = {}
    counter = 1

    if mode == 1:
        outer = 'receiver'
        inner = 'sender'
        depth_offset = BTC_DIST_DEPTH_OFFSET
    elif mode == -1:
        outer = 'sender'
        inner = 'receiver'
        depth_offset = BTC_SOURCE_DEPTH_OFFSET

    root_node = list(filter(lambda x: x[inner].lower() == wallet_address.lower(), result))
    root_node = root_node[0]

    temp_node = BTCNode(
        id=0,
        address=root_node[inner],
        depth=0,
        annotation=root_node.get(inner + '_annotation', ""),
        type=root_node.get(inner + '_type', 'Wallet')
    )

    nc.add_node(temp_node)

    exclusions = {root_node[inner]: root_node}
    for item in uniqfy_generator(result, outer, exclusions):
        item_depth = (int(item['depth']) + depth_offset) * mode
        temp_node = BTCNode(
            id=mode * counter,
            address=item[outer],
            depth=item_depth,
            annotation=item.get(outer + '_annotation', ""),
            type=item.get(outer + '_type', 'Wallet'),
            balance=item.get(outer + '_balance', None),
            amount_in=item.get(outer + '_amount_in', None),
            amount_out=item.get(outer + '_amount_out', None)
        )
        nc.add_node(temp_node)
        try:
            volume_count[item[inner]] += 1
        except KeyError:
            volume_count[item[inner]] = 1
        counter += 1
    return nc, volume_count


def assign_nodes_btc_coinpath(result, mode):
    nc = NodesCollection()
    volume_count = {}
    counter = 1

    if mode == 1:
        outer = 'receiver'
        inner = 'sender'
    elif mode == -1:
        outer = 'sender'
        inner = 'receiver'

    temp_node = BTCNode(
        id=0,
        address=result[0][inner],
        annotation=result[0][inner + '_annotation'],
        type=result[0][inner + '_type'],
        depth=0,
        balance=0
    )

    nc.add_node(temp_node)

    exclusions = {result[0][inner]: result[0]}
    for item in uniqfy_generator(result, outer, exclusions):
        item_depth = (item['depth']) * mode
        temp_node = BTCNode(
            id=mode*counter,
            address=item[outer],
            annotation=item[outer + '_annotation'],
            type=item[outer + '_type'],
            depth=item_depth,
            balance=item.get(outer + '_balance', 0),
            amount_in=item['tx_value_in'],
            amount_out=item['tx_value_out']
        )
        nc.add_node(temp_node)
        try:
            volume_count[item[inner]] += 1
        except KeyError:
            volume_count[item[inner]] = 1
        counter += 1
    return nc, volume_count


def assign_nodes_btc_path(result, mode):
    # mode = 1: distribution
    # mode = -1: source
    nc = NodesCollection()
    volume_count = {}
    counter = 1

    if mode == 1:
        outer = 'receiver'
        inner = 'sender'
        depth_offset = DIST_DEPTH_OFFSET
    elif mode == -1:
        outer = 'sender'
        inner = 'receiver'
        depth_offset = SOURCE_DEPTH_OFFSET

    temp_node = Node(
        id=0,
        address=result[0][inner],
        annotation=result[0].get(inner + '_annotation', ''),
        type=result[0].get(inner + '_type', 'Wallet'),
        depth=0
    )

    nc.add_node(temp_node)

    exclusions = {result[0][inner]: result[0]}
    for item in uniqfy_generator(result, outer, exclusions):
        item_depth = (item['depth'] + depth_offset) * mode
        temp_node = Node(
            id=mode*counter,
            address=item[outer],
            annotation=item.get(outer + '_annotation', ''),
            type=item.get(outer + '_type', 'Wallet'),
            depth=item_depth,
            balance=item.get(outer + '_balance', 0),
            amount_in=item.get('tx_value_in', 0),
            amount_out=item.get('tx_value_out', 0),
        )
        nc.add_node(temp_node)
        try:
            volume_count[item[inner]] += 1
        except KeyError:
            volume_count[item[inner]] = 1
        counter += 1
    return nc, volume_count


def depth_shift_for_source(result):
    for item_dict in result:
        item_dict.update((k, (int(v) + SOURCE_DEPTH_OFFSET) * (-1)) for k, v in item_dict.items() if k == "depth")


def depth_shift_btc(result, mode):
    for item_dict in result:
        item_dict.update((k, (int(v)) * mode) for k, v in item_dict.items() if k == "depth")


def add_keys_btc(result):
    for item_dict in result:
        item_dict["tx_hash"] = item_dict["ref_tx_id"]

def reverse_source_depth(result):
    for item_dict in result:
        item_dict.update((k, int(v) * -1) for k, v in item_dict.items() if k == "depth")

def add_missing_keys(result, mode):
    prefix_missing = 'sent_' if mode == 1 else 'received_'
    for item_dict in result:
        item_dict["amount"] = item_dict[f"{prefix_missing}amount"]

def generate_nodes_edges(result, mode, build_lossy_graph, token_type='ETH'):
    keys = list(result[0].keys())
    if token_type.upper() in ['XRP', 'XLM']:
        add_missing_keys(result, mode)
    nc, volume_count = assign_nodes(result, mode, token_type)
    edge_dict = assign_edges(result, mode, nc.get_node_enum())
    if mode == -1:
        reverse_source_depth(result)

    limited_edges = {}
    limited_nodes = []
    if nc.count > api_settings.CATV_GRAPH_NODES_CUTOFF and build_lossy_graph:
        limited_edges, limited_nodes = make_lossy_graph(nc, edge_dict, mode)
    limited_edges = limited_edges if limited_edges else edge_dict
    edge_list = list(edge_dict.values())
    add_edge_ids_to_item_list(edge_list,result)
    track_result = {'item_list': result, 'node_list': list(nc.get_nodes_as_dict().values()), 'keys': keys,
                    'node_enum': nc.get_node_enum(), 'edge_list': edge_list,
                    'volume_count_{}'.format(mode): volume_count, 'graph_node_list': list(limited_nodes),
                    'graph_edge_list': list(limited_edges.values())}
    return track_result, nc


def generate_nodes_edges_btc(result, mode, wallet_address):
    keys = list(result[0].keys())
    nc, volume_count = assign_nodes_btc(result, mode, wallet_address)
    edge_dict = assign_edges_btc(result, mode, nc.get_node_enum())
    depth_shift_btc(result, mode)
    edge_list=list(edge_dict.values())
    add_edge_ids_to_item_list(edge_list,result)
    # add_keys_btc(result)

    track_result = {'item_list': result, 'node_list': list(nc.get_nodes_as_dict().values()), 'keys': keys,
                    'node_enum': nc.get_node_enum(), 'edge_list': edge_list,
                    'volume_count_{}'.format(mode): volume_count}
    return track_result, nc


def generate_nodes_edges_coinpath(result, mode, build_lossy_graph):
    keys = list(result[0].keys())
    nc, volume_count = assign_nodes_btc_coinpath(result, mode)
    edge_dict = assign_edges(result, mode, nc.get_node_enum())
    if mode == -1:
        depth_shift_btc(result, mode)
    limited_edges = {}
    limited_nodes = []
    if nc.count > api_settings.CATV_GRAPH_NODES_CUTOFF and build_lossy_graph:
        limited_edges, limited_nodes = make_lossy_graph(nc, edge_dict, mode)
    edge_list=list(edge_dict.values())
    add_edge_ids_to_item_list(edge_list,result)
    limited_edges = limited_edges if limited_edges else edge_dict
    track_result = {'item_list': result, 'node_list': list(nc.get_nodes_as_dict().values()), 'keys': keys,
                    'node_enum': nc.get_node_enum(), 'edge_list': edge_list,
                    'volume_count_{}'.format(mode): volume_count, 'graph_node_list': list(limited_nodes),
                    'graph_edge_list': list(limited_edges.values())}
    return track_result, nc


def generate_nodes_edges_ethcoinpath(result, mode, build_lossy_graph):
    nodes = result[0]['path']
    keys = list(nodes[0].keys())
    item_list = []
    seen_tx_hash = []

    for path_info in result:
        for path in path_info['path']:
            if path['tx_hash'] not in seen_tx_hash:
                item_list.append(path)
                seen_tx_hash.append(path['tx_hash'])

    nc, volume_count = assign_nodes(item_list, mode)
    edge_dict = assign_edges(item_list, mode, nc.get_node_enum())
    edge_list=list(edge_dict.values())
    add_edge_ids_to_item_list(edge_list,result)

    if mode == -1:
        depth_shift_for_source(result)
    limited_edges = {}
    limited_nodes = []
    if nc.count > api_settings.CATV_GRAPH_NODES_CUTOFF and build_lossy_graph:
        limited_edges, limited_nodes = make_lossy_graph(nc, edge_dict, mode)
    limited_edges = limited_edges if limited_edges else edge_dict
    track_result = {'item_list': item_list, 'node_list': list(nc.get_nodes_as_dict().values()), 'keys': keys,
                    'node_enum': nc.get_node_enum(), 'edge_list': edge_list,
                    'volume_count_{}'.format(mode): volume_count, 'graph_node_list': list(limited_nodes),
                    'graph_edge_list': list(limited_edges.values())}
    return track_result, nc


def generate_nodes_edges_btccoinpath(result, mode, build_lossy_graph):
    nodes = result[0]['path']
    keys = list(nodes[0].keys())
    item_list = []
    seen_tx_hash = []

    for path_info in result:
        for path in path_info['path']:
            if path['tx_hash'] not in seen_tx_hash:
                item_list.append(path)
                seen_tx_hash.append(path['tx_hash'])

    nc, volume_count = assign_nodes_btc_path(item_list, mode)
    edge_dict = assign_edges(item_list, mode, nc.get_node_enum())

    if mode == -1:
        depth_shift_for_source(result)
    limited_edges = {}
    limited_nodes = []
    if nc.count > api_settings.CATV_GRAPH_NODES_CUTOFF and build_lossy_graph:
        limited_edges, limited_nodes = make_lossy_graph(nc, edge_dict, mode)
    limited_edges = limited_edges if limited_edges else edge_dict
    edge_list=list(edge_dict.values())
    add_edge_ids_to_item_list(edge_list,result)
    track_result = {'item_list': item_list, 'node_list': list(nc.get_nodes_as_dict().values()), 'keys': keys,
                    'node_enum': nc.get_node_enum(), 'edge_list': edge_list,
                    'volume_count_{}'.format(mode): volume_count, 'graph_node_list': list(limited_nodes),
                    'graph_edge_list': list(limited_edges.values())}
    return track_result, nc