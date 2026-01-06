'''
This module implements a functionality where all nodes originating
from exchange addresses on the distribution side are removed, and
all nodes ending in exchange addresses on the source side are removed.
This is done in the following sequence of steps:
    1. Check if exchanges exist in the source side, distribution
        side or both.
    2. The edge list is modified with a list comprehension, in order
        to remove all edges that are originating from exchanges (distribution)
        or ending in exchanges (source). We now have a disjointed graph with
        missing edges wherever there are exchange nodes.
    3. Two breadth-first search methods (one for source and one for distribution) 
        are implemented to find the node ids of all nodes originating from the 
        origin node (node['id']=0) or ending in the origin node. These nodes are 
        the ones that will be kept in the final data that is returned.
    4. Now we have all the necessary node data. Using this, the node_list, edge_list 
        and the item_list data are updated, along with the node_enum, send_count and 
        receive_count dictionaries. These are all stored in the global variables inside
        the ExchangeChecker class.
    5. Once we have all the updated data, the graph_data dictionary is updated accordingly
        and returned back from the stop_transfers_at_exchange() method.
'''

from api.models import CatvTokens
import traceback

FROM = 'from'
TO = 'to'

'''
This class performs all operations related to getting 
exchange address data like nodes and node ids for exchanges
'''


class ExchangeNodeList:
    def __init__(self, src_node_list, dist_node_list):
        self.src_node_list = src_node_list
        self.dist_node_list = dist_node_list
        self.src_exchange_nodes = []
        self.dist_exchange_nodes = []

    def find_exchange_nodes(self, send_count, receive_count):
        # extracting exchanges nodes from node_list
        self.dist_exchange_nodes = [node for node in self.dist_node_list if 'exchange' in node['group'].lower()]

        self.src_exchange_nodes = [node for node in self.src_node_list if 'exchange' in node['group'].lower()]

        # marking nodes with unique transactions above 5000 as exchange in both dist/src side
        if send_count:
            dist_nodes_marked_as_exchange = [node for node in self.dist_node_list
                                             if node['id'] != 0 and node['address'] in send_count and send_count[node['address']] >= 1000]
            # adding dist side marked nodes into dist_exchange_nodes
            self.dist_exchange_nodes.extend(node for node in dist_nodes_marked_as_exchange
                                            if node not in self.dist_exchange_nodes)

        if receive_count:
            src_nodes_marked_as_exchange = [node for node in self.src_node_list
                                            if node['id'] != 0 and node['address'] in receive_count and receive_count[node['address']] >= 1000]
            # adding src side marked nodes into src_exchange_nodes
            self.src_exchange_nodes.extend(node for node in src_nodes_marked_as_exchange
                                           if node not in self.src_exchange_nodes)

        return self.dist_exchange_nodes, self.src_exchange_nodes

    def get_exchange_nodes(self, mode):
        if mode == 1:
            return self.dist_exchange_nodes
        else:
            return self.src_exchange_nodes

    def get_exchange_node_ids(self, mode):
        if mode == 1:
            return [node['id'] for node in self.dist_exchange_nodes]
        else:
            return [node['id'] for node in self.src_exchange_nodes]


'''
This class removes the nodes coming out of or going into exchanges 
depending on whether it's on the source side or the dist side
'''


class ExchangeChecker:
    def __init__(self, source_depth, distribution_depth, token_type, graph_data):
        # data received as input
        self.source_depth = source_depth
        self.distribution_depth = distribution_depth
        self.token_type = token_type
        self.graph_data = graph_data
        # self.dist_analysis = dist_analysis
        # self.src_analysis = src_analysis

        # creating our custom objects with input data for better readability
        self.item_list = graph_data['item_list']
        self.node_list = graph_data['node_list']
        self.edge_list = graph_data['edge_list']
        self.node_enum = graph_data['node_enum']
        self.receive_count = None
        self.send_count = None
        self.exchange_nodes_obj = None

        # checking if send_count and receive_count exist
        if 'send_count' in self.graph_data.keys():
            self.send_count = graph_data['send_count']
        if 'receive_count' in self.graph_data.keys():
            self.receive_count = self.graph_data['receive_count']

        # splitting the src and dist nodes for easier BFS processing
        self.src_node_list = [
            node for node in self.node_list
            if node['id'] <= 0
        ]
        self.dist_node_list = [
            node for node in self.node_list
            if node['id'] >= 0
        ]

        # exchange data
        self.exchange_nodes = []
        self.exchange_node_ids = []

        # lists needed for BFS (both src and dist, done separately)
        self.dist_visited_connected_nodes_list = [0]
        self.dist_connected_nodes_list = []
        self.src_visited_connected_nodes_list = [0]
        self.src_connected_nodes_list = [0]

        # node address that will be part of the final data
        self.required_node_addresses = []

    def _annotate_exchange_user_wallets(self, dist_exchange_nodes):
        """
        Annotate wallets that directly send to exchanges as '[Exchange] User Wallet'.
        Only processes distribution side exchanges.

        Args:
            dist_exchange_nodes: List of exchange nodes on the distribution side
        """
        if not dist_exchange_nodes:
            return

        # Build exchange address to label mapping (lowercase for comparison)
        exchange_addr_to_label = {
            node["address"].lower(): node.get("label", node["address"][:8])
            for node in dist_exchange_nodes
        }

        # Build node address map for quick lookup
        node_address_map = {node["address"].lower(): node for node in self.node_list}

        # Track which exchanges each sender wallet sends to
        sender_to_exchanges = {}  # {sender_address_lower: set of exchange labels}

        for tx in self.item_list:
            receiver_lower = tx["receiver"].lower()
            sender_lower = tx["sender"].lower()

            # Check if receiver is a distribution exchange
            if receiver_lower not in exchange_addr_to_label:
                continue

            # Get sender node and verify it's on distribution side (level > 0)
            sender_node = node_address_map.get(sender_lower)
            if not sender_node or sender_node.get("level", 0) <= 0:
                continue

            # Skip if sender is also an exchange
            if sender_node.get("group") == "Exchange/DEX/Bridge/Mixer":
                continue

            # Collect exchange label for this sender
            exchange_label = exchange_addr_to_label[receiver_lower]
            if sender_lower not in sender_to_exchanges:
                sender_to_exchanges[sender_lower] = set()
            sender_to_exchanges[sender_lower].add(exchange_label)

        # Update annotations for sender wallets
        for sender_addr, exchange_labels in sender_to_exchanges.items():
            sender_node = node_address_map.get(sender_addr)
            if not sender_node:
                continue

            # Build the exchange user wallet annotation
            exchange_user_annotation = "/".join(sorted(exchange_labels)) + " User Wallet"

            # Append to existing annotation
            existing_annotation = sender_node.get("annotation", "")
            if existing_annotation:
                new_annotation = f"{existing_annotation}, {exchange_user_annotation}"
            else:
                new_annotation = exchange_user_annotation

            # Update node
            sender_node["annotation"] = new_annotation
            sender_node["group"] = "Annotated"

            # Update transaction annotations where this address is sender or receiver
            for tx in self.item_list:
                if tx["sender"].lower() == sender_addr:
                    tx["sender_annotation"] = new_annotation
                if tx["receiver"].lower() == sender_addr:
                    tx["receiver_annotation"] = new_annotation

    def stop_transfers_at_exchange(self):
        try:
            self.exchange_nodes_obj = ExchangeNodeList(self.src_node_list, self.dist_node_list)
            dist_exchange_nodes, src_exchange_nodes = self.exchange_nodes_obj.find_exchange_nodes(self.send_count,
                                                                                                  self.receive_count)
            # Annotate wallets that send directly to distribution exchanges
            self._annotate_exchange_user_wallets(dist_exchange_nodes)

            if not dist_exchange_nodes and not src_exchange_nodes:
                print("No exchanges found")
            elif dist_exchange_nodes and not src_exchange_nodes:
                print("Exchanges found in distribution nodes only")
                self.tracking_exchanges(mode=1)
            elif not dist_exchange_nodes and src_exchange_nodes:
                print("Exchanges found in source nodes only")
                self.tracking_exchanges(mode=-1)
            elif dist_exchange_nodes and src_exchange_nodes:
                print("Exchanges found in both source and distribution")
                self.tracking_exchanges_combined(src_exchange_nodes, dist_exchange_nodes)
        except Exception as e:
            traceback.print_exc()
            print("The following exception occurred while trying to get exchanges:", e)
            return self.graph_data

        return self.graph_data

    def tracking_exchanges_combined(self, src_exchange_nodes, dist_exchange_nodes):
        """
        Process both source and distribution exchanges in a single pass
        to properly handle cases where both types exist
        """
        # Get exchange node IDs for both sides
        src_exchange_node_ids = [node['id'] for node in src_exchange_nodes]
        dist_exchange_node_ids = [node['id'] for node in dist_exchange_nodes]

        # Separate swap edges from non-swap edges
        swap_edges = [edge for edge in self.edge_list if edge.get('is_swap', False)]
        non_swap_edges = [edge for edge in self.edge_list if not edge.get('is_swap', False)]

        # Apply both filters at once to non-swap edges
        filtered_non_swap_edges = [
            edge for edge in non_swap_edges
            if (edge['to'] not in src_exchange_node_ids or edge.get('is_swap',
                                                                    False)) and  # Not ending at source exchanges
               (edge['from'] not in dist_exchange_node_ids or edge.get('is_swap', False))
            # Not starting from dist exchanges
        ]

        # Update edge list with filtered non-swap edges plus all swap edges
        self.edge_list = filtered_non_swap_edges + swap_edges

        # Run both BFS methods to identify connected nodes
        self.bfs_src_connected_nodes()
        self.bfs_dist_connected_nodes()

        # Filter node lists based on BFS results
        self.src_node_list = [
            node for node in self.src_node_list
            if node['id'] in self.src_connected_nodes_list
        ]

        self.dist_node_list = [
            node for node in self.dist_node_list
            if node['id'] in self.dist_connected_nodes_list
        ]

        # Process nodes and edges
        self.process_node_list()

        # Process edges with combined mode
        self.process_edge_list_combined(src_exchange_node_ids, dist_exchange_node_ids)

        # Complete remaining processing
        self.validate_node_addresses()
        self.process_address_data()
        self.process_item_list()

        # Set final graph data
        self.set_graph_data(mode=0)  # Using 0 to indicate combined mode

    def process_edge_list_combined(self, src_exchange_node_ids, dist_exchange_node_ids):
        """
        Process edge list for combined source and distribution modes
        """
        node_ids = [node['id'] for node in self.node_list]

        # Get all swap edges
        swap_edges = [edge for edge in self.edge_list if edge.get('is_swap', False)]

        # Filter non-swap edges
        non_swap_edges = [
            edge for edge in self.edge_list
            if not edge.get('is_swap', False) and
               edge['from'] in node_ids and
               edge['to'] in node_ids and
               edge['to'] not in src_exchange_node_ids and  # Not ending at source exchanges
               edge['from'] not in dist_exchange_node_ids  # Not starting from dist exchanges
        ]

        # Combine filtered non-swap edges with all swap edges
        self.edge_list = non_swap_edges + swap_edges

    def tracking_exchanges(self, mode):
        self.exchange_node_ids = []
        self.required_node_addresses = []
        self.exchange_node_ids = self.exchange_nodes_obj.get_exchange_node_ids(mode)
        self.exchange_nodes = self.exchange_nodes_obj.get_exchange_nodes(mode)

        # First, separate swap edges from non-swap edges
        swap_edges = [edge for edge in self.edge_list if edge.get('is_swap', False)]
        non_swap_edges = [edge for edge in self.edge_list if not edge.get('is_swap', False)]

        if mode == -1:
            # Filter only non-swap edges - remove those ending at exchanges
            filtered_non_swap_edges = [
                edge for edge in non_swap_edges
                if edge[TO] not in self.exchange_node_ids
            ]

            # Combine filtered non-swap edges with all swap edges
            self.edge_list = filtered_non_swap_edges + swap_edges

            # Run BFS and update connected nodes
            self.bfs_src_connected_nodes()

            # Modify src node list after BFS
            self.src_node_list = [
                node for node in self.src_node_list
                if node['id'] in self.src_connected_nodes_list
            ]
        else:
            # Filter only non-swap edges - remove those starting from exchanges
            filtered_non_swap_edges = [
                edge for edge in non_swap_edges
                if edge[FROM] not in self.exchange_node_ids
            ]

            # Combine filtered non-swap edges with all swap edges
            self.edge_list = filtered_non_swap_edges + swap_edges

            # Run BFS and update connected nodes
            self.bfs_dist_connected_nodes()

            # Modify dist node list after BFS
            self.dist_node_list = [
                node for node in self.dist_node_list
                if node['id'] in self.dist_connected_nodes_list
            ]

        # Process the node list first to remove irrelevant nodes
        self.process_node_list()

        # Remove edges of already removed nodes from edge_list
        self.process_edge_list(mode=mode)

        # Find node addresses to keep
        self.validate_node_addresses()

        # Process node_enum, receive_count and send_count
        self.process_address_data()

        # Remove extra transactions from item_list
        self.process_item_list()

        # Set final graph_data
        self.set_graph_data(mode=mode)

    # breadth-first search to remove disconnected nodes from the main graph in src side
    def bfs_src_connected_nodes(self):
        node_ids = [node['id'] for node in self.src_node_list]
        src_edges = [
            edge for edge in self.edge_list
            if edge[TO] <= 0 and edge[FROM] < 0 and
               edge[TO] in node_ids and
               edge[FROM] in node_ids
        ]

        # Adding origin node to the current node list to start the while loop
        current_nodes_list = [0]
        while len(current_nodes_list) > 0:
            previously_visited_nodes = list(set(current_nodes_list))
            current_nodes_list = list(set([
                edge[FROM] for edge in src_edges
                if edge[TO] in previously_visited_nodes and
                   edge[FROM] not in self.src_visited_connected_nodes_list
            ]))
            self.src_visited_connected_nodes_list += current_nodes_list
        self.src_connected_nodes_list = list(set(self.src_visited_connected_nodes_list))

    # breadth-first search to remove disconnected nodes from the main graph in dist side
    def bfs_dist_connected_nodes(self):
        node_ids = [node['id'] for node in self.dist_node_list]
        dist_edges = [
            edge for edge in self.edge_list
            if edge[TO] > 0 and edge[FROM] >= 0 and
               edge[FROM] in node_ids and
               edge[TO] in node_ids
        ]

        # Adding origin node to the current node list to start the while loop
        current_nodes_list = [0]
        while len(current_nodes_list) > 0:
            previously_visited_nodes = list(set(current_nodes_list))
            current_nodes_list = list(set([
                edge[TO] for edge in dist_edges
                if edge[FROM] in previously_visited_nodes and
                   edge[TO] not in self.dist_visited_connected_nodes_list
            ]))
            self.dist_visited_connected_nodes_list += current_nodes_list
        self.dist_connected_nodes_list = list(set(self.dist_visited_connected_nodes_list))

    def process_node_list(self):
        src_node_ids = [node['id'] for node in self.src_node_list]
        dist_node_ids = [node['id'] for node in self.dist_node_list]
        final_node_ids = list(set(src_node_ids + dist_node_ids))
        self.node_list = [
            node for node in self.graph_data['node_list']
            if node['id'] in final_node_ids
        ]

    def process_edge_list(self, mode):
        """
        Process edge list to remove edges involving exchange nodes,
        but preserve swap edges regardless of exchange involvement.
        """
        if mode == -1:
            key = 'to'  # Using string keys instead of variables
        else:
            key = 'from'

        node_ids = [node['id'] for node in self.node_list]

        # Separate swap edges from non-swap edges in the original edge list
        swap_edges = [
            edge for edge in self.graph_data['edge_list']
            if edge.get('is_swap', False) and
               edge['from'] in node_ids and
               edge['to'] in node_ids
        ]

        # Filter non-swap edges
        non_swap_edges = [
            edge for edge in self.graph_data['edge_list']
            if not edge.get('is_swap', False) and
               edge['from'] in node_ids and
               edge['to'] in node_ids and
               edge[key] not in self.exchange_node_ids
        ]

        # Combine filtered non-swap edges with all swap edges
        self.edge_list = non_swap_edges + swap_edges
        print(f"After process_edge_list: {len(self.edge_list)} total edges, {len(swap_edges)} swap edges kept")

    # getting the node addresses that will be kept after processing
    def validate_node_addresses(self):
        self.required_node_addresses = [
            node['address'] for node in self.node_list
        ]

    def process_address_data(self):
        # verifying that all node addresses in the list are unique
        required_node_addresses = set(self.required_node_addresses)

        # filter node_enum dictionary
        node_addresses_to_be_removed_from_node_enum = [
            node_address for node_address in self.node_enum.keys()
            if node_address not in required_node_addresses
        ]
        for node_address in node_addresses_to_be_removed_from_node_enum:
            self.node_enum.pop(node_address, None)

        # check if send_count exists and modify it if it does
        if self.send_count is not None:
            node_addresses_to_be_removed_from_send_count = [
                node_address for node_address in self.send_count.keys()
                if node_address not in required_node_addresses
            ]
            for node_address in node_addresses_to_be_removed_from_send_count:
                self.send_count.pop(node_address, None)

        # check if receive_count exists and modify it if it does
        if self.receive_count is not None:
            node_addresses_to_be_removed_from_receive_count = [
                node_address for node_address in self.receive_count.keys()
                if node_address not in required_node_addresses
            ]
            for node_address in node_addresses_to_be_removed_from_receive_count:
                self.receive_count.pop(node_address, None)

    def process_item_list(self):
        # print("Processing item list for token type", self.token_type)
        tx_data_list = [edge['data'] for edge in self.edge_list]
        flat_tx_data_list = [item for sublist in tx_data_list for item in sublist]
        tx_hash_list = [tx_data['tx_hash'] for tx_data in flat_tx_data_list]
        self.item_list = [
            item for item in self.item_list
            if item['tx_hash'] in tx_hash_list
        ]

        '''        
        BTC, LTC and BCH can have multiple txns with the same hash. This
        verifies and removes those txns accordingly by searching for the 
        src and dist addresses along with the tx_hash
        '''
        if self.token_type in [
            CatvTokens.BTC.value,
            CatvTokens.LTC.value,
            CatvTokens.BCH.value
        ]:
            tx_hash_list_from_item_list = [item['tx_hash'] for item in self.item_list]
            if len(tx_hash_list_from_item_list) > len(set(tx_hash_list_from_item_list)):
                self.item_list = [
                    item for item in self.item_list
                    if item['sender'] in self.required_node_addresses and
                       item['receiver'] in self.required_node_addresses
                ]

    def set_graph_data(self, mode):
        # updating the final values for graph_data
        self.graph_data['node_list'] = self.node_list
        self.graph_data['edge_list'] = self.edge_list
        self.graph_data['item_list'] = self.item_list
        self.graph_data['node_enum'] = self.node_enum
        if self.send_count is not None:
            self.graph_data['send_count'] = self.send_count
        if self.receive_count is not None:
            self.graph_data['receive_count'] = self.receive_count
