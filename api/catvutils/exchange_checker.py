from api.models import CatvTokens

FROM = 'from'
TO = 'to'


class ExchangeNodeList:
    def __init__(self, node_list, type):
        self.node_list = node_list
        self.src_exchange_nodes = []
        self.dist_exchange_nodes = []
        self.type = type

    def find_exchange_nodes(self):
        if self.type == 'dist':
            self.dist_exchange_nodes = [node for node in self.node_list if
                                        node['id'] >= 0 and 'exchange' in node['group'].lower()]
        elif self.type == 'src':
            self.src_exchange_nodes = [node for node in self.node_list if
                                       node['id'] <= 0 and 'exchange' in node['group'].lower()]
        else:
            print("Invalid input")

    def get_exchange_nodes(self):
        if self.type == 'dist':
            return self.dist_exchange_nodes
        if self.type == 'src':
            return self.src_exchange_nodes

    def get_exchange_node_ids(self):
        if self.type == 'dist':
            return [node['id'] for node in self.dist_exchange_nodes]
        elif self.type == 'src':
            return [node['id'] for node in self.src_exchange_nodes]
        else:
            print("Invalid type")
            return []

    def get_exchange_node_addresses(self):
        if self.type == 'dist':
            return [node['address'] for node in self.dist_exchange_nodes]
        elif self.type == 'src':
            return [node['address'] for node in self.src_exchange_nodes]
        else:
            print("Invalid type")
            return []


class ExchangeChecker:
    def __init__(self, token_type, graph_data, dist_analysis, src_analysis):
        self.token_type = token_type
        self.graph_data = graph_data
        self.dist_analysis = dist_analysis
        self.src_analysis = src_analysis

        self.item_list = graph_data['item_list']
        self.node_list = graph_data['node_list']
        self.edge_list = graph_data['edge_list']
        self.node_enum = graph_data['node_enum']
        self.send_count = graph_data['send_count']

        self.exchange_nodes = []
        self.exchange_node_ids = []
        self.exchange_node_addresses = []

        self.node_ids_to_be_removed = []
        self.node_addresses_to_be_removed = []
        self.node_ids_after_exchange = []
        self.previous_nodes_iter_list = []
        self.orphan_node_ids = []

    def stop_transfers_at_exchange(self):
        try:
            if 'exchange' not in self.dist_analysis.keys():
                self.dist_analysis['exchange'] = []
            if 'exchange' not in self.src_analysis.keys():
                self.src_analysis['exchange'] = []

            if not self.dist_analysis['exchange'] and not self.src_analysis['exchange']:
                print("No exchanges found")
            elif self.dist_analysis['exchange'] and not self.src_analysis['exchange']:
                print("Exchanges found in distribution nodes only")
                self.tracking_exchanges(mode=1)
            elif not self.dist_analysis['exchange'] and self.src_analysis['exchange']:
                print("Exchanges found in source nodes only")
                self.tracking_exchanges(mode=-1)
            elif self.dist_analysis['exchange'] and self.src_analysis['exchange']:
                print("Exchanges found in both source and distribution")
                self.tracking_exchanges(1)
                self.tracking_exchanges(-1)

        except Exception as e:
            print("The following exception occurred while trying to get exchanges:", e)
            return self.graph_data

        return self.graph_data
    
    def tracking_exchanges(self, mode):
        self.exchange_node_ids = []
        self.exchange_node_addresses = []
        self.node_ids_to_be_removed = []
        self.node_addresses_to_be_removed = []
        self.node_ids_after_exchange = []
        self.previous_nodes_iter_list = []
        self.orphan_node_ids = []

        if mode == -1:
            exchange_nodes_obj = ExchangeNodeList(self.node_list, 'src')
        else:
            exchange_nodes_obj = ExchangeNodeList(self.node_list, 'dist')
        
        exchange_nodes_obj.find_exchange_nodes()
        self.exchange_node_ids = exchange_nodes_obj.get_exchange_node_ids()
        self.exchange_node_addresses = exchange_nodes_obj.get_exchange_node_addresses()
        self.exchange_nodes = exchange_nodes_obj.get_exchange_nodes()
        print("exchange nodes", self.exchange_nodes)

        # finds nodes for removal
        self.find_subsequent_nodes(node_ids_after_exchange=[], mode=mode)
        # checking for mandatory exchange nodes (lowest level nodes)
        self.check_for_mandatory_exchanges(mode=mode)
        # remove post exchange nodes from node_list
        self.process_node_list()
        # remove edges of already removed nodes from edge_list
        self.process_edge_list()
        # remove orphan nodes
        self.remove_orphan_nodes()
        # find node addresses to removed
        self.validate_node_addresses_to_be_removed()
        # remove extra transactions from item_list
        self.process_item_list()
        # set final graph_data
        self.set_graph_data(mode=mode)
        print("Final nodes to be removed", self.node_ids_to_be_removed)

    def find_subsequent_nodes(self, node_ids_after_exchange=[], mode=1, recur=0):
        recur = recur + 1
        if mode == -1:
            outer = FROM
            inner = TO
        else:
            outer = TO
            inner = FROM

        if not node_ids_after_exchange:
            nodes_iter = self.exchange_node_ids
        else:
            nodes_iter = self.filter_node_ids_after_exchange(node_ids_after_exchange)
            node_ids_after_exchange = []
        # print("====================================================================")
        # print(f"recursion {recur} nodes_iter:-", nodes_iter)
        for node_id in nodes_iter:
            current_nodes_list = [
                edge[outer] for edge in self.edge_list
                    if edge[inner] == node_id
            ]
            node_ids_after_exchange += current_nodes_list
            # if current_nodes_list:
            #     if mode == -1:
            #         print(f"node id {node_id} has incoming addresses {current_nodes_list}")
            #     else:
            #         print(f"node id {node_id} has outgoing addresses {current_nodes_list}")
            # else:
            #     if mode == -1:
            #         print(f"node id {node_id} has no incoming addresses")
            #     else:
            #         print(f"node id {node_id} has no outgoing addresses")

        unique_node_ids_after_exchange = list(set(node_ids_after_exchange))
        unique_node_ids_after_exchange.sort()
        # print(f"Unique nodes for iteration {recur}:-", unique_node_ids_after_exchange)
        if unique_node_ids_after_exchange:
            self.node_ids_to_be_removed += unique_node_ids_after_exchange
            self.node_ids_to_be_removed = list(set(self.node_ids_to_be_removed))
            self.node_ids_to_be_removed.sort()
            self.previous_nodes_iter_list += nodes_iter
            # print(f"sorted node_ids_to_be_removed after recursion {recur} :-", self.node_ids_to_be_removed)
            self.find_subsequent_nodes(unique_node_ids_after_exchange, mode, recur)
        else:
            return

    def filter_node_ids_after_exchange(self, node_ids_after_exchange):
        # print("previous_node_iter_list----------------->", self.previous_nodes_iter_list)
        # print("node_ids_after_exchange--------------->", node_ids_after_exchange)
        self.previous_nodes_iter_list = list(set(self.previous_nodes_iter_list))
        filtered_node_ids_after_exchange = [node_id for node_id in node_ids_after_exchange if
                                            node_id not in self.previous_nodes_iter_list]
        # print("filtered_node_ids_after_exchange---------------------->", filtered_node_ids_after_exchange)
        return filtered_node_ids_after_exchange

    def process_node_list(self):
        self.node_list = [
            node for node in self.graph_data['node_list']
            if node['id'] not in self.node_ids_to_be_removed
        ]

    def process_edge_list(self):
        node_ids = [node['id'] for node in self.node_list]
        self.edge_list = [
            edge for edge in self.graph_data['edge_list']
            if edge[FROM] in node_ids
            if edge[TO] in node_ids
        ]

    def process_item_list(self):
        print("Processing item list for token type", self.token_type)
        tx_data_list = [edge['data'] for edge in self.edge_list]
        flat_tx_data_list = [item for sublist in tx_data_list for item in sublist]
        tx_hash_list = [tx_data['tx_hash'] for tx_data in flat_tx_data_list]
        self.item_list = [
            item for item in self.item_list
                if item['tx_hash'] in tx_hash_list
        ]

        if self.token_type in [
            CatvTokens.BTC.value,
            CatvTokens.LTC.value,
            CatvTokens.BCH.value
        ]:
            tx_hash_list_from_item_list = [item['tx_hash'] for item in self.item_list]
            if len(tx_hash_list_from_item_list)>len(set(tx_hash_list_from_item_list)):                
                node_addresses = [node['address'] for node in self.node_list]
                self.item_list = [
                    item for item in self.item_list
                        if item['sender'] in node_addresses and item['receiver'] in node_addresses
                ]

    def validate_node_addresses_to_be_removed(self):
        combined_node_ids = self.node_ids_to_be_removed + self.orphan_node_ids
        self.node_addresses_to_be_removed = [
            node['address'] for node in self.graph_data['node_list']
                if node['id'] in combined_node_ids
        ]

    def remove_orphan_nodes(self):
        edge_to_list = [edge[TO] for edge in self.edge_list]
        edge_from_list = [edge[FROM] for edge in self.edge_list]

        self.orphan_node_ids = [node['id'] for node in self.node_list
                                    if all([
                                        node['id'] not in edge_to_list, 
                                        node['id'] not in edge_from_list
                                    ])
                                ]
        print("orphan nodes", self.orphan_node_ids)
        self.node_list = [node for node in self.node_list if node['id'] not in self.orphan_node_ids]

    def set_graph_data(self, mode):
        # process node_enum and send_count dicts
        for node_address in self.node_addresses_to_be_removed:
            self.node_enum.pop(node_address, None)
            self.send_count.pop(node_address, None)

        # updating the final values for graph_data
        self.graph_data['node_list'] = self.node_list
        self.graph_data['edge_list'] = self.edge_list
        self.graph_data['item_list'] = self.item_list
        self.graph_data['node_enum'] = self.node_enum
        self.graph_data['send_count'] = self.send_count

    def check_for_mandatory_exchanges(self, mode):
        if mode == -1:
            exchange_levels = [exchange['level'] for exchange in self.exchange_nodes]
            exchange_levels.sort(reverse = True)
        else:
            exchange_levels = [exchange['level'] for exchange in self.exchange_nodes]
            exchange_levels.sort()
        
        for exchange in self.exchange_nodes:
            if exchange['level'] == exchange_levels[0]:
                if exchange['id'] in self.node_ids_to_be_removed:
                    self.node_ids_to_be_removed.remove(exchange['id'])