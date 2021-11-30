import sys

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
    def __init__(self, graph_data, dist_analysis, src_analysis):
        self.graph_data = graph_data
        self.item_list = graph_data['item_list']
        self.node_list = graph_data['node_list']
        self.edge_list = graph_data['edge_list']
        self.node_enum = graph_data['node_enum']
        self.dist_analysis = dist_analysis
        self.src_analysis = src_analysis
        self.dist_exchange_node_ids = []
        self.dist_exchange_node_addresses = []
        self.src_exchange_node_ids = []
        self.src_exchange_node_addresses = []
        self.node_ids_to_be_removed = []
        self.node_addresses_to_be_removed = []
        self.node_ids_after_exchange = []
        self.previous_nodes_iter_list = []
        self.remove_incoming_edge = []
        self.remove_outgoing_edge = []
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
                self.dist_exchanges()
            elif not self.dist_analysis['exchange'] and self.src_analysis['exchange']:
                print("Exchanges found in source nodes only")
                self.src_exchanges()
            elif self.dist_analysis['exchange'] and self.src_analysis['exchange']:
                print("Exchanges found in both source and distribution")
                self.src_exchanges()
                self.dist_exchanges()
        except Exception as e:
            print("The following exception occurred while trying to get exchanges:", e)
            return self.graph_data

        return self.graph_data

    def get_node_level(self, node_id):
        return [
            node['level'] for node in self.node_list
            if node['id'] == node_id
        ][0]

    def src_exchanges(self):
        src_exchange_nodes_obj = ExchangeNodeList(self.node_list, 'src')
        src_exchange_nodes_obj.find_exchange_nodes()
        self.src_exchange_node_ids = src_exchange_nodes_obj.get_exchange_node_ids()
        self.src_exchange_node_addresses = src_exchange_nodes_obj.get_exchange_node_addresses()
        exchange_nodes = src_exchange_nodes_obj.get_exchange_nodes()
        print("src x-nodes", exchange_nodes)

        # finds nodes for removal pre exchange (source)
        self.find_subsequent_nodes(mode=-1)

        return self.graph_data

    def dist_exchanges(self):
        # Getting the initial list of exchange node data
        dist_exchange_nodes_obj = ExchangeNodeList(self.node_list, 'dist')
        dist_exchange_nodes_obj.find_exchange_nodes()
        self.dist_exchange_node_ids = dist_exchange_nodes_obj.get_exchange_node_ids()
        self.dist_exchange_node_addresses = dist_exchange_nodes_obj.get_exchange_node_addresses()
        exchange_nodes = dist_exchange_nodes_obj.get_exchange_nodes()
        print("dist x-nodes", exchange_nodes)

        # finds nodes for removal post exchange (distribution)
        self.find_subsequent_nodes()

        # remove post exchange nodes from node_list
        self.process_node_list()

        # remove edges of already removed nodes from edge_list
        self.process_edge_list()

        self.remove_orphan_nodes()

        # find node addresses to removed
        self.validate_node_addresses_to_be_removed()

        # set final graph_data
        self.set_graph_data()

    def find_subsequent_nodes(self, node_ids_after_exchange=[], mode=1, recur=0):
        recur = recur + 1
        if mode == -1:
            outer = FROM
            inner = TO
        else:
            outer = TO
            inner = FROM

        if not node_ids_after_exchange:
            if mode == -1:
                nodes_iter = self.src_exchange_node_ids
            else:
                nodes_iter = self.dist_exchange_node_ids
        else:
            nodes_iter = self.filter_node_ids_after_exchange(node_ids_after_exchange)
            node_ids_after_exchange = []
        print("====================================================================")
        print(f"recursion {recur} nodes_iter:-", nodes_iter)
        for node_id in nodes_iter:
            # current_node_level = self.get_node_level(node_id)
            temp_nodes_list = [
                edge[outer] for edge in self.edge_list
                if edge[inner] == node_id
                # if current_node_level < self.get_node_level(edge['to'])
            ]
            node_ids_after_exchange += temp_nodes_list
            if temp_nodes_list:
                # self.process_edge_list('from', node_id)
                if mode == -1:
                    print(f"node id {node_id} has incoming addresses {temp_nodes_list}")
                else:
                    print(f"node id {node_id} has outgoing addresses {temp_nodes_list}")
            else:
                # self.process_edge_list('to', node_id)
                if mode == -1:
                    print(f"node id {node_id} has no incoming addresses")
                else:
                    print(f"node id {node_id} has no outgoing addresses")

        unique_node_ids_after_exchange = list(set(node_ids_after_exchange))
        unique_node_ids_after_exchange.sort()
        print(f"Unique nodes for iteration {recur}:-", unique_node_ids_after_exchange)
        if unique_node_ids_after_exchange:
            self.node_ids_to_be_removed += unique_node_ids_after_exchange
            self.node_ids_to_be_removed = list(set(self.node_ids_to_be_removed))
            self.node_ids_to_be_removed.sort()
            self.previous_nodes_iter_list += nodes_iter
            print(f"sorted node_ids_to_be_removed after recursion {recur} :-", self.node_ids_to_be_removed)
            self.find_subsequent_nodes(unique_node_ids_after_exchange, mode, recur)
        else:
            print("Final nodes to be removed", self.node_ids_to_be_removed)
            return

    def filter_node_ids_after_exchange(self, node_ids_after_exchange):
        print("previous_node_iter_list----------------->", self.previous_nodes_iter_list)
        print("node_ids_after_exchange--------------->", node_ids_after_exchange)
        self.previous_nodes_iter_list = list(set(self.previous_nodes_iter_list))
        filtered_node_ids_after_exchange = [node_id for node_id in node_ids_after_exchange if
                                            node_id not in self.previous_nodes_iter_list]
        print("filtered_node_ids_after_exchange---------------------->", filtered_node_ids_after_exchange)
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
            if edge['from'] in node_ids
            if edge['to'] in node_ids
        ]

    def validate_node_addresses_to_be_removed(self):
        combined_node_ids = self.node_ids_to_be_removed + self.orphan_node_ids
        self.node_addresses_to_be_removed = [
            node['address'] for node in self.graph_data['node_list']
            if node['id'] in combined_node_ids
        ]

    def remove_orphan_nodes(self):
        edge_to_list = [edge['to'] for edge in self.edge_list]
        edge_from_list = [edge['from'] for edge in self.edge_list]

        self.orphan_node_ids = [node['id'] for node in self.node_list
                                if all([node['id'] not in edge_to_list, node['id'] not in edge_from_list])
                                ]
        print("orphan nodes", self.orphan_node_ids)
        self.node_list = [node for node in self.node_list if node['id'] not in self.orphan_node_ids]

    def set_graph_data(self):
        self.graph_data['item_list'] = [
            item for item in self.graph_data['item_list']
            if item['sender'] not in self.dist_exchange_node_addresses
            if item['sender'] not in self.node_addresses_to_be_removed
            if item['receiver'] not in self.node_addresses_to_be_removed
        ]

        # process node_enum and send_count dicts
        for node_address in self.node_addresses_to_be_removed:
            self.graph_data['node_enum'].pop(node_address, None)
            self.graph_data['send_count'].pop(node_address, None)

        self.graph_data['node_list'] = self.node_list
        self.graph_data['edge_list'] = self.edge_list
