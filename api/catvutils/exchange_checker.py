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

    def stop_transfers_at_exchange(self):
        if len(self.dist_analysis['exchange'])==0 and len(self.src_analysis['exchange'])==0:
            print("No exchanges found")

        elif len(self.dist_analysis['exchange'])>0 and len(self.src_analysis['exchange'])==0:
            print("Exchanges found in distribution nodes only")
            self.dist_exchanges()
        
        elif len(self.dist_analysis['exchange'])==0 and len(self.src_analysis['exchange'])>0:
            print("Exchanges found in source nodes only")
            self.src_exchanges()

        elif len(self.dist_analysis['exchange'])>0 and len(self.src_analysis['exchange'])>0:
            print("Exchanges found in both source and distribution")
            self.src_exchanges()
            self.dist_exchanges()
        
        return self.graph_data

    def src_exchanges(self):
        src_exchange_nodes = [node for node in self.node_list if node['id']<=0 and 'exchange' in node['group'].lower()]
        nodes_before_exchange = [edge for edge in self.edge_list if edge['to'] == src_exchange_nodes[0]['id']]
        print("src x-nodes ", src_exchange_nodes)
        print("nodes before x-node ", nodes_before_exchange)
        return self.graph_data

    def dist_exchanges(self):
        # Getting the initial list of exchange nodes
        self.dist_exchange_node_ids = [
            node['id'] for node in self.node_list 
                if node['id']>=0 and node['group'] == 'Exchange & DEX'
        ]
        self.dist_exchange_node_addresses = [
            node['address'] for node in self.node_list 
                if node['id']>=0 and node['group'] == 'Exchange & DEX'
        ]
        print("dist x-nodes", self.dist_exchange_node_ids)

        # Data modified calling the remove_graph_data method
        self.remove_graph_data()

        # Processing item_list
        print("Modifying item list...")
        self.graph_data['item_list'] = [
            item for item in self.graph_data['item_list'] 
                if item['sender'] not in self.dist_exchange_node_addresses
        ]
        print("Modified item list")

        # Processing node_list
        self.graph_data['node_list'] = [
            node for node in self.graph_data['node_list'] 
                if node['id'] not in self.node_ids_to_be_removed
        ]

        # Processing edge_list
        self.graph_data['edge_list'] = [
            edge for edge in self.graph_data['edge_list'] 
                if edge['from'] not in self.node_ids_to_be_removed 
                if edge['to'] not in self.node_ids_to_be_removed
        ]

        # Processing node_enum dict
        for node_address in self.node_addresses_to_be_removed:
            self.graph_data['node_enum'].pop(node_address, None)

    def remove_graph_data(self):       
        self.find_subsequent_nodes()
        print("nodes to be removed from inside method", self.node_ids_to_be_removed)
        print("node_addresses_to_be_removed", self.node_addresses_to_be_removed)

    def find_subsequent_nodes(self):
        if len(self.node_ids_to_be_removed)>0:
            nodes_iter = self.node_ids_to_be_removed
        else:
            nodes_iter = self.dist_exchange_node_ids
        nodes_after_exchange = []
        for node_id in nodes_iter:
            temp_nodes_list = []
            print(nodes_iter.index(node_id),"-exchange ", node_id)
            temp_nodes_list = [edge['to'] for edge in self.edge_list if edge['from'] == node_id]
            nodes_after_exchange += temp_nodes_list
            if(len(temp_nodes_list) == 0): 
                print("This address has no outgoing addresses")
        print("Nodes after exchange after for loop", nodes_after_exchange)

        if(len(nodes_after_exchange) == 0):
            return
        else:
            self.node_addresses_to_be_removed += [
                node['address'] for node in self.node_list 
                    if node['id'] in nodes_after_exchange
            ]
            self.node_ids_to_be_removed.append(nodes_after_exchange)
            self.find_subsequent_nodes()
