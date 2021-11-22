class ExchangeChecker:
    def __init__(self, graph_data, dist_analysis, src_analysis):
        self.graph_data = graph_data
        self.item_list = graph_data['item_list']
        self.node_list = graph_data['node_list']
        self.edge_list = graph_data['edge_list']
        self.node_enum = graph_data['node_enum']
        self.dist_analysis = dist_analysis
        self.src_analysis = src_analysis

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
        dist_exchange_node_ids = [node['id'] for node in self.node_list if node['id']>=0 and node['group'] == 'Exchange & DEX']
        dist_exchange_node_addresses = [node['address'] for node in self.node_list if node['id']>=0 and node['group'] == 'Exchange & DEX']
        print("dist x-nodes", dist_exchange_node_ids)

        # Data after calling the remove_graph_data method
        nodes_to_be_removed, node_enum_to_be_removed = self.remove_graph_data(dist_exchange_node_ids)

        # Processing item_list
        print("Modifying item list...")
        self.graph_data['item_list'] = [
            item for item in self.graph_data['item_list'] 
                if item['sender'] not in dist_exchange_node_addresses
        ]
        print("Modified item list")

        # Processing node_list
        self.raph_data['node_list'] = [
            node for node in self.graph_data['node_list'] 
                if node['id'] not in nodes_to_be_removed
        ]

        # Processing edge_list
        self.graph_data['edge_list'] = [
            edge for edge in self.graph_data['edge_list'] 
                if edge['from'] not in nodes_to_be_removed 
                if edge['to'] not in nodes_to_be_removed
        ]
        
        # Processing node_enum dict
        for node_address in node_enum_to_be_removed:
            self.graph_data['node_enum'].pop(node_address, None)

        print("nodes to be removed", nodes_to_be_removed)

    def remove_graph_data(self, exchange_node_ids):
        print("length of node list", len(exchange_node_ids))    
        
        nodes_to_be_removed, node_enum_to_be_removed = find_subsequent_nodes(node_list, edge_list, exchange_node_ids)
        print("nodes to be removed from inside method", nodes_to_be_removed)
        print("node_enum_to_be_removed", node_enum_to_be_removed)
        return nodes_to_be_removed, node_enum_to_be_removed

    def find_subsequent_nodes(self, exchange_node_ids):
        nodes_after_exchange = []
        node_enum_to_be_removed = []
        for node_id in exchange_node_ids:
            temp_nodes_list = []
            print(exchange_node_ids.index(node_id),"-exchange ", node_id)
            temp_nodes_list = [edge['to'] for edge in self.edge_list if edge['from'] == node_id]
            node_enum_to_be_removed += [node['address'] for node in self.node_list if node['id'] in temp_nodes_list]
            nodes_after_exchange += temp_nodes_list
            if(len(temp_nodes_list) == 0): 
                print("This exchange has no outgoing")

        print("Nodes after exchange", nodes_after_exchange)
        return nodes_after_exchange, node_enum_to_be_removed
