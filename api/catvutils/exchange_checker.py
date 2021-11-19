def stop_transfers_at_exchange(graph_data, dist_analysis, src_analysis):
    if len(dist_analysis['exchange'])==0 and len(src_analysis['exchange'])==0:
        print("No exchanges found")
        return graph_data
    
    elif len(dist_analysis['exchange'])>0 and len(src_analysis['exchange'])==0:
        print("Exchanges found in distribution nodes only")
        return dist_exchanges(graph_data)
    
    elif len(dist_analysis['exchange'])==0 and len(src_analysis['exchange'])>0:
        print("Exchanges found in source nodes only")
        return graph_data

    elif len(dist_analysis['exchange'])>0 and len(src_analysis['exchange'])>0:
        print("Exchanges found in both source and distribution") 
        return src_exchanges(graph_data)

def src_exchanges(graph_data):
    src_exchange_nodes = [node for node in graph_data['node_list'] if node['id']<=0 and 'exchange' in node['group'].lower()]
    nodes_before_exchange = [edge for edge in graph_data['edge_list'] if edge['to'] == src_exchange_nodes[0]['id']]
    print("src x-nodes ", src_exchange_nodes)
    print("nodes before x-node ", nodes_before_exchange)
    return graph_data

def dist_exchanges(graph_data):
    # Getting the initial list of exchange nodes
    dist_exchange_nodes = [node['id'] for node in graph_data['node_list'] if node['id']>=0 and node['group'] == 'Exchange & DEX']
    print("dist x-nodes", dist_exchange_nodes)

    # Data after calling the remove_graph_data method
    nodes_to_be_removed, node_enum_to_be_removed = remove_graph_data(graph_data, dist_exchange_nodes)

    # Processing node_list
    graph_data['node_list'] = [
        node for node in graph_data['node_list'] 
            if node['id'] not in nodes_to_be_removed
    ]
    # Processing edge_list
    graph_data['edge_list'] = [
        edge for edge in graph_data['edge_list'] 
            if edge['from'] not in nodes_to_be_removed 
            if edge['to'] not in nodes_to_be_removed
    ]
    # Processing node_enum dict
    for node_address in node_enum_to_be_removed:
        graph_data['node_enum'].pop(node_address, None)

    print("nodes to be removed", nodes_to_be_removed)
    return graph_data

def remove_graph_data(graph_data, exchange_node_ids):
    edge_list = graph_data['edge_list']
    node_list = graph_data['node_list']
    node_enum_to_be_removed = []
    exchange_node_ids_iter = exchange_node_ids
    nodes_to_be_removed = []
    print("length of node list", len(exchange_node_ids_iter))

    # for node_id in exchange_node_ids_iter:
    #     nodes_after_exchange = []
    #     print(exchange_node_ids_iter.index(node_id),"-exchange ", node_id)
    #     # exchange_node_ids_iter += [edge['to'] for edge in edge_list if edge['from'] == node_id]
    #     for edge in edge_list:
    #         if edge['from'] == node_id:
    #             nodes_after_exchange.append(edge['to'])
    #     print("Nodes after exchange", nodes_after_exchange)

    #     node_enum_to_be_removed += [node['address'] for node in node_list if node['id'] == node_id]
    #     nodes_to_be_removed += nodes_after_exchange
    #     exchange_node_ids_iter += nodes_after_exchange
        # break
    
    
    nodes_to_be_removed = find_subsequent_nodes(node_list, edge_list, exchange_node_ids)
    print("nodes to be removed from inside method", nodes_to_be_removed)
    print("exchange_node_ids_iter", exchange_node_ids_iter)
    print("node_enum_to_be_removed", node_enum_to_be_removed)
    return nodes_to_be_removed, node_enum_to_be_removed

def find_subsequent_nodes(node_list, edge_list, exchange_node_ids):
    exchange_node_ids_iter = exchange_node_ids
    for node_id in exchange_node_ids_iter:
        nodes_after_exchange = []
        print(exchange_node_ids_iter.index(node_id),"-exchange ", node_id)
        # exchange_node_ids_iter += [edge['to'] for edge in edge_list if edge['from'] == node_id]
        for edge in edge_list:
            if edge['from'] == node_id:
                nodes_after_exchange.append(edge['to'])
        print("Nodes after exchange", nodes_after_exchange)
    
    return nodes_after_exchange
