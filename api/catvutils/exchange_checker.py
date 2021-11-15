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
    dist_exchange_nodes = [node for node in graph_data['node_list'] if node['id']>=0 and node['group'] == 'Exchange & DEX']
    nodes_after_exchange = [edge['to'] for edge in graph_data['edge_list'] if edge['from'] == dist_exchange_nodes[0]['id']]
    new_edge_list = [edge for edge in graph_data['edge_list'] if edge['from'] == dist_exchange_nodes[0]['id']]
    print("new edge list", new_edge_list)
    print("dist x-nodes ", dist_exchange_nodes)
    print("nodes after x-node ", nodes_after_exchange)
    return graph_data