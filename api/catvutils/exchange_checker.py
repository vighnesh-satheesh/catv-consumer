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
    node_ids = []

    for node in dist_exchange_nodes:
        node_ids += [edge['to'] for edge in graph_data['edge_list'] if edge['from'] == node['id']]


    nodes_to_be_removed, edges_to_be_removed, node_enum_to_be_removed = remove_graph_data(graph_data, node_ids)
    graph_data['node_list'] = [node for node in graph_data['node_list'] if node['id'] not in nodes_to_be_removed]
    graph_data['edge_list'] = [edge for edge in graph_data['edge_list'] if edge['id'] not in edges_to_be_removed]
    graph_data['node_enum'] = [node_enum for node_enum in graph_data['node_enum'] if node_enum['address'] not in node_enum_to_be_removed]
    
    print("new node list", node_ids)
    print("dist x-nodes ", dist_exchange_nodes)
    return graph_data

def remove_graph_data(graph_data, initial_node_ids):
    edge_list = graph_data['edge_list']
    node_list = graph_data['node_list']
    node_enum_to_be_removed = []
    edges_to_be_removed = []
    node_ids = initial_node_ids
    nodes_to_be_removed = initial_node_ids
    print("length of node list", len(node_ids))

    for node_id in node_ids:
        print("first line of loop", node_id)
        node_ids += [edge['to'] for edge in edge_list if edge['from'] == node_id]
        edges_to_be_removed += [edge['id'] for edge in edge_list if edge['from'] == node_id]
        node_enum_to_be_removed += [node['address'] for node in node_list if node['id'] == node_id]
        nodes_to_be_removed += node_ids
        node_ids.remove(node_id)
        print("last line of loop", node_ids)
    
    print("node list after node removal", nodes_to_be_removed)
    return nodes_to_be_removed, edges_to_be_removed, node_enum_to_be_removed

# [1,2,3,4,5]
# [2,3,4,5,6,7]