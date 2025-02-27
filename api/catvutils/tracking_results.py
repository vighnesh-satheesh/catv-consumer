import traceback
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

from django.conf import settings

from .bloxy_graphql_interface import BloxyAPIInterface
from .graphtools import (
    generate_nodes_edges, generate_nodes_edges_coinpath, generate_nodes_edges_ethcoinpath,
    generate_nodes_edges_btccoinpath
)
from .vendor_api import BloxyEthAPIInterface
from ..models import (
    CatvTokens
)
from ..rpc.RPCClient import fetch_indicators, fetch_cara_report


def chunks(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def find_key(_dict, key):
    return _dict[key] if key in _dict else None


class TrackingResults:
    def __init__(self, **kwargs):
        self._async_source_result = None
        self._async_dist_result = None
        self._async_source_graph = None
        self._async_dist_graph = None
        self._source_graph = None
        self._dist_graph = None
        self._skip_source = True
        self._skip_dist = True

        self.wallet_address = find_key(kwargs, 'wallet_address')
        self.source_depth = find_key(kwargs, 'source_depth')
        self.distribution_depth = find_key(kwargs, 'distribution_depth')
        self.transaction_limit = find_key(kwargs, 'transaction_limit')
        self.from_date = find_key(kwargs, 'from_date')
        self.to_date = find_key(kwargs, 'to_date')
        self.token_address = find_key(kwargs, 'token_address')
        self.force_lookup = find_key(kwargs, 'force_lookup')
        self.error = None
        self.ext_api_calls = 0
        self.error_messages = {"source": "", "distribution": ""}
        self.chain = kwargs.get('chain', CatvTokens.ETH.value)

    def fetch_results(self, tx_limit, limit, save_to_db, for_source=False):
        bloxy_interface = BloxyAPIInterface()
        depth_limit = self.source_depth if for_source else self.distribution_depth
        till_date_extend = self.to_date + "T23:59:59"
        transaction_data = bloxy_interface.get_transactions(
            self.wallet_address,
            tx_limit,
            depth_limit,
            self.from_date,
            till_date_extend,
            self.token_address,
            for_source,
            self.chain
        )
        if transaction_data:
            return [item for item in transaction_data if len(item["receiver"]) > 0]
        return []

    def get_tracking_data(self, tx_limit, limit, save_to_db):
        pool = ThreadPool(processes=2)
        source_async = None
        dist_async = None

        if self.source_depth:
            self._skip_source = False
            source_async = pool.apply_async(self.fetch_results, (tx_limit, limit, save_to_db, True))

        if self.distribution_depth:
            self._skip_dist = False
            dist_async = pool.apply_async(self.fetch_results, (tx_limit, limit, save_to_db, False))

        process_source_first = False
        if self.source_depth and self.distribution_depth:
            process_source_first = self.source_depth > self.distribution_depth
        elif self.source_depth:
            process_source_first = True

        try:
            if process_source_first:
                # Process source first
                if not self._skip_source:
                    try:
                        self._async_source_result = source_async.get()
                    except Exception as e:
                        self.error_messages["source"] = str(e)
                        self._async_source_result = []
                        # Raise only if source was the only query
                        if self._skip_dist:
                            raise e

                # Then process distribution, but don't raise if it fails
                if not self._skip_dist:
                    try:
                        self._async_dist_result = dist_async.get()
                    except Exception as e:
                        self.error_messages["distribution"] = str(e)
                        self._async_dist_result = []

            else:
                # Distribution depth is >= source depth or only distribution exists
                if not self._skip_dist:
                    try:
                        self._async_dist_result = dist_async.get()
                    except Exception as e:
                        self.error_messages["distribution"] = str(e)
                        self._async_dist_result = []
                        # If distribution fails, raise immediately
                        raise e

                # Only process source if dist succeeded or wasn't queried
                if not self._skip_source:
                    try:
                        self._async_source_result = source_async.get()
                    except Exception as e:
                        self.error_messages["source"] = str(e)
                        self._async_source_result = []
                        # Only raise if source was the only query
                        if self._skip_dist:
                            raise e

        finally:
            pool.close()
            pool.join()

    def create_graph_data(self, build_lossy_graph=True):
        pool = Pool(processes=2)
        try:
            if not self._skip_source and self._async_source_result:
                self._async_source_graph = pool.apply_async(generate_nodes_edges, (
                    self._async_source_result, -1, build_lossy_graph, self.chain))
            if not self._skip_dist and self._async_dist_result:
                self._async_dist_graph = pool.apply_async(generate_nodes_edges, (
                    self._async_dist_result, 1, build_lossy_graph, self.chain))
        finally:
            pool.close()
            pool.join()

    @staticmethod
    def update_annotations(nc, item_list, token_type):
        print(f"[DEBUG] Starting update_annotations with token_type={token_type}")
        print(f"[DEBUG] Initial item_list length: {len(item_list)}")

        addr_list = nc.get_node_enum().keys()
        print(f"[DEBUG] Retrieved {len(addr_list)} addresses from node enumeration")

        addr_list_for_portal = [addr.lower() for addr in addr_list]
        request_dict = {'addr_list': addr_list_for_portal, 'token_type': str(token_type)}
        print(f"[DEBUG] Preparing to fetch indicators with token_type: {token_type}")

        indicators = fetch_indicators(request_dict)
        print(f"[DEBUG] Fetched {len(indicators)} indicators")

        # Extremely High Node
        cara_addr_list = [addr for addr in addr_list]
        request_dict_cara = {'addr_list': cara_addr_list}
        print(f"[DEBUG] Preparing to fetch CARA report")

        new_addresses = fetch_cara_report(request_dict_cara)
        print(f"[DEBUG] Fetched {len(new_addresses)} new addresses from CARA report")

        seen_indicators = []
        updated_nodes = 0
        updated_transactions = 0

        if len(indicators) > 0:
            print(f"[DEBUG] Processing indicators...")
            try:
                for i, item in enumerate(indicators):
                    # Log progress periodically
                    if i < 5 or i % 50 == 0:
                        print(f"[DEBUG] Processing indicator {i + 1}/{len(indicators)}: {item.get('pattern', 'N/A')}")

                    if "annotation" not in item.keys():
                        item["annotation"] = ""
                        print(f"[DEBUG] Added missing annotation for indicator {item.get('pattern', 'N/A')}")

                    pattern = item['pattern']
                    pattern_lower = pattern.lower()

                    if pattern_lower in seen_indicators:
                        print(f"[DEBUG] Skipping duplicate indicator: {pattern_lower}")
                        continue

                    cur_node = nc.get_node(pattern_lower)
                    if cur_node is None:
                        print(f"[DEBUG] Node not found with lowercase pattern, trying original case: {pattern}")
                        cur_node = nc.get_node(pattern)

                    if cur_node is None:
                        print(f"[DEBUG] Node not found for pattern: {pattern}, skipping")
                        continue

                    print(f"[DEBUG] Updating node trdb_info for {pattern}, category: {item['security_category']}")
                    cur_node.update(trdb_info={**item, 'uid': str(item['uid']),
                                               'security_category': item['security_category'],
                                               'pattern_type': item['pattern_type'],
                                               'pattern_subtype': item['pattern_subtype']})

                    if cur_node.group == "Exchange/DEX/Bridge/Mixer":
                        print(f"[DEBUG] Node is Exchange/DEX/Bridge/Mixer, skipping further updates")
                        seen_indicators.append(pattern_lower)
                        continue

                    security_category = item["security_category"].lower()
                    old_group = cur_node.group

                    # Update node based on security category
                    if security_category == "graylist":
                        if item["annotation"]:
                            print(f"[DEBUG] Updating graylist node with annotation: {item['annotation']}")
                            cur_node.update(annotation=item["annotation"])
                            cur_node.set_group_from_annotation()
                            print(f"[DEBUG] Group changed from '{old_group}' to '{cur_node.group}'")
                        else:
                            print(f"[DEBUG] Setting graylist node to 'No Tag'")
                            cur_node.update(group="No Tag", annotation="")
                    elif security_category == "blacklist":
                        print(f"[DEBUG] Setting node to 'Blacklist'")
                        cur_node.update(group="Blacklist", annotation="Blacklist")
                    elif security_category == "whitelist":
                        print(f"[DEBUG] Setting node to 'Whitelist'")
                        cur_node.update(group="Whitelist", annotation="Whitelist")
                    else:
                        kwargs = {}
                        kwargs["group"] = item["security_category"].title()
                        print(f"[DEBUG] Setting group to: {kwargs['group']}")

                        if item["annotation"]:
                            kwargs["annotation"] = item["annotation"]
                            print(f"[DEBUG] Setting annotation to: {item['annotation']}")

                            annotation_lower = item["annotation"].lower()
                            if "exchange" in annotation_lower or "dex" in annotation_lower or "bridge" in annotation_lower or "mixer" in annotation_lower:
                                kwargs["group"] = "Exchange/DEX/Bridge/Mixer"
                                print(f"[DEBUG] Changed group to Exchange/DEX/Bridge/Mixer based on annotation")
                            elif "smart" in annotation_lower or "contract" in annotation_lower:
                                kwargs["group"] = "Smart Contract"
                                print(f"[DEBUG] Changed group to Smart Contract based on annotation")
                        else:
                            kwargs["annotation"] = ""
                            print(f"[DEBUG] Setting empty annotation")

                        cur_node.update(**kwargs)

                    nc.update_node(pattern_lower, cur_node)
                    updated_nodes += 1

                    # Update transactions
                    tx_updates = 0
                    for transaction in item_list:
                        if not transaction.get('sender_annotation', None):
                            transaction['sender_annotation'] = ''
                            print(f"[DEBUG] Added missing sender_annotation")

                        if not transaction.get('receiver_annotation', None):
                            transaction['receiver_annotation'] = ''
                            print(f"[DEBUG] Added missing receiver_annotation")

                        if transaction['sender'].lower() == cur_node.address.lower():
                            transaction['sender_annotation'] = cur_node.annotation
                            tx_updates += 1
                        elif transaction['receiver'].lower() == cur_node.address.lower():
                            transaction['receiver_annotation'] = cur_node.annotation
                            tx_updates += 1

                    if tx_updates > 0:
                        print(f"[DEBUG] Updated {tx_updates} transaction annotations for {pattern}")
                    updated_transactions += tx_updates

                    seen_indicators.append(pattern_lower)

                    # Process new_addresses for blacklist or whitelist
                    if len(new_addresses) > 0 and (
                            security_category == "blacklist" or security_category == "whitelist"):
                        before_count = len(new_addresses)
                        removed = 0
                        for result in list(new_addresses):  # Create a copy for safe iteration
                            if pattern_lower == result[0] or pattern == result[0]:
                                new_addresses.remove(result)
                                removed += 1

                        if removed > 0:
                            print(
                                f"[DEBUG] Removed {removed} addresses from new_addresses list, {len(new_addresses)} remaining")

                print(f"[DEBUG] Successfully processed all indicators")
                print(f"[DEBUG] Updated {updated_nodes} nodes and {updated_transactions} transaction annotations")
                print(f"[DEBUG] Processed {len(seen_indicators)} unique indicators")

            except Exception as e:
                print(f"[ERROR] Exception occurred during processing: {str(e)}")
                print(f"[ERROR] Failed at indicator index {i if 'i' in locals() else 'unknown'}")
                traceback.print_exc()
                raise
        else:
            print("[DEBUG] No indicators to process")

        print(f"[DEBUG] Finished update_annotations function")

        if len(new_addresses) > 0:
            for result in new_addresses:
                add_node = nc.get_node(result[0])
                for item in cara_addr_list:
                    annotation_group = nc.get_node(item).group
                    if add_node is None:
                        continue
                    # elif result[1] == 'blacklist' or add_node.group == 'Blacklist':
                    #     add_node.update(group="Blacklist", annotation="Blacklist")
                    #     nc.update_node(result[0], add_node)
                    #     break
                    # elif result[1] == 'whitelist' or add_node.group == 'Whitelist':
                    #     add_node.update(group="Whitelist", annotation="Whitelist")
                    #     nc.update_node(result[0], add_node)
                    #     break
                    elif 'Exchange/DEX/Bridge/Mixer' in annotation_group or add_node.group == 'Exchange/DEX/Bridge/Mixer':
                        nc.update_node(result[0], add_node)
                        break
                    else:
                        add_node.update(group="Suspicious", annotation="Extremely High Risk")
                        nc.update_node(result[0], add_node)
        return nc, item_list

    def set_annotations_from_db(self, token_type='ETH'):
        try:
            if not self._skip_source and self._async_source_graph:
                tracking_results, nc = self._async_source_graph.get()
                updated_nc, updated_item_list = TrackingResults.update_annotations(nc, tracking_results['item_list'],
                                                                                   self.chain)
                tracking_results['node_list'] = list(updated_nc.get_nodes_as_dict().values())
                tracking_results['item_list'] = updated_item_list
                updated_nc.filter_update_nodes()
                tracking_results['graph_node_list'] = list(updated_nc.get_nodes_as_dict().values())
                tracking_results['node_enum'] = updated_nc.get_node_enum()
                self._source_graph = tracking_results
            if not self._skip_dist and self._async_dist_graph:
                tracking_results, nc = self._async_dist_graph.get()
                updated_nc, updated_item_list = TrackingResults.update_annotations(nc, tracking_results['item_list'],
                                                                                   self.chain)
                tracking_results['node_list'] = list(updated_nc.get_nodes_as_dict().values())
                tracking_results['item_list'] = updated_item_list
                updated_nc.filter_update_nodes()
                tracking_results['graph_node_list'] = list(updated_nc.get_nodes_as_dict().values())
                tracking_results['node_enum'] = updated_nc.get_node_enum()
                self._dist_graph = tracking_results
        except Exception as e:
            raise

    def make_graph_dict(self):
        graph_dict = {}

        if not self._skip_source and not self._skip_dist and all([self._source_graph, self._dist_graph]):
            track_dist_result = self._dist_graph
            track_source_result = self._source_graph
            graph_dict['item_list'] = track_dist_result['item_list'] + track_source_result['item_list']
            graph_dict['keys'] = track_dist_result['keys']
            pick_dist_graph = track_dist_result['node_list']
            pick_dist_edges = track_dist_result['edge_list']
            pick_src_graph = track_source_result['node_list']
            pick_src_edges = track_source_result['edge_list']
            if track_dist_result['graph_node_list']:
                pick_dist_graph = track_dist_result['graph_node_list']
                pick_dist_edges = track_dist_result['graph_edge_list']
            if track_source_result['graph_node_list']:
                pick_src_graph = track_source_result['graph_node_list']
                pick_src_edges = track_source_result['graph_edge_list']
            # the original node is the first entry in both dist and source so remove duplicates here
            graph_dict['node_list'] = track_dist_result['node_list'] + track_source_result['node_list'][1::]
            graph_dict['graph_node_list'] = pick_dist_graph + pick_src_graph[1::]
            graph_dict['edge_list'] = track_dist_result['edge_list'] + track_source_result['edge_list']
            graph_dict['graph_edge_list'] = pick_dist_edges + pick_src_edges
            graph_dict['node_enum'] = {**track_dist_result['node_enum'], **track_source_result['node_enum']}
            graph_dict['send_count'] = track_dist_result['volume_count_1']
            graph_dict['receive_count'] = track_source_result['volume_count_-1']
        elif not self._skip_dist and self._dist_graph:
            track_dist_result = self._dist_graph
            graph_dict.update(track_dist_result)
            graph_dict['send_count'] = graph_dict.pop('volume_count_1')
        elif not self._skip_source and self._source_graph:
            track_source_result = self._source_graph
            graph_dict.update(track_source_result)
            graph_dict['receive_count'] = graph_dict.pop('volume_count_-1')

        return graph_dict


class BTCCoinpathTrackingResults(TrackingResults):
    def fetch_results(self, tx_limit, limit, save_to_db, for_source=False):
        bloxy_interface = BloxyAPIInterface()
        depth_limit = self.source_depth if for_source else self.distribution_depth
        till_date_extend = self.to_date + "T23:59:59"
        transaction_data = bloxy_interface.get_transactions(
            self.wallet_address,
            tx_limit,
            depth_limit,
            from_time=self.from_date,
            till_time=till_date_extend,
            token_address=None,
            source=for_source,
            chain=self.chain
        )
        self.ext_api_calls += 1
        return transaction_data

    def create_graph_data(self, build_lossy_graph=True):
        pool = Pool(processes=2)
        try:
            if not self._skip_source and self._async_source_result:
                self._async_source_graph = pool.apply_async(generate_nodes_edges_coinpath,
                                                            (self._async_source_result, -1, build_lossy_graph))
            if not self._skip_dist and self._async_dist_result:
                self._async_dist_graph = pool.apply_async(generate_nodes_edges_coinpath,
                                                          (self._async_dist_result, 1, build_lossy_graph))
        finally:
            pool.close()
            pool.join()


class EthPathResults(TrackingResults):
    def __init__(self, **kwargs):
        super(EthPathResults, self).__init__(**kwargs)
        self.address_from = kwargs['address_from']
        self.address_to = kwargs['address_to']
        self.depth_limit = kwargs['depth']
        self.min_tx_amount = kwargs['min_tx_amount']
        self.limit_address_tx = kwargs['limit_address_tx']
        self.chain = kwargs.get('chain', CatvTokens.ETH.value)
        self._external_api_client = BloxyEthAPIInterface(settings.BLOXY_API_KEY,
                                                         settings.BLOXY_ETHCOINPATH_ENDPOINT)
        self._graph_func = generate_nodes_edges_ethcoinpath

    def fetch_results(self, tx_limit, limit, save_to_db, for_source=False):
        transaction_data = self._external_api_client.get_path_transactions(self)
        self.ext_api_calls += 1
        if not transaction_data:
            error_key = "distribution"
            self.error_messages[error_key] = "Missing {} results for the wallet address within the date range " \
                                             "specified".format(error_key)
        return transaction_data

    def get_tracking_data(self, tx_limit=None, limit=None, save_to_db=False):
        pool = ThreadPool(processes=1)
        try:
            if self.depth_limit:
                self._skip_dist = False
                self._async_dist_result = pool.apply_async(self.fetch_results, (tx_limit, limit, save_to_db, False))
        finally:
            pool.close()
            pool.join()

    def create_graph_data(self, build_lossy_graph=True):
        pool = Pool(processes=1)
        try:
            if not self._skip_dist:
                dist_result = self._async_dist_result.get()
                if dist_result:
                    self._async_dist_graph = pool.apply_async(self._graph_func, (dist_result, 1, build_lossy_graph))
        finally:
            pool.close()
            pool.join()


class BtcPathResults(EthPathResults):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._external_api_client = BloxyEthAPIInterface(settings.BLOXY_API_KEY, settings.BLOXY_BTCCOINPATH_ENDPOINT)
        self._graph_func = generate_nodes_edges_btccoinpath
