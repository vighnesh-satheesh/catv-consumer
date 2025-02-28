import traceback
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

from django.conf import settings

from .bitquery_interface import BitqueryAPIInterface
from .graphtools import (
    generate_nodes_edges, generate_nodes_edges_coinpath, generate_nodes_edges_ethcoinpath,
    generate_nodes_edges_btccoinpath
)
from .tracer_interface import TracerAPIInterface
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
        self.api_used = ""

    def fetch_results(self, tx_limit, limit, save_to_db, for_source=False):
        depth_limit = self.source_depth if for_source else self.distribution_depth
        till_date_extend = self.to_date + "T23:59:59"

        # Determine if we should use Tracer API first based on chain
        should_use_tracer_first = self.chain != 'KLAY' and self.chain in ['ETH', 'BSC', 'FTM', 'POL', 'ETC', 'AVAX']

        if should_use_tracer_first:
            try:
                # Try Tracer API first
                tracer_interface = TracerAPIInterface()
                transaction_data = tracer_interface.get_transactions(
                    self.wallet_address,
                    tx_limit,
                    depth_limit,
                    self.from_date,
                    till_date_extend,
                    self.token_address,
                    for_source,
                    self.chain
                )
                self.ext_api_calls += 1

                if transaction_data:
                    self.api_used = "tracer"
                    print(f"Tracer API successful: Retrieved {len(transaction_data)} transactions")
                    return [item for item in transaction_data if len(item["receiver"]) > 0]
                else:
                    print("Tracer API returned no data, falling back to Bitquery")

            except Exception as e:
                # Log the error but don't raise it - we'll fall back to Bitquery
                error_msg = f"Tracer API failed: {str(e)}. Falling back to Bitquery."
                print(error_msg)
                # Don't update error_messages here as we're going to try Bitquery

        # Either Tracer API failed, we're processing Klaytn, or Tracer API wasn't applicable
        try:
            # Fall back to Bitquery or use it directly for Klaytn
            bloxy_interface = BitqueryAPIInterface()
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
            self.ext_api_calls += 1
            self.api_used = "bitquery"
            if transaction_data:
                return [item for item in transaction_data if len(item["receiver"]) > 0]

        except Exception as e:
            # Both APIs failed or we went straight to Bitquery and it failed
            error_source = "source" if for_source else "distribution"
            self.error_messages[error_source] = str(e)
            print(f"Bitquery API failed for {error_source}: {str(e)}")
            raise  # Propagate the exception to be handled by get_tracking_data

        return []  # Return empty list if no data or all filtering removed items

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

    # @staticmethod
    # def update_annotations(nc, item_list, token_type):
    #     print(f"{item_list=}")
    #     addr_list = nc.get_node_enum().keys()
    #     addr_list_for_portal = [addr.lower() for addr in addr_list]
    #     request_dict = {'addr_list': addr_list_for_portal, 'token_type': str(token_type)}
    #     indicators = fetch_indicators(request_dict)
    #     print("indicators length ", len(indicators))
    #     # Extremely High Node
    #     cara_addr_list = [addr for addr in addr_list]
    #     request_dict_cara = {'addr_list': cara_addr_list}
    #     new_addresses = fetch_cara_report(request_dict_cara)
    #     seen_indicators = []
    #     if len(indicators) > 0:
    #         try:
    #             for item in indicators:
    #                 if "annotation" not in item.keys():
    #                     item["annotation"] = ""
    #                 if item['pattern'].lower() in seen_indicators:
    #                     continue
    #                 cur_node = nc.get_node(item["pattern"].lower())
    #                 cur_node = nc.get_node(item["pattern"]) if cur_node is None else cur_node
    #                 if cur_node is None:
    #                     continue
    #                 cur_node.update(trdb_info={**item, 'uid': str(item['uid']),
    #                                            'security_category': item['security_category'],
    #                                            'pattern_type': item['pattern_type'],
    #                                            'pattern_subtype': item['pattern_subtype']})
    #                 if cur_node.group == "Exchange/DEX/Bridge/Mixer":
    #                     seen_indicators.append(item['pattern'].lower())
    #                     continue
    #                 if item["security_category"].lower() == "graylist":
    #                     if item["annotation"]:
    #                         cur_node.update(annotation=item["annotation"])
    #                         cur_node.set_group_from_annotation()
    #                     else:
    #                         cur_node.update(group="No Tag", annotation="")
    #                 elif item["security_category"].lower() == "blacklist":
    #                     cur_node.update(group="Blacklist", annotation="Blacklist")
    #                 elif item["security_category"].lower() == "whitelist":
    #                     cur_node.update(group="Whitelist", annotation="Whitelist")
    #                 else:
    #                     kwargs = {}
    #                     kwargs["group"] = item["security_category"].title()
    #                     if item["annotation"]:
    #                         kwargs["annotation"] = item["annotation"]
    #                         if "Exchange" in item["annotation"] or "DEX" in item["annotation"] or "Bridge" in item[
    #                             "annotation"] or "Mixer" in item["annotation"] or "bridge" in item[
    #                             "annotation"] or "mixer" in item["annotation"]:
    #                             kwargs["group"] = "Exchange/DEX/Bridge/Mixer"
    #                         elif "Smart" in item["annotation"] or "Contract" in item["annotation"] or "smart" in item[
    #                             "annotation"] or "contract" in item["annotation"]:
    #                             kwargs["group"] = "Smart Contract"
    #                     else:
    #                         kwargs["annotation"] = ""
    #                     cur_node.update(**kwargs)
    #                 nc.update_node(item['pattern'].lower(), cur_node)
    #                 for transaction in item_list:
    #                     if not transaction.get('sender_annotation', None):
    #                         transaction['sender_annotation'] = ''
    #                     if not transaction.get('receiver_annotation', None):
    #                         transaction['receiver_annotation'] = ''
    #
    #                     if transaction['sender'].lower() == cur_node.address.lower():
    #                         transaction['sender_annotation'] = cur_node.annotation
    #                     elif transaction['receiver'].lower() == cur_node.address.lower():
    #                         transaction['receiver_annotation'] = cur_node.annotation
    #                 seen_indicators.append(item['pattern'].lower())
    #                 if len(new_addresses) > 0 and item["security_category"].lower() == "blacklist" or item[
    #                     "security_category"].lower() == "whitelist":
    #                     for result in new_addresses:
    #                         if item['pattern'].lower() == result[0] or item['pattern'] == result[0]:
    #                             new_addresses.remove(result)
    #         except Exception as e:
    #             traceback.print_exc()
    #             raise
    #
    # if len(new_addresses) > 0:
    #     for result in new_addresses:
    #         add_node = nc.get_node(result[0])
    #         for item in cara_addr_list:
    #             annotation_group = nc.get_node(item).group
    #             if add_node is None:
    #                 continue
    #             elif 'Exchange/DEX/Bridge/Mixer' in annotation_group or add_node.group == 'Exchange/DEX/Bridge/Mixer':
    #                 nc.update_node(result[0], add_node)
    #                 break
    #             else:
    #                 add_node.update(group="Suspicious", annotation="Extremely High Risk")
    #                 nc.update_node(result[0], add_node)
    #     return nc, item_list

    @staticmethod
    def update_annotations(nc, item_list, token_type):
        addr_list = nc.get_node_enum().keys()

        # Convert to lowercase once
        addr_list_for_portal = [addr.lower() for addr in addr_list]
        request_dict = {'addr_list': addr_list_for_portal, 'token_type': str(token_type)}

        indicators = fetch_indicators(request_dict)
        print(f"{len(indicators)=}")
        request_dict_cara = {'addr_list': list(addr_list)}

        addresses_with_cara_report = fetch_cara_report(request_dict_cara)
        print(f"{len(addresses_with_cara_report)=}")

        # Create a dictionary for quick lookup of CARA report addresses
        cara_addr_dict = {addr_score[0].lower(): addr_score for addr_score in addresses_with_cara_report}

        # Create sets for O(1) lookups
        seen_indicators = set()
        updated_nodes = 0
        updated_transactions = 0

        # Pre-compute lower case addresses for each transaction to avoid repeated conversions
        sender_to_tx = {}
        receiver_to_tx = {}
        for tx in item_list:
            # Initialize annotations if missing (do this once outside the main loop)
            if not tx.get('sender_annotation', None):
                tx['sender_annotation'] = ''
            if not tx.get('receiver_annotation', None):
                tx['receiver_annotation'] = ''

            # Build address to transaction mappings
            sender_lower = tx['sender'].lower()
            receiver_lower = tx['receiver'].lower()

            if sender_lower not in sender_to_tx:
                sender_to_tx[sender_lower] = []
            sender_to_tx[sender_lower].append(tx)

            if receiver_lower not in receiver_to_tx:
                receiver_to_tx[receiver_lower] = []
            receiver_to_tx[receiver_lower].append(tx)

        if indicators:
            try:
                for i, item in enumerate(indicators):
                    # Ensure annotation exists
                    if "annotation" not in item:
                        item["annotation"] = ""

                    pattern = item['pattern']
                    pattern_lower = pattern.lower()

                    # Skip if already processed
                    if pattern_lower in seen_indicators:
                        continue

                    # Get node using lowercase pattern first
                    cur_node = nc.get_node(pattern_lower)
                    if cur_node is None:
                        cur_node = nc.get_node(pattern)

                    if cur_node is None:
                        continue

                    # Update node trdb_info
                    cur_node.update(trdb_info={**item, 'uid': str(item['uid']),
                                               'security_category': item['security_category'],
                                               'pattern_type': item['pattern_type'],
                                               'pattern_subtype': item['pattern_subtype']})

                    # Skip further updates if Exchange/DEX/Bridge/Mixer
                    if cur_node.group == "Exchange/DEX/Bridge/Mixer":
                        seen_indicators.add(pattern_lower)
                        continue

                    security_category = item["security_category"].lower()

                    # Update node based on security category (logic unchanged)
                    if security_category == "graylist":
                        if item["annotation"]:
                            cur_node.update(annotation=item["annotation"])
                            cur_node.set_group_from_annotation()
                        else:
                            cur_node.update(group="No Tag", annotation="")
                    elif security_category == "blacklist":
                        cur_node.update(group="Blacklist", annotation="Blacklist")
                    elif security_category == "whitelist":
                        cur_node.update(group="Whitelist", annotation="Whitelist")
                    else:
                        kwargs = {}
                        kwargs["group"] = item["security_category"].title()

                        if item["annotation"]:
                            kwargs["annotation"] = item["annotation"]

                            annotation_lower = item["annotation"].lower()
                            if any(term in annotation_lower for term in ["exchange", "dex", "bridge", "mixer"]):
                                kwargs["group"] = "Exchange/DEX/Bridge/Mixer"
                            elif any(term in annotation_lower for term in ["smart", "contract"]):
                                kwargs["group"] = "Smart Contract"
                        else:
                            kwargs["annotation"] = ""

                        cur_node.update(**kwargs)

                    nc.update_node(pattern_lower, cur_node)
                    updated_nodes += 1

                    # Update transaction annotations using our pre-built dictionaries
                    tx_updates = 0
                    node_address_lower = cur_node.address.lower()

                    # Update sender annotations
                    if node_address_lower in sender_to_tx:
                        for tx in sender_to_tx[node_address_lower]:
                            tx['sender_annotation'] = cur_node.annotation
                            tx_updates += 1

                    # Update receiver annotations
                    if node_address_lower in receiver_to_tx:
                        for tx in receiver_to_tx[node_address_lower]:
                            tx['receiver_annotation'] = cur_node.annotation
                            tx_updates += 1

                    updated_transactions += tx_updates

                    seen_indicators.add(pattern_lower)

                    print(f"[DEBUG] Updated {updated_nodes} nodes and {updated_transactions} transaction annotations")

                    # Remove from cara_addr_dict if blacklist or whitelist
                    if security_category in ["blacklist", "whitelist"]:
                        # Check if the pattern exists in the cara report dict
                        cara_addr_dict.pop(pattern_lower, None)
                        cara_addr_dict.pop(pattern, None)

            except Exception as e:
                traceback.print_exc()
                raise

        # Process remaining CARA report items
        if cara_addr_dict:
            # Rebuild the list from the dictionary values
            remaining_addresses = list(cara_addr_dict.values())
            for address_score_list in remaining_addresses:
                address = address_score_list[0]
                addr_node = nc.get_node(address)

                if addr_node is None:
                    continue

                # Check if any node in cara_addr_list has 'Exchange/DEX/Bridge/Mixer' in its group
                is_exchange = False
                for item in addr_list:
                    item_node = nc.get_node(item)
                    if item_node and 'Exchange/DEX/Bridge/Mixer' in item_node.group:
                        is_exchange = True
                        break

                # Check if the node itself is an Exchange
                if is_exchange or 'Exchange/DEX/Bridge/Mixer' in addr_node.group:
                    nc.update_node(address, addr_node)
                else:
                    addr_node.update(group="Suspicious", annotation="Extremely High Risk")

                    nc.update_node(address, addr_node)

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
        bloxy_interface = BitqueryAPIInterface()
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
