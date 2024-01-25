import requests
import traceback
import json

from datetime import datetime as dt
from django.conf import settings
from api.models import CatvTokens


class GraphQLSmartContractQuery:
    def __init__(self, data):
        self.network = data[0]
        self.tx_hash = data[1]
        self.tx_date = data[2]
        self.smart_contract_address = data[3]
        self.tx_from_address = data[4]

    def _get_network(self):
        network_map = {
            CatvTokens.ETH.value: "ethereum",
            CatvTokens.BSC.value: "bsc",
            CatvTokens.KLAY.value: "klaytn"
        }
        return network_map[self.network]

    def get_formatted_query(self):
        try:
            GRAPHQL_SMARTCONTRACT_QUERY = f"""
                        query smart_contract_method_query{{
                          ethereum(network: {self._get_network()}) {{
                            smartContractCalls(
                              txHash: {{is: "{self.tx_hash}"}}
                              smartContractAddress: {{is: "{self.smart_contract_address}"}}
                              date: {{is: "{self.tx_date}"}}
                              txFrom: {{is: "{self.tx_from_address}"}}
                              caller: {{is: "{self.tx_from_address}"}}
                              external: true
                            ) {{
                              caller {{
                                address
                              }}
                              smartContractMethod {{
                                name
                                signature
                                signatureHash
                              }}
                            }}
                          }}
                        }}
                    """
        except Exception:
            traceback.print_exc()
            return []
        return GRAPHQL_SMARTCONTRACT_QUERY


class SmartContractMethodFinder:
    def __init__(self, network, node_list, edge_list):
        self.network = network
        self.node_list = node_list
        self.edge_list = edge_list
        self.smart_contract_nodes = [node for node in self.node_list if node["group"] == "Smart Contract"]
        self.smart_contract_node_ids = set(node["id"] for node in self.smart_contract_nodes)
        self.smart_contract_edges_dict = {}
        self.smart_contract_method_data = []


    def _get_smart_contract_edges_dict(self):
        for smart_contract_node in self.smart_contract_nodes:
            smart_contract_node_addr = smart_contract_node["address"]
            edges = [
                edge for edge in self.edge_list
                if edge["to"] == smart_contract_node["id"] and edge["from"] not in self.smart_contract_node_ids
            ]
            self.smart_contract_edges_dict[smart_contract_node_addr] = edges


    def _find_latest_tx(self):
        try:
            for smart_contract_node_addr, edges in self.smart_contract_edges_dict.items():
                for edge in edges:
                    tx_data_list = edge["data"]
                    tx_data_latest = tx_data_list[-1]
                    tx_hash = tx_data_latest["tx_hash"]
                    tx_date = str(tx_data_latest["tx_time"]).split(" ")[0]
                    tx_from_address = self._get_tx_from_address(edge["from"])
                    method_data = self._get_bitquery_response(tx_hash, tx_date, smart_contract_node_addr, tx_from_address)
                    if len(method_data) > 0 and method_data["smart_contract"]["name"] is not None:
                        self.smart_contract_method_data.append({
                            "edge_id": edge["id"],
                            "smc_address": smart_contract_node_addr,
                            "method_data": method_data
                        })
                    else:
                        continue
        except Exception as e:
            traceback.print_exc()
            return

    def _get_tx_from_address(self, node_id):
        return self.node_list[next((index for (index, node) in enumerate(self.node_list)
                                    if node["id"] == node_id), None)]["address"]

    def _get_bitquery_response(self, tx_hash, tx_date, smart_contract_address, tx_from_address):
        try:
            data = [self.network, tx_hash, tx_date, smart_contract_address, tx_from_address]
            query_obj = GraphQLInterface(data)
            response = query_obj.get_graphql_response()
        except Exception:
            traceback.print_exc()
            response = []
        return response

    def _update_edges(self):
        try:
            self._get_smart_contract_edges_dict()
            total_sm_edges = sum(len(edges) for edges in self.smart_contract_edges_dict.values())
            print(f"Smart contract nodes length: {len(self.smart_contract_nodes)}")
            print("total_sm_edges: ", total_sm_edges)
            if total_sm_edges and total_sm_edges <= 50:
                self._find_latest_tx()
                for item in self.smart_contract_method_data:
                    self.edge_list[
                        next((index for (index, edge) in enumerate(self.edge_list)
                              if edge["id"] == item["edge_id"]), None)
                    ]["smart_contract_data"] = item
        except Exception:
            traceback.print_exc()
            return

    def get_updated_edges(self):
        self._update_edges()
        return self.edge_list


class GraphQLInterface:
    def __init__(self, data):
        self._x_api_key = settings.GRAPHQL_X_API_KEY
        self._endpoint = settings.GRAPHQL_ENDPOINT
        self._headers = {'X-API-KEY': self._x_api_key}
        self.data = data

    def get_graphql_response(self, timeout=3):
        try:
            query = GraphQLSmartContractQuery(self.data).get_formatted_query()
            r = requests.post(self._endpoint, json={'query': query}, headers=self._headers)
            bitquery_response = r.json()
            bitquery_response = bitquery_response["data"]["ethereum"]["smartContractCalls"][:1]
            if len(bitquery_response) == 0:
                return []
            response = {
                "caller_address": bitquery_response[0]["caller"]["address"],
                "smart_contract": bitquery_response[0]["smartContractMethod"]
            }
            return response
        except Timeout:
            raise BitqueryFetchTimedOut
        except RequestException:
            raise BitqueryFetchTimedOut
        except Exception:
            traceback.print_exc()
            return []