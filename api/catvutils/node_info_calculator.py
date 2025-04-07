class NodeInfoCalculator:
    def __init__(self, data, token_type):
        """
        Initialize the NodeInfoCalculator with graph data and token type.

        Args:
            data (dict): Contains node_list and edge_list
            token_type (str): The blockchain type (e.g., "BTC", "ETH")
        """
        self.edge_list = data.get("edge_list", []) if data else []
        self.node_list = data.get("node_list", []) if data else []
        self.token_type = token_type

    def process_nodes(self):
        """
        Process all nodes and calculate their received/sent amounts.
        Updates the node_list with calculated values.

        Returns:
            list: The updated node_list with received and sent values
        """
        for node in self.node_list:
            node_id = node["id"]
            received, sent = self.calculate_received_and_sent(node_id)

            # Calculate from_count (number of addresses sending to this node)
            from_count = sum(1 for edge in self.edge_list if edge.get("to") == node_id)

            # Calculate to_count (number of addresses receiving from this node)
            to_count = sum(1 for edge in self.edge_list if edge.get("from") == node_id)

            node["received"] = received
            node["sent"] = sent
            node["from_count"] = from_count
            node["to_count"] = to_count

        return self.node_list

    def calculate_received_and_sent(self, node_id):
        """
        Calculate received and sent values for a single node.

        Args:
            node_id (str): The ID of the node to calculate for

        Returns:
            tuple: (received, sent) amounts for the node
        """
        if self.token_type == "BTC":
            return self._calculate_for_btc(node_id)
        else:
            return self._calculate_for_non_btc(node_id)

    def _calculate_for_non_btc(self, node_id):
        """Calculate received and sent for non-BTC chains."""
        received = 0
        sent = 0

        for edge in self.edge_list:
            # Calculate received amount
            if edge.get("to") == node_id:
                if edge.get("is_swap", False):
                    # For swap transactions, check if blockchain exists in sum_dict
                    if edge.get("sum_dict") and self.token_type in edge["sum_dict"]:
                        received += edge["sum_dict"][self.token_type]
                else:
                    # Original behavior for non-swap edges
                    received += edge.get("sum", 0)

            # Calculate sent amount
            if edge.get("from") == node_id:
                if edge.get("is_swap", False):
                    # For swap transactions, check if blockchain exists in sum_dict
                    if edge.get("sum_dict") and self.token_type in edge["sum_dict"]:
                        sent += edge["sum_dict"][self.token_type]
                else:
                    # Original behavior for non-swap edges
                    sent += edge.get("sum", 0)

        return received, sent

    def _calculate_for_btc(self, node_id):
        """Calculate received and sent for BTC (UTXO model)."""
        # Check if we should use the legacy method by checking for from_amount/to_amount
        use_legacy_method = True

        for edge in self.edge_list:
            if (edge.get("data") and isinstance(edge.get("data"), list) and
                    edge["data"] and "from_amount" in edge["data"][0] and
                    "to_amount" in edge["data"][0]):
                use_legacy_method = False
                break

        if use_legacy_method:
            # Use legacy method for older reports without from_amount/to_amount
            received = 0
            sent = 0

            for edge in self.edge_list:
                if edge.get("to") == node_id:
                    received += edge.get("sum", 0)

                if edge.get("from") == node_id:
                    sent += edge.get("sum", 0)

            return received, sent

        # Use the new method with from_amount/to_amount
        sent_tx_hashes = set()  # Track tx_hashes we've already counted for sent
        received_tx_hashes = set()  # Track tx_hashes we've already counted for received
        sent = 0
        received = 0

        # Handle sent transactions (node is the sender)
        for edge in self.edge_list:
            if edge.get("from") == node_id and edge.get("data") and isinstance(edge.get("data"), list):
                for tx in edge["data"]:
                    tx_hash = tx.get("tx_hash")
                    from_amount = tx.get("from_amount")
                    if from_amount is not None and tx_hash not in sent_tx_hashes:
                        sent_tx_hashes.add(tx_hash)
                        sent += from_amount

        # Handle received transactions (node is the receiver)
        for edge in self.edge_list:
            if edge.get("to") == node_id and edge.get("data") and isinstance(edge.get("data"), list):
                for tx in edge["data"]:
                    tx_hash = tx.get("tx_hash")
                    to_amount = tx.get("to_amount")
                    if to_amount is not None and tx_hash not in received_tx_hashes:
                        received_tx_hashes.add(tx_hash)
                        received += to_amount

        return received, sent
