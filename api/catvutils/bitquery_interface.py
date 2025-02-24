import traceback
from datetime import datetime
from typing import Optional, Any, Dict, List

from django.conf import settings

from api.catvutils.graphql_interface import GraphQLInterface, GraphQLClient


class BitqueryAPIInterface:
    def __init__(self):
        self._graphql_client = GraphQLClient(
            endpoint=settings.GRAPHQL_ENDPOINT,
            headers={'X-API-KEY': settings.GRAPHQL_X_API_KEY},
            timeout=(60, 300)
        )

    def get_transactions(
            self,
            address: str,
            tx_limit: int,
            depth_limit: int = 2,
            from_time: datetime = datetime(2015, 1, 1, 0, 0),
            till_time: datetime = datetime.now(),
            token_address: Optional[str] = None,
            source: bool = True,
            chain: str = 'ETH'
    ) -> List[Dict[str, Any]]:
        """
        Get transaction data with automatic retries and error handling.
        """
        try:
            graphql_interface = GraphQLInterface(
                chain=chain,
                source=source,
                address=address,
                token_address=token_address,
                depth_limit=depth_limit,
                from_time=from_time,
                till_time=till_time,
                limit=tx_limit,
                graphql_client=self._graphql_client
            )
            return graphql_interface.call_graphql_endpoint()
        except Exception:
            traceback.print_exc()
            raise
