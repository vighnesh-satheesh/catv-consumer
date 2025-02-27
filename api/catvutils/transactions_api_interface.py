from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class TransactionAPIInterface(ABC):
    """Base abstract class for transaction API interfaces"""

    @abstractmethod
    def get_transactions(
            self,
            address: str,
            tx_limit: int,
            depth_limit: int = 2,
            from_time: str = None,
            till_time: str = None,
            token_address: Optional[str] = None,
            source: bool = True,
            chain: str = 'ETH'
    ) -> List[Dict[str, Any]]:
        """Get transaction data from API"""
        pass
