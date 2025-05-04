from abc import ABC, abstractmethod
from typing import Callable


class AbstractServiceClient(ABC):
    """Abstract base class defining the interface for service clients.
    
    This class specifies the required methods and properties that all service client
    implementations must provide to ensure consistent behavior across different services.
    """
    
    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Connection status property.
        
        Returns:
            bool: True if the client is connected to the service, False otherwise.
        """
        pass
        
    @abstractmethod
    async def connect(self) -> None:
        """Establish connection with the service.
        
        Raises:
            ServiceConnectionError: If connection cannot be established.
        """
        pass
        
    @abstractmethod
    async def disconnect(self) -> None:
        """Close the connection with the service.
        
        Should clean up any resources and mark the client as disconnected.
        """
        pass
        
    @abstractmethod
    async def __aenter__(self):
        """Async context manager entry point.
        
        Returns:
            The connected client instance.
        """
        pass
    
    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit point.
        
        Args:
            exc_type: Exception type if an exception occurred, None otherwise.
            exc_val: Exception instance if an exception occurred, None otherwise.
            exc_tb: Traceback if an exception occurred, None otherwise.
        """
        pass
        
    @classmethod
    @abstractmethod
    def handle_rpc_response(cls, func: Callable) -> Callable:
        """Decorator for standard RPC response handling.
        
        Args:
            func: The function to be wrapped.
            
        Returns:
            Callable: Wrapped function that includes RPC error handling.
            
        The decorator should:
        1. Verify connection status before making calls
        2. Handle RPC-specific errors
        3. Parse and validate response format
        """
        pass