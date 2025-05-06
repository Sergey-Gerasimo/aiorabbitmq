from aiorabbitmq.RPC import RPCPublisher
from aiorabbitmq.RPC.RPSExceptions import RPCError
from aiorabbitmq.abc import AbstractServiceClient

from abc import ABC, abstractmethod
from typing import Any, Callable, Optional
from functools import wraps
import json
import logging
import asyncio


logger = logging.getLogger("aiorabbitmq::BaseServiceClient")


class ServiceError(Exception):
    """Base exception for all service-related errors with automatic logging."""

    def __init__(self, message: str):
        super().__init__(message)
        logger.error(f"ServiceError occurred: {message}")


class ServiceConnectionError(ServiceError):
    """Raised when connection to the service fails with connection-specific logging."""

    def __init__(self, message: str):
        error_msg = f"Connection failed: {message}"
        super().__init__(error_msg)
        logger.error(f"ServiceConnectionError: {error_msg}")


class ServiceExecuteError(ServiceError):
    """Raised when service operation fails with execution context logging."""

    def __init__(self, message: str):
        error_msg = f"Operation failed: {message}"
        super().__init__(error_msg)
        logger.error(f"ServiceExecuteError: {error_msg}")


class ResponseProcessorError(ServiceError):
    """Raised during message processing failures with detailed logging."""

    def __init__(self, message: str):
        error_msg = f"Processing failed: {message}"
        super().__init__(error_msg)
        logger.error(f"ResponseProcessorError: {error_msg}")


class BaseServiceClient(AbstractServiceClient):
    """AMQP/RPC service client with comprehensive logging and examples.

    Example Usage:
        # Initialize client
        >>> client = BaseServiceClient(
        ...     amqp_url='amqp://user:pass@localhost:5672/vhost',
        ...     exchange_name='order_processing',
        ...     routing_key='payments.process',
        ...     default_timeout=30
        ... )

        # Using context manager
        >>> async with client as connected_client:
        ...     response = await process_order(connected_client, order_data)

        # With RPC handler decorator
        >>> @BaseServiceClient.handle_rpc_response
        >>> async def process_order(client, order_data):
        ...     return {
        ...         "action": "process_order",
        ...         "data": order_data,
        ...         "metadata": {"source": "API"}
        ...     }
    """

    def __init__(
        self,
        amqp_url: str,
        exchange_name: str,
        routing_key: str,
        default_timeout: int = 30,
        max_retries: int = 3,
    ):
        """Initialize AMQP/RPC client with connection parameters.

        Args:
            amqp_url: AMQP broker connection URL.
                Example: 'amqp://user:pass@rabbitmq:5672/vhost'
            exchange_name: Name of AMQP exchange.
                Example: 'order_events'
            routing_key: Routing key for RPC messages.
                Example: 'orders.process'
            default_timeout: Default timeout for RPC calls (seconds).
            max_retries: Maximum connection attempts before failing.

        Example:
            >>> client = BaseServiceClient(
            ...     amqp_url='amqp://localhost',
            ...     exchange_name='payments',
            ...     routing_key='process',
            ...     default_timeout=45,
            ...     max_retries=5
            ... )
        """
        logger.info(f"Initializing client for exchange: {exchange_name}")
        self.amqp_url = amqp_url
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.default_timeout = default_timeout
        self.max_retries = max_retries

        self.RPS: Optional[RPCPublisher] = None
        self._is_connected: bool = False
        self._connection_lock = asyncio.Lock()

    @property
    def is_connected(self) -> bool:
        """Check connection status with debug logging.

        Returns:
            bool: True if connected to AMQP broker, False otherwise.

        Example:
            >>> if client.is_connected:
            ...     print("Client is ready")
        """
        status = self._is_connected and self.RPS is not None
        logger.debug(f"Connection status check: {status}")
        return status

    async def connect(self) -> None:
        """Establish connection to AMQP broker with retry logic and logging.

        Example:
            >>> try:
            ...     await client.connect()
            ... except ServiceConnectionError as e:
            ...     print(f"Failed to connect: {e}")

        Raises:
            ServiceConnectionError: After max retry attempts.
        """
        async with self._connection_lock:
            if self.is_connected:
                logger.debug("Already connected, skipping reconnection")
                return

            logger.info(
                f"Connecting to {self.amqp_url} (max retries: {self.max_retries})"
            )
            for attempt in range(1, self.max_retries + 1):
                try:
                    logger.debug(f"Connection attempt {attempt}/{self.max_retries}")
                    self.RPS = RPCPublisher(
                        amqp_url=self.amqp_url,
                        exchange_name=self.exchange_name,
                        routing_key=self.routing_key,
                    )
                    await self.RPS.connect()
                    self._is_connected = True
                    logger.info(f"Successfully connected to {self.exchange_name}")
                    return
                except Exception as e:
                    logger.warning(f"Connection attempt {attempt} failed: {str(e)}")
                    if attempt == self.max_retries:
                        error_msg = (
                            f"Failed after {self.max_retries} attempts: {str(e)}"
                        )
                        logger.error(error_msg)
                        raise ServiceConnectionError(error_msg)
                    await asyncio.sleep(min(attempt * 2, 10))

    async def disconnect(self) -> None:
        """Close connection and clean up resources with logging.

        Example:
            >>> await client.disconnect()
            >>> print("Client disconnected")
        """
        async with self._connection_lock:
            if self.RPS is not None:
                try:
                    logger.info("Closing AMQP connection")
                    await self.RPS.close()
                    logger.info("Connection closed successfully")
                except Exception as e:
                    logger.error(f"Error during disconnection: {str(e)}")
                    raise
                finally:
                    self.RPS = None
                    self._is_connected = False
                    logger.debug("Connection state reset")

    async def __aenter__(self):
        """Async context manager entry with connection logging.

        Example:
            >>> async with BaseServiceClient(...) as client:
            ...     await client.make_request(data)
        """
        logger.debug("Entering client context")
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Async context manager exit with error handling and logging.

        Example:
            >>> async with client:
            ...     # Do work
            ...     pass  # Auto-disconnects here
        """
        logger.debug("Exiting client context")
        await self.disconnect()
        if exc_val is not None:
            logger.error(f"Context error occurred: {str(exc_val)}")
            if not isinstance(exc_val, (ServiceConnectionError, ServiceExecuteError)):
                raise ServiceExecuteError(f"Operation failed: {str(exc_val)}")
        return False

    @classmethod
    def handle_rpc_response(cls, func: Callable) -> Callable:
        """Decorator for standardizing RPC response handling with full logging.

        Example:
            >>> class OrderClient(BaseServiceClient):
            ...     @BaseServiceClient.handle_rpc_response
            ...     async def create_order(self, order_data):
            ...         return {
            ...             "action": "create_order",
            ...             "data": order_data,
            ...             "timestamp": datetime.now().isoformat()
            ...         }
        """

        @wraps(func)
        async def wrapper(self, *args, **kwargs) -> Any:
            logger.info(f"Starting RPC call: {func.__name__}")
            try:
                if not self.is_connected:
                    error_msg = "RPC call attempted while disconnected"
                    logger.error(error_msg)
                    raise ServiceConnectionError(error_msg)

                logger.debug("Preparing RPC request")
                request = await func(self, *args, **kwargs)
                logger.debug(f"RPC request prepared: {request}")

                logger.info("Making RPC call...")
                response = await self.RPS.call(request, timeout=self.default_timeout)
                logger.debug(f"Raw RPC response: {response}")

                try:
                    result = json.loads(response)
                    logger.info("RPC call completed successfully")
                    return result
                except json.JSONDecodeError as e:
                    error_msg = f"Invalid JSON response: {response}"
                    logger.error(error_msg)
                    raise ServiceExecuteError(error_msg) from e

            except RPCError as e:
                logger.error(f"RPC protocol error: {str(e)}")
                raise ServiceExecuteError(f"RPC failure: {str(e)}") from e
            except Exception as e:
                logger.error(f"Unexpected RPC error: {str(e)}")
                raise ServiceExecuteError(f"Operation failed: {str(e)}") from e
            finally:
                logger.debug(f"Completed RPC call: {func.__name__}")

        return wrapper


__all__ = [
    "ServiceError",
    "ServiceConnectionError",
    "ServiceExecuteError",
    "BaseServiceClient",
]
