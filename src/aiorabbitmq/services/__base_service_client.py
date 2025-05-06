from aiorabbitmq.RPC import RPCPublisher
from aiorabbitmq.RPC.RPSExceptions import RPCError
from aiorabbitmq.abc import AbstractServiceClient

from abc import ABC, abstractmethod
from typing import Any, Callable
from functools import wraps
import json


class ServiceConnectionError(Exception):
    """Ошибка подключению к сервису"""


class ServiceExecuteError(Exception):
    """Ошибка выполнения операции в сервисе"""


class BaseServiceClient(AbstractServiceClient):
    """Concrete implementation of a service client using AMQP/RPC.

    This class provides default implementation for service communication using
    AMQP protocol with RPC pattern.

    Attributes:
        amqp_url (str): URL for AMQP broker connection.
        exchange_name (str): Name of the AMQP exchange.
        routing_key (str): Routing key for RPC messages.
        RPS (RPSPublisher): Instance of RPC publisher.
    """

    def __init__(self, amqp_url: str, exchange_name: str, routing_key: str):
        """Initialize the service client.

        Args:
            amqp_url: URL for connecting to AMQP broker.
            exchange_name: Name of the AMQP exchange to use.
            routing_key: Routing key for RPC messages.
        """
        self.amqp_url = amqp_url
        self.exchange_name = exchange_name
        self.routing_key = routing_key

        self.RPS = RPCPublisher(
            amqp_url=amqp_url, exchange_name=exchange_name, routing_key=routing_key
        )

        self._is_connected: bool = False

    @property
    def is_connected(self) -> bool:
        """bool: Indicates whether the client is connected to the service."""
        return self._is_connected

    async def connect(self) -> None:
        """Establish connection with the AMQP broker.

        Raises:
            ServiceConnectionError: If connection fails.
        """
        try:
            await self.RPS.connect()
            self._is_connected = True
        except Exception as e:
            self._is_connected = False
            raise ServiceConnectionError(
                f"Unknown exception during connection to {self.amqp_url}: {e}"
            )

    async def disconnect(self) -> None:
        """Close the connection and clean up resources."""
        await self.RPS.close()
        self._is_connected = False

    async def __aenter__(self):
        """Async context manager entry.

        Returns:
            BaseServiceClient: The connected client instance.
        """
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Async context manager exit.

        Args:
            exc_type: Exception type if raised in context.
            exc_val: Exception value if raised in context.
            exc_tb: Exception traceback if raised in context.

        Returns:
            bool: False to not suppress any exceptions.
        """
        await self.disconnect()
        if exc_type is not None:
            ServiceExecuteError(f"Transaction error: {exc_val}")
        return False

    @classmethod
    def handle_rpc_response(cls, func: Callable) -> Callable:
        """Decorator to standardize RPC response handling.

        Handles:
        - Connection verification
        - RPC error handling
        - Response parsing
        - Uniform exception reporting

        Args:
            func: The RPC method to wrap.

        Returns:
            Callable: Wrapped function with standardized error handling.

        Raises:
            ServiceConnectionError: If client is not connected.
            ServiceExecuteError: For RPC or parsing failures.
        """

        @wraps(func)
        async def wrapper(self, *args, **kwargs) -> Any:
            if not self.is_connected:
                raise ServiceConnectionError("Service client is not connected")

            try:
                request = await func(self, *args, **kwargs)
                response = await self.RPS.call(request, timeout=30)
                return json.loads(response)

            except RPCError as e:
                raise ServiceExecuteError(f"RPC operation failed: {str(e)}") from e
            except json.JSONDecodeError as e:
                raise ServiceExecuteError("Invalid service response format") from e
            except Exception as e:
                raise ServiceExecuteError("Service operation failed") from e

        return wrapper


__all__ = ["ServiceConnectionError", "ServiceExecuteError", "BaseServiceClient"]
