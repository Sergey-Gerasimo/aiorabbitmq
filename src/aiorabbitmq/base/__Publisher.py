from aiorabbitmq.abc import RabbitMQBase
from aio_pika import ExchangeType, Message
import json
from typing import Optional, Callable, Any


class RabbitMQError(Exception):
    """Custom exception raised for RabbitMQ-related errors.

    This exception is raised when operations with RabbitMQ fail, including connection issues,
    publishing errors, and other RabbitMQ-specific problems.
    """

    pass


class RabbitMQPublisher(RabbitMQBase):
    """Asynchronous RabbitMQ message publisher.

    This class provides functionality to publish messages to a RabbitMQ exchange with
    configurable parameters for exchange type, durability, and routing.

    Inherits from:
        RabbitMQBase: Base class providing common RabbitMQ connection functionality.

    Example:
        async with RabbitMQPublisher(
            amqp_url="amqp://localhost",
            exchange_name="my_exchange",
            routing_key="my_key"
        ) as publisher:
            await publisher.publish({"key": "value"})
    """

    def __init__(
        self,
        amqp_url: str,
        exchange_name: str,
        routing_key: str,
        exchange_type=ExchangeType.DIRECT,
        durable: bool = True,
    ):
        """Initialize the RabbitMQ publisher.

        Args:
            amqp_url: URL for connecting to RabbitMQ server (e.g., 'amqp://guest:guest@localhost/')
            exchange_name: Name of the exchange to publish messages to
            routing_key: Routing key to use for message publishing
            exchange_type: Type of exchange (default: ExchangeType.DIRECT)
            durable: Whether the exchange should survive broker restarts (default: True)
        """
        super().__init__(amqp_url, exchange_name, exchange_type, durable)
        self.routing_key = routing_key

    async def publish(
        self, message: dict, serializer: Optional[Callable[[object], str]] = None
    ):
        """Publish a message to the configured RabbitMQ exchange.

        The message will be automatically serialized to JSON format before publishing.
        For custom serialization of non-JSON-serializable objects, provide a serializer function.

        Args:
            message: Dictionary containing the message data to be published
            serializer: Optional function to handle serialization of non-JSON-serializable objects

        Raises:
            RabbitMQError: If not connected to RabbitMQ or if publishing fails
            ValueError: If message serialization fails
            TypeError: If message contains unserializable data and no serializer is provided

        Note:
            You must establish connection using connect() before calling this method.
        """

        if not self.exchange:
            raise RabbitMQError("Not connected to RabbitMQ")

        try:
            _message = json.dumps(message, default=serializer)
            _message = Message(_message.encode(), content_type="application/json")

            await self.exchange.publish(message=_message, routing_key=self.routing_key)

        except Exception as e:
            raise RabbitMQError(f"Request failed: {e}") from e

    async def __aenter__(self):
        """Asynchronous context manager entry point.

        Establishes connection when entering the context manager block.

        Returns:
            RabbitMQPublisher: The connected publisher instance

        Raises:
            RabbitMQError: If connection fails
        """
        await self.connect()
        return self

    async def disconnect(self):
        """Disconnect from RabbitMQ server.

        Alias for close() method for more explicit interface.
        """
        await self.close()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Asynchronous context manager exit point.

        Cleans up resources and handles any exceptions that occurred in the context.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred

        Returns:
            bool: False to propagate exceptions, True to suppress them

        Note:
            Always disconnects from RabbitMQ, even if an exception occurred.
        """
        await self.disconnect()
        if exc_type is not None:
            RabbitMQError(f"Transaction error: {exc_val}")
        return False


__all__ = ["RabbitMQPublisher", "RabbitMQError"]
