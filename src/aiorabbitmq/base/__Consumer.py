from aiorabbitmq.abc import RabbitMQBase
from aio_pika import ExchangeType
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from aio_pika.exceptions import ChannelClosed
import random

import asyncio

from typing import Callable, Awaitable, Dict, Optional, Any
import json

import logging


logger = logging.getLogger(__name__)


class RabbitMQConsumer(RabbitMQBase):
    """Asynchronous RabbitMQ message consumer with retry logic and error handling.

    Features:
    - Automatic reconnection with exponential backoff
    - Configurable dead-letter policy
    - Graceful shutdown handling
    - Detailed message processing metrics

    Example:
        >>> consumer = RabbitMQConsumer(
        ...     amqp_url="amqp://user:pass@localhost/vhost",
        ...     exchange_name="orders",
        ...     routing_key="orders.queue"
        ... )
        >>> async def callback(data: dict) -> None:
        ...     print(f"Processing order: {data['id']}")
        ...
        >>> asyncio.create_task(consumer.consume(callback))

    Args:
        amqp_url: RabbitMQ connection URL with credentials
        exchange_name: Name of the exchange to bind to
        routing_key: Routing key for queue binding
        exchange_type: RabbitMQ exchange type (direct/fanout/topic)
        durable: Whether the queue survives broker restart
        queue_arguments: Additional queue arguments (TTL, dead-letter, etc.)
    """

    def __init__(
        self,
        amqp_url: str,
        exchange_name: str,
        routing_key: str,
        exchange_type=ExchangeType.DIRECT,
        durable=True,
        queue_arguments: Optional[Dict[str, Any]] = None,
    ):
        self.routing_key: str = routing_key
        self.queue_arguments: Dict[str, Any] = queue_arguments or {
            "x-max-priority": 10,
            "x-queue-mode": "lazy",
            "x-dead-letter-exchange": "dead_letters",
        }
        self.callback: Optional[Callable[[Dict], Awaitable[None]]] = None
        super().__init__(amqp_url, exchange_name, exchange_type, durable)

    async def set_up_queue(self) -> AbstractQueue:
        """Declares and configures the RabbitMQ queue with specified parameters.

        Returns:
            The declared queue instance ready for message consumption.

        Raises:
            ChannelClosed: If queue declaration fails due to:
                - Insufficient permissions
                - Invalid queue arguments
                - Conflict with existing queue configuration
        """
        try:
            queue = await self.channel.declare_queue(
                self.routing_key, durable=self.durable, arguments=self.queue_arguments
            )
        except ChannelClosed as e:
            logger.error(f"Queue declaration failed: {e}")
            await self.connect()
            raise

        return queue

    async def process_message(self, message: AbstractIncomingMessage):
        """Processes a single incoming message with error handling.

        Message lifecycle:
        1. Automatic acknowledgment on success
        2. Dead-lettering on processing failure
        3. JSON decoding with validation

        Args:
            message: Raw incoming message from RabbitMQ

        Note:
            Message will be rejected to dead-letter exchange on:
            - Invalid JSON format
            - Callback exceptions
            - Encoding errors
        """

        try:
            async with message.process(requeue=False):
                data = json.loads(message.body.decode("utf-8"))

                if self.callback is None:
                    raise ValueError("Callback is not set")

                await self.callback(data)

        except UnicodeDecodeError as e:
            logger.error(f"Message encoding error: {e}")
            await message.reject(requeue=False)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e} | Body: {message.body[:100]!r}")
            await message.reject(requeue=False)
        except Exception as e:
            logger.error(f"Processing failed: {e}", exc_info=True)
            await message.reject(requeue=False)

    async def consume(self, callback: Callable[[dict], Awaitable[None]]):
        """Starts continuous message consumption with retry logic.

        Implements exponential backoff for connection failures:
        - Starts with 1 second delay
        - Doubles delay each attempt
        - Caps at 30 seconds maximum
        - Adds random jitter to prevent thundering herd

        Args:
            callback: Async function to process messages with signature:
                async def callback(data: dict) -> None:

        Raises:
            ConnectionError: After all retry attempts exhausted
            RuntimeError: For permanent configuration errors

        Example:
            >>> async def handle_message(data: dict) -> None:
            ...     print(f"Received: {data}")
            ...
            >>> await consumer.consume(handle_message)
        """
        self.callback = callback
        retry_policy = {
            "max_attempts": 5,
            "base_delay": 1.0,
            "max_delay": 30.0,
            "permanent_errors": (ValueError, RuntimeError),
        }

        attempt = 0
        while attempt < retry_policy["max_attempts"]:
            try:
                queue = await self.set_up_queue()
                async with queue.iterator() as queue_iter:
                    logger.info(f"Consuming from {self.routing_key}")
                    async for message in queue_iter:
                        await self.process_message(message)
                        attempt = 0  # Reset on successful processing

            except asyncio.CancelledError:
                logger.info("Consumption stopped by cancellation")
                break
            except retry_policy["permanent_errors"] as e:
                logger.error(f"Permanent error: {e}")
                raise
            except Exception as e:
                attempt += 1
                delay = min(
                    retry_policy["max_delay"],
                    retry_policy["base_delay"] * (2 ** (attempt - 1)),
                ) + random.uniform(0, 1)

                logger.warning(
                    f"Attempt {attempt} failed. Retrying in {delay:.1f}s. Error: {type(e).__name__}: {e}"
                )
                await asyncio.sleep(delay)

        if attempt >= retry_policy["max_attempts"]:
            logger.critical("Maximum retry attempts reached")
            raise ConnectionError("Failed to establish stable connection")
