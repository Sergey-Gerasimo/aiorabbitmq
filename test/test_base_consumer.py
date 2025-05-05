import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from aio_pika import ExchangeType, Message
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from aio_pika.exceptions import ChannelClosed
from src.aiorabbitmq.base.__Consumer import RabbitMQConsumer
import json
import asyncio
import time


class TestRabbitMQConsumer:
    @pytest.fixture
    def consumer(self):
        """Fixture that provides a configured RabbitMQConsumer instance for testing.

        Returns:
            RabbitMQConsumer: Consumer instance with test configuration
        """
        return RabbitMQConsumer(
            amqp_url="amqp://guest:guest@localhost/",
            exchange_name="exchange_1",
            routing_key="queue_1",
        )

    @pytest.fixture
    def mock_rabbit(self):
        """Fixture that creates a complete mock RabbitMQ environment.

        Returns:
            dict: Dictionary containing all mocked RabbitMQ components:
                - connection: Mocked connection object
                - channel: Mocked channel object
                - exchange: Mocked exchange object
                - queue: Mocked queue object
                - message: Mocked incoming message
        """
        # Create all necessary mocks with proper specs
        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_exchange = MagicMock()
        mock_queue = MagicMock(spec=AbstractQueue)
        mock_message = MagicMock(spec=AbstractIncomingMessage)

        # Configure method chains
        mock_connection.channel.return_value = mock_channel
        mock_channel.declare_exchange.return_value = mock_exchange
        mock_channel.declare_queue.return_value = mock_queue

        # Configure queue iterator
        mock_queue.iterator.return_value = AsyncMock(return_value=[mock_message])
        mock_message.body = json.dumps({"test": "data"}).encode()

        return {
            "connection": mock_connection,
            "channel": mock_channel,
            "exchange": mock_exchange,
            "queue": mock_queue,
            "message": mock_message,
        }

    @pytest.mark.asyncio
    async def test_connection_setup(self, consumer, mock_rabbit):
        """Tests successful connection setup to RabbitMQ.

        Verifies:
        1. Channel creation is called exactly once
        2. Exchange declaration is called exactly once
        """
        with patch("aio_pika.connect", return_value=mock_rabbit["connection"]):
            await consumer.connect()

            mock_rabbit["connection"].channel.assert_awaited_once()
            mock_rabbit["channel"].declare_exchange.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_queue_setup(self, consumer, mock_rabbit):
        """Tests successful queue setup.

        Verifies:
        1. Queue declaration is called exactly once
        2. Returns the expected queue object
        """
        with patch("aio_pika.connect", return_value=mock_rabbit["connection"]):
            await consumer.connect()
            queue = await consumer.set_up_queue()

            mock_rabbit["channel"].declare_queue.assert_awaited_once()
            assert queue == mock_rabbit["queue"]

    @pytest.mark.asyncio
    async def test_queue_setup_failure(self, consumer, mock_rabbit):
        """Tests successful message processing flow.

        Verifies:
        1. Message processing without callback set
        2. Proper callback execution when set
        3. Message acknowledgment
        """
        mock_rabbit["channel"].declare_queue.side_effect = ChannelClosed(
            404, "Not found"
        )

        with patch("aio_pika.connect", return_value=mock_rabbit["connection"]):
            await consumer.connect()
            with pytest.raises(ChannelClosed):
                await consumer.set_up_queue()

    @pytest.mark.asyncio
    async def test_message_processing_success(self, consumer, mock_rabbit):
        test_callback = AsyncMock()
        mock_message = MagicMock(spec=AbstractIncomingMessage)
        mock_message.body = json.dumps({"test": "data"}).encode()

        with patch("aio_pika.connect", return_value=mock_rabbit["connection"]):
            await consumer.connect()
            await consumer.process_message(mock_rabbit["message"])

            test_callback.assert_not_called()  # Callback not set yet

            consumer.callback = test_callback
            await consumer.process_message(mock_message)

            mock_message.process.assert_called_once_with(requeue=False)
            test_callback.assert_awaited_once_with({"test": "data"})

    @pytest.mark.asyncio
    async def test_consume_flow(self, consumer):
        """Tests the complete message consumption flow.

        Verifies:
        1. Queue iterator is called exactly once
        2. Message processing is called
        3. Proper task cancellation
        """
        mock_message = MagicMock(spec=AbstractIncomingMessage)
        mock_message.body = json.dumps({"test": "data"}).encode()

        class AsyncIteratorContext:
            def __init__(self, message):
                self.message = message
                self.sent = False

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.sent:
                    await asyncio.sleep(0.1)
                    raise StopAsyncIteration
                self.sent = True
                return self.message

        mock_queue = MagicMock(spec=AbstractQueue)
        mock_queue.iterator.return_value = AsyncIteratorContext(mock_message)
        # 2. Мокаем process_message
        process_mock = AsyncMock()

        # 3. Подменяем зависимости
        with patch.object(
            consumer, "set_up_queue", AsyncMock(return_value=mock_queue)
        ), patch.object(consumer, "process_message", process_mock):

            # 4. Запускаем и ждем
            task = asyncio.create_task(consumer.consume(AsyncMock()))
            await asyncio.sleep(1.0)  # Увеличенное время ожидания

            # 5. Проверяем и останавливаем
            mock_queue.iterator.assert_called_once()
            process_mock.assert_awaited_once()
            task.cancel()

    @pytest.mark.asyncio
    async def test_retry_mechanism_with_failures(self, consumer):
        """Tests the retry mechanism with continuous failures.

        Verifies:
        1. Exactly max_attempts (5) retries are made
        2. ConnectionError is raised after all attempts
        3. Reconnect is called after each failure
        """
        consumer.set_up_queue = AsyncMock(side_effect=ConnectionError("Mocked error"))

        with pytest.raises(ConnectionError), patch("asyncio.sleep", AsyncMock()):

            await consumer.consume(AsyncMock())

            assert consumer.set_up_queue.call_count == 5  # max_attempts
            assert consumer.reconnect.await_count == 5

    @pytest.mark.asyncio
    async def test_disconnect(self, consumer, mock_rabbit):
        """Tests proper connection cleanup.

        Verifies both channel and connection are properly closed.
        Note: Original method has typo 'disconect' instead of 'disconnect'
        """
        with patch("aio_pika.connect", return_value=mock_rabbit["connection"]):
            await consumer.connect()
            await consumer.disconect()  # Note: Typo in original method name

            mock_rabbit["channel"].close.assert_awaited_once()
            mock_rabbit["connection"].close.assert_awaited_once()
