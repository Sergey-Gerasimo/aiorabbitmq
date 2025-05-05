import pytest
import aio_pika

from unittest.mock import AsyncMock, patch, MagicMock
from src.aiorabbitmq.base.__Publisher import RabbitMQPublisher as Publisher


class TestPublisher:
    @pytest.fixture
    def publisher(self):
        """Fixture to create a Publisher instance with default configuration"""
        return Publisher(
            amqp_url="amqp://guest:guest@localhost/",
            exchange_name="exchange_1",
            routing_key="queue_1",
        )

    @pytest.fixture
    def mock_rabbit(self):
        """Fixture that creates and configures all necessary RabbitMQ mocks

        Returns:
            dict: Dictionary containing mocked RabbitMQ components:
                - queue: Mocked Queue object
                - channel: Mocked Channel object
                - connection: Mocked Connection object
                - exchange: Mocked Exchange object
        """
        # Create mocks for each RabbitMQ component
        mock_queue = AsyncMock(spec=aio_pika.Queue)
        mock_channel = AsyncMock(spec=aio_pika.Channel)
        mock_connection = AsyncMock(spec=aio_pika.Connection)
        mock_exchange = AsyncMock(spec=aio_pika.Exchange)

        # Configure the mock chain:
        # connection.channel() -> channel
        # channel.declare_queue() -> queue
        # channel.declare_exchange() -> exchange
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue.return_value = AsyncMock(return_value=mock_queue)

        return {
            "queue": mock_queue,
            "channel": mock_channel,
            "connection": mock_connection,
            "exchange": mock_exchange,
        }

    @pytest.mark.asyncio
    async def test_connection(
        self, publisher: Publisher, mock_rabbit: dict[str, AsyncMock]
    ):
        """Test successful connection establishment and RabbitMQ setup"""
        with patch(
            "aio_pika.connect",
            return_value=mock_rabbit["connection"],
        ) as mock_connect:
            # 1. Test connection establishment
            await publisher.connect()

            # Verify channel was created
            mock_rabbit["connection"].channel.assert_awaited_once()

            # Verify exchange was declared with correct parameters
            mock_rabbit["channel"].declare_exchange.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_disconnect(
        self, publisher: Publisher, mock_rabbit: dict[str, AsyncMock]
    ):
        """Test proper connection cleanup on disconnect"""
        with patch("aio_pika.connect", return_value=mock_rabbit["connection"]):
            # 1. Establish connection first
            await publisher.connect()

            # 2. Test disconnection
            await publisher.disconnect()

            # Verify connection was closed
            mock_rabbit["connection"].close.assert_awaited_once()

            # Verify channel was closed
            mock_rabbit["channel"].close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connection_error(self, publisher: Publisher):
        """Test handling of connection errors"""
        with patch(
            "aio_pika.connect", side_effect=ConnectionError("Connection failed")
        ):
            # Verify that connection error is properly raised
            with pytest.raises(ConnectionError, match="Connection failed"):
                await publisher.connect()

    @pytest.mark.asyncio
    async def test_publish_message(self, publisher, mock_rabbit):
        """Test message publishing functionality"""
        test_message = {"key": "value"}

        with patch("aio_pika.connect", return_value=mock_rabbit["connection"]):
            # 1. Establish connection
            await publisher.connect()

            # 2. Test message publishing
            await publisher.publish(test_message)

            # Verify message was published to exchange
            mock_rabbit["exchange"].publish.assert_awaited_once()

            # Verify correct routing key was used
            args, kwargs = mock_rabbit["exchange"].publish.await_args
            assert kwargs["routing_key"] == "queue_1"

    @pytest.mark.asyncio
    async def test_context_manager(self, publisher, mock_rabbit):
        """Test that Publisher works correctly as context manager"""
        with patch("aio_pika.connect", return_value=mock_rabbit["connection"]):
            async with publisher:
                # Verify connection was established
                mock_rabbit["connection"].channel.assert_awaited_once()

            # Verify connection was closed when exiting context
            mock_rabbit["connection"].close.assert_awaited_once()
