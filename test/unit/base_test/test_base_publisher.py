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

    @pytest.mark.asyncio
    async def test_connection(
        self,
        publisher: Publisher,
        mock_rabbit: dict[str, AsyncMock],
    ):
        """Test successful connection establishment and RabbitMQ setup"""
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
        async with publisher:
            # Verify connection was established
            mock_rabbit["connection"].channel.assert_awaited_once()

        # Verify connection was closed when exiting context
        mock_rabbit["connection"].close.assert_awaited_once()
