import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from aio_pika.abc import AbstractIncomingMessage
from aio_pika import Message
from aiorabbitmq.RPC import (
    RPCPublisher,
    RPCConsumer,
    RPCError,
    NoCorrelationIDException,
)
import json
import asyncio
import uuid


class TestRPCPublisher:
    @pytest.fixture
    def publisher(self):
        """Fixture providing configured RPSRabbitMQBasePublisher instance"""
        return RPCPublisher(
            amqp_url="amqp://guest:guest@localhost/",
            exchange_name="test_exchange",
            routing_key="test_routing_key",
        )

    @pytest.fixture
    def mock_message(self):
        """Fixture providing mock response message"""
        message = MagicMock(spec=AbstractIncomingMessage)
        message.correlation_id = str(uuid.uuid4())
        message.body = json.dumps({"response": "data"}).encode()
        message.headers = {}
        return message

    @pytest.mark.asyncio
    async def test_connect(self, publisher: RPCPublisher, mock_rabbit):
        """Test connection setup with callback queue"""

        await publisher.connect()

        mock_rabbit["connection"].channel.assert_awaited_once()
        mock_rabbit["channel"].declare_exchange.assert_awaited_once()
        mock_rabbit["channel"].declare_queue.assert_awaited_once_with(
            exclusive=True,
            durable=True,
            arguments={
                "x-max-priority": 10,
                "x-queue-mode": "lazy",
                "x-dead-letter-exchange": "dead_letters",
            },
        )
        mock_rabbit["queue"].consume.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_on_response_success(
        self, publisher: RPCPublisher, mock_message, mock_rabbit
    ):
        """Test successful response handling"""
        await publisher.connect()
        test_future = asyncio.Future()
        publisher.futures[mock_message.correlation_id] = test_future

        await publisher.on_response(mock_message)

        assert test_future.done()
        assert test_future.result() == mock_message.body.decode()
        assert mock_message.correlation_id not in publisher.futures

    @pytest.mark.asyncio
    async def test_on_response_error(self, publisher, mock_message, mock_rabbit):
        """Test error response handling"""
        await publisher.connect()

        mock_message.headers = {"error": True}
        test_future = asyncio.Future()
        publisher.futures[mock_message.correlation_id] = test_future

        await publisher.on_response(mock_message)

        assert test_future.done()
        with pytest.raises(RPCError):
            test_future.result()
        assert mock_message.correlation_id not in publisher.futures

    @pytest.mark.asyncio
    async def test_on_response_no_correlation(self, publisher, mock_message):
        """Test handling of message without correlation ID"""
        mock_message.correlation_id = None
        with pytest.raises(NoCorrelationIDException):
            await publisher.on_response(mock_message)

    @pytest.mark.asyncio
    async def test_call_timeout(self, publisher, mock_rabbit):
        """Test RPC call timeout"""
        await publisher.connect()
        test_data = json.dumps({"test": "data"})

        with patch.object(publisher.channel.default_exchange, "publish", AsyncMock()):
            with pytest.raises(RPCError, match="Request timed out"):
                await publisher.call(test_data, timeout=0.01)

    @pytest.mark.asyncio
    async def test_call_publish_error(self, publisher, mock_rabbit):
        """Test handling of publish errors"""
        await publisher.connect()
        test_data = json.dumps({"test": "data"})

        with patch.object(
            publisher.channel.default_exchange,
            "publish",
            AsyncMock(side_effect=Exception("Publish error")),
        ):
            with pytest.raises(RPCError, match="Request failed"):
                await publisher.call(test_data)
