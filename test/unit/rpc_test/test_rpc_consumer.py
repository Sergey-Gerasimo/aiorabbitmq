import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from aiorabbitmq.RPC import RPCConsumer
import json
import asyncio
import uuid


class TestRPCConsumer:
    @pytest.fixture
    def consumer(self):
        """Fixture providing configured RPSRabbitMQBaseConsumer instance"""
        return RPCConsumer(
            amqp_url="amqp://guest:guest@localhost/",
            exchange_name="test_exchange",
            queue_name="test_queue",
        )

    @pytest.fixture
    def mock_message(self):
        """Fixture providing mock incoming message with test data"""
        message = MagicMock(spec=AbstractIncomingMessage)
        message.body = json.dumps({"test": "data"}).encode()
        message.reply_to = "reply_queue"
        message.correlation_id = str(uuid.uuid4())
        return message

    @pytest.mark.asyncio
    async def test_set_up_queue(self, consumer: RPCConsumer, mock_rabbit):
        """Test queue declaration with correct parameters"""
        await consumer.connect()
        queue = await consumer.set_up_queue()

        mock_rabbit["channel"].declare_queue.assert_awaited_once()
        assert queue == mock_rabbit["queue"]

    @pytest.mark.asyncio
    async def test_process_message_success(
        self, consumer: RPCConsumer, mock_message, mock_rabbit
    ):
        """Test successful message processing flow"""
        test_response = "response_data"

        with patch.object(consumer, "callback", AsyncMock(return_value=test_response)):
            await consumer.connect()

            with patch.object(
                consumer.channel.default_exchange, "publish", AsyncMock()
            ) as mock_publish:
                await consumer.process_message(mock_message)

                # Verify callback was called with correct data
                consumer.callback.assert_awaited_once_with(
                    json.loads(mock_message.body.decode())
                )

                # Verify response was published
                mock_publish.assert_awaited_once()
                assert (
                    mock_publish.call_args[0][0].correlation_id
                    == mock_message.correlation_id
                )
                assert mock_publish.call_args[0][0].body == test_response.encode()

    @pytest.mark.asyncio
    async def test_process_message_json_error(
        self, consumer: RPCConsumer, mock_message, mock_rabbit
    ):
        """Test handling of invalid JSON messages"""
        mock_message.body = b"invalid json"
        with patch.object(consumer, "send_error", AsyncMock()) as mock_send_error:
            await consumer.connect()
            await consumer.process_message(mock_message)
            mock_send_error.assert_awaited_once()
            assert "Invalid JSON format" in str(mock_send_error.call_args[0][1])

    @pytest.mark.asyncio
    async def test_process_message_no_reply_to(self, consumer, mock_message):
        """Test handling of messages without reply_to"""
        mock_message.reply_to = None
        with patch.object(consumer, "send_error", AsyncMock()):
            await consumer.process_message(mock_message)
            # Shouldn't raise but should log error

    @pytest.mark.asyncio
    async def test_consume_flow(self, consumer):
        """Test full consumption flow with message processing"""
        mock_queue = MagicMock(spec=AbstractQueue)
        mock_queue.iterator.return_value = AsyncMock()

        with patch.object(
            consumer, "set_up_queue", AsyncMock(return_value=mock_queue)
        ), patch.object(consumer, "process_message", AsyncMock()):

            task = asyncio.create_task(consumer.consume(AsyncMock()))
            await asyncio.sleep(0.1)  # Allow consumption to start
            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass

            consumer.set_up_queue.assert_awaited_once()
            mock_queue.iterator.assert_called_once()
