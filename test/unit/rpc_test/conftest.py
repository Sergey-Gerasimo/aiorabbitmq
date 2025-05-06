import pytest
import aio_pika
from aio_pika.abc import AbstractChannel, AbstractQueue, AbstractIncomingMessage
from aio_pika import RobustConnection
from unittest.mock import AsyncMock, patch, MagicMock
import asyncio


@pytest.fixture
def mock_rabbit(mock_rabbit_connection):
    mock_queue = AsyncMock(spec=aio_pika.Queue)
    mock_channel = AsyncMock(spec=aio_pika.Channel)
    mock_connection = mock_rabbit_connection.return_value
    mock_exchange = AsyncMock(spec=aio_pika.Exchange)
    mock_message = MagicMock(spec=AbstractIncomingMessage)
    mock_default_exchange = AsyncMock(spec=aio_pika.Exchange)

    mock_connection.channel = AsyncMock(return_value=mock_channel)
    mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
    mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
    mock_channel.default_exchange = AsyncMock(return_value=mock_default_exchange)

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

    mock_message.body = b'{"test": "data"}'
    mock_queue.iterator.return_value = AsyncIteratorContext(mock_message)

    return {
        "connection": mock_connection,
        "channel": mock_channel,
        "exchange": mock_exchange,
        "queue": mock_queue,
        "message": mock_message,
        "default_exchange": mock_default_exchange,
    }


@pytest.fixture()
def mock_rabbit_connection():
    """Автоматическое мокирование aio_pika.connect во всех тестах"""
    with patch("aio_pika.connect", new_callable=AsyncMock) as mock_connect:
        # Настраиваем стандартное поведение мока
        mock_connection = AsyncMock(spec=RobustConnection)
        mock_connect.return_value = mock_connection
        yield mock_connect
