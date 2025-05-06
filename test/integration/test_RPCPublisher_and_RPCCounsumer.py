import pytest
import asyncio
import json
import uuid
from aio_pika import Message
from aiorabbitmq.RPC.RPSExceptions import RPCError, NoCorrelationIDException
from typing import Any, Dict
import aio_pika
from aiorabbitmq.RPC import RPCConsumer, RPCPublisher


@pytest.mark.asyncio
@pytest.mark.integration
async def test_publisher_consumer_workflow(rabbitmq_url):
    consumer = RPCConsumer(rabbitmq_url, "test_exchange", "test_queue")
    await consumer.connect()

    publisher = RPCPublisher(rabbitmq_url, "test_exchange", "test_queue")
    await publisher.connect()

    processing_result: Dict[str, Any] = {}

    async def consumer_callback(data: Dict[str, Any]) -> str:
        """Тестовый обработчик сообщений для Consumer"""
        processing_result.update({"received_data": data, "processed": True})
        return json.dumps({"result": data["value"] * 2})

    consumer_task = asyncio.create_task(consumer.consume(consumer_callback))
    await asyncio.sleep(1)

    try:

        test_data = {"action": "test", "value": 21}
        response = await publisher.call(json.dumps(test_data))

        assert json.loads(response) == {"result": 42}
        assert processing_result["received_data"] == test_data
        assert processing_result["processed"] is True

    finally:
        # 5. Очистка
        consumer_task.cancel()
        await publisher.close()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
@pytest.mark.integration
async def test_publisher_timeout(rabbitmq_url):
    """Тестируем обработку таймаута при отсутствии Consumer"""
    publisher = RPCPublisher(
        amqp_url=rabbitmq_url,
        exchange_name="nonexistent_exchange",
        routing_key="nonexistent.key",
    )
    await publisher.connect()

    try:
        with pytest.raises(RPCError, match="Request timed out"):
            await publisher.call(json.dumps({"test": "data"}), timeout=1)
    finally:
        await publisher.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_consumer_error_handling(rabbitmq_url):
    consumer = RPCConsumer(rabbitmq_url, "test_exchange", "test_queue")
    await consumer.connect()

    publisher = RPCPublisher(rabbitmq_url, "test_exchange", "test_queue")
    await publisher.connect()

    processing_result: Dict[str, Any] = {}

    async def consumer_callback(data: Dict[str, Any]) -> str:
        raise ValueError("some error")

    consumer_task = asyncio.create_task(consumer.consume(consumer_callback))
    await asyncio.sleep(1)

    with pytest.raises(RPCError):
        response = await publisher.call(json.dumps({"test": "data"}))


@pytest.mark.asyncio
@pytest.mark.integration
async def test_message_processing_without_reply_to(rabbitmq_url):
    """Тестируем обработку сообщения без reply_to"""
    exchange_name = "test_no_reply_exchange"
    queue_name = "test_no_reply_queue"

    consumer = RPCConsumer(
        amqp_url=rabbitmq_url, exchange_name=exchange_name, queue_name=queue_name
    )

    async def dummy_callback(data: Dict[str, Any]) -> str:
        return json.dumps({"status": "ok"})

    consumer_task = asyncio.create_task(consumer.consume(dummy_callback))
    await asyncio.sleep(1)

    # Создаем сообщение без reply_to
    connection = await aio_pika.connect(rabbitmq_url)
    channel = await connection.channel()
    message = Message(
        body=json.dumps({"test": "data"}).encode(), correlation_id=str(uuid.uuid4())
    )

    # Отправляем сообщение напрямую в очередь
    await channel.default_exchange.publish(message, routing_key=queue_name)

    # Ждем обработки (сообщение должно быть обработано без ошибок)
    await asyncio.sleep(1)

    consumer_task.cancel()
    await connection.close()
