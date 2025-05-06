import pytest
import json
import aio_pika
import asyncio
from aiorabbitmq.services import (
    BaseServiceClient,
    ServiceExecuteError,
    ServiceConnectionError,
    BaseResponseProcessor,
)
from tenacity import retry, stop_after_delay, wait_fixed


@pytest.mark.asyncio
@pytest.mark.integration
@retry(stop=stop_after_delay(30), wait=wait_fixed(1))
async def test_service_client_integration(rabbitmq_url, exchange_name, routing_key):
    """Тестируем базовое подключение и работу клиента"""
    async with BaseServiceClient(
        amqp_url=rabbitmq_url, exchange_name=exchange_name, routing_key=routing_key
    ) as client:
        assert client.is_connected

        @BaseServiceClient.handle_rpc_response
        async def test_call(self):
            return json.dumps({"action": "test", "data": {"value": 42}})

        with pytest.raises(ServiceExecuteError):
            await test_call(client)


@pytest.mark.asyncio
@pytest.mark.integration
@retry(stop=stop_after_delay(30), wait=wait_fixed(1))
async def test_response_processor_integration(rabbitmq_url, exchange_name, queue_name):
    """Тестируем взаимодействие клиента и процессора"""
    # Инициализация процессора
    processor = BaseResponseProcessor(
        amqp_url=rabbitmq_url, exchange_name=exchange_name, queue_name=queue_name
    )

    @processor.add("test.action")
    async def handle_test(data):
        return json.dumps({"processed": data["value"] * 2})

    processor_task = asyncio.create_task(processor.run())
    await asyncio.sleep(2)  # Ожидание инициализации

    class Caller(BaseServiceClient):
        @BaseServiceClient.handle_rpc_response
        async def make_call(self):
            return json.dumps({"action": "test.action", "data": {"value": 21}})

    async with Caller(
        amqp_url=rabbitmq_url, exchange_name=exchange_name, routing_key=queue_name
    ) as client:
        await client.make_call()
        pass

    processor._shutdown()
    processor._should_stop.set()
    await processor_task


@pytest.mark.asyncio
@pytest.mark.integration
@retry(stop=stop_after_delay(30), wait=wait_fixed(1))
async def test_error_handling(rabbitmq_url, exchange_name, queue_name):
    """Тестируем обработку ошибок в процессоре"""
    processor = BaseResponseProcessor(
        amqp_url=rabbitmq_url, exchange_name=exchange_name, queue_name=queue_name
    )

    @processor.add("error.action")
    async def error_handler(data):
        raise ValueError("Test error")

    processor_task = asyncio.create_task(processor.run())
    await asyncio.sleep(2)

    try:
        async with BaseServiceClient(
            amqp_url=rabbitmq_url, exchange_name=exchange_name, routing_key=queue_name
        ) as client:

            @BaseServiceClient.handle_rpc_response
            async def call_error(self):
                return {"action": "error.action", "data": {}}

            with pytest.raises(ServiceExecuteError):
                await call_error(client)

    finally:
        processor._shutdown()
        processor._should_stop.set()
        await processor_task
