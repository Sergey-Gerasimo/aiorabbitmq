import pytest
import docker
import asyncio
import aio_pika
import time
from tenacity import retry, stop_after_delay, wait_fixed
from aiorabbitmq.services import BaseResponseProcessor, BaseServiceClient


@pytest.fixture(scope="session")
def docker_client():
    return docker.from_env()


@pytest.fixture(scope="session", autouse=True)
def rabbitmq_container(docker_client):
    container = docker_client.containers.run(
        image="rabbitmq:3.12-management",
        ports={"5672/tcp": 5672},
        environment={
            "RABBITMQ_DEFAULT_USER": "guest",
            "RABBITMQ_DEFAULT_PASS": "guest",
            "RABBITMQ_NODENAME": "test@localhost",
        },
        detach=True,
        remove=True,
        healthcheck={
            "test": ["CMD", "rabbitmq-diagnostics", "status"],
            "interval": 1_000_000_000,
            "timeout": 3_000_000_000,
            "retries": 10,
        },
    )

    # Ждем полной инициализации RabbitMQ
    time.sleep(10)

    yield container

    # Остановка контейнера
    container.stop(timeout=1)


@pytest.fixture
async def rabbit_connection(rabbitmq_url):
    @retry(stop=stop_after_delay(30), wait=wait_fixed(1))
    async def connect():
        return await aio_pika.connect(rabbitmq_url)

    connection = await connect()
    yield connection
    await connection.close()


@pytest.fixture(scope="session")
async def test_exchange(rabbitmq_url):
    """Фикстура создает тестовый exchange и удаляет его после тестов"""
    connection = await aio_pika.connect(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            "test_exchange", aio_pika.ExchangeType.DIRECT, durable=True
        )
        yield exchange.name
        await exchange.delete()


@pytest.fixture(scope="module")
async def test_queue(rabbitmq_url, test_exchange):
    """Фикстура создает тестовую очередь и привязывает к exchange"""
    connection = await aio_pika.connect(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("test_queue", durable=True)
        await queue.bind(test_exchange, routing_key="test.routing.key")
        yield queue.name
        await queue.delete()


@pytest.fixture(scope="module")
def rabbitmq_url():
    return "amqp://guest:guest@localhost/"


@pytest.fixture(scope="module")
def exchange_name():
    return "test_integration_exchange"


@pytest.fixture(scope="module")
def queue_name():
    return "test_integration_queue"


@pytest.fixture(scope="module")
def routing_key():
    return "test.routing.key"
