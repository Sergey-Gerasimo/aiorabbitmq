from abc import ABC, abstractmethod
from aio_pika import connect, ExchangeType
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractConnection
from typing import Optional


class RabbitMQBase(ABC):
    """Базовый класс для publisher и consumer в RabbitMQ"""

    def __init__(
        self,
        amqp_url: str,
        exchange_name: str,
        exchange_type: ExchangeType = ExchangeType.DIRECT,
        durable=True,
    ):
        self.amqp_url = amqp_url
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.durable = durable
        self.connection: Optional[AbstractConnection] = None
        self.channel: Optional[AbstractChannel] = None
        self.exchange: Optional[AbstractExchange] = None

    async def connect(self) -> "RabbitMQBase":
        self.connection = await connect(self.amqp_url)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
            self.exchange_name, self.exchange_type, durable=self.durable
        )

        return self

    async def close(self) -> None:
        if self.connection:
            await self.connection.close()
