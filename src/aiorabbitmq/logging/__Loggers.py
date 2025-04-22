import json
from aio_pika import Message
from aio_pika.abc import AbstractIncomingMessage, ExchangeType
from abc import ABC
import asyncio 
import logging 
from typing import Callable, Awaitable, Literal 
import sys

from ..abc.__RabbitMQBase import RabbitMQBase


def setup_logger(name: str = "logging") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)

    logger.addHandler(handler)
    return logger

logger = setup_logger()

class LoggingSystem(ABC):
    EXCHANGE_NAME = "logs_exchange"
    EXCHANGE_TYPE = ExchangeType.TOPIC
    
    @staticmethod
    def create_message(level: str, message: str) -> Message:
        return Message(
            body=json.dumps({"level": level, "message": message}).encode(),
            delivery_mode=2  # Сохранять сообщения на диск
        )


class LogProducer(LoggingSystem, RabbitMQBase):
    def __init__(self, amqp_url):
        super().__init__(amqp_url, self.EXCHANGE_NAME, exchange_type=self.EXCHANGE_TYPE)
        self._connection_lock = asyncio.Lock()

    async def send_log(self, level: str, message: str):
        async with self._connection_lock:
            if not self.exchange:
                await self.connect()

        try:
            await self.exchange.publish(
                self.create_message(level, message),
                routing_key=level
            )
            logger.debug(f"Log sent: {level} - {message}")
        except Exception as e:
            logger.error(f"Failed to send log: {str(e)}")
            raise


class LogConsumer(LoggingSystem, RabbitMQBase):
    def __init__(self, amqp_url: str, queue_name: str, levels: list[Literal["warning", "error", "info"]]):
        super().__init__(amqp_url, self.EXCHANGE_NAME, self.EXCHANGE_TYPE)
        self.queue_name = queue_name
        self.levels = levels
        self.callback = None


    async def set_up(self): 
        await self.connect()
        await self._setup_queue()


    async def _setup_queue(self):
        self.queue = await self.channel.declare_queue(
            name=self.queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "dead_letters",
                "x-queue-mode": "lazy"
            }
        )
        
        for level in self.levels:
            await self.queue.bind(self.exchange, routing_key=level)


    async def consume(self, callback: Callable[[dict], Awaitable[None]]) -> None:
        await self._setup_queue()
        
        self._callback = callback
        self._consumer_tag = await self.queue.consume(
            self._process_message,
            no_ack=False
        )
        logger.info(f"Consumer started for queue: {self.queue_name}")


    async def _process_message(self, message: AbstractIncomingMessage) -> None:
        """Обработка входящих сообщений"""
        try:
            async with message.process():
                log_data = await self._parse_message(message)
                await self._callback(log_data)
                
        except json.JSONDecodeError:
            logger.error("Invalid JSON format in message")
            await message.reject(requeue=False)
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            await message.reject(requeue=True)


    async def _parse_message(self, message: AbstractIncomingMessage) -> dict:
        """Парсинг и валидация сообщения"""
        body = message.body.decode()
        data = json.loads(body)
        
        if not all(key in data for key in ("level", "message")):
            raise ValueError("Invalid log message structure")
            
        return data
    

    async def stop(self) -> None:
        """Остановка потребителя"""
        if self._consumer_tag:
            await self.queue.cancel(self._consumer_tag)
            logger.info(f"Consumer stopped for queue: {self.queue_name}")

