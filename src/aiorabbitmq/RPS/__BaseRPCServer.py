import asyncio
import logging
import json
import signal
from abc import ABC, abstractmethod
from typing import AsyncGenerator, Optional

from aio_pika import Message, connect, RobustConnection
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractChannel,
    AbstractExchange,
    AbstractConnection
)


from aiorabbitmq.__settings import logger 

class BaseRPCServer(ABC):
    """
        An abstract RPC server class. 
        To use it, you need: 
            1. Inherit this class 
            2. Implement all methods marked @abstractmethod
    """
    def __init__(
        self,
        amqp_url: str,
        queue_name: str,
        span: Optional[AsyncGenerator[None, None]] = None
    ):
        self.span = span
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.connection: Optional[AbstractConnection] = None
        self.channel: Optional[AbstractChannel] = None
        self.exchange: Optional[AbstractExchange] = None
        self._stop_event = asyncio.Event()
        self._consumer_task: Optional[asyncio.Task] = None
        self.logger = self._configure_logger()

    def _configure_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger


    async def _create_connection(self) -> None:
        self.connection = await connect(
            self.amqp_url,
            connection_class=RobustConnection,
            client_properties={"connection_name": "fibonacci_rpc_server"}
        )
        self.channel = await self.connection.channel(
            publisher_confirms=True,
            on_return_raises=False
        )
        self.exchange = self.channel.default_exchange
        logger.info("Connected to RabbitMQ")

    @abstractmethod
    async def validate_request(message: AbstractIncomingMessage) -> None: 
        """
        This method must be implementated!
        """
        raise NotImplementedError("Method validate_request is not implementated!")

    async def process_request(message: AbstractIncomingMessage) -> str: 
        """
        This method must be implementated!
        """
        raise NotImplementedError("Method process_request is not implementated!")
    

    async def _process_request(self, message: AbstractIncomingMessage) -> None:
        async with message.process():
            try:
                await self.validate_request(message)
                result = await self.process_request(message)

                response = json.dumps({"status": "OK", "response": result}).encode()

                await self.exchange.publish(
                    Message(
                        body=response,
                        correlation_id=message.correlation_id,
                        delivery_mode=2
                    ),
                    routing_key=message.reply_to,
                    timeout=5
                )

            except Exception as e:
                logger.error(f"Error processing request: {str(e)}")
                await self._send_error(message, str(e))

    async def _send_error(self, message: AbstractIncomingMessage, error: str) -> None:
        try:
            await self.exchange.publish(
                Message(
                    body=json.dumps({"status": "ERROR", "response": error}).encode(),
                    correlation_id=message.correlation_id,
                    headers={"error": True},
                    delivery_mode=2
                ),
                routing_key=message.reply_to,
                timeout=3
            )
        except Exception as e:
            logger.error(f"Failed to send error response: {str(e)}")

    async def _message_consumer(self) -> None:
        queue = await self.channel.declare_queue(
            self.queue_name,
            durable=True,
            arguments={
                "x-max-priority": 10,
                "x-queue-mode": "lazy",
                "x-dead-letter-exchange": "dead_letters"
            }
        )
        
        logger.info(f"Started consuming from queue '{self.queue_name}'")
        try:
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if self._stop_event.is_set():
                        break
                    asyncio.create_task(self._process_request(message))
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled")
            raise
        except Exception as e:
            logger.error(f"Consumer failed: {str(e)}")
            raise

    async def run(self) -> None: 
        if self.span is not None: 
            async for _ in self.span(): 
                await self.__run() 

        else: 
            await self.__run() 
    
    async def __run(self) -> None:
        """Главный метод для запуска сервера"""
        try:
            await self._create_connection()
            self._consumer_task = asyncio.create_task(self._message_consumer())
            
            # Создаем отдельную задачу для ожидания прерывания
            loop = asyncio.get_event_loop()
            future = loop.create_future()
            
            # Регистрируем обработчик Ctrl+C
            loop.add_signal_handler(
                signal.SIGINT,
                lambda: future.set_exception(KeyboardInterrupt()))
            
            await future
            
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
        finally:
            await self.close()

    async def close(self) -> None:
        """Корректное завершение работы"""
        logger.info("Starting graceful shutdown...")
        self._stop_event.set()
        
        if self._consumer_task and not self._consumer_task.done():
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("Connection closed")


async def main() -> None:
    server = BaseRPCServer(
        "amqp://guest:guest@localhost/",
        "rpc_queue",
    )
    await server.run()

if __name__ == "__main__":
    asyncio.run(main())