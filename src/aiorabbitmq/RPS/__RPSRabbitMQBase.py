from ..abc import RabbitMQBase

from aiorabbitmq.__settings import logger 

from typing import Optional, AsyncGenerator, Awaitable, Callable, MutableMapping
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from aio_pika import Message, connect, RobustConnection, ExchangeType
import asyncio
import json 
import uuid 
from RPSExceptions import RPCError, NoCorrelationIDException    


class RPSRabbitMQBaseConsumer(RabbitMQBase): 
    def __init__(self, 
                amqp_url:str, 
                exchange_name: str, 
                queue_name: str,
                span: Optional[AsyncGenerator[None, None]] = None):
        
        super().__init__(amqp_url, exchange_name)
        self.span = span 
        self.queue_name = queue_name

        self._stop_event = asyncio.Event()
        self._consumer_task: Optional[asyncio.Task] = None


    async def connect(self) -> "RPSRabbitMQBaseConsumer": 
        await super().connect()
        return self
    
    async def set_up_queue(self): 
        queue = await self.channel.declare_queue(
            self.queue_name,
            durable=True,
            arguments={
                "x-max-priority": 10,
                "x-queue-mode": "lazy",
                "x-dead-letter-exchange": "dead_letters"
            }
        )

        return queue
    
    async def process_message(self, message: AbstractIncomingMessage): 
        try:
            async with message.process(requeue=False):
                assert message.reply_to is not None

                data = json.loads(message.body.decode())
                response = await self.callback(data)
                response = response.encode()
                await self.channel.default_exchange.publish(
                    Message(
                        body=response,
                        correlation_id=message.correlation_id,
                    ),
                    routing_key=message.reply_to
                )

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            await self.send_error(message, "Invalid JSON format")
        except Exception as e:
            logger.error(f"Processing error: {e}")
            await self.send_error(message, str(e))


    async def send_error(self, message: AbstractIncomingMessage, error: str) -> None:
        error_response = json.dumps({"error": error})
        await self.channel.default_exchange.publish(
            Message(
                body=error_response.encode(),
                correlation_id=message.correlation_id,
                headers={"error": True},
                delivery_mode=2
            ),
            routing_key=message.reply_to
        )

    async def consume(self, callback: Callable[[dict], Awaitable[str]]) -> None: 
        self.callback = callback 
        queue = await self.set_up_queue()
        try: 
            async with queue.iterator() as queue_iter: 
                async for message in queue_iter: 
                    await self.process_message(message)

        except asyncio.CancelledError:
            logger.info("Consumer task cancelled")
        finally:
            await self.close()

        
async def callback(data: dict) -> str: 
    print(data)
    return json.dumps(data)

class RPSRabbitMQBasePublisher(RabbitMQBase): 
    def __init__(self, amqp_url: str, exchange_name: str, routing_key: str):
        super().__init__(amqp_url, exchange_name)
        self.callback_queue: Optional[AbstractQueue] = None
        self.routing_key = routing_key
        self.futures: MutableMapping[AbstractIncomingMessage, asyncio.Future] = {}

    async def connect(self):
        await super().connect()
        self.callback_queue = await self.channel.declare_queue(
            exclusive=True,
            durable=True,
            arguments={
                "x-max-priority": 10,
                "x-queue-mode": "lazy",
                "x-dead-letter-exchange": "dead_letters"
            })
        await self.callback_queue.consume(self.on_response)
        return self
    
    async def on_response(self, message: AbstractIncomingMessage) -> None: 
        try:
            async with message.process():
                if message.correlation_id is None:
                    raise NoCorrelationIDException(f"Bad message {message!r}")

                future = self.futures.pop(message.correlation_id)
                
                if message.headers.get("error"):
                    future.set_exception(RPCError(message.body.decode()))
                else:
                    future.set_result(message.body.decode())
        except KeyError:
            logger.warning(f"Unknown correlation ID: {message.correlation_id}")

    async def call(self, message: str, timeout: float = 5.0) -> AbstractIncomingMessage:
        correlation_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.futures[correlation_id] = future
        try:
            await self.channel.default_exchange.publish(
                Message(
                    message.encode(),
                    content_type="text/plain",
                    correlation_id=correlation_id,
                    reply_to=self.callback_queue.name,
                ),
                routing_key=self.routing_key,
            )

            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            self.futures.pop(correlation_id, None)
            raise RPCError("Request timed out") from None
        except Exception as e:
            self.futures.pop(correlation_id, None)
            raise RPCError(f"Request failed: {e}") from e
    