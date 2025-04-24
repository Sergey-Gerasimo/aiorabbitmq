import asyncio
import uuid
from typing import MutableMapping
from aiorabbitmq.__settings import logger 

from aio_pika import Message, connect

from aio_pika.abc import (
    AbstractChannel, AbstractConnection, AbstractIncomingMessage, AbstractQueue,
)


class RPCClient:
    connection: AbstractConnection
    channel: AbstractChannel
    callback_queue: AbstractQueue


    def __init__(self) -> None:
        self.futures: MutableMapping[AbstractIncomingMessage, asyncio.Future] = {}


    async def connect(self, amqp_url:str, routing_key:str) -> "RPCClient":
        self.connection = await connect(amqp_url)
        self.channel = await self.connection.channel()
        self.callback_queue = await self.channel.declare_queue(exclusive=True)
        self.routing_key = routing_key
        await self.callback_queue.consume(self.on_response, no_ack=True)

        return self


    async def on_response(self, message: AbstractIncomingMessage) -> None:
        if message.correlation_id is None:
            logger.error(f"Bad message {message!r}")
            return

        future: asyncio.Future = self.futures.pop(message.correlation_id)
        future.set_result(message)


    async def call(self, message: str) -> AbstractIncomingMessage:
        correlation_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        self.futures[correlation_id] = future

        await self.channel.default_exchange.publish(
            Message(
                message.encode(),
                content_type="text/plain",
                correlation_id=correlation_id,
                reply_to=self.callback_queue.name,
            ),
            routing_key=self.routing_key,
        )

        return await future


async def main() -> None:
    fibonacci_rpc = await RPCClient().connect("amqp://guest:guest@localhost/", "rpc_queue")
    response = await fibonacci_rpc.call(str(30))
    print(response.body)


if __name__ == "__main__":
    asyncio.run(main())