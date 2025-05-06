from aiorabbitmq.services import BaseResponseProcessor
import asyncio
import json

server = BaseResponseProcessor(
    "amqp://guest:guest@localhost", "test.exchange", "test.queue"
)


@server.add("hi")
async def say_hi(data: dict):
    print(f"got: {data}")
    return {"processed": "Hello"}


async def main():
    asyncio.create_task(server.run())
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
