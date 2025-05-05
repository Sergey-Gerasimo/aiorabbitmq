from aiorabbitmq.RPC import RPCConsumer
import asyncio
import json


async def callback_handler(data: dict) -> str:
    print(f"Received message: {data}")
    return json.dumps({"status": "success", "data": data})


async def main():
    consumer = RPCConsumer("amqp://guest:guest@localhost/", "rps_exchane", "rpc_queue")
    await consumer.connect()

    await asyncio.create_task(consumer.consume(callback_handler))


if __name__ == "__main__":
    asyncio.run(main())
