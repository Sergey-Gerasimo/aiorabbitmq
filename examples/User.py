from aiorabbitmq.services import BaseServiceClient
import asyncio
import json


class Client(BaseServiceClient):
    @BaseServiceClient.handle_rpc_response
    async def getHi(self):
        print(f"send: hi")
        return json.dumps({"action": "hi", "data": {"some dat": "some Value"}})


async def main():
    async with Client(
        "amqp://guest:guest@localhost", "test.exchange", "test.queue"
    ) as client:
        request = await client.getHi()
        print(request)


if __name__ == "__main__":
    asyncio.run(main())
