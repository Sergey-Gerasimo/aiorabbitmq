from aiorabbitmq.RPS import RPSConsumer
import asyncio 
import json

async def main(): 
    publisher = RPSConsumer("amqp://guest:guest@localhost/", "rps_exchane", "rpc_queue")
    await publisher.connect()

    for _ in range(3): 
        answer = await publisher.call(json.dumps({"message": "hello", "code": 1}))
        print(answer)
        await asyncio.sleep(1)
    

if __name__ == "__main__": 
    asyncio.run(main())