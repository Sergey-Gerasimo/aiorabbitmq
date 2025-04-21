from aiorabbitmq.logging import LogProducer
from asyncio import sleep, run


async def main(): 
    consumer = LogProducer("amqp://guest:guest@localhost/")
    await consumer.connect()

    while 1: 
        print("send log")
        await consumer.send_log("error", "hello, from me")
        await sleep(1)


if __name__ == "__main__": 
    run(main())
    