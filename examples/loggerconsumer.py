from aiorabbitmq.logging import LogConsumer
from asyncio import sleep, run


async def print_(message):
    print(message)


async def main():
    consumer = LogConsumer("amqp://guest:guest@localhost/", "error_queue", ["info"])
    await consumer.connect()

    await consumer.consume(print_)


if __name__ == "__main__":
    run(main())
