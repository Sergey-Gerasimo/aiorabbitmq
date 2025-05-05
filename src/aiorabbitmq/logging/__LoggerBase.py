from .__Loggers import LogProducer
from ..abc.__AbstractLogger import AbstractLogger
from typing import Optional

from aiorabbitmq.__settings import logger


class Logger(AbstractLogger):
    def __init__(self):
        self.logpruduser = Optional[LogProducer] = None

    async def connect(self, amqp_url: str):
        self.logpruduser = LogProducer(amqp_url)
        await self.logpruduser.connect()

        return self

    async def info(self, message: str) -> None:
        await self.logpruduser.create_message("info", message)

    async def error(self, message: str) -> None:
        await self.logpruduser.create_message("error", message)

    async def warning(self, message):
        await self.logpruduser("warninig", message)
