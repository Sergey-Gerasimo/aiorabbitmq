from .__RabbitMQBase import RabbitMQBase
from abc import ABC, abstractmethod
from typing import Any

class RabbitMQConsumerBase(ABC, RabbitMQBase):
    @abstractmethod
    async def consume(self): pass 