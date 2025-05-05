from .__RabbitMQBase import RabbitMQBase
from abc import ABC, abstractmethod
from typing import Any


class RabbitMQPublisherBase(ABC, RabbitMQBase):
    @abstractmethod
    async def send_message(self, message: str) -> Any:
        pass
