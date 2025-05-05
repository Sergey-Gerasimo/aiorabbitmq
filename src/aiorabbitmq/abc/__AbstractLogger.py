from abc import ABC, abstractmethod


class AbstractLogger(ABC):
    @abstractmethod
    async def info(self, message: str) -> None:
        pass

    @abstractmethod
    async def error(self, message: str) -> None:
        pass

    @abstractmethod
    async def warning(self, message: str) -> None:
        pass
