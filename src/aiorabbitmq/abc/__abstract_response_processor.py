from abc import abstractmethod, ABC
from typing import Callable, Awaitable, Any, Dict


class AbstractResponseProcessor(ABC):
    """Abstract base class defining the microservice response processor interface.

    Provides core functionality blueprint for:
    - Asynchronous action handler registration
    - Message validation and routing
    - Standardized response formatting
    - Handler lifecycle management

    Subclasses must implement:
    - add(): Decorator-based handler registration
    - handle_messages(): Main message processing logic

    Typical usage pattern:
    1. Create subclass implementing abstract methods
    2. Register handlers using @add() decorator
    3. Process messages through handle_messages()

    Example:
    class MyProcessor(AbstractResponseProcessor):
        ...implementation...
    """

    @abstractmethod
    def add(
        self, action: str
    ) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
        """Abstract method for handler registration decorator."""
        pass

    @abstractmethod
    def start(
        self, func: Callable[..., Awaitable[None]]
    ) -> Callable[..., Awaitable[None]]:
        "Abstract method for start registration decorator"
        pass

    @abstractmethod
    def stop(
        self, func: Callable[..., Awaitable[None]]
    ) -> Callable[..., Awaitable[None]]:
        "Abstract method for stop registration decorator"
        pass

    @abstractmethod
    async def handle_messages(self, data: Dict[str, Any]) -> str:
        """Abstract method for message processing."""
        pass

    @abstractmethod
    async def run(self) -> None:
        """Abstract mehtod for start ResponseProcessor implimintation"""
        pass


__all__ = [
    "AbstractResponseProcessor",
]
