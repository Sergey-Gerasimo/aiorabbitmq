from typing import Callable, Awaitable, Any, Dict, List, Union
from functools import wraps
import json
import asyncio
import signal
from aiorabbitmq.abc import AbstractResponseProcessor
from aiorabbitmq.RPC import RPCConsumer


class ResponseProcessorError(Exception):
    """Base exception class for all processor-related errors.

    Common cases:
    - Handler registration conflicts
    - Missing required message fields
    - AMQP communication failures
    - Invalid message formats
    """

    pass


class BaseResponseProcessor(AbstractResponseProcessor):
    """RabbitMQ message processor with decorator-based handler registration.

    Implements complete message processing pipeline:
    - AMQP connection management
    - Handler registration via decorators
    - Message validation and routing
    - Standardized JSON response formatting
    - Graceful shutdown handling

    Args:
        amqp_url: RabbitMQ connection URL (e.g. 'amqp://user:pass@host:port/vhost')
        exchange_name: AMQP exchange name for message routing
        queue_name: Consumer queue name

    Example:
    >>> processor = BaseResponseProcessor(
    ...     amqp_url='amqp://localhost',
    ...     exchange_name='service_exchange',
    ...     queue_name='service_queue'
    ... )
    >>>
    >>> @processor.add("user.create")
    ... async def create_user(data: dict) -> dict:
    ...     # Business logic here
    ...     return {"id": 123, "name": data["name"]}
    >>>
    >>> asyncio.run(processor.run())
    """

    def __init__(self, amqp_url: str, exchange_name: str, queue_name: str) -> None:
        """Initialize message processor with RabbitMQ connection details.

        Sets up:
        - Internal handler registry
        - AMQP consumer instance
        - Shutdown control flag
        """

        self._handlers: Dict[str, Callable[..., Awaitable[Any]]] = {}
        self._on_start: Union[Callable[..., Awaitable[None]], None] = None
        self._on_stop: Union[Callable[..., Awaitable[None]], None] = None
        self._should_stop = asyncio.Event()

        self._amqp_url: str = amqp_url
        self._exchange_name = exchange_name
        self._queue_name = queue_name

        self._rps = RPCConsumer(self._amqp_url, self._exchange_name, self._queue_name)

    def start(
        self, func: Callable[..., Awaitable[None]]
    ) -> Callable[..., Awaitable[None]]:
        """Register service startup handler via decorator.

        The decorated function will execute before message processing begins.

        Example:
        @processor.start
        async def initialize():
            await setup_database()
            await cache.warm_up()
        """
        if self._on_stop is not None:
            raise ResponseProcessorError("Shutdown handler already registered")

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            """Handler wrapper enabling future functionality extensions."""
            return await func(*args, **kwargs)

        self._on_stop = wrapper
        return wrapper

    def stop(
        self, func: Callable[..., Awaitable[None]]
    ) -> Callable[..., Awaitable[None]]:
        """Register service shutdown handler via decorator.

        The decorated function will execute during graceful shutdown.

        Example:
        @processor.stop
        async def cleanup():
            await release_resources()
            await send_metrics()
        """
        if self._on_stop is not None:
            raise ResponseProcessorError("Shutdown handler already registered")

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            """Handler wrapper enabling future functionality extensions."""
            return await func(*args, **kwargs)

        self._on_stop = wrapper
        return wrapper

    async def run(self):
        """Start message processing loop with graceful shutdown support.

        Execution flow:
        1. Connect to AMQP
        2. Register signal handlers (SIGTERM/SIGINT)
        3. Execute startup handler
        4. Begin message consumption
        5. Wait for shutdown signal
        6. Execute shutdown handler

        Handles:
        - Docker SIGTERM
        - KeyboardInterrupt (SIGINT)
        - AMQP connection errors
        - Unexpected exceptions during processing

        Note:
        Should be run via asyncio.run() or equivalent.
        """
        try:
            await self._rps.connect()

            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self._shutdown)

            if self._on_start:
                await self._on_start()

            await asyncio.create_task(self._rps.consume(self.handle_messages))
            await self._should_stop.wait()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._shutdown()
            raise ResponseProcessorError(f"Service failed: {str(e)}")
        finally:
            if not self._should_stop.is_set():
                await self._shutdown()
            if self._on_stop:
                await self._on_stop()

    async def _shutdown(self) -> None:
        """Internal graceful shutdown procedure.

        1. Close AMQP connection
        2. Cancel pending tasks
        3. Set shutdown flag
        """

        await self._rps.close()

        current = asyncio.current_task()
        for task in (t for t in asyncio.all_tasks() if t is not current):
            task.cancel()

        await asyncio.gather(*asyncio.all_tasks(), return_exceptions=True)
        self._should_stop.set()

    def add(
        self, action: str
    ) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
        """Register action handler through decorator syntax.

        Args:
            action: Unique action identifier string. Must follow
                   [service]_[entity]_[operation] naming pattern

        Returns:
            Decorator function that registers the handler

        Raises:
            ResponseProcessorError: If action identifier already registered

        Example:
        @processor.add("email_send")
        async def handle_email(data: dict) -> dict:
            ...logic...
        """
        if action in self._handlers:
            raise ResponseProcessorError(f'Action "{action}" already registered')

        def decorator(
            func: Callable[..., Awaitable[Any]],
        ) -> Callable[..., Awaitable[Any]]:
            """Inner decorator performing actual registration.

            Args:
                func: Async handler function with signature:
                    async def(data: Dict) -> Any

            Returns:
                Original function with registration side-effect

            Preserves:
                Original function metadata using functools.wraps
            """

            @wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                """Handler wrapper enabling future functionality extensions."""
                return await func(*args, **kwargs)

            self._handlers[action] = wrapper
            return wrapper

        return decorator

    async def handle_messages(self, data: Dict[str, Any]) -> str:
        """Process incoming message and generate standardized response.

        Args:
            data: Input message dictionary. Required structure:
                {
                    "action": "registered_action_name",
                    "data": {...},  # Handler-specific payload
                    "user_id": "optional_identifier"  # Tracking field
                }

        Returns:
            JSON string with standardized response format:
            {
                "status": "success"|"error",
                "response": ...,  # Handler output (success case)
                "message": "...",  # Error description (error case)
                "user_id": "..."  # Original request identifier
            }

        Raises:
            ValueError: If required fields (action/data) are missing

        Example:
        >>> message = {'action': 'calc', 'data': {'x': 2, 'y': 3}}
        >>> response = await processor.handle_messages(message)
        """
        try:
            # Validate message structure
            required_fields = {"action", "data"}
            if not required_fields.issubset(data.keys()):
                missing = required_fields - data.keys()
                raise ValueError(f"Missing required fields: {missing}")

            # Execute handler
            handler = self.get_handler(data["action"])
            result = await handler(data["data"])

            # Format success response
            return json.dumps(
                {
                    "status": "success",
                    "response": result,
                    "user_id": data.get("user_id"),
                }
            )

        except (KeyError, ResponseProcessorError) as e:
            # Format error response
            return json.dumps(
                {
                    "status": "error",
                    "message": f"Processing error: {str(e)}",
                    "user_id": data.get("user_id", "unknown"),
                }
            )

    def get_handler(self, action: str) -> Callable[..., Awaitable[Any]]:
        """Retrieve registered handler by action identifier.

        Args:
            action: String identifier of registered action

        Returns:
            Async handler function associated with action

        Raises:
            ResponseProcessorError: If handler not found

        Example:
        >>> handler = processor.get_handler("data_export")
        >>> await handler({"format": "csv"})
        """
        if action not in self._handlers:
            raise ResponseProcessorError(f'No handler registered for: "{action}"')
        return self._handlers[action]

    @property
    def registered_actions(self) -> List[str]:
        """Get list of registered action identifiers.

        Returns:
            List of action strings in insertion order

        Example:
        >>> processor.registered_actions
        ['user_create', 'email_verify']
        """
        return list(self._handlers.keys())


__all__ = ["ResponseProcessorError", "BaseResponseProcessor"]
