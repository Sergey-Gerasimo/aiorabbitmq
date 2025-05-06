from typing import Callable, Awaitable, Any, Dict, List, Union
from functools import wraps
import json
import asyncio
import signal
import logging
from aiorabbitmq.abc import AbstractResponseProcessor
from aiorabbitmq.RPC.__RPSRabbitMQBase import RPSRabbitMQBaseConsumer as RPCConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("processor.log")],
)
logger = logging.getLogger("aiorabbitmq::BaseResponseProcessor")


class ResponseProcessorError(Exception):
    """Base exception class for all processor-related errors."""

    def __init__(self, message: str):
        super().__init__(message)
        logger.error(f"ResponseProcessorError: {message}")


class BaseResponseProcessor(AbstractResponseProcessor):
    """RabbitMQ message processor with decorator-based handler registration."""

    def __init__(self, amqp_url: str, exchange_name: str, queue_name: str) -> None:
        logger.info(
            f"Initializing processor for exchange: {exchange_name}, queue: {queue_name}"
        )
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
        logger.info(f"Registering startup handler: {func.__name__}")
        if self._on_start is not None:
            logger.error("Startup handler already registered")
            raise ResponseProcessorError("Shutdown handler already registered")

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            logger.debug(f"Executing startup handler: {func.__name__}")
            return await func(*args, **kwargs)

        self._on_start = wrapper
        return wrapper

    def stop(
        self, func: Callable[..., Awaitable[None]]
    ) -> Callable[..., Awaitable[None]]:
        logger.info(f"Registering shutdown handler: {func.__name__}")
        if self._on_stop is not None:
            logger.error("Shutdown handler already registered")
            raise ResponseProcessorError("Shutdown handler already registered")

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            logger.debug(f"Executing shutdown handler: {func.__name__}")
            return await func(*args, **kwargs)

        self._on_stop = wrapper
        return wrapper

    async def run(
        self, max_connection_retries: int = 3, retry_delay: float = 5.0
    ) -> None:
        logger.info(f"Starting processor with {max_connection_retries} max retries")
        loop = asyncio.get_running_loop()
        self._should_stop = asyncio.Event()
        self._consume_task = None

        retry_count = 0
        while retry_count < max_connection_retries:
            try:
                logger.debug(
                    f"Connection attempt {retry_count + 1}/{max_connection_retries}"
                )
                await self._rps.connect()
                logger.info("Successfully connected to RabbitMQ")
                break
            except Exception as e:
                retry_count += 1
                logger.warning(f"Connection attempt {retry_count} failed: {str(e)}")
                if retry_count >= max_connection_retries:
                    error_msg = (
                        f"Connection failed after {max_connection_retries} attempts"
                    )
                    logger.error(error_msg)
                    raise ResponseProcessorError(f"{error_msg}: {str(e)}")
                await asyncio.sleep(retry_delay * retry_count)

        def signal_handler():
            if not self._should_stop.is_set():
                logger.info("Signal received, initiating shutdown")
                asyncio.run_coroutine_threadsafe(self._shutdown(), loop)

        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    loop.add_signal_handler(sig, signal_handler)
                    logger.debug(f"Registered signal handler for {sig}")
                except NotImplementedError:
                    signal.signal(sig, lambda s, f: signal_handler())
                    logger.debug(f"Used fallback signal handler for {sig}")

            if self._on_start:
                logger.info("Executing startup handler")
                await self._on_start()

            logger.info("Starting message consumer")
            self._consume_task = loop.create_task(
                self._rps.consume(self.handle_messages)
            )
            logger.info("Processor is now running")
            await self._should_stop.wait()
            logger.info("Shutdown signal processed")

        except asyncio.CancelledError:
            logger.warning("Processor task was cancelled")
        except Exception as e:
            logger.error(f"Processor runtime error: {str(e)}")
            raise ResponseProcessorError(f"Service runtime error: {str(e)}")
        finally:
            if not self._should_stop.is_set():
                logger.info("Initiating emergency shutdown")
                await self._shutdown()

            if self._on_stop:
                logger.info("Executing shutdown handler")
                try:
                    await self._on_stop()
                except Exception as e:
                    logger.error(f"Shutdown handler failed: {str(e)}")

            logger.info("Processor shutdown complete")

    async def _shutdown(self) -> None:
        logger.info("Starting graceful shutdown")
        if hasattr(self, "_consume_task"):
            logger.debug("Cancelling consume task")
            self._consume_task.cancel()
            try:
                await self._consume_task
                logger.debug("Consume task cancelled successfully")
            except asyncio.CancelledError:
                logger.debug("Consume task already cancelled")
            except Exception as e:
                logger.error(f"Error cancelling consume task: {str(e)}")

        try:
            logger.debug("Closing RabbitMQ connection")
            await self._rps.close()
            logger.info("RabbitMQ connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {str(e)}")
            raise
        finally:
            self._should_stop.set()
            logger.debug("Shutdown flag set")

    def add(
        self, action: str
    ) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
        logger.info(f"Registering handler for action: {action}")
        if action in self._handlers:
            logger.error(f"Handler already exists for action: {action}")
            raise ResponseProcessorError(f'Action "{action}" already registered')

        def decorator(
            func: Callable[..., Awaitable[Any]],
        ) -> Callable[..., Awaitable[Any]]:
            @wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                logger.debug(f"Executing handler for action: {action}")
                try:
                    result = await func(*args, **kwargs)
                    logger.debug(f"Handler for {action} executed successfully")
                    return result
                except Exception as e:
                    logger.error(f"Handler for {action} failed: {str(e)}")
                    raise

            self._handlers[action] = wrapper
            logger.info(f"Handler registered for action: {action}")
            return wrapper

        return decorator

    async def handle_messages(self, data: Dict[str, Any]) -> str:
        logger.debug(f"Received message: {data}")
        try:
            required_fields = {"action", "data"}
            if not required_fields.issubset(data.keys()):
                missing = required_fields - data.keys()
                error_msg = f"Missing required fields: {missing}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            action = data["action"]
            logger.debug(f"Processing action: {action}")

            handler = self.get_handler(action)
            result = await handler(data["data"])
            logger.debug(f"Action {action} processed successfully")

            response = {
                "status": "success",
                "response": result,
                "user_id": data.get("user_id"),
            }
            logger.debug(f"Returning response: {response}")
            return json.dumps(response)

        except (KeyError, ResponseProcessorError) as e:
            error_msg = f"Processing error for action {data.get('action')}: {str(e)}"
            logger.error(error_msg)
            return json.dumps(
                {
                    "status": "error",
                    "message": error_msg,
                    "user_id": data.get("user_id", "unknown"),
                }
            )

    def get_handler(self, action: str) -> Callable[..., Awaitable[Any]]:
        logger.debug(f"Looking up handler for action: {action}")
        if action not in self._handlers:
            logger.error(f"No handler found for action: {action}")
            raise ResponseProcessorError(f'No handler registered for: "{action}"')
        return self._handlers[action]

    @property
    def registered_actions(self) -> List[str]:
        actions = list(self._handlers.keys())
        logger.debug(f"Registered actions: {actions}")
        return actions


__all__ = ["ResponseProcessorError", "BaseResponseProcessor"]
