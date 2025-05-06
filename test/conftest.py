import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from aio_pika import RobustConnection


@pytest.fixture(scope="session")
def event_loop_policy():
    return asyncio.DefaultEventLoopPolicy()


# Common configuration for all tests
def pytest_configure(config):
    config.addinivalue_line("markers", "asyncio: mark test as coroutine")


@pytest.fixture
def capsys(request):
    return request.getfixturevalue("capsys")
