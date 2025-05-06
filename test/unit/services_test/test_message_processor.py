import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio
import json
import signal
from aiorabbitmq.services import BaseResponseProcessor, ResponseProcessorError


class TestResponseProcessor:
    @pytest.fixture
    def processor(self):
        return BaseResponseProcessor(
            amqp_url="amqp://guest:guest@localhost/",
            exchange_name="test_exchange",
            queue_name="test_queue",
        )

    @pytest.mark.asyncio
    async def test_handler_registration(self, processor: BaseResponseProcessor):

        @processor.add("test.action")
        async def handler(data):
            return {"processed": data}

        assert "test.action" in processor.registered_actions
        assert await processor.get_handler("test.action")({"key": "value"}) == {
            "processed": {"key": "value"}
        }

    @pytest.mark.asyncio
    async def test_duplicate_handler(self, processor):
        @processor.add("duplicate.action")
        async def first_handler(data):
            pass

        with pytest.raises(ResponseProcessorError):

            @processor.add("duplicate.action")
            async def second_handler(data):
                pass

    @pytest.mark.asyncio
    async def test_missing_handler(self, processor: BaseResponseProcessor):
        with pytest.raises(ResponseProcessorError):
            processor.get_handler("nonexistent.action")

    @pytest.mark.asyncio
    async def test_invalid_message(self, processor):
        invalid_message = {"wrong": "structure"}

        with pytest.raises(ValueError) as info_msg:
            response = await processor.handle_messages(invalid_message)
            response_data = json.loads(response)
            assert response_data["status"] == "error"
            assert "Missing required fields" in response_data["message"]
            assert str(info_msg.value) == "Missing required fields: {'data', 'action'}"
