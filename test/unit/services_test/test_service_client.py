import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import json
from aiorabbitmq.RPC.RPSExceptions import RPCError
from aiorabbitmq.services import (
    BaseServiceClient,
    ServiceConnectionError,
    ServiceExecuteError,
)


class TestBaseServiceClient:
    @pytest.fixture
    def client(self):
        return BaseServiceClient(
            amqp_url="amqp://guest:guest@localhost/",
            exchange_name="test_exchange",
            routing_key="test_routing_key",
        )

    @pytest.mark.asyncio
    async def test_initial_state(self, client):
        assert client.amqp_url == "amqp://guest:guest@localhost/"
        assert client.exchange_name == "test_exchange"
        assert client.routing_key == "test_routing_key"
        assert not client.is_connected

    @pytest.mark.asyncio
    async def test_successful_connection(self, client):
        with patch.object(client.RPS, "connect", AsyncMock()) as mock_connect:
            await client.connect()
            mock_connect.assert_awaited_once()
            assert client.is_connected

    @pytest.mark.asyncio
    async def test_failed_connection(self, client):
        with patch.object(
            client.RPS, "connect", AsyncMock(side_effect=Exception("Connection failed"))
        ) as mock_connect:
            with pytest.raises(ServiceConnectionError):
                await client.connect()
            mock_connect.assert_awaited_once()
            assert not client.is_connected

    @pytest.mark.asyncio
    async def test_disconnection(self, client):
        with patch.object(client.RPS, "close", AsyncMock()) as mock_close:
            client._is_connected = True
            await client.disconnect()
            mock_close.assert_awaited_once()
            assert not client.is_connected

    @pytest.mark.asyncio
    async def test_context_manager_success(self, client):
        with patch.object(client, "connect", AsyncMock()) as mock_connect, patch.object(
            client, "disconnect", AsyncMock()
        ) as mock_disconnect:

            async with client as ctx:
                assert ctx is client
                mock_connect.assert_awaited_once()

            mock_disconnect.assert_awaited_once()

    pytest.mark.asyncio

    async def test_context_manager_error(self, client):
        with patch.object(client, "connect", AsyncMock()), patch.object(
            client, "disconnect", AsyncMock()
        ) as mock_disconnect:

            with pytest.raises(ValueError, match="Test error"):
                async with client:
                    raise ValueError("Test error")

            mock_disconnect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_handle_rpc_response_success(self, client):
        @BaseServiceClient.handle_rpc_response
        async def test_method(self, *args, **kwargs):
            return {"test": "data"}

        client._is_connected = True

        with patch.object(
            client.RPS,
            "call",
            AsyncMock(return_value=json.dumps({"result": "success"})),
        ) as mock_call:
            result = await test_method(client)
            assert result == {"result": "success"}
            mock_call.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_handle_rpc_response_not_connected(self, client):
        @BaseServiceClient.handle_rpc_response
        async def test_method(self, *args, **kwargs):
            return {"test": "data"}

        with pytest.raises(ServiceConnectionError):
            await test_method(client)

    @pytest.mark.asyncio
    async def test_handle_rpc_response_rpc_error(self, client):
        @BaseServiceClient.handle_rpc_response
        async def test_method(self, *args, **kwargs):
            return {"test": "data"}

        client._is_connected = True

        with patch.object(
            client.RPS, "call", AsyncMock(side_effect=RPCError("RPC failed"))
        ), pytest.raises(ServiceExecuteError, match="RPC operation failed"):
            await test_method(client)

    @pytest.mark.asyncio
    async def test_handle_rpc_response_invalid_json(slef, client):
        @BaseServiceClient.handle_rpc_response
        async def test_method(self, *args, **kwargs):
            return {"test": "data"}

        client._is_connected = True

        with patch.object(
            client.RPS, "call", AsyncMock(return_value="invalid json")
        ), pytest.raises(ServiceExecuteError, match="Invalid service response format"):
            await test_method(client)

    @pytest.mark.asyncio
    async def test_handle_rpc_response_unexpected_error(self, client):
        @BaseServiceClient.handle_rpc_response
        async def test_method(self, *args, **kwargs):
            return {"test": "data"}

        client._is_connected = True

        with patch.object(
            client.RPS, "call", AsyncMock(side_effect=Exception("Unexpected"))
        ), pytest.raises(ServiceExecuteError, match="Service operation failed"):
            await test_method(client)
