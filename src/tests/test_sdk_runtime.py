import asyncio
import runpy
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

RUNTIME = Path(__file__).resolve().parents[2] / "deploy/local/sdk-runtime.py"


def test_local_sdk_relay_transfers_data_and_eof_in_both_directions(monkeypatch):
    relay = runpy.run_path(str(RUNTIME))["relay"]

    async def round_trip():
        downstream = asyncio.StreamReader()
        downstream.feed_data(b"sdk")
        downstream.feed_eof()
        upstream = asyncio.StreamReader()
        upstream.feed_data(b"airflow")
        upstream.feed_eof()
        downstream_writer = Mock(drain=AsyncMock())
        upstream_writer = Mock(drain=AsyncMock())
        open_connection = AsyncMock(return_value=(upstream, upstream_writer))
        monkeypatch.setattr(asyncio, "open_connection", open_connection)

        await relay(downstream, downstream_writer, host="127.0.0.1", port=1234)

        open_connection.assert_awaited_once_with("127.0.0.1", 1234)
        upstream_writer.write.assert_called_once_with(b"sdk")
        downstream_writer.write.assert_called_once_with(b"airflow")
        for writer in upstream_writer, downstream_writer:
            writer.write_eof.assert_called_once()
            writer.close.assert_called_once()

    asyncio.run(round_trip())


def test_local_sdk_relay_closes_client_if_supervisor_is_unavailable(monkeypatch):
    relay = runpy.run_path(str(RUNTIME))["relay"]
    monkeypatch.setattr(asyncio, "open_connection", AsyncMock(side_effect=ConnectionRefusedError))
    writer = Mock()
    with pytest.raises(ConnectionRefusedError):
        asyncio.run(relay(Mock(), writer, host="127.0.0.1", port=1234))
    writer.close.assert_called_once()
