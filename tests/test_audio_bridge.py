"""Phase-7 tests: the per-channel TCP audio server (no SDR)."""
from __future__ import annotations

import asyncio

from backend.rf.bridge import AudioTcpServer


def test_audio_server_streams_pushed_bytes_to_client():
    async def scenario():
        srv = AudioTcpServer(port=0)
        port = await srv.start()
        assert srv.client_count() == 0

        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        # Let the server register the client.
        await asyncio.sleep(0.05)
        assert srv.client_count() == 1

        payload = b"\x01\x02\x03\x04" * 16
        srv.feed(payload)
        await writer.drain()
        got = await asyncio.wait_for(reader.readexactly(len(payload)), timeout=2.0)
        assert got == payload

        writer.close()
        await srv.close()
        return True

    assert asyncio.run(scenario())


def test_audio_server_feed_with_no_client_is_noop():
    async def scenario():
        srv = AudioTcpServer(port=0)
        await srv.start()
        srv.feed(b"\x00\x01")  # nobody connected — must not raise
        await srv.close()
        return True

    assert asyncio.run(scenario())
