"""Glue: wideband capture → channelizer → per-channel TCP audio (Phase 7).

Each channel gets a localhost TCP server that streams PCM16 audio; the
matching dsd-fme connects with ``-i tcp:127.0.0.1:<port>``. A capture
pump reads IQ blocks, channelizes them, and feeds each channel's server.

The pump touches the SDR and runs the blocking SoapySDR read loop on a
worker thread, so the whole thing is a hardware path (``pragma: no
cover``). ``AudioTcpServer`` itself is plain asyncio and is unit-tested
with a local client — that's the piece most likely to regress.
"""
from __future__ import annotations

import asyncio
from typing import Optional


class AudioTcpServer:
    """Streams pushed PCM bytes to whatever client is connected.

    dsd-fme is the single expected consumer per channel. If no client is
    connected, pushed audio is dropped (there's nothing to decode). Slow
    clients are handled by asyncio's write buffering; we don't queue
    unbounded — a decoder that can't keep up would fall behind live audio
    regardless.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self.host = host
        self.port = port
        self._server: Optional[asyncio.AbstractServer] = None
        self._writers: set[asyncio.StreamWriter] = set()

    async def start(self) -> int:
        self._server = await asyncio.start_server(
            self._on_client, self.host, self.port,
        )
        # Resolve the actual port when port=0 was requested.
        self.port = self._server.sockets[0].getsockname()[1]
        return self.port

    async def _on_client(self, reader: asyncio.StreamReader,
                         writer: asyncio.StreamWriter) -> None:
        self._writers.add(writer)
        try:
            # dsd-fme only reads; wait until it disconnects.
            await reader.read()
        except Exception:  # noqa: BLE001
            pass
        finally:
            self._writers.discard(writer)
            try:
                writer.close()
            except Exception:  # noqa: BLE001
                pass

    def client_count(self) -> int:
        return len(self._writers)

    def feed(self, pcm: bytes) -> None:
        """Push PCM bytes to all connected clients (best-effort)."""
        dead = []
        for w in self._writers:
            try:
                w.write(pcm)
            except Exception:  # noqa: BLE001
                dead.append(w)
        for w in dead:
            self._writers.discard(w)

    async def close(self) -> None:
        for w in list(self._writers):
            try:
                w.close()
            except Exception:  # noqa: BLE001
                pass
        self._writers.clear()
        if self._server is not None:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:  # noqa: BLE001
                pass


async def run_capture_pump(  # pragma: no cover - needs an RSP + SoapySDR
    capture,
    channelizer,
    servers: dict,
    stop_event: asyncio.Event,
    active_labels_fn=None,
) -> None:
    """Pump capture → channelizer → per-channel TCP servers until stopped.

    The blocking SoapySDR read loop runs on a worker thread; each IQ block
    is channelized and fed to the matching server. ``active_labels_fn``
    (Phase 8) optionally returns the set of labels that currently deserve
    a decoder, so idle channels are skipped to save CPU.
    """
    from .channelizer import to_pcm16

    capture.open()
    blocks = iter(capture.blocks())
    try:
        while not stop_event.is_set():
            block = await asyncio.to_thread(next, blocks, None)
            if block is None:
                break
            audio = channelizer.process(block)
            active = active_labels_fn() if active_labels_fn else None
            for label, samples in audio.items():
                if active is not None and label not in active:
                    continue
                srv = servers.get(label)
                if srv is not None and srv.client_count():
                    srv.feed(to_pcm16(samples))
    finally:
        capture.close()
