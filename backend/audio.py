"""Audio broadcaster for DMR Cap+ Monitor (Phase 4b).

Reads decoded DMR audio from a PulseAudio null-sink monitor, encodes it
to MP3 with ffmpeg, and fans the chunks out to all connected HTTP
streaming clients.

Setup (once, on the Pi):
    pactl load-module module-null-sink \\
        sink_name=dsd_decoded \\
        sink_properties=device.description=DSD_Decoded_Output

Then launch dsd-fme with PULSE_SINK=dsd_decoded so its decoded voice
audio lands in that sink.  ffmpeg reads from dsd_decoded.monitor.

Usage::

    ab = AudioBroadcaster()
    ab.start()                              # spawns ffmpeg task
    srv.attach_audio(ab)                    # register with FastAPI

    # In a FastAPI route:
    return StreamingResponse(ab.subscribe(), media_type="audio/mpeg")
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import AsyncGenerator, BinaryIO

PULSE_SINK = "dsd_decoded"   # dsd-fme outputs here; ffmpeg reads .monitor
CHUNK_SIZE = 4096


class AudioBroadcaster:
    """Encode audio from a PulseAudio monitor and broadcast MP3 chunks."""

    PULSE_SINK: str = PULSE_SINK

    def __init__(self) -> None:
        self._subscribers: list[asyncio.Queue[bytes | None]] = []
        self._task: asyncio.Task | None = None
        self._recorders: dict[str, BinaryIO] = {}  # rec_id -> open file

    # ── Public API ────────────────────────────────────────────────────

    def start(self) -> None:
        """Schedule the ffmpeg reader as an asyncio task."""
        self._task = asyncio.create_task(self._run())

    def stop(self) -> None:
        """Cancel the ffmpeg task."""
        if self._task is not None:
            self._task.cancel()

    async def subscribe(self) -> AsyncGenerator[bytes, None]:
        """Async generator that yields MP3 chunks until the stream ends."""
        q: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=50)
        self._subscribers.append(q)
        try:
            while True:
                chunk = await q.get()
                if chunk is None:
                    break
                yield chunk
        finally:
            try:
                self._subscribers.remove(q)
            except ValueError:
                pass

    @property
    def listener_count(self) -> int:
        return len(self._subscribers)

    def start_recording(self, rec_id: str, path: Path) -> None:
        """Open ``path`` for binary write; subsequent MP3 chunks are mirrored
        into it until ``stop_recording`` is called."""
        path.parent.mkdir(parents=True, exist_ok=True)
        self._recorders[rec_id] = open(path, "wb")

    def stop_recording(self, rec_id: str) -> None:
        """Close the file opened by ``start_recording``; no-op if unknown."""
        f = self._recorders.pop(rec_id, None)
        if f is not None:
            try:
                f.close()
            except Exception:
                pass

    # ── Internal ──────────────────────────────────────────────────────

    async def _run(self) -> None:
        """Spawn ffmpeg reading from PulseAudio, fan MP3 chunks to subscribers."""
        cmd = [
            "ffmpeg",
            "-loglevel", "warning",
            "-f", "pulse",
            "-i", f"{PULSE_SINK}.monitor",
            "-vn",
            "-codec:a", "libmp3lame",
            "-b:a", "16k",
            "-ac", "1",
            "-f", "mp3",
            "pipe:1",
        ]
        proc: asyncio.subprocess.Process | None = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=None,  # inherit: ffmpeg warnings visible in the CLI log
            )
            assert proc.stdout is not None
            while True:
                chunk = await proc.stdout.read(CHUNK_SIZE)
                if not chunk:
                    break
                dead: list[asyncio.Queue[bytes | None]] = []
                for q in list(self._subscribers):
                    try:
                        q.put_nowait(chunk)
                    except asyncio.QueueFull:
                        dead.append(q)
                for q in dead:
                    try:
                        self._subscribers.remove(q)
                    except ValueError:
                        pass
                # Mirror the chunk into every active per-call recorder.
                for rec_id, f in list(self._recorders.items()):
                    try:
                        f.write(chunk)
                    except Exception:
                        try:
                            f.close()
                        except Exception:
                            pass
                        self._recorders.pop(rec_id, None)
        except asyncio.CancelledError:
            pass
        finally:
            for q in list(self._subscribers):
                try:
                    q.put_nowait(None)
                except asyncio.QueueFull:
                    pass
            for rec_id, f in list(self._recorders.items()):
                try:
                    f.close()
                except Exception:
                    pass
                self._recorders.pop(rec_id, None)
            if proc is not None:
                try:
                    proc.terminate()
                except ProcessLookupError:
                    pass
