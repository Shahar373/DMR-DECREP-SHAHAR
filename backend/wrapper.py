"""Live DSD-FME subprocess wrapper.

Pumps lines from a source (subprocess stderr or a captured log file) through
the parser into a StateManager, optionally firing a callback per event.

Two concrete sources:
  * `stream_subprocess(args, stop_event)` — spawn dsd-fme and read its stderr
  * `stream_file(path, delay, stop_event)` — replay a captured log file

Both are async generators of decoded `str` lines, suitable for passing to
`LineRunner.consume_lines(...)`. The runner is fully testable by feeding it
any async iterator of strings.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Optional

from .models import Event
from .parser import DSDLogParser
from .state import StateManager


class LineRunner:
    """Consume an async stream of log lines, parse, update state, notify."""

    def __init__(
        self,
        state: StateManager,
        on_event: Optional[Callable[[Event], None]] = None,
    ) -> None:
        self.state = state
        self.on_event = on_event
        self.parser = DSDLogParser()
        self._stop = asyncio.Event()

    async def consume_lines(self, line_iter: AsyncIterator[str]) -> None:
        async for line in line_iter:
            if self._stop.is_set():
                break
            ev = self.parser.parse_line(line)
            if ev is None:
                continue
            self.state.apply(ev)
            if self.on_event is not None:
                # Don't let a misbehaving printer take down the loop.
                try:
                    self.on_event(ev)
                except Exception:  # noqa: BLE001 - intentional broad catch
                    pass

    def stop(self) -> None:
        self._stop.set()


async def stream_subprocess(
    args: list[str],
    stop_event: Optional[asyncio.Event] = None,
    env: Optional[dict] = None,
) -> AsyncIterator[str]:
    """Spawn a subprocess and yield its stderr line by line.

    When `stop_event` fires the child is SIGTERM'd; on shutdown we wait up to
    2s before SIGKILL.  Pass ``env`` to override the inherited environment
    (e.g. to set PULSE_SINK for dsd-fme).
    """
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    assert proc.stderr is not None

    async def _terminate_on_stop() -> None:
        if stop_event is None:
            return
        await stop_event.wait()
        if proc.returncode is None:
            proc.terminate()

    watcher = asyncio.create_task(_terminate_on_stop())
    try:
        async for raw in proc.stderr:
            yield raw.decode("utf-8", errors="replace")
    finally:
        watcher.cancel()
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()


async def stream_file(
    path: str,
    delay: float = 0.0,
    stop_event: Optional[asyncio.Event] = None,
) -> AsyncIterator[str]:
    """Yield lines from a captured log. `delay` (seconds) sleeps between
    lines to simulate live timing for end-to-end testing."""
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if stop_event is not None and stop_event.is_set():
                break
            yield line
            if delay > 0:
                await asyncio.sleep(delay)
