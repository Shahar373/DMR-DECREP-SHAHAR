"""Run N per-channel dsd-fme decoders into one event pipeline (Phase 7).

The RSP1B is a single-client device, so we can't run N dsd-fme instances
each opening the SDR. Instead one wideband capture is channelized in
software (``channelizer``) and each narrowband channel is served as PCM
audio over a localhost TCP port; each dsd-fme connects to its channel
with ``-i tcp:127.0.0.1:<port>``. Every instance gets its own
``LineRunner`` (own parser, own channel tag) writing into the *shared*
``StateManager`` + ``EventLog`` — safe because they all cooperate on one
asyncio loop.

This module is the orchestration only; it takes a ``source_factory`` so
tests can feed captured logs instead of spawning real decoders.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Optional

from ..channel_plan import Channel, ChannelPlan
from ..event_log import EventLog
from ..state import StateManager
from ..wrapper import LineRunner, stream_subprocess_with_retry

# A factory: channel -> async iterator of dsd-fme stderr lines.
SourceFactory = Callable[[Channel], AsyncIterator[str]]


def build_channel_command(
    channel: Channel,
    dsd_bin: str,
    calls_dir: Path,
    host: str,
    port: int,
) -> list[str]:
    """dsd-fme command for one channelized audio stream over TCP.

    Each channel records into its own subdir so per-call WAV filenames
    from parallel decoders can't collide. ``-7 <dir>`` precedes ``-P``.
    """
    ch_dir = Path(calls_dir) / channel.label
    return [
        dsd_bin, "-fs",
        "-i", f"tcp:{host}:{port}",
        "-7", str(ch_dir),
        "-P",
    ]


def default_source_factory(
    plan: ChannelPlan,
    dsd_bin: str,
    calls_dir: Path,
    stop_event: asyncio.Event,
    host: str = "127.0.0.1",
    base_port: int = 7355,
    liveness_timeout: Optional[float] = 60.0,
) -> SourceFactory:
    """Spawn one auto-restarting dsd-fme per channel, reading channelized
    audio from ``base_port + index``."""
    port_of = {ch.label: base_port + i for i, ch in enumerate(plan.channels)}

    def factory(channel: Channel) -> AsyncIterator[str]:
        (Path(calls_dir) / channel.label).mkdir(parents=True, exist_ok=True)
        cmd = build_channel_command(
            channel, dsd_bin, calls_dir, host, port_of[channel.label],
        )
        return stream_subprocess_with_retry(
            cmd, stop_event=stop_event, liveness_timeout=liveness_timeout,
        )

    return factory


async def run_multichannel(
    plan: ChannelPlan,
    state: StateManager,
    source_factory: SourceFactory,
    event_log: Optional[EventLog] = None,
    on_event=None,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    """Run one LineRunner per channel concurrently until all sources end
    (or ``stop_event`` fires). Events from every channel are stamped with
    that channel and merged into the shared state/log."""
    runners: list[LineRunner] = []
    tasks: list[asyncio.Task] = []
    for channel in plan.channels:
        runner = LineRunner(
            state, on_event=on_event, event_log=event_log, channel=channel,
        )
        runners.append(runner)
        tasks.append(asyncio.create_task(
            runner.consume_lines(source_factory(channel))
        ))

    async def _watch_stop() -> None:
        if stop_event is None:
            return
        await stop_event.wait()
        for r in runners:
            r.stop()

    watcher = asyncio.create_task(_watch_stop())
    try:
        await asyncio.gather(*tasks)
    finally:
        watcher.cancel()
        try:
            await watcher
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
