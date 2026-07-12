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
import sys
from collections.abc import AsyncIterator, Callable
from typing import Optional, Union

from .event_log import EventLog
from .models import Event
from .parser import DSDLogParser
from .state import StateManager


class LineRunner:
    """Consume an async stream of log lines, parse, update state, notify."""

    def __init__(
        self,
        state: StateManager,
        on_event: Optional[Callable[[Event], None]] = None,
        event_log: Optional[EventLog] = None,
        channel=None,
    ) -> None:
        self.state = state
        self.on_event = on_event
        self.event_log = event_log
        self.parser = DSDLogParser()
        self._stop = asyncio.Event()
        # Optional channel_plan.Channel — when set, every parsed event is
        # stamped with this channel's label + frequency (multi-frequency
        # mode). None → single-channel, events carry no channel tag.
        self.channel = channel

    async def consume_lines(self, line_iter: AsyncIterator[str]) -> None:
        async for line in line_iter:
            if self._stop.is_set():
                break
            ev = self.parser.parse_line(line)
            if ev is None:
                continue
            if self.channel is not None:
                ev.channel_label = self.channel.label
                ev.frequency = self.channel.frequency_hz
            self.state.apply(ev)
            if self.event_log is not None:
                self.event_log.append(ev)
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
    liveness_timeout: Optional[float] = None,
    interrupt_event: Optional[asyncio.Event] = None,
) -> AsyncIterator[str]:
    """Spawn a subprocess and yield its stderr line by line.

    When `stop_event` fires the child is SIGTERM'd; on shutdown we wait up
    to 2 s before SIGKILL.  Pass ``env`` to override the inherited
    environment (e.g. to set PULSE_SINK for dsd-fme).

    When ``liveness_timeout`` is set, the subprocess is killed and this
    generator returns if no stderr line arrives for that many seconds in a
    row. dsd-fme normally emits a sync line every ~60 ms, so a 60 s silence
    almost always means a stuck child (audio/RF source dropped: PulseAudio
    sink gone, SoapySDR / SDRplay API service hiccup, USB power dipped) —
    systemd ``Restart=on-failure`` then brings us back. ``None`` disables
    the watchdog (the default, so existing callers don't change behaviour).

    ``interrupt_event`` (0.26.0) terminates the child exactly like
    ``stop_event`` but is a distinct signal — it's how live SDR retuning
    (a changed frequency/gain/etc.) asks the current child to exit so
    ``stream_subprocess_with_retry`` can respawn it with fresh tuning,
    without treating the whole stream as finished the way ``stop_event``
    does. This generator itself doesn't care which fired; it's the
    caller's job to tell them apart afterward via ``.is_set()``.
    """
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    if proc.stderr is None:
        raise RuntimeError("failed to capture subprocess stderr")

    async def _terminate_on_stop() -> None:
        waits = [e.wait() for e in (stop_event, interrupt_event) if e is not None]
        if not waits:
            return
        tasks = [asyncio.ensure_future(w) for w in waits]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
        if proc.returncode is None:
            proc.terminate()

    watcher = asyncio.create_task(_terminate_on_stop())
    timed_out = False
    try:
        while True:
            try:
                if liveness_timeout is not None:
                    raw = await asyncio.wait_for(
                        proc.stderr.readline(), timeout=liveness_timeout
                    )
                else:
                    raw = await proc.stderr.readline()
            except asyncio.TimeoutError:
                timed_out = True
                print(
                    f"# liveness: no subprocess output for {liveness_timeout}s "
                    "— terminating child so systemd can restart us",
                    file=sys.stderr,
                )
                break
            if not raw:
                break  # EOF
            yield raw.decode("utf-8", errors="replace")
    finally:
        watcher.cancel()
        # Await the cancellation so asyncio doesn't emit
        # "Task was destroyed but it is pending" on shutdown.
        try:
            await watcher
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
        if timed_out:
            # Non-zero exit so systemd's Restart=on-failure kicks in.
            raise RuntimeError(
                f"subprocess liveness timeout ({liveness_timeout}s) — no output"
            )


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


async def stream_subprocess_with_retry(
    args: Union[list[str], Callable[[], list[str]]],
    stop_event: Optional[asyncio.Event] = None,
    env: Optional[dict] = None,
    liveness_timeout: Optional[float] = None,
    backoff_seconds: float = 2.0,
    interrupt_event: Optional[asyncio.Event] = None,
    retune_backoff_seconds: float = 0.2,
) -> AsyncIterator[str]:
    """Like ``stream_subprocess`` but respawns the child instead of
    bubbling the timeout to the top of the process.

    ``stream_subprocess`` raises ``RuntimeError`` when the liveness
    watchdog fires (and just returns on EOF). Both paths would otherwise
    propagate out of ``LineRunner.consume_lines`` → ``_run`` → ``asyncio.run``
    and exit the whole service, leaning on systemd's ``Restart=on-failure``
    to bring everything back up. That works but the dashboard goes dark
    for 5–10 s on every recovery — and dsd-fme stalling silently for 60 s
    is common enough on a slightly unstable PulseAudio chain to make
    that visible to the operator.

    This wrapper keeps the asyncio loop alive: on timeout / EOF we just
    log, sleep ``backoff_seconds``, and spawn a fresh dsd-fme. WS clients,
    HTTP handlers, the event log, the alerts pipeline and the snapshot
    file all keep ticking — only the live event stream has a ~``liveness_timeout
     + backoff_seconds`` gap.

    ``stop_event`` is honoured before every restart attempt so a clean
    shutdown still tears down promptly.

    Live SDR retuning (0.26.0): ``args`` may be a zero-arg callable
    instead of a fixed list — it's re-invoked on every spawn, so a caller
    that mutates the underlying tuning between spawns (e.g. via
    ``RfController``) gets a fresh command each time without restarting
    this generator. ``interrupt_event`` terminates the current child (like
    ``stop_event``, but doesn't end the stream) — pass a caller-owned
    event, ``.set()`` it after changing the tuning, and the next line
    onward comes from a freshly spawned child. Retune respawns use the
    short ``retune_backoff_seconds`` instead of the full ``backoff_seconds``
    (which is sized for genuine stalls, not an operator-requested change).
    """
    while True:
        if stop_event is not None and stop_event.is_set():
            return
        cmd = args() if callable(args) else args
        restart_reason: Optional[str] = None
        was_interrupt = False
        try:
            async for line in stream_subprocess(
                cmd, stop_event=stop_event, env=env,
                liveness_timeout=liveness_timeout,
                interrupt_event=interrupt_event,
            ):
                yield line
        except RuntimeError as exc:
            restart_reason = str(exc)
        except OSError as exc:
            # The child couldn't even be spawned — e.g. the binary vanished
            # mid-run, or it's the wrong architecture ("Exec format error").
            # Without this, a bare OSError would bubble out of the retry loop
            # and crash the whole service with an opaque traceback. Keep the
            # asyncio loop alive and surface a clear, throttled message so the
            # operator can see exactly which binary failed and why.
            restart_reason = f"could not spawn child ({exc})"
        else:
            if interrupt_event is not None and interrupt_event.is_set():
                interrupt_event.clear()
                was_interrupt = True
                restart_reason = "retuned"
            else:
                restart_reason = "child exited (EOF)"
        if stop_event is not None and stop_event.is_set():
            return
        print(
            f"# wrapper: restarting child — {restart_reason}",
            file=sys.stderr,
        )
        try:
            await asyncio.sleep(retune_backoff_seconds if was_interrupt else backoff_seconds)
        except asyncio.CancelledError:
            return
