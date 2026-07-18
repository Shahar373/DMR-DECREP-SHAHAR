"""--port 8080 is DMR's dmr-web.service on the shared Pi — refuse it outright
(a stale systemd unit or hand-typed flag once actually squatted that port in
production; the guard in backend.cli.main must fail fast, before any asyncio
server ever binds, regardless of --live/--replay/etc.)."""
import asyncio

import pytest

from backend.cli import main


def test_serve_port_8080_refused(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--live", "--serve", "--port", "8080"])
    assert exc.value.code == 2
    assert "8080" in capsys.readouterr().err


def test_serve_other_port_not_refused_by_this_guard(monkeypatch):
    """Sanity check: the guard is specific to 8080, not --serve in general —
    a non-8080 port must reach past it (asyncio.run is stubbed so the test
    doesn't actually try to spawn dsd-fme or bind a socket)."""
    monkeypatch.setattr(asyncio, "run", lambda coro: coro.close())
    main(["--live", "--serve", "--port", "9000"])  # would raise if the guard misfired
